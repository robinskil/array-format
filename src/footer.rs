//! File footer: the index at the end of an immutable file.
//!
//! A 12-byte trailer (`footer_size: u64 LE` + `MAGIC`) lets a reader locate
//! and validate the footer from the tail of the file.
//!
//! ```text
//! [block 0][block 1]...[footer_bytes][footer_size: u64 LE][MAGIC b"ARRF"]
//! ```

use rkyv::{Archive, Deserialize, Serialize};

use crate::address::ChunkAddress;
use crate::block::BlockMeta;
use crate::dtype::DType;
use crate::error::{Error, Result};
use crate::layout::{ChunkEntry, FillValue};
use crate::stats::ArrayStats;
use crate::storage::Storage;

use crate::attr::DiskValue;

/// Magic bytes written at the very end of the file.
pub(crate) const MAGIC: [u8; 4] = *b"ARRF";

/// Footer format version. Version 5 and below are delta-layer files, which
/// this reader rejects.
pub(crate) const FOOTER_VERSION: u32 = 6;

/// Size of the trailer in bytes (`u64` footer size + 4-byte magic).
pub(crate) const TRAILER_SIZE: usize = 12;

/// The file footer: block table, string pool and array table.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct Footer {
    /// Format version, see [`FOOTER_VERSION`].
    pub version: u32,
    /// Every block in the data region, in file order. Block ids are dense, so
    /// `blocks[id]` is the block with that id.
    pub blocks: Vec<BlockMeta>,
    /// Attribute keys and string values, each stored once.
    /// [`DiskValue`] and [`ArrayMeta::attributes`] index into this.
    pub strings: Vec<String>,
    /// Every array in the file, in definition order.
    pub arrays: Vec<ArrayMeta>,
}

/// Metadata for one array.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ArrayMeta {
    /// Unique name within the file.
    pub name: String,
    /// Element type.
    pub dtype: DType,
    /// Length of each dimension.
    pub shape: Vec<u32>,
    /// Name of each dimension.
    pub dimension_names: Vec<String>,
    /// Extent of one chunk along each dimension. Never zero.
    pub chunk_shape: Vec<u32>,
    /// Written chunks, sorted by coordinate. A coordinate absent here was
    /// never written and reads as the fill value.
    pub chunks: Vec<ChunkEntry>,
    /// Fill value for unwritten elements. `None` means zero or empty.
    pub fill_value: Option<FillValue>,
    /// `(key, value)` pairs. The key is an index into [`Footer::strings`].
    pub attributes: Vec<(u32, DiskValue)>,
    /// Aggregate statistics over every chunk, if the writer computed them.
    pub stats: Option<ArrayStats>,
}

/// Binary search over a chunk table sorted by coordinate.
pub(crate) fn find_chunk<'a>(chunks: &'a [ChunkEntry], coord: &[u32]) -> Option<&'a ChunkAddress> {
    chunks
        .binary_search_by(|e| e.coord.as_slice().cmp(coord))
        .ok()
        .map(|i| &chunks[i].address)
}

impl Footer {
    /// Creates an empty footer at the current version.
    pub(crate) fn new() -> Self {
        Self {
            version: FOOTER_VERSION,
            blocks: Vec::new(),
            strings: Vec::new(),
            arrays: Vec::new(),
        }
    }

    /// Serializes the footer and appends the trailer.
    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let footer_size = rkyv_bytes.len() as u64;
        let mut out = Vec::with_capacity(rkyv_bytes.len() + TRAILER_SIZE);
        out.extend_from_slice(&rkyv_bytes);
        out.extend_from_slice(&footer_size.to_le_bytes());
        out.extend_from_slice(&MAGIC);
        Ok(out)
    }

    /// Deserializes a footer from bytes that end with the trailer.
    pub(crate) fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < TRAILER_SIZE {
            return Err(Error::InvalidFooter("data too short for trailer".into()));
        }

        let magic_start = data.len() - 4;
        if data[magic_start..] != MAGIC {
            return Err(Error::InvalidFooter("invalid magic bytes".into()));
        }

        let size_start = magic_start - 8;
        let footer_size =
            u64::from_le_bytes(data[size_start..magic_start].try_into().unwrap()) as usize;

        if footer_size > size_start {
            return Err(Error::InvalidFooter(
                "footer_size exceeds available data".into(),
            ));
        }

        let rkyv_start = size_start - footer_size;
        let rkyv_bytes = &data[rkyv_start..size_start];

        // Bytes read from storage carry no alignment guarantee, and the
        // archived types need one.
        let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
        aligned.extend_from_slice(rkyv_bytes);

        let footer = rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        if footer.version != FOOTER_VERSION {
            return Err(Error::InvalidFooter(format!(
                "unsupported footer version {}, expected {}",
                footer.version, FOOTER_VERSION
            )));
        }

        Ok(footer)
    }
}

impl Default for Footer {
    fn default() -> Self {
        Self::new()
    }
}

/// Reads and deserializes the footer from storage.
///
/// Two reads: the 12-byte trailer to learn the footer size, then the footer.
pub(crate) async fn read_footer(storage: &(dyn Storage + Sync)) -> Result<Footer> {
    let file_size = storage.size().await?;
    if (file_size as usize) < TRAILER_SIZE {
        return Err(Error::InvalidFooter("file too short for trailer".into()));
    }

    let trailer = storage
        .read_range(file_size - TRAILER_SIZE as u64..file_size)
        .await?;

    if trailer[8..] != MAGIC {
        return Err(Error::InvalidFooter("invalid magic bytes".into()));
    }
    let footer_size = u64::from_le_bytes(trailer[..8].try_into().unwrap()) as usize;
    let total = footer_size + TRAILER_SIZE;

    let start = file_size - total as u64;
    let data = storage.read_range(start..file_size).await?;
    Footer::deserialize(&data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::BlockId;
    use crate::attr::{AttributeValue, StringPool};
    use crate::block::CodecId;
    use crate::stats::StatValue;
    use crate::storage::InMemoryStorage;

    fn chunk(coord: Vec<u32>, block: u32, offset: u32, size: u32) -> ChunkEntry {
        ChunkEntry {
            coord,
            address: ChunkAddress {
                block_id: BlockId(block),
                offset,
                size,
            },
        }
    }

    fn sample_footer() -> Footer {
        let mut pool = StringPool::default();
        let units_key = pool.intern("units");
        let units = DiskValue::encode(&AttributeValue::String("m/s".into()), &mut pool);
        let scale_key = pool.intern("scale");
        let scale = DiskValue::encode(&AttributeValue::Float64(0.01), &mut pool);

        Footer {
            version: FOOTER_VERSION,
            blocks: vec![BlockMeta {
                id: BlockId(0),
                file_offset: 0,
                compressed_size: 8192,
                uncompressed_size: 16000,
                codec: CodecId::Named("lz4".into()),
            }],
            strings: pool.into_strings(),
            arrays: vec![ArrayMeta {
                name: "temperature".into(),
                dtype: DType::Float32,
                shape: vec![100, 40],
                dimension_names: vec!["x".into(), "y".into()],
                chunk_shape: vec![50, 40],
                chunks: vec![
                    chunk(vec![0, 0], 0, 0, 8000),
                    chunk(vec![1, 0], 0, 8000, 8000),
                ],
                fill_value: Some(FillValue::Float(f64::NAN)),
                attributes: vec![(units_key, units), (scale_key, scale)],
                stats: Some(ArrayStats {
                    name: "temperature".into(),
                    min: Some(StatValue::Float(-3.5)),
                    max: Some(StatValue::Float(41.0)),
                    null_count: 0,
                    row_count: 4000,
                }),
            }],
        }
    }

    #[test]
    fn roundtrip_empty_footer() {
        let footer = Footer::new();
        let bytes = footer.serialize().unwrap();
        assert_eq!(Footer::deserialize(&bytes).unwrap(), footer);
    }

    #[test]
    fn roundtrip_with_data() {
        let footer = sample_footer();
        let bytes = footer.serialize().unwrap();
        let restored = Footer::deserialize(&bytes).unwrap();
        assert_eq!(restored, footer);

        // The attribute survives the pool indirection.
        let strings: Vec<ecow::EcoString> =
            restored.strings.iter().map(ecow::EcoString::from).collect();
        let (key, value) = &restored.arrays[0].attributes[0];
        assert_eq!(strings[*key as usize], "units");
        assert_eq!(
            value.decode(&strings).unwrap(),
            AttributeValue::String("m/s".into())
        );
    }

    #[test]
    fn chunk_lookup_uses_the_sorted_order() {
        let footer = sample_footer();
        let chunks = &footer.arrays[0].chunks;
        assert_eq!(find_chunk(chunks, &[1, 0]).map(|a| a.offset), Some(8000));
        assert_eq!(find_chunk(chunks, &[0, 0]).map(|a| a.offset), Some(0));
        assert!(find_chunk(chunks, &[2, 0]).is_none());
        assert!(find_chunk(chunks, &[0, 1]).is_none());
    }

    #[test]
    fn older_versions_are_rejected() {
        let mut footer = Footer::new();
        footer.version = 5;
        let bytes = footer.serialize().unwrap();
        let err = Footer::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, Error::InvalidFooter(ref m) if m.contains("version 5")));
    }

    #[test]
    fn invalid_magic_detected() {
        let mut bytes = Footer::new().serialize().unwrap();
        let len = bytes.len();
        bytes[len - 1] = b'X';
        assert!(Footer::deserialize(&bytes).is_err());
    }

    #[test]
    fn too_short_data() {
        assert!(Footer::deserialize(&[0u8; 4]).is_err());
    }

    #[test]
    fn trailer_has_correct_structure() {
        let bytes = Footer::new().serialize().unwrap();
        let len = bytes.len();
        assert_eq!(&bytes[len - 4..], b"ARRF");
        let footer_size = u64::from_le_bytes(bytes[len - 12..len - 4].try_into().unwrap());
        assert_eq!(footer_size as usize, len - TRAILER_SIZE);
    }

    #[tokio::test]
    async fn read_footer_finds_it_behind_a_data_region() {
        let footer = sample_footer();
        let mut file = vec![0xAAu8; 8192]; // pretend data region
        file.extend(footer.serialize().unwrap());
        let storage = InMemoryStorage::from_bytes(file);
        assert_eq!(read_footer(&storage).await.unwrap(), footer);
    }
}
