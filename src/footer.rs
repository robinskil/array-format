//! File footer: the index that maps array names to block addresses.
//!
//! The footer is appended at the end of the file. A 12-byte trailer
//! (`footer_size: u64 LE` + `MAGIC`) allows the reader to locate and
//! validate the footer from the tail of the file.
//!
//! ```text
//! [footer_bytes][footer_size: u64 LE][MAGIC b"ARRF"]
//! ```

use rkyv::{Archive, Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::error::{Error, Result};
use crate::layout::ArrayMeta;
use crate::storage::Storage;

/// Magic bytes written at the very end of the file.
pub const MAGIC: [u8; 4] = *b"ARRF";

/// Current footer format version.
pub const FOOTER_VERSION: u32 = 1;

/// Size of the trailer in bytes (`u64` footer size + 4-byte magic).
pub const TRAILER_SIZE: usize = 12;

/// The file footer containing the block table and array table.
///
/// Serialized with [`rkyv`] for zero-copy access to the archived form.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct Footer {
    /// Format version.
    pub version: u32,
    /// Block table: metadata for every block in the data region.
    pub blocks: Vec<BlockMeta>,
    /// Array table: metadata for every array stored in the file.
    pub arrays: Vec<ArrayMeta>,
}

impl Footer {
    /// Creates a new empty footer.
    pub fn new() -> Self {
        Self {
            version: FOOTER_VERSION,
            blocks: Vec::new(),
            arrays: Vec::new(),
        }
    }

    /// Serializes the footer to bytes, appending the trailer.
    ///
    /// Layout: `[rkyv_bytes][footer_size: u64 LE][MAGIC]`
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let footer_size = rkyv_bytes.len() as u64;
        let mut out = Vec::with_capacity(rkyv_bytes.len() + TRAILER_SIZE);
        out.extend_from_slice(&rkyv_bytes);
        out.extend_from_slice(&footer_size.to_le_bytes());
        out.extend_from_slice(&MAGIC);
        Ok(out)
    }

    /// Deserializes a footer from bytes that include the trailer.
    ///
    /// `data` must contain at least the trailer and the footer payload.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
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

        // Copy into an aligned buffer – the slice may not be aligned to the
        // requirements of the archived types after being read from storage.
        let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
        aligned.extend_from_slice(rkyv_bytes);

        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned)
            .map_err(|e| Error::Serialization(e.to_string()))
    }
}

/// Reads and deserializes the footer from storage.
///
/// Performs a two-pass read: first reads the 12-byte trailer to learn
/// the footer size, then reads the full footer payload if needed.
pub async fn read_footer(storage: &(dyn Storage + Sync)) -> Result<Footer> {
    let file_size = storage.size().await?;
    if (file_size as usize) < TRAILER_SIZE {
        return Err(Error::InvalidFooter("file too short for trailer".into()));
    }

    // First pass: read the trailer to learn the footer size.
    let trailer = storage
        .read_range(file_size - TRAILER_SIZE as u64..file_size)
        .await?;

    if trailer[8..] != MAGIC {
        return Err(Error::InvalidFooter("invalid magic bytes".into()));
    }
    let footer_size = u64::from_le_bytes(trailer[..8].try_into().unwrap()) as usize;
    let total = footer_size + TRAILER_SIZE;

    // Second pass: read footer payload + trailer.
    let start = file_size - total as u64;
    let data = storage.read_range(start..file_size).await?;
    Footer::deserialize(&data)
}

impl Default for Footer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::{BlockId, ChunkAddress};
    use crate::block::CodecId;
    use crate::dtype::DType;
    use crate::layout::{ArrayLayout, StorageLayout};

    #[test]
    fn roundtrip_empty_footer() {
        let footer = Footer::new();
        let bytes = footer.serialize().unwrap();
        let restored = Footer::deserialize(&bytes).unwrap();
        assert_eq!(footer, restored);
    }

    #[test]
    fn roundtrip_with_data() {
        let footer = Footer {
            version: FOOTER_VERSION,
            blocks: vec![BlockMeta {
                id: BlockId(0),
                file_offset: 0,
                compressed_size: 8192,
                uncompressed_size: 8192,
                codec: CodecId::None,
            }],
            arrays: vec![ArrayMeta {
                name: "temperature".into(),
                dtype: DType::Float32,
                layout: ArrayLayout {
                    shape: vec![1000, 1000],
                    dimension_names: vec!["x".into(), "y".into()],
                    storage: StorageLayout::Flat {
                        address: ChunkAddress {
                            block_id: BlockId(0),
                            offset: 0,
                            size: 4000,
                        },
                    },
                },
                fill_value: Some(crate::layout::FillValue::Float(f64::NAN)),
                deleted: false,
            }],
        };
        let bytes = footer.serialize().unwrap();
        let restored = Footer::deserialize(&bytes).unwrap();
        assert_eq!(footer, restored);
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

        // Last 4 bytes are magic
        assert_eq!(&bytes[len - 4..], b"ARRF");

        // Preceding 8 bytes are footer_size as u64 LE
        let footer_size = u64::from_le_bytes(bytes[len - 12..len - 4].try_into().unwrap());
        assert_eq!(footer_size as usize, len - TRAILER_SIZE);
    }
}
