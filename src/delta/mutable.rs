use std::sync::Arc;

use bytes::Bytes;
use indexmap::IndexMap;

use crate::{
    DType, Error, Result,
    address::{BlockAllocAddress, ChunkAddress},
    layout::{
        ArrayLayout, ArrayMeta, AttrIndexKind, AttributeValue, Attributes, ChunkEntry, FillValue,
        StorageLayout,
    },
};

use super::{
    Delta,
    allocator::{AllocatorOutput, DeltaAllocator},
    immutable::DeltaImmutable,
};

pub struct DeltaMutable {
    pub delta_index: u32,
    pub array_meta: IndexMap<String, ArrayMeta>,
    pub allocator: DeltaAllocator,
    pub attr_keys: Vec<String>,
    pub attr_values: Vec<AttributeValue>,
}

fn alloc_addr_from_chunk(addr: &ChunkAddress) -> BlockAllocAddress {
    BlockAllocAddress::new(addr.block_id, addr.offset as u64, addr.size as u64)
}

impl Delta<DeltaMutable> {
    pub fn new(
        codec: Arc<dyn crate::codec::CompressionCodec>,
        block_target_size: usize,
        delta_index: u32,
    ) -> Self {
        Delta {
            inner: DeltaMutable {
                delta_index,
                array_meta: IndexMap::new(),
                allocator: DeltaAllocator::new(codec, block_target_size),
                attr_keys: Vec::new(),
                attr_values: Vec::new(),
            },
        }
    }

    pub fn define_array(
        &mut self,
        name: impl Into<String>,
        dtype: DType,
        shape: Vec<usize>,
        dimension_names: Vec<String>,
        chunk_shape: Option<Vec<usize>>,
        fill_value: Option<FillValue>,
    ) -> Result<()> {
        let name = name.into();
        let shape_u32: Vec<u32> = shape.iter().map(|&s| s as u32).collect();
        let ndim = shape_u32.len();
        let chunk_shape_u32: Vec<u32> = chunk_shape
            .map(|cs| cs.iter().map(|&s| s as u32).collect())
            .unwrap_or_else(|| shape_u32.clone());
        let dim_names = if dimension_names.len() == ndim {
            dimension_names
        } else {
            (0..ndim).map(|i| format!("dim{i}")).collect()
        };
        let layout = ArrayLayout {
            shape: shape_u32,
            dimension_names: dim_names,
            storage: StorageLayout {
                chunk_shape: chunk_shape_u32,
                chunks: Vec::new(),
            },
        };
        self.inner.array_meta.insert(
            name.clone(),
            ArrayMeta {
                name,
                dtype,
                layout,
                fill_value,
                deleted: false,
                attributes: Attributes::empty(AttrIndexKind::U16),
            },
        );
        Ok(())
    }

    pub fn array_meta(&self, name: &str) -> Option<&ArrayMeta> {
        self.inner.array_meta.get(name)
    }

    /// Allocates chunk bytes and records the chunk address in the array meta.
    pub fn write_raw_chunk(&mut self, name: &str, coord: Vec<u32>, raw: &[u8]) -> Result<()> {
        let alloc = self.inner.allocator.allocate(raw);
        let address = ChunkAddress::from(alloc);
        let meta = self
            .inner
            .array_meta
            .get_mut(name)
            .ok_or_else(|| Error::ArrayNotFound {
                name: name.to_string(),
            })?;
        if let Some(entry) = meta
            .layout
            .storage
            .chunks
            .iter_mut()
            .find(|e| e.coord == coord)
        {
            entry.address = address;
        } else {
            meta.layout
                .storage
                .chunks
                .push(ChunkEntry { coord, address });
        }
        Ok(())
    }

    /// Returns a mutable reference to the ArrayMeta for `name`, if present.
    pub fn array_meta_mut(&mut self, name: &str) -> Option<&mut ArrayMeta> {
        self.inner.array_meta.get_mut(name)
    }

    /// Inserts or replaces the ArrayMeta for `meta.name`.
    pub fn upsert_array_meta(&mut self, meta: ArrayMeta) {
        self.inner.array_meta.insert(meta.name.clone(), meta);
    }

    /// Stamps `meta` as deleted, clears its chunks, and upserts it.
    pub fn mark_deleted(&mut self, mut meta: ArrayMeta) {
        meta.deleted = true;
        meta.layout.storage.chunks.clear();
        self.upsert_array_meta(meta);
    }

    /// Interns `key` into the attribute key dictionary, returning its index.
    pub fn intern_attr_key(&mut self, key: &str) -> usize {
        if let Some(i) = self.inner.attr_keys.iter().position(|k| k == key) {
            i
        } else {
            self.inner.attr_keys.push(key.to_string());
            self.inner.attr_keys.len() - 1
        }
    }

    /// Interns `value` into the attribute value dictionary, returning its index.
    pub fn intern_attr_value(&mut self, value: AttributeValue) -> usize {
        if let Some(i) = self.inner.attr_values.iter().position(|v| *v == value) {
            i
        } else {
            self.inner.attr_values.push(value);
            self.inner.attr_values.len() - 1
        }
    }

    /// Reads raw (uncompressed) chunk bytes previously written into this
    /// mutable delta. Returns `None` if the array or coord is not present.
    pub fn read_raw_chunk(&self, name: &str, coord: &[u32]) -> Option<Bytes> {
        let meta = self.inner.array_meta.get(name)?;
        let entry = meta
            .layout
            .storage
            .chunks
            .iter()
            .find(|e| e.coord.as_slice() == coord)?;
        self.inner
            .allocator
            .fetch(&alloc_addr_from_chunk(&entry.address))
    }

    /// Commits this delta: compresses all buffered blocks, serializes the
    /// footer, and writes the complete delta bytes to `storage`.
    pub async fn commit(
        self,
        storage: Arc<dyn crate::storage::Storage>,
        path: Arc<str>,
        cache: Option<Arc<super::DeltaCache>>,
        base_file_hint: impl Into<String>,
    ) -> Result<Delta<DeltaImmutable>> {
        use crate::footer::{FOOTER_VERSION, Footer};

        let overlay_index = self.inner.delta_index;
        let arrays: Vec<ArrayMeta> = self.inner.array_meta.into_values().collect();
        let attr_keys = self.inner.attr_keys;
        let attr_values = self.inner.attr_values;
        let AllocatorOutput {
            mut file,
            output_size,
            blocks,
        } = self.inner.allocator.commit().await;

        let footer = Footer {
            version: FOOTER_VERSION,
            blocks,
            arrays,
            attr_keys,
            attr_values,
            overlay_index,
            base_file_hint: base_file_hint.into(),
        };
        let footer_bytes = footer.serialize()?;

        super::write_file_then_bytes(&mut file, output_size, &footer_bytes, &*storage).await?;
        Delta::<DeltaImmutable>::open(storage, path, cache).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{DType, NoCompression, codec::CompressionCodec, storage::InMemoryStorage};

    fn codec() -> Arc<dyn CompressionCodec> {
        Arc::new(NoCompression)
    }

    fn make_mutable() -> Delta<DeltaMutable> {
        Delta::<DeltaMutable>::new(codec(), 512, 0)
    }

    #[test]
    fn define_array_stores_meta() {
        let mut d = make_mutable();
        d.define_array(
            "temp",
            DType::Float32,
            vec![100],
            vec!["x".into()],
            None,
            None,
        )
        .unwrap();
        let meta = d.array_meta("temp").expect("array_meta returned None");
        assert_eq!(meta.name, "temp");
        assert_eq!(meta.dtype, DType::Float32);
        assert_eq!(meta.layout.shape, vec![100u32]);
        assert!(!meta.deleted);
    }

    #[test]
    fn define_array_default_chunk_shape_equals_shape() {
        let mut d = make_mutable();
        d.define_array("a", DType::Int32, vec![50], vec![], None, None)
            .unwrap();
        let meta = d.array_meta("a").unwrap();
        assert_eq!(meta.layout.storage.chunk_shape, meta.layout.shape);
    }

    #[test]
    fn define_array_custom_chunk_shape() {
        let mut d = make_mutable();
        d.define_array("a", DType::UInt8, vec![200], vec![], Some(vec![50]), None)
            .unwrap();
        let meta = d.array_meta("a").unwrap();
        assert_eq!(meta.layout.storage.chunk_shape, vec![50u32]);
    }

    #[test]
    fn write_raw_chunk_records_entry() {
        let mut d = make_mutable();
        d.define_array("x", DType::UInt8, vec![4], vec![], None, None)
            .unwrap();
        d.write_raw_chunk("x", vec![0], &[1u8, 2, 3, 4]).unwrap();
        let meta = d.array_meta("x").unwrap();
        assert_eq!(meta.layout.storage.chunks.len(), 1);
        assert_eq!(meta.layout.storage.chunks[0].coord, vec![0u32]);
    }

    #[test]
    fn write_raw_chunk_overwrites_same_coord() {
        let mut d = make_mutable();
        d.define_array("x", DType::UInt8, vec![4], vec![], None, None)
            .unwrap();
        d.write_raw_chunk("x", vec![0], &[0u8; 4]).unwrap();
        d.write_raw_chunk("x", vec![0], &[99u8; 4]).unwrap();
        let meta = d.array_meta("x").unwrap();
        assert_eq!(meta.layout.storage.chunks.len(), 1);
    }

    #[test]
    fn write_raw_chunk_unknown_array_returns_error() {
        let mut d = make_mutable();
        let err = d.write_raw_chunk("nope", vec![0], &[1, 2, 3]).unwrap_err();
        assert!(matches!(err, crate::Error::ArrayNotFound { .. }));
    }

    #[tokio::test]
    async fn commit_produces_readable_delta() {
        let values: Vec<f64> = vec![1.0, 2.0, 3.0];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let mut d = make_mutable();
        d.define_array(
            "temps",
            DType::Float64,
            vec![3],
            vec!["t".into()],
            None,
            None,
        )
        .unwrap();
        d.write_raw_chunk("temps", vec![0], &raw).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d
            .commit(storage, Arc::from("test"), None, "base")
            .await
            .unwrap();
        let meta = immutable
            .array_meta("temps")
            .expect("array not found after commit");
        assert_eq!(meta.dtype, DType::Float64);
        assert_eq!(meta.layout.shape, vec![3u32]);
    }

    #[tokio::test]
    async fn multiple_chunks_across_blocks() {
        let d_codec = codec();
        let mut d = Delta::<DeltaMutable>::new(Arc::clone(&d_codec), 16, 0);
        d.define_array("m", DType::UInt8, vec![64], vec![], Some(vec![8]), None)
            .unwrap();
        let chunks: Vec<Vec<u8>> = (0..8u8).map(|i| vec![i; 8]).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            d.write_raw_chunk("m", vec![i as u32 * 8], chunk).unwrap();
        }
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d
            .commit(storage, Arc::from("test"), None, "base")
            .await
            .unwrap();
        for (i, expected) in chunks.iter().enumerate() {
            let bytes = immutable
                .read_raw_chunk("m", &[i as u32 * 8])
                .await
                .unwrap()
                .expect("chunk missing");
            assert_eq!(bytes.as_ref(), expected.as_slice(), "mismatch at chunk {i}");
        }
    }
}
