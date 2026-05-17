use std::sync::Arc;

use bytes::Bytes;

use crate::{
    Error, Result,
    codec::decompress_by_id,
    footer::{Footer, read_footer},
    layout::ArrayMeta,
    storage::Storage,
};

use super::Delta;

pub struct DeltaImmutable {
    pub footer: Footer,
    pub storage: Arc<dyn Storage>,
}

impl Delta<DeltaImmutable> {
    /// Opens a committed delta file from the given storage.
    pub async fn open(storage: Arc<dyn Storage>) -> Result<Self> {
        let footer = read_footer(&*storage).await?;
        Ok(Delta { inner: DeltaImmutable { footer, storage } })
    }

    /// Returns the array metadata for `name` if present and not deleted.
    pub fn array_meta(&self, name: &str) -> Option<&ArrayMeta> {
        self.inner.footer.arrays.iter().find(|a| a.name == name && !a.deleted)
    }

    /// Returns the raw (uncompressed) bytes for the chunk at `coord`, or
    /// `None` if this delta does not have that chunk.
    pub async fn read_raw_chunk(&self, name: &str, coord: &[u32]) -> Result<Option<Bytes>> {
        let meta = match self.inner.footer.arrays.iter().find(|a| a.name == name) {
            Some(m) => m,
            None => return Ok(None),
        };
        if meta.deleted {
            return Ok(None);
        }
        let entry = match meta
            .layout
            .storage
            .chunks
            .iter()
            .find(|e| e.coord.as_slice() == coord)
        {
            Some(e) => e,
            None => return Ok(None),
        };
        let block = self
            .inner
            .footer
            .blocks
            .iter()
            .find(|b| b.id == entry.address.block_id)
            .ok_or(Error::BlockOutOfRange { block_id: entry.address.block_id.0 })?;

        let compressed = self.inner.storage.read_range(block.file_range()).await?;
        let decompressed =
            decompress_by_id(&block.codec, &compressed, block.uncompressed_size as usize)?;

        let start = entry.address.offset as usize;
        let end = start + entry.address.size as usize;
        Ok(Some(Bytes::copy_from_slice(&decompressed[start..end])))
    }

}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        DType, NoCompression, codec::CompressionCodec,
        delta::{Delta, DeltaMutable},
        storage::InMemoryStorage,
    };

    fn codec() -> Arc<dyn CompressionCodec> {
        Arc::new(NoCompression)
    }

    fn make_mutable() -> Delta<DeltaMutable> {
        Delta::<DeltaMutable>::new(codec(), 512, 0)
    }

    #[tokio::test]
    async fn immutable_read_raw_chunk_matches_written_bytes() {
        let raw = vec![0xCAu8; 32];
        let mut d = make_mutable();
        d.define_array("data", DType::UInt8, vec![32], vec![], None, None).unwrap();
        d.write_raw_chunk("data", vec![0], &raw).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d.commit(storage, "base").await.unwrap();
        let bytes = immutable.read_raw_chunk("data", &[0]).await.unwrap().expect("chunk missing");
        assert_eq!(bytes.as_ref(), raw.as_slice());
    }

    #[tokio::test]
    async fn immutable_read_raw_chunk_unknown_array_returns_none() {
        let mut d = make_mutable();
        d.define_array("a", DType::UInt8, vec![4], vec![], None, None).unwrap();
        d.write_raw_chunk("a", vec![0], &[0u8; 4]).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d.commit(storage, "base").await.unwrap();
        assert!(immutable.read_raw_chunk("missing", &[0]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn immutable_read_raw_chunk_unknown_coord_returns_none() {
        let mut d = make_mutable();
        d.define_array("a", DType::UInt8, vec![4], vec![], None, None).unwrap();
        d.write_raw_chunk("a", vec![0], &[0u8; 4]).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d.commit(storage, "base").await.unwrap();
        assert!(immutable.read_raw_chunk("a", &[99]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn overlay_index_survives_commit() {
        let d = Delta::<DeltaMutable>::new(codec(), 512, 7);
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d.commit(storage, "myfile").await.unwrap();
        assert_eq!(immutable.inner.footer.overlay_index, 7);
        assert_eq!(immutable.inner.footer.base_file_hint, "myfile");
    }
}
