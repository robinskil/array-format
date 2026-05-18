use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::{
    Error, Result,
    codec::decompress_by_id,
    footer::{Footer, read_footer},
    layout::ArrayMeta,
    storage::Storage,
};

use super::{Delta, DeltaCache};

pub struct DeltaImmutable {
    pub footer: Footer,
    pub storage: Arc<dyn Storage>,
    pub path: Arc<str>,
    pub cache: Option<Arc<DeltaCache>>,
    /// Maps array name → index in `footer.arrays` for O(1) lookup.
    pub array_index: HashMap<String, usize>,
}

impl Delta<DeltaImmutable> {
    /// Opens a committed delta file from the given storage.
    pub async fn open(
        storage: Arc<dyn Storage>,
        path: Arc<str>,
        cache: Option<Arc<DeltaCache>>,
    ) -> Result<Self> {
        let footer = read_footer(&*storage).await?;
        let array_index = footer
            .arrays
            .iter()
            .enumerate()
            .map(|(i, a)| (a.name.clone(), i))
            .collect();
        Ok(Delta {
            inner: DeltaImmutable {
                footer,
                storage,
                path,
                cache,
                array_index,
            },
        })
    }

    /// Returns the array metadata for `name` if present and not deleted.
    pub fn array_meta(&self, name: &str) -> Option<&ArrayMeta> {
        let idx = self.inner.array_index.get(name)?;
        let a = &self.inner.footer.arrays[*idx];
        if a.deleted { None } else { Some(a) }
    }

    /// Returns the raw (uncompressed) bytes for the chunk at `coord`, or
    /// `None` if this delta does not have that chunk.
    pub async fn read_raw_chunk(&self, name: &str, coord: &[u32]) -> Result<Option<Bytes>> {
        let meta = match self.inner.array_index.get(name).map(|&i| &self.inner.footer.arrays[i]) {
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
            .ok_or(Error::BlockOutOfRange {
                block_id: entry.address.block_id.0,
            })?;

        let block_bytes = if let Some(cache) = &self.inner.cache {
            cache
                .get_or_load(&self.inner.path, block, &*self.inner.storage)
                .await?
        } else {
            let compressed = self.inner.storage.read_range(block.file_range()).await?;
            Bytes::from(decompress_by_id(
                &block.codec,
                &compressed,
                block.uncompressed_size as usize,
            )?)
        };

        let start = entry.address.offset as usize;
        let end = start + entry.address.size as usize;
        Ok(Some(block_bytes.slice(start..end)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        DType, NoCompression,
        codec::CompressionCodec,
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
        d.define_array("data", DType::UInt8, vec![32], vec![], None, None)
            .unwrap();
        d.write_raw_chunk("data", vec![0], &raw).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d
            .commit(storage, Arc::from("test"), None, "base")
            .await
            .unwrap();
        let bytes = immutable
            .read_raw_chunk("data", &[0])
            .await
            .unwrap()
            .expect("chunk missing");
        assert_eq!(bytes.as_ref(), raw.as_slice());
    }

    #[tokio::test]
    async fn immutable_read_raw_chunk_unknown_array_returns_none() {
        let mut d = make_mutable();
        d.define_array("a", DType::UInt8, vec![4], vec![], None, None)
            .unwrap();
        d.write_raw_chunk("a", vec![0], &[0u8; 4]).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d
            .commit(storage, Arc::from("test"), None, "base")
            .await
            .unwrap();
        assert!(
            immutable
                .read_raw_chunk("missing", &[0])
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn immutable_read_raw_chunk_unknown_coord_returns_none() {
        let mut d = make_mutable();
        d.define_array("a", DType::UInt8, vec![4], vec![], None, None)
            .unwrap();
        d.write_raw_chunk("a", vec![0], &[0u8; 4]).unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d
            .commit(storage, Arc::from("test"), None, "base")
            .await
            .unwrap();
        assert!(
            immutable
                .read_raw_chunk("a", &[99])
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn overlay_index_survives_commit() {
        let d = Delta::<DeltaMutable>::new(codec(), 512, 7);
        let storage = Arc::new(InMemoryStorage::new());
        let immutable = d
            .commit(storage, Arc::from("test"), None, "myfile")
            .await
            .unwrap();
        assert_eq!(immutable.inner.footer.overlay_index, 7);
        assert_eq!(immutable.inner.footer.base_file_hint, "myfile");
    }
}
