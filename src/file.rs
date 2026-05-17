use std::sync::Arc;

use bytes::Bytes;
use indexmap::IndexMap;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::{
    DType, Error, Result,
    address::ChunkAddress,
    array::ArrayElement,
    codec::CompressionCodec,
    delta::{Delta, DeltaAllocator, DeltaImmutable, write_file_then_bytes},
    footer::{FOOTER_VERSION, Footer},
    layout::{
        ArrayLayout, ArrayMeta, AttrIndexKind, AttributeValue, Attributes, ChunkEntry, FillValue,
        StorageLayout,
    },
    storage::{InMemoryStorage, ObjectStoreBackend, Storage, discover_sidecars},
};

// ── Constants ───────────────────────────────────────────────────────

pub const DEFAULT_BLOCK_TARGET_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
pub const DEFAULT_CACHE_CAPACITY: usize = 128;

// ── FileConfig ──────────────────────────────────────────────────────

pub struct FileConfig<C: CompressionCodec> {
    pub codec: C,
    pub block_target_size: usize,
    pub cache_capacity: usize,
}

impl<C: CompressionCodec> FileConfig<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            block_target_size: DEFAULT_BLOCK_TARGET_SIZE,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
        }
    }
}

// ── MergedArrayMeta ─────────────────────────────────────────────────

/// Array metadata visible to the caller after merging all delta layers.
#[derive(Debug, Clone)]
pub struct MergedArrayMeta {
    pub name: String,
    pub dtype: DType,
    pub shape: Vec<u32>,
    pub chunk_shape: Vec<u32>,
    pub dimension_names: Vec<String>,
    pub fill_value: Option<FillValue>,
}

// ── PendingState ────────────────────────────────────────────────────

#[derive(Default)]
struct PendingState {
    /// Arrays defined or modified (or deleted) in this session.
    arrays: IndexMap<String, ArrayMeta>,
    /// Raw uncompressed chunk bytes for dirty chunks.
    dirty_chunks: IndexMap<String, IndexMap<Vec<u32>, Bytes>>,
    /// Global attribute key dictionary (accumulated across all sessions).
    attr_keys: Vec<String>,
    /// Global attribute value dictionary.
    attr_values: Vec<AttributeValue>,
}

impl PendingState {
    fn is_empty(&self) -> bool {
        self.arrays.is_empty() && self.dirty_chunks.is_empty()
    }
}

// ── File ────────────────────────────────────────────────────────────

/// Object store and base-file stem for an on-disk file.
struct StoreDir {
    store: Arc<dyn ObjectStore>,
    stem: String,
}

/// Schema information returned by [`File::get_chunked_schema`].
pub(crate) struct ChunkedSchema {
    pub full_shape: Vec<u32>,
    pub chunk_shape: Vec<u32>,
    pub dtype: DType,
    pub all_coords: Vec<Vec<u32>>,
}

/// The top-level file handle.
///
/// Layers are stacked oldest → newest in `deltas`. Uncommitted writes
/// accumulate in `pending` and are flushed by [`flush`](File::flush).
pub struct File {
    deltas: Vec<Delta<DeltaImmutable>>,
    pending: PendingState,
    codec: Arc<dyn CompressionCodec>,
    block_target_size: usize,
    /// Object store and stem for on-disk files; `None` for in-memory files.
    store_dir: Option<StoreDir>,
}

// ── Constructors ────────────────────────────────────────────────────

impl File {
    /// Creates a new empty file at `path` on the local filesystem.
    pub async fn create<C: CompressionCodec + 'static>(
        path: &std::path::Path,
        config: FileConfig<C>,
    ) -> Result<Self> {
        let ctx = local_dir_and_storage(path)?;
        write_empty_base(&*ctx.storage).await?;
        let base_delta = Delta::<DeltaImmutable>::open(ctx.storage).await?;
        Ok(File {
            deltas: vec![base_delta],
            pending: PendingState::default(),
            codec: Arc::new(config.codec),
            block_target_size: config.block_target_size,
            store_dir: Some(StoreDir { store: ctx.store, stem: ctx.stem }),
        })
    }

    /// Opens an existing file (base + any sidecars) from the local filesystem.
    pub async fn open<C: CompressionCodec + 'static>(
        path: &std::path::Path,
        config: FileConfig<C>,
    ) -> Result<Self> {
        let ctx = local_dir_and_storage(path)?;
        let base_delta = Delta::<DeltaImmutable>::open(ctx.storage).await?;
        let mut deltas = vec![base_delta];

        let sidecar_paths = discover_sidecars(path)?;
        for scar in sidecar_paths {
            let filename = scar
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| Error::Storage("sidecar filename is invalid UTF-8".into()))?
                .to_owned();
            let scar_storage = Arc::new(ObjectStoreBackend::new(
                Arc::clone(&ctx.store),
                object_store::path::Path::from(filename.as_str()),
            )) as Arc<dyn Storage>;
            deltas.push(Delta::<DeltaImmutable>::open(scar_storage).await?);
        }

        Ok(File {
            deltas,
            pending: PendingState::default(),
            codec: Arc::new(config.codec),
            block_target_size: config.block_target_size,
            store_dir: Some(StoreDir { store: ctx.store, stem: ctx.stem }),
        })
    }

    /// Creates a new empty in-memory file.
    pub async fn create_memory<C: CompressionCodec + 'static>(
        config: FileConfig<C>,
    ) -> Result<Self> {
        let storage = Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>;
        write_empty_base(&*storage).await?;
        let base_delta = Delta::<DeltaImmutable>::open(storage).await?;
        Ok(File {
            deltas: vec![base_delta],
            pending: PendingState::default(),
            codec: Arc::new(config.codec),
            block_target_size: config.block_target_size,
            store_dir: None,
        })
    }
}

// ── Schema & attribute access ────────────────────────────────────────

impl File {
    /// Returns a reference to the merged array metadata for `name`,
    /// searching from the newest layer towards the oldest.
    pub fn get_array(&self, name: &str) -> Result<&ArrayMeta> {
        self.resolve_array_meta(name)
            .ok_or_else(|| Error::ArrayNotFound {
                name: name.to_string(),
            })
    }

    fn resolve_array_meta(&self, name: &str) -> Option<&ArrayMeta> {
        if let Some(m) = self.pending.arrays.get(name) {
            return if m.deleted { None } else { Some(m) };
        }
        for delta in self.deltas.iter().rev() {
            if let Some(m) = delta.inner.footer.arrays.iter().find(|a| a.name == name) {
                return if m.deleted { None } else { Some(m) };
            }
        }
        None
    }

    /// Returns the array schema in the form expected by the ndarray write path.
    pub(crate) fn get_chunked_schema(&self, name: &str) -> Result<ChunkedSchema> {
        let meta = self.get_array(name)?;
        let full_shape = meta.layout.shape.clone();
        let chunk_shape = meta.layout.storage.chunk_shape.clone();
        let dtype = meta.dtype.clone();
        // Collect existing chunk coords from all layers (newest wins, so just union).
        let mut existing: IndexMap<Vec<u32>, ()> = IndexMap::new();
        for delta in &self.deltas {
            if let Some(m) = delta.inner.footer.arrays.iter().find(|a| a.name == name) {
                for e in &m.layout.storage.chunks {
                    existing.entry(e.coord.clone()).or_default();
                }
            }
        }
        if let Some(m) = self.pending.arrays.get(name) {
            for e in &m.layout.storage.chunks {
                existing.entry(e.coord.clone()).or_default();
            }
        }
        if let Some(chunks) = self.pending.dirty_chunks.get(name) {
            for c in chunks.keys() {
                existing.entry(c.clone()).or_default();
            }
        }
        Ok(ChunkedSchema {
            full_shape,
            chunk_shape,
            dtype,
            all_coords: existing.into_keys().collect(),
        })
    }

    /// Returns all non-deleted visible arrays (newest-wins merge).
    pub fn list_arrays(&self) -> Vec<MergedArrayMeta> {
        let mut seen: IndexMap<String, MergedArrayMeta> = IndexMap::new();

        // Walk from oldest to newest so later entries overwrite earlier ones.
        for delta in &self.deltas {
            for a in &delta.inner.footer.arrays {
                if a.deleted {
                    seen.shift_remove(&a.name);
                } else {
                    seen.insert(
                        a.name.clone(),
                        MergedArrayMeta {
                            name: a.name.clone(),
                            dtype: a.dtype.clone(),
                            shape: a.layout.shape.clone(),
                            chunk_shape: a.layout.storage.chunk_shape.clone(),
                            dimension_names: a.layout.dimension_names.clone(),
                            fill_value: a.fill_value.clone(),
                        },
                    );
                }
            }
        }
        for (name, a) in &self.pending.arrays {
            if a.deleted {
                seen.shift_remove(name);
            } else {
                seen.insert(
                    name.clone(),
                    MergedArrayMeta {
                        name: a.name.clone(),
                        dtype: a.dtype.clone(),
                        shape: a.layout.shape.clone(),
                        chunk_shape: a.layout.storage.chunk_shape.clone(),
                        dimension_names: a.layout.dimension_names.clone(),
                        fill_value: a.fill_value.clone(),
                    },
                );
            }
        }
        seen.into_values().collect()
    }

    /// Number of committed (immutable) delta layers.
    pub fn num_layers(&self) -> usize {
        self.deltas.len()
    }

    pub fn get_attribute(&self, name: &str, key: &str) -> Result<Option<&AttributeValue>> {
        let meta = self.get_array(name)?;
        let key_idx = match self
            .pending
            .attr_keys
            .iter()
            .position(|k| k == key)
            .or_else(|| {
                // Check global dicts in most-recent delta
                self.deltas
                    .iter()
                    .rev()
                    .find_map(|d| d.inner.footer.attr_keys.iter().position(|k| k == key))
            }) {
            Some(i) => i,
            None => return Ok(None),
        };
        let val_idx = match meta.attributes.get(key_idx) {
            Some(i) => i,
            None => return Ok(None),
        };
        // Look up in pending first, then deltas
        if val_idx < self.pending.attr_values.len() {
            return Ok(Some(&self.pending.attr_values[val_idx]));
        }
        for delta in self.deltas.iter().rev() {
            if val_idx < delta.inner.footer.attr_values.len() {
                return Ok(Some(&delta.inner.footer.attr_values[val_idx]));
            }
        }
        Ok(None)
    }

    pub fn set_attribute(&mut self, name: &str, key: &str, value: AttributeValue) -> Result<()> {
        // Ensure the array exists (in deltas or pending).
        self.get_array(name)?;

        // Upsert key into pending dict.
        let key_idx = if let Some(i) = self.pending.attr_keys.iter().position(|k| k == key) {
            i
        } else {
            self.pending.attr_keys.push(key.to_string());
            self.pending.attr_keys.len() - 1
        };

        // Upsert value into pending dict.
        let val_idx = if let Some(i) = self.pending.attr_values.iter().position(|v| *v == value) {
            i
        } else {
            self.pending.attr_values.push(value);
            self.pending.attr_values.len() - 1
        };

        // Update the array meta in pending (create a copy if only in deltas).
        let meta = if let Some(m) = self.pending.arrays.get_mut(name) {
            m
        } else {
            let m = self.get_array(name)?.clone();
            self.pending.arrays.insert(name.to_string(), m);
            self.pending.arrays.get_mut(name).unwrap()
        };
        meta.attributes.upsert(key_idx, val_idx);
        Ok(())
    }
}

// ── Array definition / deletion ──────────────────────────────────────

impl File {
    /// Defines a new array. `chunk_shape = None` means one chunk per array.
    pub fn define_array<T: ArrayElement>(
        &mut self,
        name: impl Into<String>,
        dimension_names: Vec<String>,
        shape: Vec<usize>,
        chunk_shape: Option<Vec<usize>>,
        fill_value: Option<FillValue>,
    ) -> Result<()> {
        let name = name.into();
        if self.resolve_array_meta(&name).is_some() {
            return Err(Error::ArrayAlreadyExists { name });
        }
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
                chunks: vec![],
            },
        };
        self.pending.arrays.insert(
            name.clone(),
            ArrayMeta {
                name,
                dtype: T::DTYPE,
                layout,
                fill_value,
                deleted: false,
                attributes: Attributes::empty(AttrIndexKind::U16),
            },
        );
        Ok(())
    }

    /// Marks an array as deleted in the pending layer.
    pub fn delete(&mut self, name: &str) -> Result<()> {
        let meta = self.get_array(name)?.clone();
        let mut tombstone = meta;
        tombstone.deleted = true;
        tombstone.layout.storage.chunks.clear();
        self.pending.arrays.insert(name.to_string(), tombstone);
        Ok(())
    }
}

// ── Chunk-level read/write (pub(crate) for ndarray_ext) ──────────────

impl File {
    pub(crate) async fn read_chunk<T: ArrayElement>(
        &self,
        name: &str,
        coord: &[u32],
    ) -> Result<Vec<T>> {
        if let Some(bytes) = self.resolve_raw_chunk(name, coord).await? {
            return Ok(T::decode_chunk(&bytes));
        }
        let meta = self.get_array(name)?;
        let chunk_elems: usize = meta
            .layout
            .storage
            .chunk_shape
            .iter()
            .enumerate()
            .map(|(i, &cs)| {
                let axis_len = meta.layout.shape[i] as usize;
                let start = coord[i] as usize * cs as usize;
                (cs as usize).min(axis_len.saturating_sub(start))
            })
            .product();
        Ok(vec![T::fill_element(meta.fill_value.as_ref()); chunk_elems])
    }

    pub(crate) fn write_chunk_raw(
        &mut self,
        name: &str,
        coord: Vec<u32>,
        bytes: Vec<u8>,
    ) -> Result<()> {
        self.get_array(name)?;
        self.pending
            .dirty_chunks
            .entry(name.to_string())
            .or_default()
            .insert(coord, Bytes::from(bytes));
        Ok(())
    }

    async fn resolve_raw_chunk(&self, name: &str, coord: &[u32]) -> Result<Option<Bytes>> {
        if let Some(chunks) = self.pending.dirty_chunks.get(name)
            && let Some(b) = chunks.get(coord)
        {
            return Ok(Some(b.clone()));
        }
        for delta in self.deltas.iter().rev() {
            if let Some(bytes) = delta.read_raw_chunk(name, coord).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }
}

// ── ndarray read/write ───────────────────────────────────────────────

impl File {
    pub async fn write_array<T: ArrayElement>(
        &mut self,
        name: &str,
        start: Vec<usize>,
        data: ndarray::ArrayView<'_, T, ndarray::IxDyn>,
    ) -> Result<()> {
        crate::ndarray_ext::write_nd(self, name, data, &start).await
    }

    pub async fn read_array<T: ArrayElement>(
        &self,
        name: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<ndarray::ArcArray<T, ndarray::IxDyn>> {
        use std::ops::Range;
        let slice: Option<Vec<Range<usize>>> = if start.is_empty() && shape.is_empty() {
            None
        } else {
            let meta = self.get_array(name)?;
            let ndim = meta.layout.shape.len();
            let effective_start =
                if start.len() == ndim { start.clone() } else { vec![0; ndim] };
            let effective_shape: Vec<usize> = if shape.len() == ndim {
                shape.clone()
            } else {
                meta.layout.shape.iter().map(|&s| s as usize).collect()
            };
            Some(
                effective_start
                    .iter()
                    .zip(&effective_shape)
                    .map(|(&s, &sz)| s..s + sz)
                    .collect(),
            )
        };
        crate::ndarray_ext::assemble_nd(self, name, slice.as_deref()).await
    }
}

// ── Flush ────────────────────────────────────────────────────────────

impl File {
    /// Commits pending writes to a new sidecar file on disk.
    pub async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let sd = self
            .store_dir
            .as_ref()
            .ok_or_else(|| Error::Storage("in-memory file: use flush_memory instead".into()))?;
        let overlay_index = self.deltas.len() as u32;
        let sidecar_name = format!("{}.{overlay_index}.af", sd.stem);
        let storage = Arc::new(ObjectStoreBackend::new(
            Arc::clone(&sd.store),
            object_store::path::Path::from(sidecar_name.as_str()),
        )) as Arc<dyn Storage>;
        self.flush_to(storage, overlay_index, sd.stem.clone()).await
    }

    /// Commits pending writes to `storage` (for in-memory testing).
    pub async fn flush_memory(&mut self, storage: &InMemoryStorage) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let overlay_index = self.deltas.len() as u32;
        let arc: Arc<dyn Storage> = Arc::new(storage.clone());
        self.flush_to(arc, overlay_index, String::new()).await
    }

    async fn flush_to(
        &mut self,
        storage: Arc<dyn Storage>,
        overlay_index: u32,
        base_file_hint: String,
    ) -> Result<()> {
        let (mut file, output_size, footer_bytes) =
            self.build_pending_output(overlay_index, &base_file_hint).await?;
        write_file_then_bytes(&mut file, output_size, &footer_bytes, &*storage).await?;
        let immutable = Delta::<DeltaImmutable>::open(Arc::clone(&storage)).await?;
        self.deltas.push(immutable);
        self.pending = PendingState::default();
        Ok(())
    }

    /// Allocates all pending dirty chunks into an `DeltaAllocator` output file
    /// and serializes the footer. Returns the output file (seeked to 0), its
    /// byte count, and the footer bytes ready to be appended.
    async fn build_pending_output(
        &self,
        overlay_index: u32,
        base_file_hint: &str,
    ) -> Result<(tokio::fs::File, u64, Vec<u8>)> {
        let mut allocator = DeltaAllocator::new(Arc::clone(&self.codec), self.block_target_size);

        // Collect all array names touched in this flush.
        let mut all_names: indexmap::IndexSet<String> = indexmap::IndexSet::new();
        all_names.extend(self.pending.arrays.keys().cloned());
        all_names.extend(self.pending.dirty_chunks.keys().cloned());

        let mut arrays: Vec<ArrayMeta> = Vec::new();
        for name in &all_names {
            let mut meta: ArrayMeta = if let Some(m) = self.pending.arrays.get(name) {
                m.clone()
            } else {
                self.resolve_array_meta(name)
                    .ok_or_else(|| Error::ArrayNotFound { name: name.clone() })?
                    .clone()
            };

            // This delta only stores dirty chunks.
            let mut delta_chunks: Vec<ChunkEntry> = Vec::new();
            if let Some(dirty) = self.pending.dirty_chunks.get(name) {
                for (coord, raw) in dirty {
                    let alloc = allocator.allocate(raw);
                    delta_chunks.push(ChunkEntry {
                        coord: coord.clone(),
                        address: ChunkAddress::from(alloc),
                    });
                }
            }
            meta.layout.storage.chunks = delta_chunks;
            arrays.push(meta);
        }

        let crate::delta::AllocatorOutput { mut file, output_size, blocks } =
            allocator.commit().await;

        let footer = Footer {
            version: FOOTER_VERSION,
            blocks,
            arrays,
            attr_keys: self.pending.attr_keys.clone(),
            attr_values: self.pending.attr_values.clone(),
            overlay_index,
            base_file_hint: base_file_hint.to_string(),
        };
        let footer_bytes = footer.serialize()?;

        // Re-seek so the caller can stream from position 0.
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(0)).await.map_err(Error::Io)?;

        Ok((file, output_size, footer_bytes))
    }
}

// ── Compact ──────────────────────────────────────────────────────────

impl File {
    /// Merges all committed layers into a single base file.
    pub async fn compact(&mut self) -> Result<()> {
        // Build the merged view.
        let merged_names: Vec<String> = self.list_arrays().into_iter().map(|m| m.name).collect();

        // Allocate all chunks for merged arrays.
        let mut allocator = DeltaAllocator::new(Arc::clone(&self.codec), self.block_target_size);
        let mut arrays: Vec<ArrayMeta> = Vec::new();

        for name in &merged_names {
            let meta = self
                .resolve_array_meta(name)
                .ok_or_else(|| Error::ArrayNotFound { name: name.clone() })?
                .clone();

            // Collect all chunk coords across all layers for this array.
            let mut all_coords: indexmap::IndexSet<Vec<u32>> = indexmap::IndexSet::new();
            for delta in &self.deltas {
                if let Some(m) = delta.inner.footer.arrays.iter().find(|a| a.name == *name) {
                    for e in &m.layout.storage.chunks {
                        all_coords.insert(e.coord.clone());
                    }
                }
            }

            let mut new_chunks: Vec<ChunkEntry> = Vec::new();
            for coord in &all_coords {
                // Read from newest layer that has this chunk.
                if let Some(raw) = self.resolve_raw_chunk(name, coord).await? {
                    let alloc = allocator.allocate(&raw);
                    new_chunks.push(ChunkEntry {
                        coord: coord.clone(),
                        address: ChunkAddress::from(alloc),
                    });
                }
            }

            let mut new_meta = meta;
            new_meta.layout.storage.chunks = new_chunks;
            arrays.push(new_meta);
        }

        let crate::delta::AllocatorOutput { mut file, output_size, blocks } =
            allocator.commit().await;

        // Build attr dictionaries from all layers (simple union).
        let mut attr_keys: Vec<String> = Vec::new();
        let mut attr_values: Vec<crate::layout::AttributeValue> = Vec::new();
        for delta in &self.deltas {
            for k in &delta.inner.footer.attr_keys {
                if !attr_keys.contains(k) {
                    attr_keys.push(k.clone());
                }
            }
            for v in &delta.inner.footer.attr_values {
                if !attr_values.contains(v) {
                    attr_values.push(v.clone());
                }
            }
        }

        let footer = Footer {
            version: FOOTER_VERSION,
            blocks,
            arrays,
            attr_keys,
            attr_values,
            overlay_index: 0,
            base_file_hint: String::new(),
        };
        let footer_bytes = footer.serialize()?;

        // Write the new base file.
        let base_storage: Arc<dyn Storage> = if let Some(sd) = &self.store_dir {
            // Delete old sidecars first.
            for i in 1..self.deltas.len() {
                let scar_name = format!("{}.{i}.af", sd.stem);
                let _ = sd
                    .store
                    .delete(&object_store::path::Path::from(scar_name.as_str()))
                    .await;
            }
            // Write new base.
            let filename = format!("{}.af", sd.stem);
            Arc::new(ObjectStoreBackend::new(
                Arc::clone(&sd.store),
                object_store::path::Path::from(filename.as_str()),
            ))
        } else {
            // In-memory: reuse the first layer's storage.
            Arc::clone(&self.deltas[0].inner.storage)
        };

        write_file_then_bytes(&mut file, output_size, &footer_bytes, &*base_storage).await?;
        let new_base = Delta::<DeltaImmutable>::open(base_storage).await?;
        self.deltas = vec![new_base];
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Parsed context for a local on-disk file path.
struct LocalFileContext {
    store: Arc<dyn ObjectStore>,
    stem: String,
    storage: Arc<dyn Storage>,
}

fn local_dir_and_storage(path: &std::path::Path) -> Result<LocalFileContext> {
    use object_store::local::LocalFileSystem;

    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(Error::Io)?.join(path)
    };

    let dir = abs
        .parent()
        .ok_or_else(|| Error::Storage("path has no parent directory".into()))?;
    let filename = abs
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::Storage("invalid filename".into()))?
        .to_owned();
    let stem = abs
        .file_stem()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::Storage("invalid filename stem".into()))?
        .to_owned();

    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir).map_err(|e| Error::Storage(e.to_string()))?)
            as Arc<dyn ObjectStore>;

    let storage = Arc::new(ObjectStoreBackend::new(
        Arc::clone(&store),
        object_store::path::Path::from(filename.as_str()),
    )) as Arc<dyn Storage>;

    Ok(LocalFileContext { store, stem, storage })
}

async fn write_empty_base(storage: &dyn Storage) -> Result<()> {
    let footer = Footer::new();
    let bytes = footer.serialize()?;
    storage.write(Bytes::from(bytes)).await
}

