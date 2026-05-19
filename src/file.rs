use std::sync::Arc;

use bytes::Bytes;
use indexmap::IndexMap;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::{
    DType, Error, Result,
    address::ChunkAddress,
    array::ArrayElement,
    codec::CompressionCodec,
    delta::{
        Delta, DeltaAllocator, DeltaCache, DeltaImmutable, DeltaMutable, write_file_then_bytes,
    },
    footer::{FOOTER_VERSION, Footer},
    layout::{
        ArrayLayout, ArrayMeta, AttrIndexKind, AttributeValue, Attributes, ChunkEntry, FillValue,
        StorageLayout,
    },
    stats::{ArrayStats, StatsFile, compute_chunk_partial, merge_partial, read_stats_file},
    storage::{InMemoryStorage, ObjectStoreBackend, Storage},
};

// ── Constants ───────────────────────────────────────────────────────

pub const DEFAULT_BLOCK_TARGET_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
pub const DEFAULT_CACHE_CAPACITY: usize = 256 * 1024 * 1024; // 256 MiB
pub const DEFAULT_IO_CACHE_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB; enable for object-store workloads

// ── FileConfig ──────────────────────────────────────────────────────

pub struct FileConfig<C: CompressionCodec> {
    pub codec: C,
    pub block_target_size: usize,
    pub cache_capacity: usize,
    pub io_cache_capacity: usize,
}

impl<C: CompressionCodec> FileConfig<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            block_target_size: DEFAULT_BLOCK_TARGET_SIZE,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            io_cache_capacity: DEFAULT_IO_CACHE_CAPACITY,
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

// ── File ────────────────────────────────────────────────────────────

/// Object store and base-file path for an on-disk file.
struct StoreDir {
    store: Arc<dyn ObjectStore>,
    base_path: object_store::path::Path,
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
/// accumulate in a disk-backed mutable delta (`pending`) and are flushed
/// by [`flush`](File::flush). The mutable delta is created lazily on the
/// first mutation after open/flush.
pub struct ArrayFile {
    deltas: Vec<Delta<DeltaImmutable>>,
    pending: Option<Delta<DeltaMutable>>,
    codec: Arc<dyn CompressionCodec>,
    block_target_size: usize,
    cache: Option<Arc<DeltaCache>>,
    /// Object store and stem for on-disk files; `None` for in-memory files.
    store_dir: Option<StoreDir>,
    /// Per-array aggregate statistics; `None` until first flush or open.
    stats: Option<StatsFile>,
}

// ── Constructors ────────────────────────────────────────────────────

impl ArrayFile {
    /// Creates a new empty file at `path` within `store`.
    pub async fn create<C: CompressionCodec + 'static>(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
        config: FileConfig<C>,
    ) -> Result<Self> {
        let cache = make_cache(config.cache_capacity, config.io_cache_capacity);
        let delta_path = Arc::<str>::from(path.as_ref());
        let storage =
            Arc::new(ObjectStoreBackend::new(Arc::clone(&store), path.clone())) as Arc<dyn Storage>;
        write_empty_base(&*storage).await?;
        let base_delta = Delta::<DeltaImmutable>::open(storage, delta_path, cache.clone()).await?;
        Ok(ArrayFile {
            deltas: vec![base_delta],
            pending: None,
            codec: Arc::new(config.codec),
            block_target_size: config.block_target_size,
            cache,
            store_dir: Some(StoreDir {
                store,
                base_path: path,
            }),
            stats: None,
        })
    }

    /// Opens an existing file (base + any sidecars) from `store`.
    pub async fn open<C: CompressionCodec + 'static>(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
        config: FileConfig<C>,
    ) -> Result<Self> {
        let cache = make_cache(config.cache_capacity, config.io_cache_capacity);
        let delta_path = Arc::<str>::from(path.as_ref());
        let storage =
            Arc::new(ObjectStoreBackend::new(Arc::clone(&store), path.clone())) as Arc<dyn Storage>;
        let base_delta = Delta::<DeltaImmutable>::open(storage, delta_path, cache.clone()).await?;
        let mut deltas = vec![base_delta];

        let sidecars = discover_sidecars_store(&*store, &path).await?;
        for (_, scar_path) in sidecars {
            let scar_delta_path = Arc::<str>::from(scar_path.as_ref());
            let scar_storage = Arc::new(ObjectStoreBackend::new(Arc::clone(&store), scar_path))
                as Arc<dyn Storage>;
            deltas.push(
                Delta::<DeltaImmutable>::open(scar_storage, scar_delta_path, cache.clone()).await?,
            );
        }

        let stats = {
            let s_storage = ObjectStoreBackend::new(Arc::clone(&store), stats_path(&path));
            read_stats_file(&s_storage).await.ok()
        };

        Ok(ArrayFile {
            deltas,
            pending: None,
            codec: Arc::new(config.codec),
            block_target_size: config.block_target_size,
            cache,
            store_dir: Some(StoreDir {
                store,
                base_path: path,
            }),
            stats,
        })
    }

    /// Creates a new empty in-memory file.
    pub async fn create_memory<C: CompressionCodec + 'static>(
        config: FileConfig<C>,
    ) -> Result<Self> {
        let cache = make_cache(config.cache_capacity, config.io_cache_capacity);
        let storage = Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>;
        write_empty_base(&*storage).await?;
        let base_delta =
            Delta::<DeltaImmutable>::open(storage, Arc::from("__memory_0__"), cache.clone())
                .await?;
        Ok(ArrayFile {
            deltas: vec![base_delta],
            pending: None,
            codec: Arc::new(config.codec),
            block_target_size: config.block_target_size,
            cache,
            store_dir: None,
            stats: None,
        })
    }
}

// ── Schema & attribute access ────────────────────────────────────────

impl ArrayFile {
    /// Returns a reference to the merged array metadata for `name`,
    /// searching from the newest layer towards the oldest.
    pub fn get_array(&self, name: &str) -> Result<&ArrayMeta> {
        self.resolve_array_meta(name)
            .ok_or_else(|| Error::ArrayNotFound {
                name: name.to_string(),
            })
    }

    fn resolve_array_meta(&self, name: &str) -> Option<&ArrayMeta> {
        if let Some(p) = self.pending.as_ref()
            && let Some(m) = p.inner.array_meta.get(name)
        {
            return if m.deleted { None } else { Some(m) };
        }
        for delta in self.deltas.iter().rev() {
            if let Some(&i) = delta.inner.array_index.get(name) {
                let m = &delta.inner.footer.arrays[i];
                return if m.deleted { None } else { Some(m) };
            }
        }
        None
    }

    /// Lazily creates a mutable pending delta and returns a mutable reference.
    fn pending_mut(&mut self) -> &mut Delta<DeltaMutable> {
        if self.pending.is_none() {
            let overlay_index = self.deltas.len() as u32;
            self.pending = Some(Delta::<DeltaMutable>::new(
                Arc::clone(&self.codec),
                self.block_target_size,
                overlay_index,
            ));
        }
        self.pending.as_mut().unwrap()
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
            if let Some(&i) = delta.inner.array_index.get(name) {
                for e in &delta.inner.footer.arrays[i].layout.storage.chunks {
                    existing.entry(e.coord.clone()).or_default();
                }
            }
        }
        if let Some(p) = self.pending.as_ref()
            && let Some(m) = p.inner.array_meta.get(name)
        {
            for e in &m.layout.storage.chunks {
                existing.entry(e.coord.clone()).or_default();
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
        if let Some(p) = self.pending.as_ref() {
            for (name, a) in &p.inner.array_meta {
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
        }
        seen.into_values().collect()
    }

    /// Returns aggregate statistics for `name`, or `None` if no stats exist yet.
    pub fn array_stats(&self, name: &str) -> Option<&ArrayStats> {
        self.stats.as_ref()?.get_array(name)
    }

    /// Number of committed (immutable) delta layers.
    pub fn num_layers(&self) -> usize {
        self.deltas.len()
    }

    pub fn get_attribute(&self, name: &str, key: &str) -> Result<Option<&AttributeValue>> {
        let meta = self.get_array(name)?;
        let key_idx = match self
            .pending
            .as_ref()
            .and_then(|p| p.inner.attr_keys.iter().position(|k| k == key))
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
        if let Some(p) = self.pending.as_ref()
            && val_idx < p.inner.attr_values.len()
        {
            return Ok(Some(&p.inner.attr_values[val_idx]));
        }
        for delta in self.deltas.iter().rev() {
            if val_idx < delta.inner.footer.attr_values.len() {
                return Ok(Some(&delta.inner.footer.attr_values[val_idx]));
            }
        }
        Ok(None)
    }

    pub fn set_attribute(&mut self, name: &str, key: &str, value: AttributeValue) -> Result<()> {
        // Ensure the array exists (in deltas or pending), and snapshot its meta
        // in case we need to copy it down into the pending mutable delta.
        // Clear the cloned chunks list so we don't carry stale block addresses
        // from a lower layer into this delta's footer.
        let mut existing_meta = self.get_array(name)?.clone();
        existing_meta.layout.storage.chunks.clear();

        let pending = self.pending_mut();
        let key_idx = pending.intern_attr_key(key);
        let val_idx = pending.intern_attr_value(value);

        // Update the array meta in pending (copy from lower layer if absent).
        if pending.array_meta_mut(name).is_none() {
            pending.upsert_array_meta(existing_meta);
        }
        let meta = pending.array_meta_mut(name).unwrap();
        meta.attributes.upsert(key_idx, val_idx);
        Ok(())
    }
}

// ── Array definition / deletion ──────────────────────────────────────

impl ArrayFile {
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
        self.pending_mut().upsert_array_meta(ArrayMeta {
            name,
            dtype: T::DTYPE,
            layout,
            fill_value,
            deleted: false,
            attributes: Attributes::empty(AttrIndexKind::U16),
        });
        Ok(())
    }

    /// Marks an array as deleted in the pending layer.
    pub fn delete(&mut self, name: &str) -> Result<()> {
        let meta = self.get_array(name)?.clone();
        self.pending_mut().mark_deleted(meta);
        Ok(())
    }
}

// ── Chunk-level read/write (pub(crate) for ndarray_ext) ──────────────

impl ArrayFile {
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
        // If the array isn't yet present in pending, copy its meta down so the
        // mutable delta has an entry to attach the chunk to. Clear the cloned
        // chunks list — the lower-layer addresses don't apply to this delta's
        // data file, and only chunks written into this session belong here.
        let snapshot = if self
            .pending
            .as_ref()
            .and_then(|p| p.inner.array_meta.get(name))
            .is_none()
        {
            let mut m = self.get_array(name)?.clone();
            m.layout.storage.chunks.clear();
            Some(m)
        } else {
            None
        };
        let pending = self.pending_mut();
        if let Some(meta) = snapshot {
            pending.upsert_array_meta(meta);
        }
        pending.write_raw_chunk(name, coord, &bytes)
    }

    async fn resolve_raw_chunk(&self, name: &str, coord: &[u32]) -> Result<Option<Bytes>> {
        if let Some(p) = self.pending.as_ref()
            && let Some(bytes) = p.read_raw_chunk(name, coord)
        {
            return Ok(Some(bytes));
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

impl ArrayFile {
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
            let effective_start = if start.len() == ndim {
                start.clone()
            } else {
                vec![0; ndim]
            };
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

impl ArrayFile {
    /// Commits pending writes to a new sidecar file on disk.
    pub async fn flush(&mut self) -> Result<()> {
        if self.pending.is_none() {
            return Ok(());
        }
        let (store, base_path) = match &self.store_dir {
            Some(sd) => (Arc::clone(&sd.store), sd.base_path.clone()),
            None => {
                return Err(Error::Storage(
                    "in-memory file: use flush_memory instead".into(),
                ));
            }
        };
        let overlay_index = self.deltas.len() as u32;
        let scar_path = sidecar_path(&base_path, overlay_index);
        let delta_path = Arc::<str>::from(scar_path.as_ref());
        let storage =
            Arc::new(ObjectStoreBackend::new(Arc::clone(&store), scar_path)) as Arc<dyn Storage>;
        let hint = base_path.as_ref().to_string();
        let dirty_names = self.commit_pending(storage, delta_path, hint).await?;

        let merged = self.compute_stats_for(&dirty_names).await?;
        let s_storage = ObjectStoreBackend::new(Arc::clone(&store), stats_path(&base_path));
        s_storage
            .write(bytes::Bytes::from(merged.serialize()?))
            .await?;
        self.stats = Some(merged);
        Ok(())
    }

    /// Commits pending writes to `storage` (for in-memory testing).
    pub async fn flush_memory(&mut self, storage: &InMemoryStorage) -> Result<()> {
        if self.pending.is_none() {
            return Ok(());
        }
        let overlay_index = self.deltas.len() as u32;
        let delta_path = Arc::<str>::from(format!("__memory_{overlay_index}__").as_str());
        let arc: Arc<dyn Storage> = Arc::new(storage.clone());
        let dirty_names = self.commit_pending(arc, delta_path, String::new()).await?;

        let merged = self.compute_stats_for(&dirty_names).await?;
        self.stats = Some(merged);
        Ok(())
    }

    async fn compute_stats_for(&self, dirty_names: &[String]) -> Result<StatsFile> {
        let mut merged = self.stats.clone().unwrap_or_default();
        for name in dirty_names {
            let schema = match self.get_chunked_schema(name) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let fill_value = self
                .resolve_array_meta(name)
                .and_then(|m| m.fill_value.clone());
            let shape_product: u64 = schema.full_shape.iter().map(|&s| s as u64).product();
            let mut stats = ArrayStats::new(name.clone());
            let mut written_non_null: u64 = 0;
            for coord in &schema.all_coords {
                if let Some(bytes) = self.resolve_raw_chunk(name, coord).await? {
                    let (min, max, nc, rc) =
                        compute_chunk_partial(&bytes, &schema.dtype, fill_value.as_ref());
                    written_non_null += rc - nc;
                    merge_partial(&mut stats, min, max, nc, rc);
                }
            }
            stats.row_count = shape_product;
            stats.null_count = shape_product - written_non_null;
            merged.upsert(stats);
        }
        Ok(merged)
    }

    /// Commits the pending mutable delta to `storage`, appends the resulting
    /// immutable delta to `self.deltas`, and returns the names of arrays that
    /// had dirty chunks (used to recompute stats).
    async fn commit_pending(
        &mut self,
        storage: Arc<dyn Storage>,
        delta_path: Arc<str>,
        base_file_hint: String,
    ) -> Result<Vec<String>> {
        let mutable = self
            .pending
            .take()
            .expect("commit_pending: no pending delta");
        let dirty_names: Vec<String> = mutable
            .inner
            .array_meta
            .iter()
            .filter(|(_, m)| !m.layout.storage.chunks.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        let immutable = mutable
            .commit(storage, delta_path, self.cache.clone(), base_file_hint)
            .await?;
        self.deltas.push(immutable);
        Ok(dirty_names)
    }
}

// ── Compact ──────────────────────────────────────────────────────────

impl ArrayFile {
    /// Merges all committed layers into a single base file.
    pub async fn compact(&mut self) -> Result<()> {
        // Build the merged view.
        let merged_names: Vec<String> = self.list_arrays().into_iter().map(|m| m.name).collect();

        // Allocate all chunks for merged arrays.
        let mut allocator = DeltaAllocator::new(Arc::clone(&self.codec), self.block_target_size);
        let mut arrays: Vec<ArrayMeta> = Vec::new();
        let mut per_array_stats: Vec<ArrayStats> = Vec::new();

        for name in &merged_names {
            let meta = self
                .resolve_array_meta(name)
                .ok_or_else(|| Error::ArrayNotFound { name: name.clone() })?
                .clone();

            // Collect all chunk coords across all layers for this array.
            let mut all_coords: indexmap::IndexSet<Vec<u32>> = indexmap::IndexSet::new();
            for delta in &self.deltas {
                if let Some(&i) = delta.inner.array_index.get(name.as_str()) {
                    for e in &delta.inner.footer.arrays[i].layout.storage.chunks {
                        all_coords.insert(e.coord.clone());
                    }
                }
            }

            let shape_product: u64 = meta.layout.shape.iter().map(|&s| s as u64).product();
            let mut new_chunks: Vec<ChunkEntry> = Vec::new();
            let mut array_stats = ArrayStats::new(name.clone());
            let mut written_non_null: u64 = 0;
            for coord in &all_coords {
                // Read from newest layer that has this chunk.
                if let Some(raw) = self.resolve_raw_chunk(name, coord).await? {
                    let (min, max, nc, rc) =
                        compute_chunk_partial(&raw, &meta.dtype, meta.fill_value.as_ref());
                    written_non_null += rc - nc;
                    merge_partial(&mut array_stats, min, max, nc, rc);
                    let alloc = allocator.allocate(&raw);
                    new_chunks.push(ChunkEntry {
                        coord: coord.clone(),
                        address: ChunkAddress::from(alloc),
                    });
                }
            }
            array_stats.row_count = shape_product;
            array_stats.null_count = shape_product - written_non_null;
            per_array_stats.push(array_stats);

            let mut new_meta = meta;
            new_meta.layout.storage.chunks = new_chunks;
            arrays.push(new_meta);
        }

        let crate::delta::AllocatorOutput {
            mut file,
            output_size,
            blocks,
        } = allocator.commit().await;

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
                let _ = sd
                    .store
                    .delete(&sidecar_path(&sd.base_path, i as u32))
                    .await;
            }
            // Write new base.
            Arc::new(ObjectStoreBackend::new(
                Arc::clone(&sd.store),
                sd.base_path.clone(),
            ))
        } else {
            // In-memory: reuse the first layer's storage.
            Arc::clone(&self.deltas[0].inner.storage)
        };

        write_file_then_bytes(&mut file, output_size, &footer_bytes, &*base_storage).await?;
        let base_delta_path: Arc<str> = if let Some(sd) = &self.store_dir {
            Arc::from(sd.base_path.as_ref())
        } else {
            Arc::from("__memory_0__")
        };
        let new_base =
            Delta::<DeltaImmutable>::open(base_storage, base_delta_path, self.cache.clone())
                .await?;
        self.deltas = vec![new_base];

        let mut new_stats = StatsFile::default();
        for s in per_array_stats {
            new_stats.upsert(s);
        }
        if let Some(sd) = &self.store_dir {
            let s_storage =
                ObjectStoreBackend::new(Arc::clone(&sd.store), stats_path(&sd.base_path));
            s_storage
                .write(bytes::Bytes::from(new_stats.serialize()?))
                .await?;
        }
        self.stats = Some(new_stats);
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn make_cache(block_capacity: usize, io_capacity: usize) -> Option<Arc<DeltaCache>> {
    if block_capacity == 0 && io_capacity == 0 {
        None
    } else {
        Some(Arc::new(DeltaCache::new(
            block_capacity as u64,
            io_capacity as u64,
        )))
    }
}

fn sidecar_path(base: &object_store::path::Path, n: u32) -> object_store::path::Path {
    let s = base.as_ref();
    let without_af = s.strip_suffix(".af").unwrap_or(s);
    object_store::path::Path::from(format!("{without_af}.{n}.af").as_str())
}

fn stats_path(base: &object_store::path::Path) -> object_store::path::Path {
    let s = base.as_ref();
    let without_af = s.strip_suffix(".af").unwrap_or(s);
    object_store::path::Path::from(format!("{without_af}.stats").as_str())
}

async fn discover_sidecars_store(
    store: &dyn ObjectStore,
    base_path: &object_store::path::Path,
) -> Result<Vec<(u32, object_store::path::Path)>> {
    use futures::TryStreamExt;
    let base_str = base_path.as_ref();
    let stem_prefix = base_str
        .strip_suffix(".af")
        .ok_or_else(|| Error::Storage("path must end with .af".into()))?;
    let list_prefix = base_str
        .rfind('/')
        .map(|pos| object_store::path::Path::from(&base_str[..pos]));
    let objects: Vec<_> = store
        .list(list_prefix.as_ref())
        .try_collect()
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let mut sidecars: Vec<(u32, object_store::path::Path)> = objects
        .into_iter()
        .filter_map(|meta| {
            let s = meta.location.as_ref();
            let rest = s.strip_prefix(stem_prefix)?.strip_prefix('.')?;
            let (num_str, ext) = rest.rsplit_once('.')?;
            if ext != "af" {
                return None;
            }
            let n: u32 = num_str.parse().ok()?;
            if n == 0 {
                return None;
            }
            Some((n, meta.location))
        })
        .collect();
    sidecars.sort_by_key(|(n, _)| *n);
    Ok(sidecars)
}

async fn write_empty_base(storage: &dyn Storage) -> Result<()> {
    let footer = Footer::new();
    let bytes = footer.serialize()?;
    storage.write(Bytes::from(bytes)).await
}
