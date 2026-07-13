//! The runtime: [`ArrayFile`], the top-level read/write/compact handle.
//!
//! [`ArrayFile`] ties the lower layers together — it defines and reads/writes
//! arrays, manages the stack of delta layers (flushing pending writes into
//! sidecars and compacting them back down), and is configured through
//! [`FileConfig`].

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
    storage::{ObjectStoreBackend, Storage},
};

// ── Constants ───────────────────────────────────────────────────────

/// Default target size for a data block before a new one is started (8 MiB).
pub const DEFAULT_BLOCK_TARGET_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
/// Default byte budget for the decompressed-block cache (256 MiB).
pub const DEFAULT_CACHE_CAPACITY: usize = 256 * 1024 * 1024; // 256 MiB
/// Default byte budget for the raw I/O slab cache (64 MiB); useful for object-store workloads.
pub const DEFAULT_IO_CACHE_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB; enable for object-store workloads

// ── FileConfig ──────────────────────────────────────────────────────

/// Configuration for opening or creating an [`ArrayFile`].
///
/// Construct with [`FileConfig::new`] for the defaults, then override fields as
/// needed:
///
/// ```
/// use array_format::{FileConfig, ZstdCodec};
///
/// let config = FileConfig {
///     block_target_size: 4 * 1024 * 1024,
///     ..FileConfig::new(ZstdCodec { level: 9 })
/// };
/// ```
pub struct FileConfig<C: CompressionCodec> {
    /// Compression codec applied to data blocks on write.
    pub codec: C,
    /// Target size of a data block before a new block is started, in bytes.
    pub block_target_size: usize,
    /// Byte budget for this file's decompressed-block cache.
    ///
    /// Ignored when [`cache`](Self::cache) is `Some`.
    pub cache_capacity: usize,
    /// Byte budget for this file's raw I/O slab cache (0 disables it).
    ///
    /// Ignored when [`cache`](Self::cache) is `Some`.
    pub io_cache_capacity: usize,
    /// Optional pre-built cache to share across multiple [`ArrayFile`]s.
    ///
    /// When `Some`, [`cache_capacity`](Self::cache_capacity) and
    /// [`io_cache_capacity`](Self::io_cache_capacity) are ignored and every file
    /// sharing this cache is bounded by one combined byte budget. Entries are
    /// keyed by `(file_path, block_id)`, so files do not interfere.
    pub cache: Option<Arc<DeltaCache>>,
}

impl<C: CompressionCodec> FileConfig<C> {
    /// Creates a config using `codec` and the `DEFAULT_*` capacities, with no
    /// shared cache.
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            block_target_size: DEFAULT_BLOCK_TARGET_SIZE,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            io_cache_capacity: DEFAULT_IO_CACHE_CAPACITY,
            cache: None,
        }
    }
}

// ── MergedArrayMeta ─────────────────────────────────────────────────

/// Array metadata visible to the caller after merging all delta layers.
///
/// Returned by [`ArrayFile::list_arrays`].
#[derive(Debug, Clone)]
pub struct MergedArrayMeta {
    /// Array name (unique within the file).
    pub name: String,
    /// Element type.
    pub dtype: DType,
    /// Full array shape, one entry per dimension.
    pub shape: Vec<u32>,
    /// Chunk shape; equals [`shape`](Self::shape) for single-chunk arrays.
    pub chunk_shape: Vec<u32>,
    /// Name of each dimension.
    pub dimension_names: Vec<String>,
    /// Fill value used for unwritten elements, if one was set at definition.
    pub fill_value: Option<FillValue>,
}

// ── File ────────────────────────────────────────────────────────────

/// Object store and base-file path backing a file.
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
/// by [`flush`](Self::flush). The mutable delta is created lazily on the
/// first mutation after open/flush.
pub struct ArrayFile {
    deltas: Vec<Delta<DeltaImmutable>>,
    pending: Option<Delta<DeltaMutable>>,
    codec: Arc<dyn CompressionCodec>,
    block_target_size: usize,
    cache: Option<Arc<DeltaCache>>,
    /// Object store and stem backing this file (an in-memory file uses
    /// `object_store`'s in-memory backend).
    store_dir: StoreDir,
    /// Per-array aggregate statistics; `None` until first flush or open.
    stats: Option<StatsFile>,
}

// ── Constructors ────────────────────────────────────────────────────

impl ArrayFile {
    /// Creates a new empty file at `path` within `store`.
    ///
    /// `path` is the base file object and should end in `.af`; sidecars
    /// (`{stem}.N.af`) and the stats file (`{stem}.stats`) are written alongside
    /// it in the same prefix. Fails if an object already exists at `path` only
    /// insofar as the backend allows overwriting — the base is (re)written empty.
    pub async fn create<C: CompressionCodec + 'static>(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
        config: FileConfig<C>,
    ) -> Result<Self> {
        let cache = resolve_cache(&config);
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
            store_dir: StoreDir {
                store,
                base_path: path,
            },
            stats: None,
        })
    }

    /// Opens an existing file from `store`, discovering the base and all
    /// sidecar layers under the same stem.
    ///
    /// `path` must end in `.af`. Aggregate statistics are loaded from
    /// `{stem}.stats` if present; a missing or unreadable stats file is not an
    /// error (see [`array_stats`](Self::array_stats)).
    pub async fn open<C: CompressionCodec + 'static>(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
        config: FileConfig<C>,
    ) -> Result<Self> {
        let cache = resolve_cache(&config);
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
            store_dir: StoreDir {
                store,
                base_path: path,
            },
            stats,
        })
    }

    /// Creates a new empty in-memory file.
    ///
    /// Backed by `object_store`'s in-memory backend, so it behaves exactly like
    /// an on-disk file (commit pending writes with [`flush`](Self::flush)) but
    /// keeps everything in process. Useful for tests and ephemeral pipelines.
    pub async fn create_memory<C: CompressionCodec + 'static>(
        config: FileConfig<C>,
    ) -> Result<Self> {
        let store = Arc::new(object_store::memory::InMemory::new());
        Self::create(store, object_store::path::Path::from("memory.af"), config).await
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

    /// Returns the value of attribute `key` on array `name`, or `None` if the
    /// array has no such attribute. Errors if the array does not exist.
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

    /// Returns attribute `key` for every visible array as `(array_name, value)`.
    ///
    /// The result is a full column over all non-deleted arrays: `Some(value)`
    /// for arrays that carry the attribute, `None` for those that don't.
    /// Intended for coarse pruning — scan the returned values to select arrays
    /// without walking each one via [`get_attribute`](Self::get_attribute).
    /// Logically deleted arrays are omitted entirely.
    pub fn attribute_index(&self, key: &str) -> Vec<(String, Option<&AttributeValue>)> {
        self.list_arrays()
            .into_iter()
            .map(|m| {
                let value = self.get_attribute(&m.name, key).ok().flatten();
                (m.name, value)
            })
            .collect()
    }

    /// Sets attribute `key` on array `name` to `value`, inserting or replacing
    /// any existing entry. The change lands in the pending layer and is
    /// persisted on the next [`flush`](Self::flush). Errors if the array does
    /// not exist.
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
    /// Defines a new array in the pending layer.
    ///
    /// `shape` is the full array shape; `chunk_shape` tiles it into a grid of
    /// independently stored chunks, or `None` to store the whole array as a
    /// single chunk. If `dimension_names` does not have one entry per dimension
    /// it is replaced with `dim0`, `dim1`, … . `fill_value` is returned for
    /// elements that are never written.
    ///
    /// Errors with [`Error::ArrayAlreadyExists`] if an array of this name is
    /// already visible. The definition is persisted on the next
    /// [`flush`](Self::flush).
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

    /// Logically deletes an array by writing a tombstone to the pending layer.
    ///
    /// The array is excluded from [`list_arrays`](Self::list_arrays) and all
    /// reads immediately, but its bytes remain on disk until
    /// [`compact`](Self::compact) reclaims them.
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
    /// Writes `data` into array `name` with its origin at coordinate `start`.
    ///
    /// The region may span multiple chunks and need not be chunk-aligned;
    /// partially covered chunks are read-modify-written automatically. `T` must
    /// match the array's declared dtype, otherwise [`Error::DTypeMismatch`] is
    /// returned. Writes accumulate in the pending layer until
    /// [`flush`](Self::flush).
    pub async fn write_array<T: ArrayElement>(
        &mut self,
        name: &str,
        start: Vec<usize>,
        data: ndarray::ArrayView<'_, T, ndarray::IxDyn>,
    ) -> Result<()> {
        crate::ndarray_ext::write_nd(self, name, data, &start).await
    }

    /// Reads the sub-region of array `name` starting at `start` with the given
    /// `shape`.
    ///
    /// Pass `vec![], vec![]` to read the whole array. Chunks that were never
    /// written are materialized from the array's fill value. `T` must match the
    /// array's declared dtype, otherwise [`Error::DTypeMismatch`] is returned.
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
    /// Commits pending writes to a new sidecar layer and refreshes the
    /// `{stem}.stats` file.
    ///
    /// A no-op if there are no pending changes.
    pub async fn flush(&mut self) -> Result<()> {
        if self.pending.is_none() {
            return Ok(());
        }
        let store = Arc::clone(&self.store_dir.store);
        let base_path = self.store_dir.base_path.clone();
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
    /// Merges all committed layers into a single new base file, deleting the
    /// sidecars and reclaiming space held by overwritten and tombstoned chunks.
    ///
    /// After a successful compaction [`num_layers`](Self::num_layers) returns
    /// `1`. Recomputes and rewrites the `{stem}.stats` file.
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
        let sd = &self.store_dir;
        // Delete old sidecars first.
        for i in 1..self.deltas.len() {
            let _ = sd
                .store
                .delete(&sidecar_path(&sd.base_path, i as u32))
                .await;
        }
        let base_storage: Arc<dyn Storage> = Arc::new(ObjectStoreBackend::new(
            Arc::clone(&sd.store),
            sd.base_path.clone(),
        ));

        write_file_then_bytes(&mut file, output_size, &footer_bytes, &*base_storage).await?;
        let base_delta_path: Arc<str> = Arc::from(sd.base_path.as_ref());
        let new_base =
            Delta::<DeltaImmutable>::open(base_storage, base_delta_path, self.cache.clone())
                .await?;
        self.deltas = vec![new_base];

        let mut new_stats = StatsFile::default();
        for s in per_array_stats {
            new_stats.upsert(s);
        }
        let s_storage = ObjectStoreBackend::new(
            Arc::clone(&self.store_dir.store),
            stats_path(&self.store_dir.base_path),
        );
        s_storage
            .write(bytes::Bytes::from(new_stats.serialize()?))
            .await?;
        self.stats = Some(new_stats);
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn resolve_cache<C: CompressionCodec>(config: &FileConfig<C>) -> Option<Arc<DeltaCache>> {
    if let Some(c) = &config.cache {
        Some(Arc::clone(c))
    } else if config.cache_capacity == 0 && config.io_cache_capacity == 0 {
        None
    } else {
        Some(Arc::new(DeltaCache::new(
            config.cache_capacity as u64,
            config.io_cache_capacity as u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::NoCompression;

    #[tokio::test]
    async fn shared_cache_is_reused_across_files() {
        let shared = Arc::new(DeltaCache::new(1024 * 1024, 0));

        let mut cfg_a = FileConfig::new(NoCompression);
        cfg_a.cache = Some(Arc::clone(&shared));
        let file_a = ArrayFile::create_memory(cfg_a).await.unwrap();

        let mut cfg_b = FileConfig::new(NoCompression);
        cfg_b.cache = Some(Arc::clone(&shared));
        let file_b = ArrayFile::create_memory(cfg_b).await.unwrap();

        let a = file_a.cache.as_ref().expect("file_a has cache");
        let b = file_b.cache.as_ref().expect("file_b has cache");
        assert!(Arc::ptr_eq(a, &shared));
        assert!(Arc::ptr_eq(b, &shared));
    }

    /// Looks up `name` in an `attribute_index` result.
    fn find<'a>(
        index: &'a [(String, Option<&AttributeValue>)],
        name: &str,
    ) -> Option<&'a Option<&'a AttributeValue>> {
        index.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    #[tokio::test]
    async fn attribute_index_returns_full_column() {
        let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
            .await
            .unwrap();

        file.define_array::<f32>("a", vec!["x".into()], vec![4], None, None)
            .unwrap();
        file.define_array::<f32>("b", vec!["x".into()], vec![4], None, None)
            .unwrap();
        file.define_array::<f32>("c", vec!["x".into()], vec![4], None, None)
            .unwrap();

        file.set_attribute("a", "units", AttributeValue::String("hPa".into()))
            .unwrap();
        file.set_attribute("b", "units", AttributeValue::String("Pa".into()))
            .unwrap();
        // "c" deliberately has no "units" attribute.

        let index = file.attribute_index("units");
        assert_eq!(index.len(), 3, "every visible array appears once");
        assert_eq!(
            find(&index, "a"),
            Some(&Some(&AttributeValue::String("hPa".into())))
        );
        assert_eq!(
            find(&index, "b"),
            Some(&Some(&AttributeValue::String("Pa".into())))
        );
        assert_eq!(find(&index, "c"), Some(&None), "absent attribute -> None");
    }

    #[tokio::test]
    async fn attribute_index_unknown_key_is_all_none() {
        let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
            .await
            .unwrap();
        file.define_array::<f32>("a", vec!["x".into()], vec![4], None, None)
            .unwrap();
        file.set_attribute("a", "units", AttributeValue::String("hPa".into()))
            .unwrap();

        let index = file.attribute_index("nonexistent");
        assert_eq!(index.len(), 1);
        assert_eq!(find(&index, "a"), Some(&None));
    }

    #[tokio::test]
    async fn attribute_index_omits_deleted_arrays() {
        let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
            .await
            .unwrap();
        file.define_array::<f32>("a", vec!["x".into()], vec![4], None, None)
            .unwrap();
        file.define_array::<f32>("b", vec!["x".into()], vec![4], None, None)
            .unwrap();
        file.set_attribute("a", "units", AttributeValue::String("hPa".into()))
            .unwrap();
        file.set_attribute("b", "units", AttributeValue::String("Pa".into()))
            .unwrap();

        file.delete("b").unwrap();

        let index = file.attribute_index("units");
        assert_eq!(index.len(), 1, "deleted array dropped from column");
        assert_eq!(find(&index, "b"), None);
        assert_eq!(
            find(&index, "a"),
            Some(&Some(&AttributeValue::String("hPa".into())))
        );
    }

    #[tokio::test]
    async fn attribute_index_survives_flush() {
        let mut file = ArrayFile::create_memory(FileConfig::new(NoCompression))
            .await
            .unwrap();
        file.define_array::<f32>("a", vec!["x".into()], vec![4], None, None)
            .unwrap();
        file.set_attribute("a", "units", AttributeValue::String("hPa".into()))
            .unwrap();

        // After flush the attribute lives in a committed sidecar footer, not the
        // pending layer — the query must still read it from the delta stack.
        file.flush().await.unwrap();

        let index = file.attribute_index("units");
        assert_eq!(
            find(&index, "a"),
            Some(&Some(&AttributeValue::String("hPa".into())))
        );
    }
}
