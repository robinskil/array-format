pub mod cache;
mod allocator;
mod immutable;
mod mutable;

pub use allocator::{AllocatorOutput, DeltaAllocator};
pub use cache::DeltaCache;
pub use immutable::DeltaImmutable;
pub use mutable::DeltaMutable;

use bytes::Bytes;

use crate::{Error, Result, storage::Storage};

pub struct Delta<D> {
    pub inner: D,
}

// ── Streaming helper ─────────────────────────────────────────────────

const STREAM_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB

/// Reads `file` sequentially in 1 MiB slices, appends `suffix`, and writes
/// the result to `storage`. The file must already be seeked to position 0.
pub(crate) async fn write_file_then_bytes(
    file: &mut tokio::fs::File,
    file_size: u64,
    suffix: &[u8],
    storage: &dyn Storage,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::with_capacity(file_size as usize + suffix.len());
    let mut remaining = file_size as usize;
    while remaining > 0 {
        let to_read = remaining.min(STREAM_CHUNK_SIZE);
        let prev_len = buf.len();
        buf.resize(prev_len + to_read, 0);
        file.read_exact(&mut buf[prev_len..]).await.map_err(Error::Io)?;
        remaining -= to_read;
    }
    buf.extend_from_slice(suffix);
    storage.write(Bytes::from(buf)).await
}
