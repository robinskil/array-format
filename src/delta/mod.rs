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

/// Streams `file` in 1 MiB chunks to `storage`, then appends `suffix`.
/// The file must already be seeked to position 0.
pub(crate) async fn write_file_then_bytes(
    file: &mut tokio::fs::File,
    file_size: u64,
    suffix: &[u8],
    storage: &dyn Storage,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut writer = storage.write_multipart().await?;
    let mut remaining = file_size as usize;
    while remaining > 0 {
        let to_read = remaining.min(STREAM_CHUNK_SIZE);
        let mut chunk = vec![0u8; to_read];
        file.read_exact(&mut chunk).await.map_err(Error::Io)?;
        writer.write_chunk(Bytes::from(chunk)).await?;
        remaining -= to_read;
    }
    if !suffix.is_empty() {
        writer.write_chunk(Bytes::copy_from_slice(suffix)).await?;
    }
    writer.complete().await
}
