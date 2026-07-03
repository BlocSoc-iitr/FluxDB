pub const MAX_PAGE_SIZE: usize = 4 * 1024; // 4KB, Maybe 8KB, 16KB, etc, will have to check
pub const MAX_FRAMES: usize = 80;
pub const INVALID_FRAME_ID: u64 = u64::MAX;
pub const NUM_SHARDS: usize = 8;
pub const SHARD_MASK: u64 = (NUM_SHARDS - 1) as u64;

pub const MAX_KEY_SIZE: usize = 512;
/// Maximum value size in bytes for a single record.
///
/// Largest value guaranteed to fit on a rightmost leaf page (no high key).
/// Non-rightmost pages lose space to the high key region, so a max-key +
/// max-value record may need a split there. Overflow pages will lift this
/// limit entirely (post-WAL manager).
pub const MAX_VALUE_SIZE: usize = 2048;
