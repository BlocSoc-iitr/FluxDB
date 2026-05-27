pub const MAX_PAGE_SIZE: usize = 4 * 1024; // 4KB, Maybe 8KB, 16KB, etc, will have to check
pub const MAX_FRAMES: usize = 80;
pub const INVALID_FRAME_ID: u64 = u64::MAX;
pub const NUM_SHARDS: usize = 8;
pub const SHARD_MASK: u64 = (NUM_SHARDS - 1) as u64;
pub const LOCK_MANAGER_SHARDS: usize = 128;
pub const MAX_KEY_SIZE: usize = 512;
