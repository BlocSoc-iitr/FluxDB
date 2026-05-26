use std::{
    collections::{HashMap, VecDeque},
    sync::Condvar,
};

enum LockType {
    SharedLock,
    ExclusiveLock,
}

struct LockRequest {
    transaction_id: u64,
    lock_type: LockType,
    is_granted: bool,
}

struct LockQueue {
    queue: VecDeque<LockRequest>,
    cond_var: Condvar,
    upgrading_flag: bool,
}

pub struct LockTable {
    map: HashMap<u64, LockQueue>,
}
