use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Condvar, Mutex},
};

use common::LockManagerError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    SharedLock,
    ExclusiveLock,
}

struct LockRequest {
    transaction_id: u64,
    lock_type: LockType,
    is_granted: bool,
}

struct LockQueue {
    queue: Mutex<VecDeque<LockRequest>>,
    cond_var: Condvar,
}

pub struct LockManager {
    map: Mutex<HashMap<u64, Arc<LockQueue>>>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn acquire_lock(&self, transaction_id: u64, page_id: u64, lock_type: LockType) {
        let queue = {
            let mut map_guard = self.map.lock().unwrap();
            map_guard
                .entry(page_id)
                .or_insert_with(|| {
                    Arc::new(LockQueue {
                        queue: Mutex::new(VecDeque::new()),
                        cond_var: Condvar::new(),
                    })
                })
                .clone()
        };

        let mut guard = queue.queue.lock().unwrap();

        let can_grant = match lock_type {
            LockType::SharedLock => {
                let has_exclusive_lock =
                    guard.iter().any(|r| r.lock_type == LockType::ExclusiveLock);
                !has_exclusive_lock
            }
            LockType::ExclusiveLock => guard.is_empty(),
        };

        guard.push_back(LockRequest {
            transaction_id,
            lock_type,
            is_granted: can_grant,
        });

        if can_grant {
            return;
        }

        loop {
            guard = queue.cond_var.wait(guard).unwrap();
            let my_request = guard
                .iter()
                .find(|r| r.transaction_id == transaction_id)
                .unwrap();

            if my_request.is_granted {
                break;
            }
        }
    }

    pub fn release_lock(&self, transaction_id: u64, page_id: u64) -> Result<(), LockManagerError> {
        let queue_guard = {
            let map_guard = self.map.lock().unwrap();
            map_guard.get(&page_id).unwrap().clone()
        };

        let mut guard = queue_guard.queue.lock().unwrap();
        let index = guard
            .iter()
            .position(|r| r.transaction_id == transaction_id)
            .ok_or(LockManagerError::RecordNotFound)?;

        guard.remove(index);

        let mut any_granted = false;
        let mut exclusive_seen = false;

        for request in guard.iter_mut() {
            if request.is_granted {
                any_granted = true;
                if request.lock_type == LockType::ExclusiveLock {
                    exclusive_seen = true;
                }
            } else {
                let can_grant = match request.lock_type {
                    LockType::SharedLock => !exclusive_seen,
                    LockType::ExclusiveLock => !any_granted,
                };

                if can_grant {
                    request.is_granted = true;
                    any_granted = true;
                    if request.lock_type == LockType::ExclusiveLock {
                        exclusive_seen = true;
                    }
                } else {
                    if request.lock_type == LockType::ExclusiveLock {
                        exclusive_seen = true;
                    }
                }
            }
        }

        queue_guard.cond_var.notify_all();
        Ok(())
    }
}
