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

struct LockQueueState {
    requests: VecDeque<LockRequest>,
    is_upgrading: bool,
}

struct LockQueue {
    state: Mutex<LockQueueState>,
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
                        state: Mutex::new(LockQueueState {
                            requests: VecDeque::new(),
                            is_upgrading: false,
                        }),
                        cond_var: Condvar::new(),
                    })
                })
                .clone()
        };

        let mut guard = queue.state.lock().unwrap();

        let can_grant = match lock_type {
            LockType::SharedLock => {
                let has_exclusive_lock = guard
                    .requests
                    .iter()
                    .any(|r| r.lock_type == LockType::ExclusiveLock);
                !has_exclusive_lock
            }
            LockType::ExclusiveLock => guard.requests.is_empty(),
        };

        guard.requests.push_back(LockRequest {
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
                .requests
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
            map_guard.get(&page_id).ok_or(LockManagerError::RecordNotFound)?.clone()
        };

        let mut guard = queue_guard.state.lock().unwrap();
        let index = guard
            .requests
            .iter()
            .position(|r| r.transaction_id == transaction_id)
            .ok_or(LockManagerError::RecordNotFound)?;

        if guard.is_upgrading && guard.requests[index].lock_type == LockType::ExclusiveLock {
            guard.is_upgrading = false;
        }

        guard.requests.remove(index);

        let mut any_granted = false;
        let mut exclusive_seen = false;

        for request in guard.requests.iter_mut() {
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

    pub fn upgrade_lock(&self, transaction_id: u64, page_id: u64) -> Result<(), LockManagerError> {
        let queue_guard = {
            let map_guard = self.map.lock().unwrap();
            map_guard.get(&page_id).ok_or(LockManagerError::RecordNotFound)?.clone()
        };

        let mut guard = queue_guard.state.lock().unwrap();

        let index = guard
            .requests
            .iter()
            .position(|r| r.transaction_id == transaction_id)
            .ok_or(LockManagerError::RecordNotFound)?;

        let request = &guard.requests[index];

        if request.lock_type == LockType::ExclusiveLock {
            return Ok(());
        }
        if !request.is_granted {
            return Err(LockManagerError::InvalidLockState);
        }

        if guard.is_upgrading {
            return Err(LockManagerError::UpgradeConflict); 
        }

        guard.is_upgrading = true;

        let mut req = guard.requests.remove(index).unwrap();
        req.lock_type = LockType::ExclusiveLock;
        req.is_granted = false;

        let mut insert_index = 0;
        for (i, r) in guard.requests.iter().enumerate() {
            if !r.is_granted {
                insert_index = i;
                break;
            }
            insert_index = i + 1;
        }
        guard.requests.insert(insert_index, req);

        loop {
            let my_req = guard.requests.iter().find(|r| r.transaction_id == transaction_id).unwrap();
            if my_req.is_granted {
                guard.is_upgrading = false;
                break;
            }

            guard = queue_guard.cond_var.wait(guard).unwrap();
        }

        Ok(())
    }
}
