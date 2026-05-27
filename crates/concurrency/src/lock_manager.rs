use std::{
    collections::{HashMap, HashSet, VecDeque},
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

struct WaitForGraph {
    waits: HashMap<u64, HashSet<u64>>,
}

pub struct LockManager {
    map: Mutex<HashMap<u64, Arc<LockQueue>>>,
    waits: Mutex<WaitForGraph>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self {
            waits: HashMap::new(),
        }
    }

    pub fn add_wait(&mut self, waiter: u64, holder: u64) {
        self.waits
            .entry(waiter)
            .or_insert_with(HashSet::new)
            .insert(holder);
    }

    pub fn remove_wait(&mut self, waiter: u64, holder: u64) {
        if let Some(set) = self.waits.get_mut(&waiter) {
            set.remove(&holder);
            if set.is_empty() {
                self.waits.remove(&waiter);
            }
        }
    }

    pub fn has_cycle(&self, start_tx: u64) -> bool {
        let mut visited = HashSet::new();
        self.dfs(start_tx, &mut visited)
    }

    fn dfs(&self, curr_tx: u64, visited: &mut HashSet<u64>) -> bool {
        if visited.contains(&curr_tx) {
            return true;
        }
        visited.insert(curr_tx);

        if let Some(waits_for) = self.waits.get(&curr_tx) {
            for &next_tx in waits_for {
                if self.dfs(next_tx, visited) {
                    return true;
                }
            }
        }

        visited.remove(&curr_tx);
        false
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            waits: Mutex::new(WaitForGraph::new()),
        }
    }

    pub fn acquire_lock(
        &self,
        transaction_id: u64,
        page_id: u64,
        lock_type: LockType,
    ) -> Result<(), LockManagerError> {
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
            return Ok(());
        }

        let holders: Vec<u64> = guard
            .requests
            .iter()
            .filter(|r| r.is_granted)
            .map(|r| r.transaction_id)
            .collect();

        {
            let mut wait_guard = self.waits.lock().unwrap();
            // Edge direction is: WAITER -> HOLDER
            for &holder_id in &holders {
                wait_guard.add_wait(transaction_id, holder_id);
            }

            if wait_guard.has_cycle(transaction_id) {
                for &holder_id in &holders {
                    wait_guard.remove_wait(transaction_id, holder_id);
                }
                
                let idx = guard.requests.iter().position(|r| r.transaction_id == transaction_id).unwrap();
                guard.requests.remove(idx);
                
                return Err(LockManagerError::DeadLock);
            }
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

        Ok(())
    }

    pub fn release_lock(&self, transaction_id: u64, page_id: u64) -> Result<(), LockManagerError> {
        let queue_guard = {
            let map_guard = self.map.lock().unwrap();
            map_guard
                .get(&page_id)
                .ok_or(LockManagerError::RecordNotFound)?
                .clone()
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
        let mut newly_granted_txs = Vec::new();

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
                    newly_granted_txs.push(request.transaction_id);
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

        let remaining_waiters: Vec<u64> = guard
            .requests
            .iter()
            .filter(|r| !r.is_granted)
            .map(|r| r.transaction_id)
            .collect();

        {
            let mut wait_guard = self.waits.lock().unwrap();
            for waiter_id in newly_granted_txs {
                wait_guard.remove_wait(waiter_id, transaction_id);
            }
            
            for waiter_id in remaining_waiters {
                wait_guard.remove_wait(waiter_id, transaction_id);
            }
        }

        queue_guard.cond_var.notify_all();
        Ok(())
    }

    pub fn upgrade_lock(&self, transaction_id: u64, page_id: u64) -> Result<(), LockManagerError> {
        let queue_guard = {
            let map_guard = self.map.lock().unwrap();
            map_guard
                .get(&page_id)
                .ok_or(LockManagerError::RecordNotFound)?
                .clone()
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
            let my_req = guard
                .requests
                .iter()
                .find(|r| r.transaction_id == transaction_id)
                .unwrap();
            if my_req.is_granted {
                guard.is_upgrading = false;
                break;
            }

            guard = queue_guard.cond_var.wait(guard).unwrap();
        }

        Ok(())
    }
}
