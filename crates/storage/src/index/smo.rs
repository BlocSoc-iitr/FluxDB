//! Structure-modification operations: leaf/internal splits, downlink
//! propagation, and lazy split completion (self-heal).

use crate::page::ChildSide::{Left, Right};
use db_core::transaction_manager::TransactionManager;

use super::*;

impl<K: Key, V: Value> BTreeIndex<K, V> {
    // ── Split ─────────────────────────────────────────────────────────────────

    /// Split a full leaf then insert `(key, value)` into the correct half.
    /// Takes ownership of `leaf_guard` so it can be dropped when inserting
    /// into the right page. Propagates the new separator up via `stack`.
    /// `stored_val` is the bytes to place in the leaf record — either the inline
    /// value or a serialized `OverflowDescriptor`; `rec_type` distinguishes the
    /// two. The caller has already spilled large values via `materialize_value`.
    pub(super) fn split_and_insert(
        &self,
        mut leaf_guard: PageWriteGuard<'_>,
        key: &K::SelfType<'_>,
        stored_val: &[u8],
        rec_type: u8,
        txn: &Transaction,
        stack: &BTStack,
    ) -> Result<()> {
        let leaf_pid_actual = leaf_guard.page_id;

        // ── Try compaction first (design doc 25: bottom-up deletion) ──
        let global_xmin = txn.tm.global_xmin();
        let (dead_count, chains) = LeafPageMutator::<K, V>::compact(
            leaf_pid_actual,
            &mut leaf_guard[..],
            global_xmin,
            &txn.tm,
        );

        // PageCompact FPI — only when compaction actually repacked the page.
        if dead_count > 0 {
            let fpi = <&[u8; PAGE_SIZE]>::try_from(&leaf_guard[..]).unwrap();
            let lsn = self
                .wal
                .log_page_compact(SYSTEM_TXN_ID, leaf_pid_actual, fpi)?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);

            // Free overflow chains orphaned by the compaction (log-before-delete).
            for first_page_id in &chains {
                let ids = collect_overflow_page_ids(*first_page_id, &self.pool)?;
                self.wal.log_overflow_free(SYSTEM_TXN_ID, &ids)?;
                free_overflow_chain(*first_page_id, &self.pool)?;
            }

            let key_bytes = K::as_bytes(key);
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);

            if acc.can_fit_direct(key_bytes.as_ref().len(), stored_val.len()) {
                let (slot, _) = acc.position(key);

                let mut mutator = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
                mutator.insert_raw(slot, key_bytes.as_ref(), stored_val, rec_type)?;
                mutator.set_xmin(slot, txn.txn_id);

                // Insert logged separately under the real txn; compact avoided the split.
                let lsn = self.wal.log_insert(
                    txn.txn_id,
                    leaf_pid_actual,
                    slot as u16,
                    key_bytes.as_ref(),
                    stored_val,
                    rec_type,
                    txn.txn_id,
                )?;
                LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
                return Ok(());
            }
        }

        let key_bytes = K::as_bytes(key);
        let split = self.split_leaf_ly(&mut leaf_guard)?;

        let target_pid =
            if K::compare(key_bytes.as_ref(), split.separator_key.as_slice()) != Ordering::Less {
                split.new_page_id
            } else {
                leaf_pid_actual
            };

        // Insert the new tuple into its target half and log it under the real txn.
        if target_pid == leaf_pid_actual {
            let (s, _) = LeafPageAccessor::<K, V>::new(&leaf_guard[..]).position(key);
            let mut mutator = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
            mutator.insert_raw(s, key_bytes.as_ref(), stored_val, rec_type)?;
            mutator.set_xmin(s, txn.txn_id);
            let lsn = self.wal.log_insert(
                txn.txn_id,
                leaf_pid_actual,
                s as u16,
                key_bytes.as_ref(),
                stored_val,
                rec_type,
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
            // Release before propagation so the downlink step can latch the leaf
            // to clear its flag (no descendant latch held across the ancestor walk).
            drop(leaf_guard);
        } else {
            drop(leaf_guard);
            // Propagate our split's downlink FIRST: the insert below runs
            // unlatched-then-relatched and may need another split, which must
            // find a well-formed tree (and the same ancestor stack).
            self.insert_separator_via_stack(
                stack,
                split.separator_key,
                split.new_page_id,
                leaf_pid_actual,
            )?;

            // Between the latch drop above and here, concurrent inserts can
            // refill (or re-split) the new right page. Correct along
            // rightlinks and, if the tuple no longer fits, split again
            // instead of letting InsufficientSpace escape to the caller.
            let mut right = self.pool.fetch_page_mut(target_pid)?;
            loop {
                let acc = LeafPageAccessor::<K, V>::new(&right[..]);
                if let Some(hk) = acc.high_key_bytes()
                    && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                {
                    let r = acc.rightlink().unwrap();
                    drop(right);
                    right = self.pool.fetch_page_mut(r)?;
                    continue;
                }
                break;
            }
            {
                let acc = LeafPageAccessor::<K, V>::new(&right[..]);
                if !acc.can_fit_direct(key_bytes.as_ref().len(), stored_val.len()) {
                    return self.split_and_insert(right, key, stored_val, rec_type, txn, stack);
                }
            }
            let right_pid = right.page_id;
            let (s, _) = LeafPageAccessor::<K, V>::new(&right[..]).position(key);
            let mut mutator = LeafPageMutator::<K, V>::new(&mut right[..]);
            mutator.insert_raw(s, key_bytes.as_ref(), stored_val, rec_type)?;
            mutator.set_xmin(s, txn.txn_id);
            let lsn = self.wal.log_insert(
                txn.txn_id,
                right_pid,
                s as u16,
                key_bytes.as_ref(),
                stored_val,
                rec_type,
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut right[..]).set_lsn(lsn);
            return Ok(());
        }

        self.insert_separator_via_stack(
            stack,
            split.separator_key,
            split.new_page_id,
            leaf_pid_actual,
        )
    }

    /// Abandon a leaf we marked half-dead but cannot finish unlinking this
    fn abandon_unlink(&self, leaf_pid: PageId) -> Result<()> {
        let mut guard = self.pool.fetch_page_mut(leaf_pid)?;
        if page::is_half_dead(&guard[..]) {
            page::clear_half_dead(&mut guard[..]);
            let image: &[u8; PAGE_SIZE] = (&guard[..]).try_into().unwrap();
            let lsn = self.wal.log_page_compact(SYSTEM_TXN_ID, leaf_pid, image)?;
            page::set_lsn(&mut guard[..], lsn);
        }
        Ok(())
    }

    pub(super) fn delete_empty_leaf(
        &self,
        leaf_pid: PageId,
        tm: &TransactionManager,
    ) -> Result<()> {
        // high key under a shared latch — the descent target below. The
        // sibling links are NOT read here: they can go stale at any time and
        // are re-derived under exclusive latches before the unlink is logged.
        let high_key = {
            let guard = self.pool.fetch_page(leaf_pid)?;
            let acc = LeafPageAccessor::<K, V>::new(&guard[..]);
            acc.high_key_bytes().map(|b| b.to_vec())
        };

        // re-descend from the root toward the leaf's key range, collecting the
        // ancestor stack, same pattern as insert's phase 1
        // target is "just below high_key" — high_key
        // == None means rightmost spine
        let mut stack = BTStack::new();
        let mut pid = *self.root.lock().unwrap();
        loop {
            let page = self.pool.fetch_page(pid)?;
            if crate::page::is_incomplete_split(&page[..]) {
                drop(page);
                self.finish_split(pid, &stack)?;
                stack.clear();
                pid = *self.root.lock().unwrap();
                continue;
            }
            match page[0] {
                INTERNAL => {
                    let acc = InternalPageAccessor::<K>::new(&page[..]);
                    // move right only when the leaf's range can't sit under this
                    // node, strictly greater for a keyed target, always for the
                    // rightmost target while a rightlink exists
                    let move_right = match (&high_key, acc.high_key_bytes()) {
                        (Some(hk), Some(node_hk)) => K::compare(hk, node_hk) == Ordering::Greater,
                        (None, _) => acc.rightlink().is_some(),
                        (_, None) => false,
                    };
                    if move_right {
                        let right = acc.rightlink().unwrap();
                        drop(page);
                        pid = right;
                        continue;
                    }
                    let n = acc.num_keys() as usize;
                    let child_idx = match &high_key {
                        // Route left on equality- the leaf's high key is its
                        // separator in the parent, and the leaf sits left of it
                        // (find_child would route to the right sibling)
                        Some(hk) => (0..n)
                            .find(|&i| {
                                K::compare(K::as_bytes(&acc.key_at(i)).as_ref(), hk)
                                    != Ordering::Less
                            })
                            .unwrap_or(n),
                        None => n,
                    };
                    let child = acc.child_page_at(child_idx);
                    stack.push(BTStackEntry { page_id: pid });
                    drop(page);
                    pid = child;
                }
                LEAF => {
                    drop(page);
                    break;
                }
                found => {
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        }

        // parent is last internal on the stack, empty stack means
        //the leaf is the root
        let Some(entry) = stack.last() else {
            return Ok(());
        };
        let mut parent_pid = entry.page_id;

        // Pre-check under a shared latch, moving right if the parent split
        // after the stack was collected. On a fresh sweep the leaf is not yet
        // marked, so bailing is harmless. On crash-resume the leaf enters here
        // ALREADY half-dead, so every bail must clear the flag (via
        // abandon_unlink) — otherwise a leaf that can never be unlinked makes
        // inserts routed to it back off forever.
        loop {
            let guard = self.pool.fetch_page(parent_pid)?;
            let acc = InternalPageAccessor::<K>::new(&guard[..]);
            let n = acc.num_keys() as usize;
            if (0..=n).any(|i| acc.child_page_at(i) == leaf_pid) {
                // Only-child leaves are skipped: removing the downlink would
                // leave a degenerate empty internal. Concurrent splits only
                // ADD parent keys, so this pre-check cannot be invalidated.
                if n == 0 {
                    drop(guard);
                    return self.abandon_unlink(leaf_pid);
                }
                break;
            }
            match acc.rightlink() {
                Some(right) => {
                    drop(guard);
                    parent_pid = right;
                }
                // downlink gone, now a concurrent completer already unlinked the
                // leaf (or it was never linked), nothing to do this sweep
                None => {
                    drop(guard);
                    return self.abandon_unlink(leaf_pid);
                }
            }
        }

        // Mark the leaf half-dead and take a fresh read of its links. After
        // the mark the leaf can never split again (it is empty and inserts
        // back off), so its rightlink is frozen from here on; its prev is
        // only a hint — the true left neighbor is verified below.
        let (prev_hint, right_sibling) = {
            let mut guard = self.pool.fetch_page_mut(leaf_pid)?;
            if !page::is_half_dead(&guard[..]) {
                if LeafPageAccessor::<K, V>::new(&guard[..]).num_pairs() != 0 {
                    return Ok(());
                }
                let lsn = self.wal.log_mark_half_dead(SYSTEM_TXN_ID, leaf_pid)?;
                page::set_half_dead(&mut guard[..]);
                LeafPageMutator::<K, V>::new(&mut guard[..]).set_lsn(lsn);
            }
            let acc = LeafPageAccessor::<K, V>::new(&guard[..]);
            (acc.prev_page(), acc.rightlink())
        };

        // Find the true left neighbor and HOLD its exclusive latch through
        // logging and both chain splices: the hint goes stale the moment the
        // neighbor splits, and splicing with a stale left would orphan the
        // split's new page from the chain. Walking right converges — pages
        // are only ever inserted between the hint and the leaf, never removed
        // (vacuum is the only remover and this sweep IS vacuum).
        let mut left_guard = match prev_hint {
            None => None,
            Some(mut pid) => loop {
                let guard = self.pool.fetch_page_mut(pid)?;
                match LeafPageAccessor::<K, V>::new(&guard[..]).rightlink() {
                    Some(next) if next == leaf_pid => break Some(guard),
                    Some(next) => {
                        drop(guard);
                        pid = next;
                    }
                    // Chain ended before reaching the leaf — it is no longer
                    // linked. Clear the mark and leave it for a later sweep.
                    None => {
                        drop(guard);
                        return self.abandon_unlink(leaf_pid);
                    }
                }
            },
        };
        let left_sibling = left_guard.as_ref().map(|g| g.page_id);

        // Re-find the downlink under the parent's EXCLUSIVE latch and hold it
        // through logging: concurrent splits insert downlinks and shift
        // indices, so `remove_index` is only trustworthy while the latch that
        // serializes those inserts is held. Taking an internal latch while
        // holding a leaf latch matches finish_split's leaf→parent order.
        let (mut parent_guard, remove_index, keep_right_child) = loop {
            let guard = self.pool.fetch_page_mut(parent_pid)?;
            let acc = InternalPageAccessor::<K>::new(&guard[..]);
            let n = acc.num_keys() as usize;
            if let Some(j) = (0..=n).find(|&i| acc.child_page_at(i) == leaf_pid) {
                if n == 0 {
                    drop(guard);
                    drop(left_guard);
                    return self.abandon_unlink(leaf_pid);
                }
                // Mapping for remove_key_at(index, ChildSide): removing child
                // 0 keeps the right child of key 0; any other child j is the
                // right child of key j-1.
                let mapping = if j == 0 {
                    (guard, 0u16, true) // UNLINK_KEEP_RIGHT
                } else {
                    (guard, (j - 1) as u16, false) // UNLINK_KEEP_LEFT
                };
                break mapping;
            }
            match acc.rightlink() {
                Some(right) => {
                    drop(guard);
                    parent_pid = right;
                }
                None => {
                    drop(guard);
                    drop(left_guard);
                    return self.abandon_unlink(leaf_pid);
                }
            }
        };

        // Every arm below was validated under a latch that is still held, so
        // the record can no longer go stale between append and apply.
        let unlink_lsn = self.wal.log_unlink_page(
            SYSTEM_TXN_ID,
            leaf_pid,
            left_sibling,
            right_sibling,
            parent_pid,
            remove_index,
            keep_right_child,
        )?;

        // Parent first, and its latch dropped before any leaf is acquired —
        // holding an internal latch while waiting on a leaf would deadlock
        // against finish_split's leaf→parent order. The leaf stays reachable
        // through the left sibling's rightlink until the splice below.
        {
            let mut mutator = InternalPageMutator::<K>::new(&mut parent_guard[..]);
            mutator.set_lsn(unlink_lsn);
            mutator.remove_key_at(
                remove_index as usize,
                if keep_right_child { Right } else { Left },
            );
        }
        drop(parent_guard);

        // Chain splice. The left latch stays held until the right sibling's
        // back-link is fixed, so a left-sibling split cannot race its own
        // back-link fix against this one.
        if let Some(guard) = left_guard.as_mut() {
            let mut mutator = LeafPageMutator::<K, V>::new(&mut guard[..]);
            mutator.set_lsn(unlink_lsn);
            mutator.set_rightlink(right_sibling);
        }
        if let Some(right) = right_sibling {
            let mut guard = self.pool.fetch_page_mut(right)?;
            let mut mutator = LeafPageMutator::<K, V>::new(&mut guard[..]);
            mutator.set_lsn(unlink_lsn);
            mutator.set_prev_page(left_sibling);
        }
        drop(left_guard);
        // snapshot that could still walk to it has drained
        self.pending_recycle
            .lock()
            .unwrap()
            .push((leaf_pid, tm.global_xmin()));

        Ok(())
    }

    fn split_leaf_ly(
        &self,
        leaf_guard: &mut crate::buffer_pool::PageWriteGuard<'_>,
    ) -> Result<SplitResult> {
        let leaf_pid = leaf_guard.page_id;
        let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
        let n = acc.num_pairs() as usize;

        // Find a split point where the key CHANGES.
        // Start at n/2 and scan forward until we hit a different key.
        // This ensures all versions of the same key stay on the same page.
        let mid = {
            let target = n / 2;
            let target_key = K::as_bytes(&acc.get_key(target)).as_ref().to_vec();
            let mut split_at = target;
            // Scan forward past all slots with the same key as target.
            while split_at < n {
                let key_val = acc.get_key(split_at);
                let k = K::as_bytes(&key_val);
                if K::compare(k.as_ref(), &target_key) != Ordering::Equal {
                    break;
                }
                split_at += 1;
            }
            // If we reached the end (all remaining keys are duplicates),
            // try scanning backward from target instead.
            if split_at >= n {
                split_at = target;
                while split_at > 0 {
                    let key_val = acc.get_key(split_at - 1);
                    let k = K::as_bytes(&key_val);
                    if K::compare(k.as_ref(), &target_key) != Ordering::Equal {
                        break;
                    }
                    split_at -= 1;
                }
            }
            // If split_at is 0, the entire page has the same key
            // (pathological case — can only happen if MAX versions of one key
            // fill the page). Fall back to n/2 and accept the cross-page split.
            if split_at == 0 { target } else { split_at }
        };

        let separator_key = K::as_bytes(&acc.get_key(mid)).as_ref().to_vec();
        let old_rightlink = acc.rightlink();
        let old_high_key: Option<Vec<u8>> = acc.high_key_bytes().map(|b| b.to_vec());

        // Snapshot ALL entries (including dead versions) to preserve MVCC history.
        // Value bytes and rec_type are captured verbatim so overflow pointers in
        // existing records survive the split unchanged.
        type LeftEntry = (Vec<u8>, Vec<u8>, u8, u64, u64);
        let left_entries: Vec<LeftEntry> = (0..mid)
            .map(|i| {
                let k = K::as_bytes(&acc.get_key(i)).as_ref().to_vec();
                let v = acc.raw_value(i).to_vec();
                (k, v, acc.get_rec_type(i), acc.get_xmin(i), acc.get_xmax(i))
            })
            .collect();

        // Allocate right page.
        let mut right_guard = self.pool.new_page()?;
        let right_pid = right_guard.page_id;

        {
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
            let mut builder = LeafPageBuilder::<K, V>::new(right_pid, &mut right_guard[..]);
            if let Some(ref hk) = old_high_key {
                builder.set_high_key(hk);
            }
            builder.set_rightlink(old_rightlink);
            builder.set_prev_page(Some(leaf_pid));
            for i in mid..n {
                builder.push_with_mvcc_raw(
                    K::as_bytes(&acc.get_key(i)).as_ref(),
                    acc.raw_value(i),
                    acc.get_rec_type(i),
                    acc.get_xmin(i),
                    acc.get_xmax(i),
                );
            }
            builder.finish();
        }

        // Fix the old neighbour's back-link; held until logging so its post-fix
        // image lands in the LeafSplit FPI set.
        let mut old_right_guard = match old_rightlink {
            Some(old_right_pid) => {
                let mut g = self.pool.fetch_page_mut(old_right_pid)?;
                LeafPageMutator::<K, V>::new(&mut g[..]).set_prev_page(Some(right_pid));
                Some(g)
            }
            None => None,
        };

        // Rebuild left page from scratch (high_key changes slot base).
        {
            let prev = LeafPageAccessor::<K, V>::new(&leaf_guard[..]).prev_page();
            let mut builder = LeafPageBuilder::<K, V>::new(leaf_pid, &mut leaf_guard[..]);
            builder.set_high_key(&separator_key);
            builder.set_rightlink(Some(right_pid));
            builder.set_prev_page(prev);
            for (k, v, rt, xmin, xmax) in &left_entries {
                builder.push_with_mvcc_raw(k, v, *rt, *xmin, *xmax);
            }
            builder.finish();
        }

        // INCOMPLETE_SPLIT until the downlink lands; captured in the left FPI.
        crate::page::set_incomplete_split(&mut leaf_guard[..]);

        // Atomic LeafSplit FPI set; stamp the record LSN on every touched page.
        let left_fpi = <&[u8; PAGE_SIZE]>::try_from(&leaf_guard[..]).unwrap();
        let right_fpi = <&[u8; PAGE_SIZE]>::try_from(&right_guard[..]).unwrap();
        let neigh = old_right_guard
            .as_ref()
            .map(|g| (g.page_id, <&[u8; PAGE_SIZE]>::try_from(&g[..]).unwrap()));
        let lsn = self.wal.log_leaf_split(
            SYSTEM_TXN_ID,
            (leaf_pid, left_fpi),
            (right_pid, right_fpi),
            neigh,
        )?;
        LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
        LeafPageMutator::<K, V>::new(&mut right_guard[..]).set_lsn(lsn);
        if let Some(g) = old_right_guard.as_mut() {
            LeafPageMutator::<K, V>::new(&mut g[..]).set_lsn(lsn);
        }
        drop(right_guard);
        drop(old_right_guard);

        Ok(SplitResult {
            separator_key,
            new_page_id: right_pid,
        })
    }

    /// Insert the downlink `(sep_key, right_pid)` into the parent of `left_child`
    /// (the page that split) and clear `left_child`'s INCOMPLETE_SPLIT flag.
    /// Idempotent: a downlink already present (concurrent completion / redo) is a
    /// no-op that still clears the flag.
    fn insert_separator_via_stack(
        &self,
        stack: &BTStack,
        mut sep_key: Vec<u8>,
        mut right_pid: PageId,
        mut left_child: PageId,
    ) -> Result<()> {
        // Walk the ancestor path bottom-up by index — never consume the
        // stack: callers (recursive splits, finish_split) still need it.
        let mut depth = stack.len();
        loop {
            if depth == 0 {
                // The splitting page is the root unless one was created concurrently.
                if *self.root.lock().unwrap() != left_child {
                    crate::page::clear_incomplete_split(
                        &mut self.pool.fetch_page_mut(left_child)?[..],
                    );
                    return Ok(());
                }
                let mut new_root_guard = self.pool.new_page()?;
                let new_root_pid = new_root_guard.page_id;

                let mut builder =
                    InternalPageBuilder::<K>::new(new_root_pid, &mut new_root_guard[..]);
                builder.push_first_child(left_child);
                builder.push_key_and_right_child(&K::from_bytes(&sep_key), right_pid);
                builder.finish();

                // Point page 0 at the new root in memory.
                let mut meta_guard = self.pool.fetch_page_mut(0)?;
                crate::page::meta::set_root(&mut meta_guard[..], new_root_pid);

                // NewRoot: new-root FPI + page-0 pointer in one atomic record.
                let new_root_fpi = <&[u8; PAGE_SIZE]>::try_from(&new_root_guard[..]).unwrap();
                let lsn = self.wal.log_new_root(
                    SYSTEM_TXN_ID,
                    (new_root_pid, new_root_fpi),
                    left_child,
                )?;
                InternalPageMutator::<K>::new(&mut new_root_guard[..]).set_lsn(lsn);
                crate::page::meta::set_lsn(&mut meta_guard[..], lsn);
                drop(new_root_guard);
                drop(meta_guard);

                // Old root's split is now linked via the new root — clear its flag.
                {
                    let mut old = self.pool.fetch_page_mut(left_child)?;
                    crate::page::clear_incomplete_split(&mut old[..]);
                    crate::page::set_lsn(&mut old[..], lsn);
                }

                *self.root.lock().unwrap() = new_root_pid;
                return Ok(());
            }

            depth -= 1;
            let mut parent_pid = stack[depth].page_id;

            let mut parent_guard = self.pool.fetch_page_mut(parent_pid)?;
            loop {
                let acc = InternalPageAccessor::<K>::new(&parent_guard[..]);
                if let Some(hk) = acc.high_key_bytes()
                    && K::compare(&sep_key, hk) != Ordering::Less
                {
                    let right = acc.rightlink().unwrap();
                    drop(parent_guard);
                    parent_pid = right;
                    parent_guard = self.pool.fetch_page_mut(parent_pid)?;
                    continue;
                }
                break;
            }

            // insert-if-absent: skip if the downlink is already present.
            let already = {
                let acc = InternalPageAccessor::<K>::new(&parent_guard[..]);
                let n = acc.num_keys() as usize;
                (0..=n).any(|i| acc.child_page_at(i) == right_pid)
            };
            if already {
                drop(parent_guard);
                crate::page::clear_incomplete_split(&mut self.pool.fetch_page_mut(left_child)?[..]);
                return Ok(());
            }

            let acc = InternalPageAccessor::<K>::new(&parent_guard[..]);
            let (idx, _) = acc.find_child(&K::from_bytes(&sep_key));
            if acc.can_fit(sep_key.len()) {
                InternalPageMutator::<K>::new(&mut parent_guard[..]).insert_key_and_right_child(
                    idx,
                    &K::from_bytes(&sep_key),
                    right_pid,
                )?;
                let lsn = self.wal.log_insert_downlink(
                    SYSTEM_TXN_ID,
                    parent_pid,
                    idx as u16,
                    &sep_key,
                    right_pid,
                    left_child,
                )?;
                InternalPageMutator::<K>::new(&mut parent_guard[..]).set_lsn(lsn);
                // Clear the child's flag — logged via the InsertDownlink child block.
                let mut child = self.pool.fetch_page_mut(left_child)?;
                crate::page::clear_incomplete_split(&mut child[..]);
                crate::page::set_lsn(&mut child[..], lsn);
                return Ok(());
            }

            // Parent splits: split_internal_ly fuses in the downlink. Clear the
            // child's flag in memory (self-heals on redo until that path logs it).
            let parent_split = self.split_internal_ly(&mut parent_guard, &sep_key, right_pid)?;
            drop(parent_guard);
            crate::page::clear_incomplete_split(&mut self.pool.fetch_page_mut(left_child)?[..]);

            sep_key = parent_split.separator_key;
            right_pid = parent_split.new_page_id;
            left_child = parent_pid;
        }
    }

    /// Complete an incomplete split lazily: insert the missing downlink for
    /// `child_pid` (insert-if-absent) and clear its flag. Re-checks under a write
    /// latch so a racing completer / already-finished split is a no-op.
    pub(super) fn finish_split(&self, child_pid: PageId, stack: &BTStack) -> Result<()> {
        let mut guard = self.pool.fetch_page_mut(child_pid)?;
        if !crate::page::is_incomplete_split(&guard[..]) {
            return Ok(());
        }
        // separator = the page's high key; right sibling = its rightlink.
        let (sep_key, right_pid) = match guard[0] {
            LEAF => {
                let acc = LeafPageAccessor::<K, V>::new(&guard[..]);
                (acc.high_key_bytes().map(|b| b.to_vec()), acc.rightlink())
            }
            INTERNAL => {
                let acc = InternalPageAccessor::<K>::new(&guard[..]);
                (acc.high_key_bytes().map(|b| b.to_vec()), acc.rightlink())
            }
            _ => return Ok(()),
        };
        if let (Some(sep_key), Some(right_pid)) = (sep_key, right_pid) {
            drop(guard);
            self.insert_separator_via_stack(stack, sep_key, right_pid, child_pid)
        } else {
            // Spurious flag (no right sibling) — clear it so descent can proceed.
            crate::page::clear_incomplete_split(&mut guard[..]);
            Ok(())
        }
    }

    fn split_internal_ly(
        &self,
        guard: &mut crate::buffer_pool::PageWriteGuard<'_>,
        sep_key: &[u8],
        right_child: PageId,
    ) -> Result<SplitResult> {
        let internal_pid = guard.page_id;
        let acc = InternalPageAccessor::<K>::new(&guard[..]);
        let n = acc.num_keys() as usize;

        let old_rightlink = acc.rightlink();
        let old_high_key: Option<Vec<u8>> = acc.high_key_bytes().map(|b| b.to_vec());

        let mut children: Vec<PageId> = (0..=n).map(|i| acc.child_page_at(i)).collect();
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|i| K::as_bytes(&acc.key_at(i)).as_ref().to_vec())
            .collect();

        let insert_idx = {
            let mut lo = 0usize;
            let mut hi = keys.len();
            while lo < hi {
                let mid_i = lo + (hi - lo) / 2;
                if K::compare(&keys[mid_i], sep_key) == Ordering::Greater {
                    hi = mid_i;
                } else {
                    lo = mid_i + 1;
                }
            }
            lo
        };
        keys.insert(insert_idx, sep_key.to_vec());
        children.insert(insert_idx + 1, right_child);

        let total_keys = keys.len();
        let mid = total_keys / 2;
        let push_up_key = keys[mid].clone();

        let mut right_guard = self.pool.new_page()?;
        let right_pid = right_guard.page_id;
        {
            let mut builder = InternalPageBuilder::<K>::new(right_pid, &mut right_guard[..]);
            builder.push_first_child(children[mid + 1]);
            for i in (mid + 1)..total_keys {
                builder.push_key_and_right_child(&K::from_bytes(&keys[i]), children[i + 1]);
            }
            builder.set_rightlink(old_rightlink);
            if let Some(ref hk) = old_high_key {
                builder.set_high_key(hk);
            }
            builder.finish();
        }
        {
            let mut builder = InternalPageBuilder::<K>::new(internal_pid, &mut guard[..]);
            builder.set_rightlink(Some(right_pid));
            builder.set_high_key(&push_up_key);
            builder.push_first_child(children[0]);
            for i in 0..mid {
                builder.push_key_and_right_child(&K::from_bytes(&keys[i]), children[i + 1]);
            }
            builder.finish();
        }

        // INCOMPLETE_SPLIT until the downlink lands; captured in the left FPI.
        crate::page::set_incomplete_split(&mut guard[..]);

        // Atomic InternalSplit FPI set — left + new right.
        let left_fpi = <&[u8; PAGE_SIZE]>::try_from(&guard[..]).unwrap();
        let right_fpi = <&[u8; PAGE_SIZE]>::try_from(&right_guard[..]).unwrap();
        let lsn = self.wal.log_internal_split(
            SYSTEM_TXN_ID,
            (internal_pid, left_fpi),
            (right_pid, right_fpi),
        )?;
        InternalPageMutator::<K>::new(&mut guard[..]).set_lsn(lsn);
        InternalPageMutator::<K>::new(&mut right_guard[..]).set_lsn(lsn);
        drop(right_guard);

        Ok(SplitResult {
            separator_key: push_up_key,
            new_page_id: right_pid,
        })
    }
}
