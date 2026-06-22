//! Structure-modification operations: leaf/internal splits, downlink
//! propagation, and lazy split completion (self-heal).

use super::*;

impl<K: Key, V: Value> BTreeIndex<K, V> {
    // ── Split ─────────────────────────────────────────────────────────────────

    /// Split a full leaf then insert `(key, value)` into the correct half.
    /// Takes ownership of `leaf_guard` so it can be dropped when inserting
    /// into the right page. Propagates the new separator up via `stack`.
    pub(super) fn split_and_insert(
        &self,
        mut leaf_guard: PageWriteGuard<'_>,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
        txn: &Transaction,
        stack: &mut BTStack,
    ) -> Result<()> {
        let leaf_pid_actual = leaf_guard.page_id;

        // ── Try compaction first (design doc 25: bottom-up deletion) ──
        let global_xmin = txn.tm.global_xmin();
        let dead_count = LeafPageMutator::<K, V>::compact(
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

            let key_bytes = K::as_bytes(key);
            let val_bytes = V::as_bytes(value);
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);

            if acc.can_fit_direct(key_bytes.as_ref().len(), val_bytes.as_ref().len()) {
                let (slot, _) = acc.position(key);

                let mut mutator = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
                mutator.insert(slot, key, value)?;
                mutator.set_xmin(slot, txn.txn_id);

                // Insert logged separately under the real txn; compact avoided the split.
                let lsn = self.wal.log_insert(
                    txn.txn_id,
                    leaf_pid_actual,
                    slot as u16,
                    key_bytes.as_ref(),
                    val_bytes.as_ref(),
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
        let val_bytes = V::as_bytes(value);
        if target_pid == leaf_pid_actual {
            let (s, _) = LeafPageAccessor::<K, V>::new(&leaf_guard[..]).position(key);
            let mut mutator = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
            mutator.insert(s, key, value)?;
            mutator.set_xmin(s, txn.txn_id);
            let lsn = self.wal.log_insert(
                txn.txn_id,
                leaf_pid_actual,
                s as u16,
                key_bytes.as_ref(),
                val_bytes.as_ref(),
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
            // Release before propagation so the downlink step can latch the leaf
            // to clear its flag (no descendant latch held across the ancestor walk).
            drop(leaf_guard);
        } else {
            drop(leaf_guard);
            let mut right = self.pool.fetch_page_mut(target_pid)?;
            let (s, _) = LeafPageAccessor::<K, V>::new(&right[..]).position(key);
            let mut mutator = LeafPageMutator::<K, V>::new(&mut right[..]);
            mutator.insert(s, key, value)?;
            mutator.set_xmin(s, txn.txn_id);
            let lsn = self.wal.log_insert(
                txn.txn_id,
                target_pid,
                s as u16,
                key_bytes.as_ref(),
                val_bytes.as_ref(),
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut right[..]).set_lsn(lsn);
        }

        self.insert_separator_via_stack(
            stack,
            split.separator_key,
            split.new_page_id,
            leaf_pid_actual,
        )
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
        let left_entries: Vec<(Vec<u8>, Vec<u8>, u64, u64)> = (0..mid)
            .map(|i| {
                let k = K::as_bytes(&acc.get_key(i)).as_ref().to_vec();
                let v = V::as_bytes(&acc.get_value(i)).as_ref().to_vec();
                (k, v, acc.get_xmin(i), acc.get_xmax(i))
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
                builder.push_with_mvcc(
                    &acc.get_key(i),
                    &acc.get_value(i),
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
            for (k, v, xmin, xmax) in &left_entries {
                builder.push_with_mvcc(&K::from_bytes(k), &V::from_bytes(v), *xmin, *xmax);
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
        stack: &mut BTStack,
        mut sep_key: Vec<u8>,
        mut right_pid: PageId,
        mut left_child: PageId,
    ) -> Result<()> {
        loop {
            if stack.is_empty() {
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
                let lsn = self
                    .wal
                    .log_new_root(SYSTEM_TXN_ID, (new_root_pid, new_root_fpi))?;
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

            let entry = stack.pop().unwrap();
            let mut parent_pid = entry.page_id;

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
            self.insert_separator_via_stack(&mut stack.clone(), sep_key, right_pid, child_pid)
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
