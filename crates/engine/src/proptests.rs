//! Property-based tests for the B+Tree, driven through the real engine API.
//!
//! The harness, walker, and oracle are **generic over the key/value types**, so
//! the same machinery runs against fixed-width `u32` keys *and* variable-length
//! `String` / `Vec<u8>` keys (varint length prefixes, variable-width page
//! layout, prefix/length ordering). Types are constrained to owned `SelfType`
//! (`for<'a> K: Value<SelfType<'a> = K>`) so generated values can be passed and
//! stored directly — true of `u32`, `String`, and `Vec<u8>`.

use common::{EngineError, IndexError, Key, Value};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use storage::buffer_pool::BufferPoolManager;
use storage::index::BTreeIndex;
use storage::page::{
    INTERNAL, InternalPageAccessor, LEAF, LeafPageAccessor, PageId, is_incomplete_split, meta,
};
use tempfile::TempDir;

use crate::Engine;

// ── Harness ──────────────────────────────────────────────────────────────────

/// Fresh engine in a throwaway dir. The `TempDir` is returned so it drops with
/// the tuple — no DB files leak across the thousands of cases. `Engine::create`
/// wires disk + segmented WAL + pool + index + txn manager internally, so this
/// is immune to lower-level WAL/pool API churn.
fn tmp_engine<K: Key, V: Value>() -> (TempDir, Engine<K, V>) {
    let dir = TempDir::new().unwrap();
    let engine = Engine::<K, V>::create(dir.path()).unwrap();
    (dir, engine)
}

/// Raw serialized bytes of a leaf slot's key (the form `K::compare` consumes).
fn leaf_key_bytes<K: Key, V: Value>(acc: &LeafPageAccessor<K, V>, i: usize) -> Vec<u8> {
    K::as_bytes(&acc.get_key(i)).as_ref().to_vec()
}

// structural invariant walker (generic over K, V) ───────────────────
//
// Returns `Err(msg)` rather than panicking, so it is reusable outside proptest.
// Ordering is checked with `K::compare` on raw key bytes — the real `Key` order —
// so it is correct for every key type. Asserts FluxDB's *real* invariants, which
// are weaker than textbook B+Tree (duplicate version chains are allowed by MVCC).
//
// ASSUMES A QUIESCENT TREE: every SMO has completed. This holds between oracle
// ops (each finishes synchronously) but NOT for a raw crash-recovered tree —
// FluxDB completes splits/deletions lazily (on the next descent), so recovery can
// legitimately leave a page mid-SMO. Such a state breaks two checks here: the
// `INCOMPLETE_SPLIT`-at-rest assertion, AND `rightlink set == top-down set` (an
// incomplete split's new sibling is linked sideways but not yet downlinked, so it
// shows up sideways-only — indistinguishable from a leak without the flag). §8.5
// adds `HALF_DEAD` as a third such marker. A crash harness must therefore either
// settle the tree first (force lazy completion) then call this, or use a
// flag-tolerant variant that accepts discrepancies explained by INCOMPLETE_SPLIT /
// HALF_DEAD. Do NOT call this directly on an as-recovered tree.

fn check_invariants<K: Key, V: Value>(
    index: &BTreeIndex<K, V>,
    pool: &BufferPoolManager,
) -> Result<(), String> {
    let root = index.root_page_id();

    // Top-down descent. `reached` is the visited set: `walk` errors immediately
    // on a repeat (cycle or page reachable by two paths), and the same set is the
    // reachable-page set reused by the leak-accounting check below. `walk` also
    // checks separators are sorted and every key falls in its inherited window.
    let mut reached: BTreeSet<PageId> = BTreeSet::new();
    let mut leaf_depths: Vec<usize> = Vec::new();
    let mut topdown_leaves: BTreeSet<PageId> = BTreeSet::new();
    walk::<K, V>(
        pool,
        root,
        0,
        None,
        None,
        &mut reached,
        &mut leaf_depths,
        &mut topdown_leaves,
    )?;

    // distance from root to leaf is equal for all leaves
    // d0 is the distance of first leaf
    // iterate all the leaves and the depth should be equal to d0
    if let Some(&d0) = leaf_depths.first()
        && leaf_depths.iter().any(|&d| d != d0)
    {
        return Err(format!("unbalanced: leaf depths {leaf_depths:?}"));
    }

    // (Cycles / cross-links were already caught by the visited set inside `walk`.)

    // No INCOMPLETE_SPLIT flag at a quiescent point. INCOMPLETE_SPLIT flag shouldn't be set unless a crash occured mid-way
    for &pid in &reached {
        let page = pool
            .fetch_page(pid)
            .map_err(|e| format!("fetch {pid}: {e}"))?;
        if is_incomplete_split(&page[..]) {
            return Err(format!("page {pid} has INCOMPLETE_SPLIT set at rest"));
        }
    }

    // Leaf rightlink chain: ascending, version-grouped, equal to top-down leaves.
    let chain = leaf_chain::<K, V>(pool, root)?;
    let chain_set: BTreeSet<PageId> = chain.iter().copied().collect();
    if chain_set != topdown_leaves {
        return Err(format!(
            "rightlink leaf set {chain_set:?} != top-down leaf set {topdown_leaves:?} (incomplete split?)"
        ));
    }

    // Prev-link chain (maintained on splits, otherwise exercised only by backward
    // scan) must be the exact reverse of the rightlinks.
    let last_leaf = *chain.last().expect("at least the root leaf exists");
    let pchain = prev_chain::<K, V>(pool, last_leaf)?;
    let mut rev = chain.clone();
    rev.reverse();
    if pchain != rev {
        return Err(format!(
            "prev chain {pchain:?} != reverse of rightlink chain {rev:?}"
        ));
    }

    // When the HALF_DEAD flag / free-list are tracked, a stronger check can exist
    // here: every non-reachable in-range page is HALF_DEAD or free. That would
    // need a page-count accessor on BufferPoolManager (removed for now — no caller).

    // Meta page-0 root agrees with the in-memory root.
    let meta_page = pool.fetch_page(0).map_err(|e| format!("fetch meta: {e}"))?;
    match meta::read_root(&meta_page[..]) {
        Some(r) if r == root => {}
        other => return Err(format!("meta root {other:?} != in-memory root {root}")),
    }

    Ok(())
}

/// check parent's separator and child_high_key is same.
/// expected is the separator that is passed as "high"
/// pid is passed just for debugging
fn check_high_key<K: Key>(
    stored: Option<&[u8]>,
    expected: Option<&[u8]>,
    pid: PageId,
) -> Result<(), String> {
    match (stored, expected) {
        (None, None) => Ok(()),
        (Some(s), Some(e)) if K::compare(s, e) == Ordering::Equal => Ok(()),
        (s, e) => Err(format!(
            "page {pid}: high_key {s:?} disagrees with threaded subtree upper bound {e:?}"
        )),
    }
}

/// recursive DFS; checks separators are sorted, every leaf key falls inside its
/// inherited routing window, and each page's high key matches the parent's
/// separator. `low` is inclusive. `high` is *normally* exclusive (routing sends
/// `key == high` to the right sibling), but a mid-key split (smo.rs:156) can
/// leave dead versions of a key equal to the separator in the left child — so
/// the check tolerates `key == high` and flags only keys strictly above it.
#[allow(clippy::too_many_arguments)]
fn walk<K: Key, V: Value>(
    pool: &BufferPoolManager,
    pid: PageId,
    depth: usize,
    low: Option<&[u8]>,
    high: Option<&[u8]>,
    reached: &mut BTreeSet<PageId>,
    leaf_depths: &mut Vec<usize>,
    topdown_leaves: &mut BTreeSet<PageId>,
) -> Result<(), String> {
    // A valid tree reaches each page exactly once. `insert` returning false means
    // we've been here before — a link cycle or a page with two parents. This one
    // check covers both, immediately and by page id.
    if !reached.insert(pid) {
        return Err(format!(
            "page {pid} reached more than once — cycle or cross-link"
        ));
    }
    let page = pool
        .fetch_page(pid)
        .map_err(|e| format!("fetch {pid}: {e}"))?;
    match page[0] {
        LEAF => {
            leaf_depths.push(depth);
            topdown_leaves.insert(pid);
            let acc = LeafPageAccessor::<K, V>::new(&page[..]);
            for i in 0..acc.num_pairs() as usize {
                let k = leaf_key_bytes::<K, V>(&acc, i);
                if let Some(lo) = low
                    && K::compare(&k, lo) == Ordering::Less
                {
                    return Err(format!(
                        "leaf {pid}: key below inherited lower bound — misrouted"
                    ));
                }
                if let Some(hi) = high
                    && K::compare(&k, hi) == Ordering::Greater
                {
                    return Err(format!(
                        "leaf {pid}: key above inherited upper bound — misrouted"
                    ));
                }
            }
            check_high_key::<K>(acc.high_key_bytes(), high, pid)
        }
        INTERNAL => {
            let acc = InternalPageAccessor::<K>::new(&page[..]);
            let nkeys = acc.num_keys() as usize;
            let children: Vec<PageId> = (0..=nkeys).map(|i| acc.child_page_at(i)).collect();
            let seps: Vec<Vec<u8>> = (0..nkeys)
                .map(|i| K::as_bytes(&acc.key_at(i)).as_ref().to_vec())
                .collect();
            check_high_key::<K>(acc.high_key_bytes(), high, pid)?;
            drop(page);

            for w in seps.windows(2) {
                if K::compare(&w[0], &w[1]) == Ordering::Greater {
                    return Err(format!("internal {pid}: separators descend"));
                }
            }
            for i in 0..=nkeys {
                let child_low = if i == 0 {
                    low
                } else {
                    Some(seps[i - 1].as_slice())
                };
                let child_high = if i == nkeys {
                    high
                } else {
                    Some(seps[i].as_slice())
                };
                walk::<K, V>(
                    pool,
                    children[i],
                    depth + 1,
                    child_low,
                    child_high,
                    reached,
                    leaf_depths,
                    topdown_leaves,
                )?;
            }
            Ok(())
        }
        other => Err(format!("page {pid}: unexpected page-type byte {other}")),
    }
}

fn leftmost_leaf<K: Key>(pool: &BufferPoolManager, root: PageId) -> Result<PageId, String> {
    let mut pid = root;
    loop {
        let page = pool
            .fetch_page(pid)
            .map_err(|e| format!("fetch {pid}: {e}"))?;
        match page[0] {
            LEAF => return Ok(pid),
            INTERNAL => {
                let child = InternalPageAccessor::<K>::new(&page[..]).child_page_at(0);
                drop(page);
                pid = child;
            }
            other => return Err(format!("page {pid}: unexpected page-type byte {other}")),
        }
    }
}

/// Number of internal levels above the leaves (0 = root is itself a leaf).
fn tree_height<K: Key>(pool: &BufferPoolManager, root: PageId) -> usize {
    let mut pid = root;
    let mut h = 0;
    loop {
        let page = pool.fetch_page(pid).unwrap();
        match page[0] {
            INTERNAL => {
                let c = InternalPageAccessor::<K>::new(&page[..]).child_page_at(0);
                drop(page);
                pid = c;
                h += 1;
            }
            _ => return h,
        }
    }
}

/// Walk leaves left-to-right via rightlinks. Assert keys are non-descending.
fn leaf_chain<K: Key, V: Value>(
    pool: &BufferPoolManager,
    root: PageId,
) -> Result<Vec<PageId>, String> {
    let mut pid = leftmost_leaf::<K>(pool, root)?;
    let mut chain = Vec::new();
    let mut seen: BTreeSet<PageId> = BTreeSet::new();
    // `global_last` carries the last key across leaves
    let mut global_last: Option<Vec<u8>> = None;
    loop {
        if !seen.insert(pid) {
            return Err(format!("rightlink chain revisits page {pid} — link cycle"));
        }
        chain.push(pid);
        let page = pool
            .fetch_page(pid)
            .map_err(|e| format!("fetch {pid}: {e}"))?;
        let acc = LeafPageAccessor::<K, V>::new(&page[..]);
        let n = acc.num_pairs() as usize;

        let mut within_prev: Option<Vec<u8>> = None;
        let mut first: Option<Vec<u8>> = None;
        for i in 0..n {
            let k = leaf_key_bytes::<K, V>(&acc, i);
            if let Some(p) = &within_prev
                && K::compare(p, &k) == Ordering::Greater
            {
                return Err(format!("leaf {pid}: keys descend"));
            }
            if first.is_none() {
                first = Some(k.clone());
            }
            within_prev = Some(k);
        }
        // compare last key of previous page with first key of current page
        if let (Some(gl), Some(f)) = (&global_last, &first)
            && K::compare(gl, f) == Ordering::Greater
        {
            return Err(format!(
                "leaf {pid}: first key < previous leaf's last — order break across boundary"
            ));
        }
        if within_prev.is_some() {
            global_last = within_prev;
        }

        let next = acc.rightlink();
        drop(page);
        match next {
            Some(p) => pid = p,
            None => break,
        }
    }
    Ok(chain)
}

fn prev_chain<K: Key, V: Value>(
    pool: &BufferPoolManager,
    rightmost: PageId,
) -> Result<Vec<PageId>, String> {
    let mut pid = rightmost;
    let mut chain = Vec::new();
    let mut seen: BTreeSet<PageId> = BTreeSet::new();
    loop {
        if !seen.insert(pid) {
            return Err(format!("prev chain revisits page {pid} — link cycle"));
        }
        chain.push(pid);
        let page = pool
            .fetch_page(pid)
            .map_err(|e| format!("fetch {pid}: {e}"))?;
        let prev = LeafPageAccessor::<K, V>::new(&page[..]).prev_page();
        drop(page);
        match prev {
            Some(p) => pid = p,
            None => break,
        }
    }
    Ok(chain)
}

// ── Scans (decode keys AND values back for model comparison) ──────────────────
//
// Both directions decode the value too, so a scan that returns the right key set
// but resolves the wrong version's value for a key is caught — RangeScan's
// per-version visibility is a different code path from point `get`.

fn scan_forward<K, V>(engine: &Engine<K, V>) -> Result<Vec<(K, V)>, String>
where
    K: Key + 'static,
    V: Value + 'static,
    for<'a> K: Value<SelfType<'a> = K>,
    for<'a> V: Value<SelfType<'a> = V>,
{
    let txn = engine.transaction_manager.begin();
    let out: Result<Vec<(K, V)>, _> = engine
        .index
        .range(.., &txn)
        .map(|r| r.map(|(kb, vb)| (K::from_bytes(&kb), V::from_bytes(&vb))))
        .collect();
    engine.transaction_manager.mark_committed(txn.txn_id);
    out.map_err(|e| format!("range: {e}"))
}

fn scan_backward<K, V>(engine: &Engine<K, V>) -> Result<Vec<(K, V)>, String>
where
    K: Key + 'static,
    V: Value + 'static,
    for<'a> K: Value<SelfType<'a> = K>,
    for<'a> V: Value<SelfType<'a> = V>,
{
    let txn = engine.transaction_manager.begin();
    let out: Result<Vec<(K, V)>, _> = engine
        .index
        .range_backward(.., &txn)
        .map(|r| r.map(|(kb, vb)| (K::from_bytes(&kb), V::from_bytes(&vb))))
        .collect();
    engine.transaction_manager.mark_committed(txn.txn_id);
    out.map_err(|e| format!("range_backward: {e}"))
}

// ── Step 2: insert-all → readable + sorted (u32) ──────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Insert a unique key set, then every key reads back its value, a full
    /// forward scan is exactly the sorted set, and the tree is structurally
    /// sound. Unique keys ⇒ no write conflicts; isolates splits + ordering.
    /// All inserts ride one transaction (one fsync), so this stays fast.
    #[test]
    fn insert_all_readable_and_sorted(keys in prop::collection::hash_set(any::<u32>(), 0..1500)) {
        let (_dir, engine) = tmp_engine::<u32, u32>();

        // value != key (a distinct transform) so a key/value swap or reading
        // key-bytes-as-value is caught here, not only by the oracle.
        let val = |k: u32| k.wrapping_mul(0x9E37_79B1);
        let mut w = engine.begin();
        for &k in &keys {
            w.insert(&k, &val(k)).map_err(|e| TestCaseError::fail(format!("insert {k}: {e}")))?;
        }
        w.commit().map_err(|e| TestCaseError::fail(format!("commit: {e}")))?;

        let mut r = engine.begin();
        for &k in &keys {
            let got = r.get(&k).map_err(|e| TestCaseError::fail(format!("get {k}: {e}")))?;
            prop_assert_eq!(got.as_deref(), Some(&val(k).to_le_bytes()[..]), "key {} wrong value", k);
        }
        r.commit().map_err(|e| TestCaseError::fail(format!("reader commit: {e}")))?;

        check_invariants(&engine.index, &engine.buffer_pool).map_err(TestCaseError::fail)?;

        let fwd = scan_forward(&engine).map_err(TestCaseError::fail)?;
        let mut expected: Vec<(u32, u32)> = keys.iter().map(|&k| (k, val(k))).collect();
        expected.sort_unstable();
        prop_assert_eq!(fwd, expected);
    }
}

// ── Step 3: BTreeMap oracle (generic over K, V) ───────────────────────────────

#[derive(Clone, Debug)]
enum Op<K, V> {
    Insert(K, V),
    Delete(K),
    Update(K, V),
    Get(K),
}

fn op_strategy<K, V, KS, VS>(ks: KS, vs: VS) -> impl Strategy<Value = Op<K, V>>
where
    K: std::fmt::Debug + 'static,
    V: std::fmt::Debug + 'static,
    KS: Strategy<Value = K> + Clone + 'static,
    VS: Strategy<Value = V> + Clone + 'static,
{
    prop_oneof![
        3 => (ks.clone(), vs.clone()).prop_map(|(k, v)| Op::Insert(k, v)),
        1 => ks.clone().prop_map(Op::Delete),
        2 => (ks.clone(), vs).prop_map(|(k, v)| Op::Update(k, v)),
        2 => ks.prop_map(Op::Get),
    ]
}

/// Replay an op sequence against the engine and a reference `BTreeMap`,
/// asserting answer + error agreement, with the structural walker after every
/// op. Each write op is its own real autocommit transaction, so committed-op
/// visibility is well-defined. Generic over the key/value type.
fn run_oracle<K, V>(ops: &[Op<K, V>]) -> Result<(), TestCaseError>
where
    K: Key + Ord + Clone + std::fmt::Debug + 'static,
    V: Value + Clone + PartialEq + std::fmt::Debug + 'static,
    for<'a> K: Value<SelfType<'a> = K>,
    for<'a> V: Value<SelfType<'a> = V>,
{
    let (_dir, engine) = tmp_engine::<K, V>();
    let mut model: BTreeMap<K, V> = BTreeMap::new();

    for op in ops {
        match op {
            Op::Insert(k, v) => {
                let r = engine.insert(k, v);
                match model.entry(k.clone()) {
                    Entry::Vacant(slot) => {
                        prop_assert!(r.is_ok(), "insert {:?}: expected Ok, got {:?}", k, r);
                        slot.insert(v.clone());
                    }
                    Entry::Occupied(_) => prop_assert!(
                        matches!(r, Err(EngineError::Index(IndexError::DuplicateKey))),
                        "insert dup {:?}: expected DuplicateKey, got {:?}",
                        k,
                        r
                    ),
                }
            }
            Op::Delete(k) => {
                let r = engine.delete(k);
                match model.entry(k.clone()) {
                    Entry::Occupied(slot) => {
                        prop_assert!(r.is_ok(), "delete {:?}: expected Ok, got {:?}", k, r);
                        slot.remove();
                    }
                    Entry::Vacant(_) => prop_assert!(
                        matches!(r, Err(EngineError::Index(IndexError::KeyNotFound))),
                        "delete missing {:?}: expected KeyNotFound, got {:?}",
                        k,
                        r
                    ),
                }
            }
            Op::Update(k, v) => {
                let r = engine.update(k, v);
                match model.entry(k.clone()) {
                    Entry::Occupied(mut slot) => {
                        prop_assert!(r.is_ok(), "update {:?}: expected Ok, got {:?}", k, r);
                        slot.insert(v.clone());
                    }
                    Entry::Vacant(_) => prop_assert!(
                        matches!(r, Err(EngineError::Index(IndexError::KeyNotFound))),
                        "update missing {:?}: expected KeyNotFound, got {:?}",
                        k,
                        r
                    ),
                }
            }
            Op::Get(k) => {
                let got = engine
                    .get(k)
                    .map_err(|e| TestCaseError::fail(format!("get {k:?}: {e}")))?;
                let expected = model.get(k).map(|v| V::as_bytes(v).as_ref().to_vec());
                prop_assert_eq!(got, expected, "get {:?} disagreement", k);
            }
        }

        check_invariants(&engine.index, &engine.buffer_pool)
            .map_err(|e| TestCaseError::fail(format!("invariant violated after {op:?}: {e}")))?;
    }

    // Final state: forward scan == sorted model (key, value); backward reversed.
    let fwd = scan_forward(&engine).map_err(TestCaseError::fail)?;
    let bwd = scan_backward(&engine).map_err(TestCaseError::fail)?;
    let entries: Vec<(K, V)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut entries_rev = entries.clone();
    entries_rev.reverse();
    prop_assert_eq!(fwd, entries, "forward scan != sorted model entries");
    prop_assert_eq!(bwd, entries_rev, "backward scan != reverse sorted entries");

    Ok(())
}

/// Like `run_oracle`, but each mutation runs through an explicit `begin` →
/// op → **commit-or-abort** handle (the bool decides), and the model applies a
/// mutation **only on commit**. This exercises the MVCC rollback path —
/// aborted writes must be invisible (xmin/xmax of an aborted txn don't count)
/// and must leave no half-finished SMO (the per-op walker checks structure).
/// Errors (`DuplicateKey`/`KeyNotFound`) are asserted, then the txn aborted.
fn run_abort_oracle<K, V>(ops: &[(Op<K, V>, bool)]) -> Result<(), TestCaseError>
where
    K: Key + Ord + Clone + std::fmt::Debug + 'static,
    V: Value + Clone + PartialEq + std::fmt::Debug + 'static,
    for<'a> K: Value<SelfType<'a> = K>,
    for<'a> V: Value<SelfType<'a> = V>,
{
    let (_dir, engine) = tmp_engine::<K, V>();
    let mut model: BTreeMap<K, V> = BTreeMap::new();

    for (op, commit) in ops {
        match op {
            Op::Get(k) => {
                let got = engine
                    .get(k)
                    .map_err(|e| TestCaseError::fail(format!("get {k:?}: {e}")))?;
                let expected = model.get(k).map(|v| V::as_bytes(v).as_ref().to_vec());
                prop_assert_eq!(got, expected, "get {:?} disagreement", k);
            }
            Op::Insert(k, v) => {
                let mut t = engine.begin();
                let r = t.insert(k, v);
                match model.entry(k.clone()) {
                    Entry::Occupied(_) => {
                        prop_assert!(
                            matches!(r, Err(EngineError::Index(IndexError::DuplicateKey))),
                            "insert dup {:?}: expected DuplicateKey, got {:?}",
                            k,
                            r
                        );
                        t.abort();
                    }
                    Entry::Vacant(slot) => {
                        prop_assert!(r.is_ok(), "insert {:?}: expected Ok, got {:?}", k, r);
                        if *commit {
                            t.commit()
                                .map_err(|e| TestCaseError::fail(format!("commit: {e}")))?;
                            slot.insert(v.clone());
                        } else {
                            t.abort();
                        }
                    }
                }
            }
            Op::Delete(k) => {
                let mut t = engine.begin();
                let r = t.delete(k);
                match model.entry(k.clone()) {
                    Entry::Occupied(slot) => {
                        prop_assert!(r.is_ok(), "delete {:?}: expected Ok, got {:?}", k, r);
                        if *commit {
                            t.commit()
                                .map_err(|e| TestCaseError::fail(format!("commit: {e}")))?;
                            slot.remove();
                        } else {
                            t.abort();
                        }
                    }
                    Entry::Vacant(_) => {
                        prop_assert!(
                            matches!(r, Err(EngineError::Index(IndexError::KeyNotFound))),
                            "delete missing {:?}: expected KeyNotFound, got {:?}",
                            k,
                            r
                        );
                        t.abort();
                    }
                }
            }
            Op::Update(k, v) => {
                let mut t = engine.begin();
                let r = t.update(k, v);
                match model.entry(k.clone()) {
                    Entry::Occupied(mut slot) => {
                        prop_assert!(r.is_ok(), "update {:?}: expected Ok, got {:?}", k, r);
                        if *commit {
                            t.commit()
                                .map_err(|e| TestCaseError::fail(format!("commit: {e}")))?;
                            slot.insert(v.clone());
                        } else {
                            t.abort();
                        }
                    }
                    Entry::Vacant(_) => {
                        prop_assert!(
                            matches!(r, Err(EngineError::Index(IndexError::KeyNotFound))),
                            "update missing {:?}: expected KeyNotFound, got {:?}",
                            k,
                            r
                        );
                        t.abort();
                    }
                }
            }
        }

        check_invariants(&engine.index, &engine.buffer_pool)
            .map_err(|e| TestCaseError::fail(format!("invariant violated after {op:?}: {e}")))?;
    }

    // Only committed mutations are visible.
    let fwd = scan_forward(&engine).map_err(TestCaseError::fail)?;
    let entries: Vec<(K, V)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    prop_assert_eq!(fwd, entries, "post-abort scan != committed-only model");

    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// `u32` keys, small space (0..32): heavy collisions drive duplicate-key
    /// version chains, delete/reinsert, update chains — the MVCC core.
    #[test]
    fn oracle_u32_small(ops in prop::collection::vec(op_strategy(0u32..32, any::<u32>()), 0..200)) {
        run_oracle(&ops)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// `u32` keys, wider space (0..512) × more ops: a fuller 2-level tree while
    /// ops still collide — splits *and* MVCC together. (3-level depth is covered
    /// deterministically by `deep_tree_internal_splits`.)
    #[test]
    fn oracle_u32_stress(ops in prop::collection::vec(op_strategy(0u32..512, any::<u32>()), 0..600)) {
        run_oracle(&ops)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// **Variable-length `String` keys** from a tiny alphabet (`[a-c]{0,4}`) so
    /// they collide *and* exercise prefix/length ordering (`""` < `"a"` < `"aa"`)
    /// and the variable-width leaf/internal page layout. The walker validates
    /// structure with `String::compare`, not a fixed-width assumption.
    #[test]
    fn oracle_string(ops in prop::collection::vec(op_strategy("[a-c]{0,4}", "[a-z]{0,8}"), 0..200)) {
        run_oracle::<String, String>(&ops)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// **Variable-length `Vec<u8>` keys** (varint-length-prefixed encoding) from
    /// a small byte/length pool to force collisions and prefix cases. Exercises
    /// the `Vec<T>` codec + `Key` ordering end-to-end through the tree.
    #[test]
    fn oracle_bytes(
        ops in prop::collection::vec(
            op_strategy(
                prop::collection::vec(0u8..4, 0..4),
                prop::collection::vec(any::<u8>(), 0..8),
            ),
            0..200,
        )
    ) {
        run_oracle::<Vec<u8>, Vec<u8>>(&ops)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Oracle with **aborts**: each mutation commits or aborts per the bool; the
    /// model applies only committed ones. Verifies MVCC rollback — aborted
    /// writes invisible, structure sound after every abort. Small key space so
    /// aborts collide with later ops (abort-an-insert then insert the same key).
    #[test]
    fn oracle_with_aborts(
        ops in prop::collection::vec((op_strategy(0u32..32, any::<u32>()), any::<bool>()), 0..200)
    ) {
        run_abort_oracle::<u32, u32>(&ops)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// **Snapshot isolation**: a reader's view, captured at `begin`, must stay
    /// stable across concurrent-in-time writers that commit afterward — the core
    /// SI guarantee, untouched by the serial oracle. Hold reader `r1` open, run
    /// a batch of committed writes, then assert `r1` still sees its original
    /// view while a fresh reader sees the new committed state.
    #[test]
    fn snapshot_isolation(
        initial in prop::collection::hash_map(0u32..64, any::<u32>(), 0..40),
        writes in prop::collection::vec((0u32..64, any::<u32>(), 0u8..3), 0..40),
    ) {
        let (_dir, engine) = tmp_engine::<u32, u32>();
        let decode = |o: Option<Vec<u8>>| o.map(|b| u32::from_le_bytes(b.try_into().unwrap()));

        // Seed committed initial state.
        let mut model = initial.clone();
        for (&k, &v) in &initial {
            engine.insert(&k, &v).map_err(|e| TestCaseError::fail(format!("seed {k}: {e}")))?;
        }

        // Reader r1 captures its snapshot now.
        let mut r1 = engine.begin();
        let view0: Vec<Option<u32>> = (0..64u32)
            .map(|k| Ok::<_, TestCaseError>(decode(r1.get(&k).map_err(|e| TestCaseError::fail(format!("r1 get {k}: {e}")))?)))
            .collect::<Result<_, _>>()?;

        // Concurrent-in-time writers commit while r1 stays open.
        for (k, v, kind) in &writes {
            match kind {
                0 => { if engine.insert(k, v).is_ok() { model.insert(*k, *v); } }
                1 => { if engine.update(k, v).is_ok() { model.insert(*k, *v); } }
                _ => { if engine.delete(k).is_ok() { model.remove(k); } }
            }
        }

        // r1 must STILL see its original snapshot — no later commit leaks in.
        let view1: Vec<Option<u32>> = (0..64u32)
            .map(|k| Ok::<_, TestCaseError>(decode(r1.get(&k).map_err(|e| TestCaseError::fail(format!("r1 reget {k}: {e}")))?)))
            .collect::<Result<_, _>>()?;
        prop_assert_eq!(&view1, &view0, "snapshot isolation violated: reader saw a later commit");
        r1.commit().map_err(|e| TestCaseError::fail(format!("r1 commit: {e}")))?;

        // A fresh reader sees the new committed state.
        let mut r2 = engine.begin();
        for k in 0..64u32 {
            let got = decode(r2.get(&k).map_err(|e| TestCaseError::fail(format!("r2 get {k}: {e}")))?);
            prop_assert_eq!(got, model.get(&k).copied(), "post-write reader mismatch at {}", k);
        }
        r2.commit().map_err(|e| TestCaseError::fail(format!("r2 commit: {e}")))?;
    }
}

/// Deterministic companion to `snapshot_isolation` that pins the three sharpest
/// cases — the writer mutates keys the reader has **already observed**, so the
/// assertion is "old reader still sees the old value" rather than "nothing it
/// read changed". The `delete`-invisible-to-`r1` case (r1 still returns a key a
/// later txn committed a delete on) is the strongest single SI assertion and is
/// guaranteed to run here, not left to random collision.
#[test]
fn snapshot_isolation_observed_keys() {
    let (_dir, engine) = tmp_engine::<u32, u32>();
    let decode = |o: Option<Vec<u8>>| o.map(|b| u32::from_le_bytes(b.try_into().unwrap()));

    // Seed: k1 will be updated, k2 deleted, k3 starts absent and is inserted.
    engine.insert(&1u32, &10u32).unwrap();
    engine.insert(&2u32, &20u32).unwrap();

    // r1's snapshot observes all three: k1=10, k2=20, k3=None.
    let mut r1 = engine.begin();
    assert_eq!(decode(r1.get(&1).unwrap()), Some(10));
    assert_eq!(decode(r1.get(&2).unwrap()), Some(20));
    assert_eq!(decode(r1.get(&3).unwrap()), None);

    // Committed writers mutate keys r1 already observed.
    engine.update(&1, &11).unwrap(); // update an observed key
    engine.delete(&2).unwrap(); // delete an observed key  ← strongest
    engine.insert(&3, &30).unwrap(); // insert a key r1 observed as absent

    // r1 must STILL see its original snapshot for every observed key.
    assert_eq!(
        decode(r1.get(&1).unwrap()),
        Some(10),
        "SI: update leaked to old reader"
    );
    assert_eq!(
        decode(r1.get(&2).unwrap()),
        Some(20),
        "SI: delete leaked to old reader"
    );
    assert_eq!(
        decode(r1.get(&3).unwrap()),
        None,
        "SI: insert leaked to old reader"
    );
    r1.commit().unwrap();

    // A fresh reader sees the new committed state.
    let mut r2 = engine.begin();
    assert_eq!(decode(r2.get(&1).unwrap()), Some(11));
    assert_eq!(decode(r2.get(&2).unwrap()), None);
    assert_eq!(decode(r2.get(&3).unwrap()), Some(30));
    r2.commit().unwrap();
}

/// One key, updated until its dead-version chain overflows a leaf and forces a
/// **mid-key split** (`split_leaf_ly` fallback, smo.rs:156). The same key then
/// spans a leaf boundary — so the walker must accept *non-descending* (not
/// strictly increasing) keys across boundaries. Logically it's still one visible
/// key with the latest value. ~300 versions far exceeds a 4 KB leaf's ~120 u32
/// tuples, so the split is forced (no vacuum runs to reclaim dead versions).
#[test]
fn single_key_version_chain_spans_pages() {
    let (_dir, engine) = tmp_engine::<u32, u32>();
    engine.insert(&7u32, &0u32).unwrap();

    // Hold an old snapshot open: compaction runs before any split (smo.rs:22)
    // gated on `global_xmin`, so without an old reader the dead versions are
    // reclaimed and the leaf never overflows. This keeper pins the horizon so
    // the chain actually grows and forces the mid-key split.
    let keeper = engine.begin();

    for v in 1..=300u32 {
        engine.update(&7u32, &v).unwrap();
    }

    // Non-vacuous: the chain must actually have split across pages.
    let h = tree_height::<u32>(&engine.buffer_pool, engine.index.root_page_id());
    assert!(
        h >= 1,
        "expected the version chain to split into multiple leaves, height={h}"
    );

    check_invariants(&engine.index, &engine.buffer_pool).unwrap();

    let mut r = engine.begin();
    let got = r
        .get(&7)
        .unwrap()
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()));
    assert_eq!(got, Some(300), "latest version must be visible");
    r.commit().unwrap();
    assert_eq!(
        scan_forward(&engine).unwrap(),
        vec![(7u32, 300u32)],
        "exactly one visible key+value"
    );

    keeper.abort();
}

/// Deterministic deep-tree coverage: ~25k distinct keys forces a **3-level**
/// tree (height ≥ 2), exercising `split_internal_ly` / cascading internal splits
/// that the small-key-space oracle can't reach. Three insertion orders stress
/// different split shapes; each tree is structurally validated and both scan
/// directions checked. All inserts ride one transaction (one fsync).
#[test]
fn deep_tree_internal_splits() {
    const N: u32 = 25_000;
    let ascending: Vec<u32> = (0..N).collect();
    let descending: Vec<u32> = (0..N).rev().collect();
    // (i * 7919) mod N is a permutation of 0..N (7919 prime, coprime to 25000).
    let scattered: Vec<u32> = (0..N).map(|i| i.wrapping_mul(7919) % N).collect();

    for (name, order) in [
        ("ascending", ascending),
        ("descending", descending),
        ("scattered", scattered),
    ] {
        let (_dir, engine) = tmp_engine::<u32, u32>();
        let mut w = engine.begin();
        for &k in &order {
            w.insert(&k, &k)
                .unwrap_or_else(|e| panic!("{name}: insert {k}: {e}"));
        }
        w.commit().unwrap_or_else(|e| panic!("{name}: commit: {e}"));

        let height = tree_height::<u32>(&engine.buffer_pool, engine.index.root_page_id());
        assert!(
            height >= 2,
            "{name}: expected 3-level tree, got height {height}"
        );
        check_invariants(&engine.index, &engine.buffer_pool)
            .unwrap_or_else(|e| panic!("{name}: {e}"));

        let expected: Vec<(u32, u32)> = (0..N).map(|k| (k, k)).collect();
        let fwd = scan_forward(&engine).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(fwd, expected, "{name}: forward scan");
        let bwd = scan_backward(&engine).unwrap_or_else(|e| panic!("{name}: {e}"));
        let rev: Vec<(u32, u32)> = (0..N).rev().map(|k| (k, k)).collect();
        assert_eq!(bwd, rev, "{name}: backward scan");
    }
}

// ── #6 Read-your-own-writes ───────────────────────────────────────────────────

/// Within one transaction, a `get` after an uncommitted write sees the new value
/// (read-your-own-writes); a *separate* reader opened concurrently sees only the
/// committed baseline until the writer commits. All oracle reads are autocommit,
/// so this own-writes path is otherwise untested.
#[test]
fn read_your_own_writes() {
    let (_dir, engine) = tmp_engine::<u32, u32>();
    let decode = |o: Option<Vec<u8>>| o.map(|b| u32::from_le_bytes(b.try_into().unwrap()));

    engine.insert(&1u32, &10).unwrap(); // committed baseline

    let mut w = engine.begin();
    w.insert(&2u32, &20).unwrap(); // new key, uncommitted
    w.update(&1u32, &11).unwrap(); // change existing, uncommitted
    w.insert(&3u32, &30).unwrap();
    w.delete(&3u32).unwrap(); // insert + delete within the same txn

    // The writer sees its own uncommitted effects.
    assert_eq!(
        decode(w.get(&2u32).unwrap()),
        Some(20),
        "RYOW: own insert not visible"
    );
    assert_eq!(
        decode(w.get(&1u32).unwrap()),
        Some(11),
        "RYOW: own update not visible"
    );
    assert_eq!(
        decode(w.get(&3u32).unwrap()),
        None,
        "RYOW: own delete not applied"
    );

    // A concurrent reader (snapshot taken now) sees only the committed baseline.
    {
        let mut r = engine.begin();
        assert_eq!(
            decode(r.get(&1u32).unwrap()),
            Some(10),
            "isolation: saw uncommitted update"
        );
        assert_eq!(
            decode(r.get(&2u32).unwrap()),
            None,
            "isolation: saw uncommitted insert"
        );
        r.commit().unwrap();
    }

    w.commit().unwrap();

    // After commit, a fresh reader sees the new state.
    let mut r = engine.begin();
    assert_eq!(decode(r.get(&1u32).unwrap()), Some(11));
    assert_eq!(decode(r.get(&2u32).unwrap()), Some(20));
    assert_eq!(decode(r.get(&3u32).unwrap()), None);
    r.commit().unwrap();
}

// ── #5 Multi-write transaction atomicity ──────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A batch of writes in ONE transaction is atomic: commit ⇒ all apply, abort
    /// ⇒ none. Within the txn the ops see each other (read-your-own-writes), so
    /// per-op expectations track a `shadow` that evolves with the txn (e.g. a
    /// second insert of a key inserted earlier in the same txn must be
    /// `DuplicateKey`). After commit/abort the committed state must equal `model`.
    ///
    /// The batch operates on keys (100..116) **disjoint** from `setup` (0..16),
    /// so every key it touches is created within the txn. That deliberately
    /// avoids modifying a pre-existing committed key inside the txn — which trips
    /// the known own-delete/own-update RYOW bug pinned by
    /// `known_bug_txn_does_not_see_own_modify_of_committed_row`.
    #[test]
    fn multi_write_txn_atomic(
        setup in prop::collection::hash_map(0u32..16, any::<u32>(), 0..12),
        batch in prop::collection::vec((100u32..116, any::<u32>(), 0u8..3), 1..16),
        commit in any::<bool>(),
    ) {
        let (_dir, engine) = tmp_engine::<u32, u32>();
        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        for (k, v) in setup {
            engine.insert(&k, &v).map_err(|e| TestCaseError::fail(format!("seed {k}: {e}")))?;
            model.insert(k, v);
        }

        // shadow = model + the txn's own evolving writes (read-your-own-writes).
        let mut shadow = model.clone();
        let mut t = engine.begin();
        for (k, v, kind) in &batch {
            match kind {
                0 => {
                    let r = t.insert(k, v);
                    match shadow.entry(*k) {
                        Entry::Vacant(s) => { prop_assert!(r.is_ok(), "insert {}: {:?}", k, r); s.insert(*v); }
                        Entry::Occupied(_) => prop_assert!(
                            matches!(r, Err(EngineError::Index(IndexError::DuplicateKey))),
                            "insert dup {}: {:?}", k, r),
                    }
                }
                1 => {
                    let r = t.update(k, v);
                    match shadow.entry(*k) {
                        Entry::Occupied(mut s) => { prop_assert!(r.is_ok(), "update {}: {:?}", k, r); s.insert(*v); }
                        Entry::Vacant(_) => prop_assert!(
                            matches!(r, Err(EngineError::Index(IndexError::KeyNotFound))),
                            "update missing {}: {:?}", k, r),
                    }
                }
                _ => {
                    let r = t.delete(k);
                    match shadow.entry(*k) {
                        Entry::Occupied(s) => { prop_assert!(r.is_ok(), "delete {}: {:?}", k, r); s.remove(); }
                        Entry::Vacant(_) => prop_assert!(
                            matches!(r, Err(EngineError::Index(IndexError::KeyNotFound))),
                            "delete missing {}: {:?}", k, r),
                    }
                }
            }
        }

        if commit {
            t.commit().map_err(|e| TestCaseError::fail(format!("commit: {e}")))?;
            model = shadow; // all-or-nothing: everything applies
        } else {
            t.abort(); // all-or-nothing: nothing applies
        }

        check_invariants(&engine.index, &engine.buffer_pool).map_err(TestCaseError::fail)?;
        let fwd = scan_forward(&engine).map_err(TestCaseError::fail)?;
        let expected: Vec<(u32, u32)> = model.iter().map(|(k, v)| (*k, *v)).collect();
        prop_assert_eq!(fwd, expected, "atomicity: committed state mismatch (commit={})", commit);
    }
}

/// KNOWN BUG — pinned, not fixed (reported separately).
///
/// A transaction does NOT observe its own DELETE (or UPDATE) of a row created by
/// an earlier *committed* transaction. Standard MVCC read-your-own-writes
/// requires the deleting txn to stop seeing the row, but the own-write shortcut
/// in `Transaction::is_visible` (db-core/transaction.rs:57) only covers rows the
/// txn itself created (`rec_xmin == txn_id`). A committed row deleted by the
/// current txn (`rec_xmin = committed`, `rec_xmax = self`) falls through to the
/// snapshot path, which sees the deleter as "not committed" and so reports the
/// row as still visible. Surfaced by `multi_write_txn_atomic` (delete-then-
/// reinsert of a committed key in one txn returned `DuplicateKey`).
///
/// This test locks the CURRENT (buggy) behavior so the eventual fix
/// (`if rec_xmax == self.txn_id { return false }` in `Transaction::is_visible`)
/// makes it fail and prompts an update. Note: only *within-txn* visibility is
/// wrong — the delete still takes effect for readers after commit.
#[test]
fn known_bug_txn_does_not_see_own_modify_of_committed_row() {
    let (_dir, engine) = tmp_engine::<u32, u32>();
    let decode = |o: Option<Vec<u8>>| o.map(|b| u32::from_le_bytes(b.try_into().unwrap()));

    engine.insert(&1u32, &10).unwrap(); // committed row

    let mut t = engine.begin();
    t.delete(&1u32).unwrap(); // delete it inside a new txn

    // BUG: the txn still sees the committed row it just deleted (should be None).
    assert_eq!(
        decode(t.get(&1u32).unwrap()),
        Some(10),
        "current behavior: own delete of a committed row is not observed within the txn"
    );
    // ...and a re-insert is therefore rejected as a duplicate (should be Ok).
    assert!(
        matches!(
            t.insert(&1u32, &20),
            Err(EngineError::Index(IndexError::DuplicateKey))
        ),
        "current behavior: reinsert after own-delete returns DuplicateKey"
    );
    t.commit().unwrap();

    // Post-commit the delete DID take effect (only within-txn visibility was
    // wrong) and the reinsert never happened, so the row is gone.
    let mut r = engine.begin();
    assert_eq!(
        decode(r.get(&1u32).unwrap()),
        None,
        "post-commit: row is deleted"
    );
    r.commit().unwrap();
}

// ── #4 Bounded range scans ────────────────────────────────────────────────────

/// Build a `(start, end)` bound pair from a kind selector, covering Included /
/// Excluded / Unbounded combinations while avoiding the one degenerate case
/// (`Excluded(x)..Excluded(x)`) that `BTreeMap::range` panics on.
fn make_bounds(kind: u8, lo: u32, hi: u32) -> (Bound<u32>, Bound<u32>) {
    match kind {
        0 => (Bound::Included(lo), Bound::Excluded(hi)),
        1 => (Bound::Included(lo), Bound::Included(hi)),
        2 => (Bound::Unbounded, Bound::Excluded(hi)),
        3 => (Bound::Included(lo), Bound::Unbounded),
        4 => (Bound::Unbounded, Bound::Unbounded),
        _ if lo < hi => (Bound::Excluded(lo), Bound::Excluded(hi)),
        _ => (Bound::Included(lo), Bound::Included(hi)),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Bounded `range`/`range_backward` (Included/Excluded/Unbounded combos) match
    /// `BTreeMap::range` exactly, forward and backward. The existing scans only
    /// pass `..`, so Excluded bounds + `end_inclusive` + `duplicate_slot_bounds`
    /// edge handling are otherwise untested.
    #[test]
    fn bounded_range_matches_model(
        keys in prop::collection::hash_set(0u32..64, 0..40),
        a in 0u32..64,
        b in 0u32..64,
        kind in 0u8..6,
    ) {
        let (_dir, engine) = tmp_engine::<u32, u32>();
        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        for &k in &keys {
            let v = k.wrapping_mul(7);
            engine.insert(&k, &v).map_err(|e| TestCaseError::fail(format!("insert {k}: {e}")))?;
            model.insert(k, v);
        }
        let (lo, hi) = (a.min(b), a.max(b));
        let bounds = make_bounds(kind, lo, hi);

        let dec = |kb: Vec<u8>, vb: Vec<u8>| {
            (u32::from_le_bytes(kb.try_into().unwrap()), u32::from_le_bytes(vb.try_into().unwrap()))
        };
        let txn = engine.transaction_manager.begin();
        let got: Result<Vec<(u32, u32)>, _> = engine.index.range(bounds, &txn)
            .map(|r| r.map(|(kb, vb)| dec(kb, vb))).collect();
        let gotb: Result<Vec<(u32, u32)>, _> = engine.index.range_backward(bounds, &txn)
            .map(|r| r.map(|(kb, vb)| dec(kb, vb))).collect();
        engine.transaction_manager.mark_committed(txn.txn_id);
        let got = got.map_err(|e| TestCaseError::fail(format!("range: {e}")))?;
        let gotb = gotb.map_err(|e| TestCaseError::fail(format!("range_backward: {e}")))?;

        let expected: Vec<(u32, u32)> = model.range(bounds).map(|(k, v)| (*k, *v)).collect();
        let mut expected_rev = expected.clone();
        expected_rev.reverse();
        prop_assert_eq!(got, expected, "forward range {:?}", bounds);
        prop_assert_eq!(gotb, expected_rev, "backward range {:?}", bounds);
    }
}
