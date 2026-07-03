# FluxDB — Durability & Recovery Design (Standalone Spec)

This is the **single authoritative design document** for FluxDB's durability,
recovery, transaction-status, and vacuum subsystems. It is self-contained: no
other document is required (or should be consulted) to implement it. Older
sub-documents are superseded by this spec.

FluxDB is a PostgreSQL-inspired, **index-organized** engine: the B+Tree leaf
*is* the table (key → value, with per-version `xmin`/`xmax`). Transaction ids
are **u64** — they never wrap in any realistic lifetime, and several decisions
below lean on that.

---

## 1. Architecture: Postgres-style, redo-only, no undo

Textbook recovery (ARIES) has a **redo** pass and an **undo** pass with CLRs.
FluxDB follows PostgreSQL: keep the redo half, **discard undo entirely**.

| Concern | Textbook ARIES | FluxDB |
|---|---|---|
| Redo | repeat history, pageLSN-gated, FPI | **same** |
| Undo of losers | undo pass + CLRs | **none** |
| Transaction abort | physical undo | **CLOG status flip** |
| Reclaiming aborted/dead rows | undo | **vacuum** (MVCC + `is_vacuumable`) |
| Bounding replay | checkpoints | **same** |

"Undo" is replaced by **MVCC + CLOG + vacuum + checkpoints**. A crash leaves
uncommitted changes physically present after redo; they're invisible (their
txn is recorded Aborted in the rebuilt CLOG — see §3.3/§6.3) and reclaimed
later by vacuum. No CLRs, no undo pass. Structure modifications (splits, page
deletion) are **not transactional** — they survive whether the txn commits or
aborts.

### Locked decisions

1. **Redo granularity = hybrid.** Physiological (small) records for leaf tuple
   ops; **full-page images (FPI)** for internal-page ops, splits, new-root,
   and vacuum compaction; plus a first-dirty-after-checkpoint FPI for
   torn-write repair. (Internal pages rewrite whole sections, the split
   heuristic isn't cleanly replayable, and compaction depends on live txn
   state — FPI is the only sound form for those.)
2. **Self-describing record framing** (§4.2): `rec_len`-prefixed, multi-block,
   CRC-terminated. LSN is a **logical counter**, not a byte offset.
3. **A new top-level `engine` crate** owns the WAL, recovery, and
   checkpointing (§2).
4. **Compaction is logged as an LSN-advancing FPI**; CLOG truncation is gated
   on physical reclamation (§8.4).
5. **Durable CLOG seed rides in the Checkpoint record** (`pinned_aborted[]`),
   so WAL retention is decoupled from vacuum (§3.4, §7.3).

### Sequencing: ship pure-FPI first

The hybrid cut (decision 1) is the *end-state*, not the first milestone. The
physiological leaf path is a second replay branch plus the
slot-addressing-under-reorg correctness burden — the likeliest place to get
redo wrong. Get a correct, recoverable engine working with **FPI for every
record** first (have `Insert`/`SetXmax` carry an FPI block too), then
introduce physiological records as a measured size optimization. The framing
supports both; this is purely sequencing.

---

## 2. Crate layering

`storage` already depends on `db-core`, so `db-core` cannot hold an
`Arc<Wal>` (dependency cycle), and no layer above both exists today. Recovery,
checkpoint scheduling, and the shared `Wal` need a single owner:

```
            ┌─────────────────────────────────────────────┐
            │  engine  (NEW)                               │
            │  • owns Wal (log manager)                    │
            │  • runs recovery in its constructor          │
            │  • schedules checkpoints + vacuum            │
            │  • holds Arc<Wal> shared down into the pool  │
            └───────────────┬───────────────┬─────────────┘
                            │               │
                   ┌────────▼──────┐  ┌─────▼───────────┐
                   │   storage     │─▶│   db-core       │
                   │ buffer pool,  │  │ TransactionMgr, │
                   │ B+Tree, pages │  │ CLOG, MVCC      │
                   └───────────────┘  └─────────────────┘
```

- **`engine`** opens the DB: `DiskManager` → `Wal` → `BufferPoolManager`
  (with `Arc<Wal>` for WAL-before-page) → recovery → `TransactionManager` →
  `BTreeIndex::open`. Owns the checkpoint/vacuum loop.
- **`db-core`** exposes CLOG hooks (`mark_committed`/`mark_aborted`,
  `settled_status`) that `engine` calls after the corresponding WAL record is
  durable. It gains no dependency on `storage` or `Wal`.
- **`storage`**'s buffer pool gains an `Arc<Wal>` (or `flush_up_to` callback)
  so the single flush seam can enforce WAL-before-page, evictions included.

Rejected alternatives (recorded so the choice stays legible): moving `Wal`
down into `common`, or injecting a `common`-defined append/flush trait. A
dedicated `engine` crate is preferred — it gives startup-recovery and the
checkpoint/vacuum loop a natural home and leaves `storage → db-core` intact.

---

## 3. Transaction status: the CLOG and its convention

### 3.1 The structure

`clog: RwLock<HashMap<u64, TransactionStatus>>` with
`TransactionStatus ∈ {Active, Committed, Aborted}`, owned by
`TransactionManager`. In-memory; its durable shadow is §3.4.

### 3.2 The convention: presumed commit (and why)

**A txn id that is settled (below every live snapshot's xmin) and absent from
the CLOG is treated as COMMITTED.** This is the opposite of Postgres (whose
clog bits default to in-progress, i.e. presumed abort). It is sound for
FluxDB *because txn ids are u64 and never wrap*: "old ⇒ committed" can never
be confused by id reuse.

What the convention buys: **committed entries need no storage, ever.**
Committed entries below `global_xmin` are simply dropped from the map; the
default re-derives them. The CLOG stays small without any tuple-freezing
machinery (which Postgres needs only because its 32-bit ids wrap).

What the convention costs — the discipline, all four parts mandatory:

1. **Aborted entries are pinned**: an Aborted entry may be dropped only after
   vacuum has physically removed every tuple stamped with that id (§8.4).
   Dropping it early makes aborted garbage visible.
2. **Every consumer applies the same default** via one function (§3.5).
3. **Crash victims are explicitly marked Aborted at the end of recovery**
   (§6.3). Without this, a txn that died in flight reads as committed.
4. **The pinned aborted set survives WAL truncation** via the Checkpoint
   record (§3.4).

### 3.3 Status transitions & durability rules

- **Commit:** append `Commit` record → **fsync (`flush_up_to`)** → only then
  set `CLOG[id] = Committed` and remove from the active set. A commit must
  never be observable before its record is durable.
- **Abort:** append `Abort` record (**no fsync**) → set `CLOG[id] = Aborted`,
  remove from active set. A lost Abort record is harmless: the txn then looks
  like a crash victim and recovery marks it Aborted anyway (§6.3). *This
  optimization is sound only because §6.3 exists.*
- **Crash in flight:** no record at all. Recovery marks the id Aborted (§6.3).
- Both commit and abort hold the **status guard** (§7.2) in shared mode across
  their two steps (WAL append → map update), so a checkpoint can never
  snapshot between them.

### 3.4 Durable shadow: `pinned_aborted[]` in the Checkpoint record

The only CLOG state that must outlive WAL truncation is the set of **Aborted
ids whose tuples vacuum hasn't removed yet** (real aborts + crash victims).
Each Checkpoint record carries that set as a `u64` list (§4.3). Committed and
Active entries are never persisted: committed re-derives by default; active
sets are meaningless after a crash.

**Authority rule:** the Checkpoint's saved set is authoritative for everything
*below* the redo point; the WAL is authoritative *from the redo point
forward*. The saved set is not a cache — once WAL behind the redo point is
deleted, it is the only carrier of those aborts (crash victims never had a WAL
record to begin with).

Size: 8 bytes per pinned abort, bounded by vacuum lag. If an abort-heavy,
vacuum-starved workload ever makes Checkpoint records uncomfortably large, the
escape hatch is a separate pg_xact-style file (load-before-replay, written at
checkpoint, atomic rename) — same invariants, different carrier. Not built
until proven necessary.

### 3.5 One function: `settled_status`

All status questions go through a single method on `TransactionManager`:

```rust
fn settled_status(&self, txn_id: u64) -> Status {
    // txn_id == 0 → Committed (system/auto sentinel)
    // CLOG entry present → that status
    // absent ∧ txn_id < global_xmin → Committed   (the presumed-commit default)
    // absent otherwise → InProgress
}
```

Consumers — visibility (`Snapshot::is_committed`), vacuum (`is_vacuumable`,
both rules), the first-writer-wins conflict path (blocker status checks), and
recovery — **must not** do bare CLOG lookups. The old bug class (truncation
dropping an Aborted entry, then one call site's fallback reading "finished" as
"committed" while another strands the tuple as un-vacuumable) is eliminated
structurally, not patched per call site.

---

## 4. The WAL

### 4.1 Record set

Every record exists because a redo or vacuum step consumes it. `phys` = small
physiological record; `FPI` = block(s) carry full-page images.

| Value | Type | Form | Purpose |
|:---:|---|:---:|---|
| 0 | `Insert` | phys | insert tuple `(key, value, xmin)` at a slot |
| 1 | `SetXmax` | phys | stamp `xmax` at `(page_id, slot)` — slot-addressed, no key |
| 2 | `Commit` | phys | mark txn committed in CLOG |
| 3 | `Abort` | phys | mark txn aborted in CLOG |
| 4 | `LeafSplit` | FPI | atomic leaf split (left + right + old-neighbor prev-fix) |
| 5 | `InternalSplit` | FPI | atomic branch-page split |
| 6 | `InsertDownlink` | phys | separator+child into parent; clear child `INCOMPLETE_SPLIT` |
| 7 | `NewRoot` | FPI | new root on height increase + page-0 root update |
| 8 | `PageAllocate` | phys | advance `next_page_id` across a crash |
| 9 | `PageCompact` | FPI | vacuum repack (depends on live txn state → FPI) |
| 10 | `MarkHalfDead` | phys | step 1 of empty-page deletion |
| 11 | `UnlinkPage` | FPI | unlink an empty page from siblings + parent |
| 12 | `Checkpoint` | phys | redo point + snapshot (§4.3) |
| 13 | `Fpi` | FPI | standalone first-dirty-after-checkpoint full-page image |

Not record types:
- **Update** = `SetXmax(old)` + `Insert(new)` under one txn. Order-dependent:
  `SetXmax` carries the *pre-insert* slot; the pair replays in LSN order.
- **SetRoot** — the page-0 root update is a block inside `NewRoot` (and
  re-asserted by `Checkpoint`).
- `INCOMPLETE_SPLIT` / `HALF_DEAD` are page-header flags (reserved byte at
  offset 1), not records.
- `PageAllocate` is load-bearing only if an allocation can become durable
  before the structural FPI that names the page; otherwise
  `1 + max page_id touched` already recovers the counter and it can be
  dropped from v1.
- The third page in `LeafSplit` (the old right-neighbor's `prev_page` fix)
  is the *backward* chain, which carries **no crash invariant** — only
  rightlinks are load-bearing for search correctness. Folding it into the
  atomic FPI set keeps it tidy; a torn backward link would not corrupt search.

> **Is the standalone `Fpi` (type 13) needed?** Every page mutation already
> produces a typed record that can carry `HAS_FPI` on its block, so a separate
> `Fpi` type only earns its place for a page dirtied with **no logical
> record** — e.g. a per-tuple status-hint write that changes bytes but logs
> nothing (PostgreSQL's `XLOG_FPI_FOR_HINT`). FluxDB has no such unlogged page
> change today; type 13 is **redundant until one exists** (the Phase-8
> hint-bit idea would be exactly that consumer). Name the unlogged-write path
> or drop the type before building it.

**Scope:** v1 = `Insert`/`SetXmax`/`Commit`/`Abort`/`LeafSplit`/
`InternalSplit`/`InsertDownlink`/`NewRoot`/`PageAllocate`/`PageCompact`/`Fpi`.
v2 = `MarkHalfDead`/`UnlinkPage`/`Checkpoint`.

### 4.2 Wire format — block-reference framing

One record = common header + N block refs (each names a page and carries its
redo payload and/or an FPI) + type-specific main data + CRC. Multi-block
atomicity and FPI are first-class; the parser is written once. All integers
little-endian.

```
WAL record =
  ┌ RecordHeader (24 bytes) ────────────────────────────────────┐
  │ lsn u64 · rec_len u32 · type u8 · nblocks u8 · txn_id u64   │
  │ main_len u16                                                 │
  └──────────────────────────────────────────────────────────────┘
  ┌ BlockRef × nblocks ─────────────────────────────────────────┐
  │ page_id u64 · blk_flags u8 (bit0 HAS_FPI, bit1 HAS_DATA)     │
  │ data_len u16 · [fpi: PAGE_SIZE] · [data: data_len]           │
  └──────────────────────────────────────────────────────────────┘
  MainData [main_len] · crc u32 (over all preceding bytes)
```

Per-type payloads:

| Type | blocks | block payload(s) | main data |
|---|---|---|---|
| `Insert` | 1 leaf `D` | `slot u16, key_len u16, val_len u16, xmin u64, key, val` | — |
| `SetXmax` | 1 leaf `D` | `slot u16, xmax u64` | — |
| `Commit`/`Abort` | 0 | — | — (subject = header `txn_id`) |
| `LeafSplit` | 3 `F` | left, new right, old right-neighbor (prev fix) | — |
| `InternalSplit` | 2 `F` | left, new right | — |
| `InsertDownlink` | 2 `D` | parent: `at_index u16, sep_len u16, right_child u64, sep_key`; left child: clear flag | — |
| `NewRoot` | 2 | new root `F`; page-0 `D`: `root_page_id u64` | — |
| `PageAllocate` | 0 | — | `page_id u64` |
| `PageCompact` | 1 `F` | compacted leaf image (advances page LSN) | — |
| `MarkHalfDead` | 1 `D` | set `HALF_DEAD` | — |
| `UnlinkPage` | 2–3 | left sib: `new_rightlink`; right sib: `new_prev`; parent: `remove_index` | deleted `page_id u64` |
| `Checkpoint` | 0 | — | `redo_point lsn, next_txn_id u64, active_txns[], pinned_aborted[], vacuum_horizon u64, root_pid u64, next_page_id u64` |
| `Fpi` | 1 `F` | full page image | — |

Payloads carry `xmin`/`xmax` explicitly (not derived from header `txn_id`) so
records are self-describing and unit-testable; header `txn_id` is used for
CLOG and ownership only.

### 4.3 The Checkpoint record

Snapshots, under the status guard (§7.2): the **redo point** (min `rec_lsn`
over dirty frames, or the checkpoint's own LSN if none), `next_txn_id`, the
**active set**, the **pinned aborted set** (§3.4), `vacuum_horizon`, the root
page id, and `next_page_id`.

### 4.4 LSN allocation, FlushedLSN, torn tails

- `Wal` holds `lsn: AtomicU64`; `append` claims via `fetch_add` and returns
  the assigned LSN. On open, scan resumes the counter at `max_lsn + 1`; an
  existing-but-empty file resumes at `0`, consistent with a missing file.
- The WAL tracks **`FlushedLSN`** (highest durable LSN). `flush_up_to(lsn)`
  fsyncs and advances it (no-op when already durable). WAL-before-page and
  commit durability both compare against it.
- **Tail/corruption classification is by position, not error kind.** The CRC
  sits at a record's *end*, so a torn final record surfaces as a checksum
  mismatch, not EOF. A bad CRC / unknown type / short read **at the physical
  tail** = recoverable torn tail — truncate at the last good boundary and
  resume. The same damage **with valid records after it** = real mid-log
  corruption — refuse to open. Never skip-and-continue past damage.
- `append` validates payload shape per type and rejects malformed input
  (`InvalidRecord`); all length reads are bounded by `rec_len`.

> **⚠ DECISION PENDING — how "is anything valid after it?" is decided.** A
> bounded resync look-ahead at `rec_len` boundaries is **not reliably
> decidable** in a pure byte stream — if `rec_len` itself is corrupt there is
> no anchor to resync on. Resolve before building recovery, one of two ways:
> **(a) add a resync anchor** — a record-start magic and/or a `prev_lsn`
> back-pointer chain (PostgreSQL's `xl_prev`; not needed for redo, but exactly
> what makes tail-vs-mid-log decidable), or **(b) keep the plain byte stream
> and simplify the rule** — in a single-appender append-only log a torn write
> can only be at the tail, so: first bad CRC ⇒ truncate and resume, and any
> damage that *can* be shown to be mid-log (e.g. by file length) ⇒ **halt
> loudly, never silently truncate** (silent truncation discards committed
> transactions). Pick (a) for robustness or (b) for simplicity; do **not**
> ship the optimistic look-ahead middle ground.

### 4.5 Deferred performance work (correctness first)

In order of value once the engine is correct: **group commit**
(leader/follower fsync batching — the `TransactionManager` condvar waiters are
the wake mechanism; the ack rule `FlushedLSN >= commit LSN` is unchanged);
**WAL segmentation** (fixed-size, pre-allocated, recycled only behind the redo
point — also the point to reconsider byte-offset LSNs); **concurrent WAL
buffer** (slot reservation → lock-free `fetch_add` with hole/ready-bitmask
tracking, cache-line-padded atomics); backpressure via the bounded buffer.
Sharded WALs and replication are out of scope for a single node.

One caveat that is *not* purely performance: **without segmentation the WAL is
a single ever-growing file with no physical reclamation mechanism** — §7.3's
"delete WAL before the redo point" has nothing to delete *with*. Checkpoints
bound where replay *starts*, not the physical log size. Until segments (or
periodic file rewrite) exist, disk use grows monotonically; recovery time is
still bounded by the redo point.

---

## 5. Cross-cutting invariants

1. **WAL-before-page (WBL).** A page must not reach disk before the record
   that dirtied it is durable. Enforced at the single flush seam
   (`write_frame_to_disk`, flush *and* eviction): read the frame's page LSN,
   `wal.flush_up_to(page_lsn)`, then write. Any other page-writer must route
   through this seam.
   **No data fsync on the eviction path.** WBL requires the *log* durable
   before the page write — not the data file. The page itself must be durable
   only by the time a checkpoint advances the redo point past its changes, so
   the data-file fsync is **batched at the checkpoint** (§7.3 step 3); an
   evicted page sits in the OS page cache until then. A per-eviction fsync
   forces a device flush on every frame replacement and destroys throughput.
   *Sequencing caveat:* today `write_frame_to_disk` calls `sync_data()` on
   every write, and pre-WAL that fsync is the **only** durability the engine
   has — it may be dropped **only once** the WAL exists *and* the checkpoint
   performs the batched data fsync; otherwise an evicted dirty page can be
   lost with no redo source to replay it.
2. **Page-LSN stamping & gating.** Every mutation stamps
   `page.set_lsn(record.lsn)` at the mutation site (the only place the LSN is
   known). Redo applies a record to a block **iff `record.lsn > page.lsn`**.
   Every page-*rebuild* site — compaction and both split rebuilds — must stamp
   the **new** record's LSN, never preserve the old one: a stale LSN lets an
   already-applied record re-fire, or a stale `Insert` resurrect a vacuumed
   tuple.
3. **Commit durability.** `Commit` fsync-durable **before** the commit is
   observable in CLOG. (Distinct from WBL: a data page may legitimately reach
   disk before its txn's `Commit` — the txn is then lost on crash, which is
   correct. Aborts are never fsynced — §3.3.)
4. **One status convention.** All consumers use `settled_status` (§3.5);
   Aborted entries are pinned until vacuum has removed their tuples.
5. **Checkpoint bounds replay; the seed is authoritative below it.** Redo
   starts at the last checkpoint's redo point; CLOG = checkpoint's
   `pinned_aborted[]` seed + replay + crash-victim marking. WAL before the
   redo point is deletable, full stop (§7.3).
6. **Status-guard exclusion.** A checkpoint's snapshot moment is mutually
   exclusive with any commit/abort's two-step window (§7.2).
7. **Point lookups are version-chain-aware.** When one key's MVCC versions
   fill a leaf, the split lands *inside* the duplicate run (no key-change
   split point exists), leaving the left page with `high_key == key`. The
   "move right on `key >= high_key`" rule then routes every lookup to the
   rightmost chain page, so `get`/`update`/`delete` must walk `prev_page`
   left when a page has no visible version but starts with the key — or is
   empty (compaction may strip a chain-middle page bare while its
   `high_key`/links remain). The walk **drops its latch before each leftward
   step** (holding right while latching left inverts the split path's
   left→right order — deadlock) and revalidates the chain signature
   (`left.high_key == key`) after re-latching, restarting the descent if the
   structure moved.

---

## 6. Recovery

### 6.1 Startup sequence

The `engine` crate runs recovery in its constructor, before any client read or
write. The buffer pool must exist **before** redo (redo mutates pages through
it):

```
Engine::open(path):
  1. DiskManager::new(path)
  2. Wal::open(wal_path)                  // tail scan (§4.4), resume LSN
  3. read superblock → last checkpoint → redo_point, seed snapshot
  4. BufferPoolManager::new(disk, Arc<Wal>)
  5. seed CLOG from checkpoint.pinned_aborted[]            (§3.4)
  6. one forward scan from redo_point (§6.2):
       redo each record/block via fetch_for_redo
       apply Commit/Abort into CLOG (idempotent re-marking)
       track watermarks: max txn_id over ALL headers, max page_id touched
  7. mark crash victims Aborted (§6.3)
  8. inject watermarks:
       next_txn_id  = max(checkpoint.next_txn_id, 1 + max txn_id seen)
       next_page_id = max(checkpoint.next_page_id, 1 + max page_id touched)
     (constructors must accept recovered state — `from_recovered`/setters;
      `new` hard-codes both today and would silently discard them)
  9. TransactionManager::from_recovered(clog, next_txn_id)
 10. BTreeIndex::open(pool)               // root from recovered superblock
 11. spawn checkpoint + vacuum loop
```

No reads are served before step 10: visibility is CLOG-authoritative-first,
so the CLOG must be complete (seed + replay + crash victims) first. The redo
apply and the CLOG rebuild share the single step-6 scan.

### 6.2 The redo pass

```
for record in wal.iter_from(redo_point):
    for blk in record.blocks:
        page = fetch_for_redo(blk.page_id)      // recovery-only fetch
        if record.lsn <= page.lsn: continue     // idempotent skip
        if blk.has_fpi: page.bytes = blk.fpi    // torn-page repair / structural
        else:           apply_physiological(blk, page)
        page.set_lsn(record.lsn)
    if record.type in {Commit, Abort}: mark CLOG (idempotent)
```

- Idempotency comes from the page-LSN gate + idempotent CLOG marking; the log
  can replay twice safely.
- **`fetch_for_redo(page_id)`** is a recovery-only pool path that bypasses the
  `next_page_id` bound check and **extends the file with zeroed pages** for
  holes (`new_page` doesn't grow the file, so the log can reference pages past
  EOF; normal fetches reject them). A fresh hole has `page.lsn = 0`, so the
  first record for it always applies.
- Physiological replay is safe under reorgs because every reorg (compaction,
  split) was itself logged with a higher LSN and replays in order — that is
  what makes `(page_id, slot)` addressing stable.
- **Re-entrancy:** a crash *during* recovery replays cleanly from the same
  redo point — recovery-time page writes are WBL-ordered, and a half-extended
  file is safe to re-run.

### 6.3 CLOG completion — crash victims (CRITICAL)

After the scan: every txn id in `(checkpoint.active_txns ∪ ids seen in any
record header)` with **no `Commit`/`Abort` record** gets `CLOG[id] = Aborted`.

Under presumed commit (§3.2) this is what keeps a txn that died in flight
invisible — without it, its id is settled-and-absent after restart, i.e.
*committed*: silent corruption. These crash victims join the pinned aborted
set like any other abort: vacuum reclaims their tuples, and until then every
subsequent checkpoint carries them in `pinned_aborted[]` (they have no WAL
record, so the checkpoint is their only durable carrier — §3.4).

The same scan's header watermark also fixes txn-id reuse: an in-flight txn can
hold the highest id in the log while contributing no Commit/Abort, so
`next_txn_id` **must** derive from all record headers, never from
Commit/Abort subjects only — a reused id resurrects its surviving tuples via
the read-your-own-writes branch.

### 6.4 Page allocation recovery

`next_page_id = max(checkpoint.next_page_id, 1 + max page_id touched in
redo)`, injected after the scan (the pool otherwise fixes it to
`num_pages()`, wrong once holes exist). All-zero holes below the watermark are
left as-is (`lsn = 0`; first real record overwrites) or zeroed defensively.

### 6.5 Incomplete structure modifications

Multi-page operations are deliberately split across records; Lehman-Yao
right-links keep the tree correct between them, so **recovery needs no fix-up
pass**:

- **Split without downlink** (crash between `LeafSplit`/`InternalSplit` and
  `InsertDownlink`): the new right page is reachable via the right-link;
  searches "move right." Completion is **lazy** — the next descent crossing an
  `INCOMPLETE_SPLIT` page inserts the downlink. Option (a) is the current
  reality: the flag is maintained (set by the split rebuild, cleared by
  `insert_separator_via_stack`, checked on the insert descent), and
  completion is insert-separator-if-absent; redo of `InsertDownlink` likewise
  insert-if-absent. (Read/delete descents build no parent stack today.)

  **Right-target insert order:** when the splitting txn's own tuple belongs
  in the new right page, the separator is propagated *before* the tuple
  insert, and the re-latched target is rightlink-corrected and fit-checked —
  concurrent inserts can refill (or re-split) the new page in the unlatched
  window, so a failed fit recurses into another split rather than letting
  `InsufficientSpace` escape to the caller. Separator propagation walks the
  parent stack **without consuming it** (index cursor, not pops) so recursive
  splits and `finish_split` always see the original ancestors.

  **Latch-order rule for whatever protocol is chosen:** never hold a
  descendant latch while acquiring an ancestor outside the established
  bottom-up split-propagation order — that inverts the descend-only
  discipline the tree relies on. The safe pattern (PostgreSQL's
  `_bt_insert_parent`): release the child latch, relocate the parent via the
  right-link / parent stack, then latch the parent and
  insert-the-separator-if-absent. (The historical fragility here — the
  left-target branch holding the leaf latch across the parent fetch — is
  resolved: both `split_and_insert` branches release the leaf before
  propagation, which also lets the downlink step latch the leaf to clear its
  `INCOMPLETE_SPLIT` flag.)
- **Page deletion** (crash between `MarkHalfDead` and `UnlinkPage`): the next
  vacuum pass completes it. Recycling is horizon-gated (§8.5), so a deleted
  page is never handed out mid-recovery.

---

## 7. Checkpoints & WAL retention

### 7.1 Dirty-page tracking (recLSN)

Each frame gains `rec_lsn: Option<Lsn>`, set on the **clean→dirty edge,
driven from the mutation site** (a `mark_dirty(lsn)` where the record's LSN is
known — not from the write-guard's deref, which has no LSN). The **redo
point** = min `rec_lsn` over dirty frames at checkpoint start; if none dirty,
the checkpoint's own LSN.

**The first-dirty FPI decision is the same edge — implement one hook.** A
page-touching record attaches an FPI to its block on the first modification
of that page since the last checkpoint, for torn-write protection. The test
is **old `page.lsn <= redo_point`** (the page hasn't been full-page-imaged
since the checkpoint — PostgreSQL's `RedoRecPtr` check), and the clean→dirty
edge is the one place where the *old* `page.lsn` and the new record's LSN are
both known. So `mark_dirty(lsn)` has two outputs on a clean→dirty transition:
record `rec_lsn = lsn`, and decide the FPI by comparing the prior `page.lsn`
against the current redo point. The FPI mechanism and the recLSN mechanism
are not independent — they are two outputs of this one edge.

### 7.2 The status guard (commit/abort vs checkpoint)

Commit and abort are two steps (WAL append → CLOG/active-set update). A
checkpoint snapshotting between them loses the transaction: after a crash the
record sits *below* the redo point (unreplayed) while the snapshot still lists
the txn active → crash-victim marking would flip a durably-committed txn to
Aborted. Sharpest case: zero dirty pages, redo point = the checkpoint's own
LSN.

Fix: one `RwLock`. `tm.commit`/`tm.abort` hold it **shared** across their two
steps; the checkpoint holds it **exclusive** while choosing the redo point and
snapshotting `active_txns[]` + `pinned_aborted[]`. (This is the FluxDB
equivalent of PostgreSQL's `DELAY_CHKPT_IN_COMMIT` interlock.)

### 7.3 Checkpoint procedure (fuzzy — writers continue)

```
1. acquire status guard (exclusive):
     note redo_point; snapshot next_txn_id, active_txns[],
     pinned_aborted[], vacuum_horizon, root_pid, next_page_id
   release guard
2. append Checkpoint record; flush WAL
3. write all currently-dirty pages (WBL-honoring; don't block writers),
   then ONE batched data-file fsync for the whole set
4. atomically update the superblock checkpoint pointer
   (atomic_write_file: temp + fsync + rename + dir-fsync)
5. delete/recycle WAL strictly before redo_point
```

- Step 3's **single batched fsync** is what licenses the no-fsync eviction
  path (§5.1): it guarantees every page dirtied since the last checkpoint is
  durable before the redo point advances past its records.

- A crash before step 4 leaves the previous checkpoint authoritative; the new
  Checkpoint record is just an unreferenced (or torn-tail) record. A crash
  during step 4 is covered by the atomic write.
- Step 5 needs no vacuum condition — **`discard_point = redo_point`** — because
  the Checkpoint record durably carries every CLOG entry still load-bearing
  below it (§3.4). Replaying Commit/Abort records between `redo_point` and the
  Checkpoint record over the seed is harmless (idempotent re-marking).
- **Honest retention claim:** WAL is bounded by checkpoint cadence, *given
  checkpoints complete and every dirty frame is flushable*. A stalled
  checkpoint loop or a perpetually-pinned hot dirty frame freezes the redo
  point — log a warning when it fails to advance across consecutive
  checkpoints. (One genuinely nice property of redo-only: long-running
  transactions never pin WAL — only dirty pages do.)
- **Clean shutdown** runs a final checkpoint so normal restarts replay a
  near-empty log; this also keeps the rebuilt CLOG small.

### 7.4 Superblock / root-pointer crash-atomicity (interim, before WAL lands)

The live root-split path rewrites page 0 **in place**; a torn page-0 write =
an unopenable DB. Until `NewRoot` + redo cover it, route every page-0 update
through `atomic_write_file` (or a two-slot versioned superblock) and don't
publish the new in-memory root until page 0 is durable.

---

## 8. Vacuum & space reclamation

### 8.1 Strategy: prevent / tolerate / reclaim — never merge

No online merge/rebalance of underfull pages (the PostgreSQL choice): a merge
touches two siblings + parent at once, breaking the descend-only latch
protocol. Instead: **prevent** (pre-split dead-tuple cleanup: when a leaf is
full, run compaction on it before splitting — attacks MVCC version-churn bloat
at the moment it would split); **tolerate** (sparse pages stay; under
random keys the tree self-heals to ~69% fill); **reclaim** (in-page
compaction → empty-page deletion → file truncation; the relief valve for
monotonic-key stranding is a periodic bulk-load rebuild, not merging).

Being index-organized with u64 ids removes three heavy Postgres subsystems:
no Free Space Map (inserts are key-addressed to one leaf), no separate index
cleanup (the leaf *is* the index), and **no tuple freezing / wraparound
handling** (u64; presumed commit re-derives old committed status — §3.2).

### 8.2 In-page compaction (the core primitive — built)

`compact()` repacks a leaf, dropping every version for which `is_vacuumable`
holds, preserving `high_key`/`rightlink`/`prev_page`. Once WAL lands it is
logged as a **`PageCompact` FPI that advances the page LSN** (it depends on
live txn state, so it isn't replayable from a logical record; the
LSN-advance prevents a stale `Insert` from resurrecting a vacuumed tuple).

### 8.3 `is_vacuumable(xmin, xmax, horizon)` — via `settled_status`

A version is definitely dead iff:
1. `settled_status(xmin) == Aborted` — creator aborted; never valid. Or:
2. `settled_status(xmax) == Committed && xmax < horizon` — deleted by a
   committed txn older than every live snapshot.

`horizon = global_xmin` — the **oldest live snapshot's `xmin`** (min over
active txns of their snapshot's xmin; `next_txn_id` when none). NOT the oldest
active txn *id*: a snapshot can be arbitrarily older than its holder's id
(the holder began while older txns were in flight), and an id-based horizon
lets vacuum remove the version an old snapshot still needs — a committed key
then transiently reads as missing. `W < min(snap.xmin)` implies
`W < snap.xmax ∧ W ∉ snap.active` for every live snapshot, so only below that
bound is every deleter provably committed for all observers. To support this,
the TM's active set maps `txn_id → snapshot.xmin`. (Matches §3.2's "below
every live snapshot's xmin"; this section previously said "oldest active txn
id", and the implementation faithfully reproduced that bug.)

Routing through `settled_status` is load-bearing: with committed entries
truncated, a bare CLOG lookup on `xmax` returns false forever and the tuple
becomes invisible-yet-unvacuumable — a permanent leak.

### 8.4 Two-tier CLOG truncation

- **Committed** entries below `global_xmin`: droppable anytime — the
  presumed-commit default re-derives them (§3.2).
- **Aborted** entries: droppable only below **`vacuum_horizon`** — the
  `global_xmin` (oldest live snapshot xmin, §8.3) captured at the *start* of
  the most recent **completed** full vacuum sweep (`AtomicU64`, `fetch_max`
  published only when the sweep reaches the last leaf; partial progress
  publishes nothing). After a full
  sweep, no tuple with an aborted `xmin < vacuum_horizon` survives anywhere,
  so the entries carry no information.

Why a completed sweep suffices: records only move *right* under splits (lower
half stays in place), the sweep moves left→right following rightlinks to the
end, and the only leaf-removing reorg fires on already-empty pages — so a
dead version cannot slip behind the cursor. Nothing creates old-xid versions.

On restart `vacuum_horizon = 0` (safe: aborted entries just aren't dropped
until the first post-restart sweep completes). Truncation shrinks the
in-memory map and, transitively, the next checkpoint's `pinned_aborted[]` —
durable forgetting of an abort is the next checkpoint after its truncation.

### 8.5 Empty-page deletion & recycle (leaf-only v1)

When compaction leaves a leaf with zero live versions: `MarkHalfDead` (flag;
concurrent descents route via rightlink) → `UnlinkPage` (splice out of the
sibling chain, remove the parent downlink). **Recycle is delayed**: stamp the
deletion with the current horizon and return the page to free space only once
`global_xmin` has passed it (a concurrent scan may still be walking toward
it). Leaf-only: internal pages have no backward link, so the right-sibling
prev-fix is unimplementable for them until an internal `prev` pointer exists
(empty internals are rare — a whole subtree must empty).

Free-space management itself: prefer a **bitmap free-space page** (covered by
the ordinary FPI/redo path, so push/pop is crash-consistent for free) over an
intrusive free-list chain (which would need its own WAL record). Until it
lands, **leak** pages rather than recycle unsafely. `new_page` pops free else
bumps the high-water mark; `next_page_id` persists via the checkpoint. A run
of trailing free pages may be truncated off the file (lower priority — space
is reused internally without it).

### 8.6 Vacuum concurrency

- Vacuum holds **one exclusive leaf latch at a time**; between pages it holds
  none — no deadlock, foreground writers proceed on other pages.
- Following `rightlink` is stable under concurrent splits: a page inserted
  ahead of the cursor is simply visited later; nothing is lost.
- Empty-page deletion (§8.5) is the one place touching >1 page; it must use
  the right-link + half-dead protocol, never naive multi-latching.

### 8.7 Autovacuum (later)

Dead-tuple counter as trigger; bounded batches with a resumable cursor;
cost-based sleep between batches; stats (pages scanned, versions reclaimed).
A per-page "nothing to reclaim" bit (visibility-map analog) lets vacuum skip
clean pages — pure performance, defer.

> **Operational hazard: a long-running or idle transaction pins
> `global_xmin`.** A single long-lived or abandoned transaction holds the
> horizon back, which stalls **vacuum** (nothing newer becomes reclaimable),
> **CLOG truncation** (both tiers trail the horizon), and therefore the
> **`pinned_aborted[]` set grows** in every checkpoint. Note what it does
> *not* pin under this design: **WAL retention** — the redo point is min
> recLSN over dirty frames, independent of `global_xmin` (this is a benefit
> of the §3.4 design; under WAL-derived CLOG it would have pinned the log
> too). Mitigations a production engine needs: surface the oldest-active-txn
> age as a stat at minimum; optionally an old-snapshot age threshold that
> cancels offenders.

---

## 9. Testing the crash paths

Unit tests cannot find a missing fsync, a wrong LSN gate, or a checkpoint
race. The recovery subsystem is signed off only with a **crash-injection
harness**:

- A failpoint `DiskManager` wrapper: deterministic kill after N writes/fsyncs;
  torn-write injection (truncate a page or WAL record mid-write).
- Driver loop: run workload → kill at point k → reopen → verify:
  every acknowledged-committed txn's data visible; no
  unacknowledged/uncommitted txn's data visible; tree walkable end-to-end;
  a **second crash during recovery** replays cleanly from the same redo point.
- First consumers: torn-tail classification (§4.4), WBL ordering (§5.1), redo
  idempotency (§5.2), crash-victim marking (§6.3), the status guard (§7.2),
  checkpoint mid-write safety (§7.3).

Key unit-level tests alongside: vacuum removes aborted-xmin versions; keeps
versions visible to active snapshots; committed-entry truncation keeps old
committed data visible; aborted-entry truncation after a full sweep keeps
aborted data invisible; recycled pages not reused while a reader is active;
pre-split cleanup avoids splits under version churn;
`recovery_reuses_no_txn_id` (an in-flight txn that wrote tuples but never
settled must not have its id reissued after restart — the §6.3 watermark);
`recovery_torn_tail_opens` (a DB whose last record is torn opens and
recovers; mid-log damage halts — §4.4).

---

## 10. Current reality & build order

Recon findings (what exists today): the WAL is dead code with only `Put`/
`Delete` record types and caller-supplied LSNs; commit/abort touch no WAL and
fsync nothing; the CLOG is in-memory only with `truncate_clog` dropping *both*
statuses (the visible-aborted-data bug — dormant, no callers); `set_lsn` only
preserves LSNs; compaction re-stamps the old LSN; `new_page` doesn't grow the
file; the root pointer is in-memory only and page 0 is rewritten in place;
`next_txn_id` resets to 1 on every start. None of this spec is wired.

Build order (each step one issue):

1. **`engine` crate** + `Engine::open` skeleton; `Arc<Wal>` into the pool (§2).
2. **Log manager**: framing + record set (§4.1–4.2), LSN allocator +
   `FlushedLSN` + `flush_up_to`, torn-tail classification (§4.4).
3. **Write-ahead protocol**: page-LSN stamping at every mutation site; WBL at
   the flush seam; emit records from index mutations — pure-FPI first (§1).
4. **Recovery**: redo pass + CLOG rebuild as seed + replay + crash-victim
   marking (§6) — recovery from a checkpoint-less log first, with the seed
   abstracted so checkpoints slot in without a rewrite. **Crash-injection
   harness lands here** (§9) and gates everything after it.
5. **Checkpointing**: recLSN tracking, the status guard, the Checkpoint record
   with `pinned_aborted[]`, atomic superblock pointer, WAL discard at the redo
   point, shutdown checkpoint (§7).
6. **Vacuum hardening**: `settled_status` everywhere, compact-as-FPI,
   `vacuum_horizon`, two-tier truncation, recycle horizon (§8).
7. **Isolation & robustness**: lock-manager enforcement; lock-poisoning
   strategy.
8. **Performance**: group commit, segmentation, concurrent WAL buffer (§4.5);
   insert fastpath, prefetch, bulk-load rebuild, overflow pages.

---

## 11. Appendix: PostgreSQL reference mapping

For implementers familiar with PostgreSQL internals. Structural records mirror
`nbtree` (`src/include/access/nbtxlog.h`; replay in `nbtxlog.c::btree_redo`):

- `SPLIT_L`/`SPLIT_R` → `LeafSplit`/`InternalSplit`
- `INSERT_UPPER` → `InsertDownlink`
- `NEWROOT` → `NewRoot`
- `VACUUM`/`DELETE` → `PageCompact`
- `MARK_PAGE_HALFDEAD`/`UNLINK_PAGE` → `MarkHalfDead`/`UnlinkPage`
- `REUSE_PAGE` → page recycle

Transaction markers are a separate resource manager in PostgreSQL
(`XLOG_XACT_COMMIT`/`_ABORT`, `access/xact.h`); checkpoints in
`access/xlog.h`. FluxDB's MVCC `Insert`/`SetXmax` are conceptually the
**heap** records (`XLOG_HEAP_INSERT`/`_DELETE`) folded onto a B+Tree leaf,
because FluxDB is index-organized. Not adopted: `DEDUP` / `INSERT_POST` /
`META_CLEANUP`. FluxDB's LSN is a logical counter where PostgreSQL's is the
WAL byte offset (`pg_lsn`) — see the reconsideration note in §4.5. The
checkpoint status guard (§7.2) is `DELAY_CHKPT_IN_COMMIT`; the durable CLOG
seed (§3.4) replaces pg_xact, which FluxDB doesn't need thanks to u64 ids +
presumed commit.
