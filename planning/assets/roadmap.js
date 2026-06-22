/* FluxDB roadmap — authored content. Status reflects the codebase.
   Each phase has overview fields plus a `detail` (its own page):
     detail: [ { h: 'Heading', body: [ 'paragraph', ['bullet','bullet'] ] } ]
   Inline `code` is supported everywhere. */
window.FLUXDB = {
  hero: {
    eyebrow: 'FluxDB · storage engine',
    title: 'A transactional storage engine, from scratch.',
    lead: 'FluxDB brings PostgreSQL-style concurrency to a single-process, SQLite-shaped engine — ' +
          'written in Rust, with durability and crash recovery built in. This is where it stands and where it’s going.',
    today: 'Today it’s a working transactional engine: open it, run concurrent transactions — insert, read, ' +
           'update, delete, range-scan — then close and reopen to find committed data intact, even after a simulated crash.',
  },

  spec: [
    ['Type', 'Embedded · in-process'],
    ['Model', 'MVCC · snapshot isolation'],
    ['Workload', 'OLTP — small reads &amp; writes'],
    ['Language', 'Rust · 8-crate workspace'],
    ['Stage', '<span class="dot ok"></span>Crash recovery shipped — checkpointing next'],
  ],

  legend: [
    ['shipped', 'Shipped'],
    ['progress', 'In progress'],
    ['planned', 'Planned'],
    ['future', 'Future'],
  ],

  phases: [
    {
      id: 'foundation', num: '01', title: 'Storage foundation', status: 'shipped',
      summary: 'Fixed-size pages moved between memory and disk through a buffer pool, over a crash-safe write path that everything else builds on.',
      points: [
        { text: 'Data lives in 4 KB pages; each carries a `CRC32` checksum verified every time it is read back from disk.' },
        { text: 'A buffer pool of eight independent shards caches hot pages and evicts cold ones with a clock (second-chance) policy.' },
        { text: 'Writes are made durable with `fsync`, and the root pointer is updated atomically so a crash can never leave it half-written.' },
        { text: 'Every page records the log position of its last change and cannot reach disk until that log record is durable — the write-ahead invariant.' },
      ],
      detail: [
        { h: 'Pages and the disk manager', body: [
          'The unit of storage is a fixed 4 KB page, addressed by a numeric page id. The disk manager reads and writes pages at exact byte offsets, and the file grows simply by writing past its current end. Durability is explicit: data is forced to stable storage with `fsync`, and a separate path syncs both a file and its parent directory so a freshly created or renamed file is itself durable.',
          'Every page is checksummed. A `CRC32` is written into the page on its way out and verified on the way back in, so silent corruption or a torn write is caught at read time rather than surfacing as a confusing logic error later.',
        ]},
        { h: 'The buffer pool', body: [
          'Hot pages are cached in a fixed pool of frames. To keep concurrent access from serialising on one lock, the pool is split into eight independent shards selected by the low bits of the page id — each shard has its own frames and its own replacement state.',
          'When a shard is full it evicts with a clock (second-chance) policy: a lightweight approximation of least-recently-used that needs only a single reference bit per frame. Pages are pinned while in use and marked dirty when modified, and concurrent attempts to load the same page coordinate through a condition variable so a page is read from disk exactly once.',
        ]},
        { h: 'The write-ahead invariant', body: [
          'The foundation’s most important rule connects pages to the log: a page may not be written to disk before the log record describing its change is durable. Every page carries the sequence number of its most recent change, and the eviction path flushes the log up to that number before writing the page out.',
          'This is what makes recovery possible — on disk, the log is always at least as current as the data pages, never behind them.',
        ]},
        { h: 'The superblock', body: [
          'Page zero is a superblock holding a magic marker, a format version, and the page id of the index root. It is updated atomically — written to a temporary file, synced, and renamed — so a crash mid-update leaves the previous, consistent superblock in place rather than a half-written pointer.',
        ]},
      ],
    },
    {
      id: 'index', num: '02', title: 'Ordered index', status: 'shipped',
      summary: 'An ordered key/value index for point lookups and range scans that stays correct under concurrent readers and writers.',
      points: [
        { text: 'A Lehman-Yao B-link tree: lookups take only shared latches, and sibling right-links keep a search correct even while a node is mid-split.' },
        { text: 'Leaf and internal splits propagate up the tree; a split interrupted partway is finished automatically by the next traversal that crosses it.' },
        { text: 'Range scans follow the leaf right-links and return only the versions visible to the reader.' },
      ],
      detail: [
        { h: 'A B-link tree', body: [
          'The index is a B+Tree in the Lehman-Yao style. Alongside the usual parent-to-child pointers, every node keeps a right-link to its immediate sibling and a copy of its highest key. Those two additions are what let the tree be searched safely while it is also being modified.',
          'A search descends taking only shared (read) latches. If it lands on a node whose high key shows the target has since moved right — because the node split after the search read its parent — the search simply follows the right-link to the correct sibling instead of restarting from the top.',
        ]},
        { h: 'Latching and splits', body: [
          'Reads never take exclusive latches; an exclusive latch is taken only on the specific leaf being mutated. When an insert overflows a leaf it splits, and the new separator key is propagated to the parent through a stack of ancestors collected on the way down. Internal nodes split the same way, recursively, up to (and including) a new root when needed.',
          'A split is two steps — create the sibling, then post the separator upward — so a crash or a racing thread can observe the in-between state. The page is marked with an incomplete-split flag during that window; the next traversal that crosses it finishes the posting itself, idempotently. Searches remain correct throughout because the right-link already connects the two halves.',
        ]},
        { h: 'Versions and scans', body: [
          'Because the engine is multi-version, a single key can have several row versions live at once. The leaf keeps a key’s versions contiguous and on the same page, so visibility checks and conflict checks walk a short local run rather than chasing pointers across the tree.',
          'Range scans walk the leaves left to right through their right-links, returning only the versions visible to the requesting transaction — so a scan sees a consistent slice of the data even while other transactions write.',
        ]},
      ],
    },
    {
      id: 'mvcc', num: '03', title: 'Transactions & MVCC', status: 'shipped',
      summary: 'Multi-version concurrency control: many transactions read and write at once, each on a stable snapshot of the data.',
      points: [
        { text: 'Every row version is tagged with the transaction that created it and the one that deleted it; visibility is decided at read time against the reader’s snapshot.' },
        { text: 'Readers never block writers and writers never block readers — each transaction works from its own consistent view.' },
        { text: 'When two transactions write the same row the first to commit wins; a wait-die rule keeps competing writers from deadlocking.' },
        { text: 'Commit status lives in a compact commit log, with older transactions presumed committed so it stays small.' },
      ],
      detail: [
        { h: 'Snapshots and visibility', body: [
          'Each row version records two transaction ids: the one that created it and, once deleted, the one that removed it. When a transaction begins it takes a snapshot — the next transaction id, plus the set of transactions still in flight at that instant. That snapshot is the lens through which every read is interpreted.',
          'A version is visible to a reader when its creator had committed as of the snapshot and its deleter had not. Crucially, the snapshot is authoritative over the live commit log: a transaction that commits after the snapshot was taken stays invisible to it, so reads are repeatable for the life of the transaction. Taking the snapshot is carefully ordered — the in-flight set is locked before the next id is read — so a transaction starting concurrently can never slip through the gap.',
        ]},
        { h: 'Write conflicts and deadlock', body: [
          'Readers and writers never block each other; the only contention is writer-versus-writer on the same row. There the rule is first-writer-wins: the second writer either aborts immediately or waits for the first to settle and then retries against a fresh version.',
          'To keep two waiting writers from deadlocking, waits follow a wait-die ordering by age: an older transaction may wait for a younger one, but a younger transaction asked to wait for an older one dies and retries instead. Because waits only ever go one direction in age, a cycle is impossible. This, together with the snapshots and the page latches, is the whole of the concurrency control — there is no separate lock manager.',
        ]},
        { h: 'The commit log', body: [
          'Transaction outcomes are tracked in a commit log mapping a transaction id to committed, aborted, or in-flight. It is kept compact by a presumed-commit convention: a transaction old enough that no live snapshot can still be examining it, with no recorded outcome, is taken to have committed. Only in-flight transactions and the comparatively rare aborts need explicit entries.',
        ]},
      ],
    },
    {
      id: 'wal', num: '04', title: 'Write-ahead log', status: 'shipped',
      summary: 'Every change is recorded in a log before its page is written, so an acknowledged commit is never lost.',
      points: [
        { text: 'Log records are self-describing and checksummed, each stamped with a strictly increasing sequence number (`LSN`).' },
        { text: 'A commit is not acknowledged until its log record has been flushed to disk through `fsync`.' },
        { text: 'Full-page images guard against torn writes, and a log tail left incomplete by a crash is detected and trimmed on the next open.' },
      ],
      detail: [
        { h: 'Record format', body: [
          'The log is an append-only stream of self-describing records. Each record carries a sequence number (its `LSN`), a type, the transaction that produced it, one or more block references naming the pages it touches, the change payload, and a trailing `CRC32` over the whole record. A record may carry either a compact description of a change or a full-page image, and a flag says which.',
          'Each kind of structural and data change has its own record: inserting a version, marking a version deleted, the two halves of a split, posting a downlink, installing a new root, compacting a page, and committing or aborting a transaction. These are emitted at the matching points in the index as changes happen.',
        ]},
        { h: 'Durability', body: [
          'Sequence numbers are handed out from a single counter that resumes after the highest number seen on restart. The log tracks how far it has been flushed, and a flush forces the buffered records to disk with `fsync` on both the log file and its directory. A commit is only acknowledged once its record is durable; aborts deliberately are not forced, because a lost abort is indistinguishable from a crash and recovery handles it either way.',
        ]},
        { h: 'Torn writes and torn tails', body: [
          'Full-page images protect pages that would be vulnerable to a torn write — the first time a page is dirtied after a checkpoint, its whole image is logged so recovery can restore it wholesale regardless of how the in-place write was interrupted.',
          'A crash can also leave the final record half-written. Because the checksum sits at the end of each record, a torn final record reads as a checksum failure at the very tail with nothing valid after it; on open that tail is truncated and the engine continues. Corruption in the middle of the log — damage with valid records on both sides — is treated as a real fault and refuses to open.',
        ]},
      ],
    },
    {
      id: 'recovery', num: '05', title: 'Crash recovery', status: 'shipped',
      summary: 'On restart the engine replays the log to rebuild the exact state that was committed before the crash.',
      points: [
        { text: 'Replay is redo-only and idempotent — each record is skipped if the page already reflects it, so recovery is safe to run more than once.' },
        { text: 'The commit log is rebuilt from the replay; a transaction that was writing but never committed is marked aborted, so its changes stay invisible.' },
        { text: 'Pages the log references beyond the end of the file are materialized on the fly during replay.' },
      ],
      note: 'Proven end-to-end for the reopen-after-commit path; broader crash-point coverage is part of Verification below.',
      detail: [
        { h: 'The startup sequence', body: [
          'Recovery runs once, before the engine serves any query. It opens the log (repairing a torn tail if present), replays the records forward, reconstructs the commit log, restores the transaction and page id counters, and persists the rebuilt pages. Only then does the index open and accept work.',
          'It is redo-only: there is no undo pass. The job of undo — hiding the effects of transactions that didn’t commit — is done instead by MVCC and the commit log, by marking those transactions aborted so their versions are simply never visible.',
        ]},
        { h: 'The redo pass', body: [
          'Each record is applied to the page it names, but only if the page is not already at or beyond that record’s sequence number. That single gate makes replay idempotent: re-applying a change is a no-op, so recovery can itself be interrupted and re-run from the start without harm.',
          'A full-page image is restored by copying it over the page and then stamping the record’s sequence number on top, since the captured image carries a stale one. A compact record is re-applied at its recorded slot. A record may also name a page beyond the current end of the file — a split’s new sibling, or a new root — so replay materializes those pages as it reaches them.',
        ]},
        { h: 'Crash victims', body: [
          'While replaying, commit and abort records drive the rebuilt commit log directly. Any transaction that appears in the log but has neither committed nor aborted by the end is a crash victim and is marked aborted. Without that step the presumed-commit rule would later treat its half-finished writes as committed — so this is the line between a clean recovery and silent corruption.',
          'Incomplete splits need no special handling: the right-links keep search correct, and the same self-healing that runs in normal operation finishes them on the next traversal.',
        ]},
      ],
    },
    {
      id: 'checkpointing', num: '06', title: 'Checkpointing & log retention', status: 'planned',
      summary: 'Bound how much log a restart has to replay, and let the engine discard log it no longer needs.',
      points: [
        { text: 'Each dirty page will remember the log position that first dirtied it; the oldest of those marks where replay can safely begin.' },
        { text: 'Periodic checkpoints will record that starting point and the set of live transactions without pausing ongoing writes.' },
        { text: 'A short guard around commit will keep a checkpoint from mistaking an in-flight commit for a crash victim.' },
        { text: 'Once a checkpoint is durable, older log can be reclaimed and the per-eviction disk sync can collapse into a single batched flush.' },
      ],
      detail: [
        { h: 'Why it comes next', body: [
          'Today recovery replays the entire log from the beginning, and the log is never discarded. A checkpoint fixes both: it marks a point the log can safely be replayed from, and once durable it lets everything older be reclaimed. It is also the gate for two efficiency wins — collapsing the per-eviction disk sync into one batched flush, and letting the set of aborted transactions be forgotten durably rather than re-derived every restart.',
        ]},
        { h: 'Where replay can begin', body: [
          'Every dirty page will record the log position that first dirtied it. The oldest such position across all dirty pages is the earliest point replay could need to start from — anything before it is already reflected on disk. That same moment a page goes from clean to dirty is also when the engine decides whether to log a full-page image of it, so the two are handled by a single hook driven from the mutation site, where the log position is actually known.',
        ]},
        { h: 'A consistent snapshot', body: [
          'A checkpoint records the replay-start position together with the set of live transactions, the transaction and page id counters, the root pointer, and the durable set of aborted transactions. Committing is itself two steps — write the commit record, then update the commit log — and a checkpoint that snapshotted between them could mistake a just-committed transaction for a crash victim. A light guard prevents that: commit and abort hold it briefly while they cross those two steps, and the checkpoint takes its snapshot only when none is mid-flight.',
        ]},
        { h: 'The procedure', body: [
          'A checkpoint is fuzzy — writers keep running throughout. It takes its snapshot under the guard, writes and flushes a checkpoint record, writes out the currently dirty pages followed by a single batched sync, atomically updates the superblock to point at the new checkpoint, and finally reclaims log older than the replay-start position.',
          'Recovery then seeds itself from the most recent durable checkpoint — starting replay at its recorded position and priming the commit log from it — instead of scanning from the beginning. The honest bound on log growth is "as fast as checkpoints complete and dirty pages can be flushed", so a stalled checkpoint or a perpetually hot page is surfaced as a warning rather than silently letting the log grow. A checkpointer runs on a background thread, with a final checkpoint on clean shutdown so an ordinary restart replays almost nothing.',
        ]},
      ],
    },
    {
      id: 'reclamation', num: '07', title: 'Space reclamation', status: 'progress',
      summary: 'Reclaim space from dead row versions while the tree stays online and queryable.',
      points: [
        { status: 'shipped', text: 'In-page compaction already removes versions no live snapshot can see, including a cleanup pass before a page would otherwise split.' },
        { status: 'planned', text: 'A background sweep will visit leaves on a schedule and compact them, following right-links so it never blocks foreground work.' },
        { status: 'planned', text: 'Pages left completely empty will be unlinked from the tree and recycled once no snapshot can still reach them.' },
        { status: 'planned', text: 'The commit log will be trimmed as the oldest active snapshot advances, keeping records exactly as long as they are needed.' },
      ],
      detail: [
        { h: 'Prevent, tolerate, reclaim — never merge', body: [
          'Multi-version storage accumulates dead row versions; reclaiming them must not take the tree offline or fight the descend-only latch protocol. So underfull pages are never merged with their siblings (that would touch three pages at once and break the protocol). Instead dead space is prevented where cheap, tolerated where harmless, and reclaimed in place where it counts.',
          'Being index-organized with stable numeric page ids removes several subsystems a general-purpose engine needs: there is no separate free-space map to maintain, no separate index to clean, and no transaction-id wraparound to freeze against.',
        ]},
        { h: 'In-page compaction — already running', body: [
          'The core primitive exists today: a page can be compacted to drop versions that no live snapshot can see, preserving its key order and its right-link. A full leaf is compacted before it would split, often avoiding the split entirely. The compaction is logged as a full-page image that advances the page’s sequence number, which also stops a stale older record from resurrecting a reclaimed version during recovery.',
        ]},
        { h: 'The sweep and the predicate', body: [
          'A background sweep will walk the leaves through their right-links and compact each in turn, holding only one leaf latch at a time so foreground work proceeds everywhere else. Following right-links keeps the sweep correct under concurrent splits — a page inserted ahead of the cursor is simply visited later.',
          'Whether a version is reclaimable routes through a single status function rather than a raw commit-log lookup: a version is dead when its creator aborted, or when its deleter committed before the oldest live snapshot. Centralising that question is what keeps reclamation, visibility, and log trimming from ever disagreeing.',
        ]},
        { h: 'Empty pages and trimming', body: [
          'A page emptied by compaction will be marked half-dead and then unlinked from the sibling chain and its parent, with concurrent searches routing around it via the right-link. Recycling is delayed until no snapshot can still reach the page; until a crash-safe free-space map lands, an emptied page is left in place rather than reused unsafely.',
          'In parallel, the commit log is trimmed in two tiers as the oldest snapshot advances — committed entries can go as soon as they are presumed, aborted entries only once a full sweep has cleared the versions that depended on them. A note for operators: a single long-running or idle transaction pins the oldest snapshot and stalls all reclamation, so that age is worth surfacing.',
        ]},
      ],
    },
    {
      id: 'verification', num: '08', title: 'Verification & hardening', status: 'progress',
      summary: 'Prove the engine correct under random workloads, concurrency, and crashes — not just hand-written examples.',
      points: [
        { status: 'shipped', text: 'Unit and integration tests cover the index, the buffer pool, MVCC visibility, and reopen-after-commit.' },
        { status: 'planned', text: 'A model-based suite will replay random operation sequences against a reference map and walk the tree to check its structural invariants.' },
        { status: 'planned', text: 'A crash-injection harness will stop the engine at chosen points, reopen it, and confirm every committed transaction survived and no uncommitted one is visible.' },
        { status: 'planned', text: 'Synchronization-sensitive paths will be checked with deterministic interleavings and exhaustive interleaving search.' },
      ],
      note: 'FluxDB deliberately has no traditional lock manager — snapshots, first-writer-wins, and page latches handle concurrency. Full serializability, if ever needed, would come from dependency tracking, not locks.',
      detail: [
        { h: 'Property-based testing', body: [
          'Hand-written example tests catch the cases their author thought of; the combinatorial bugs in a B+Tree live between those cases. A model-based suite will generate long random sequences of inserts, deletes, updates, and reads and replay them against both the real index and a reference ordered map, asserting the two agree on every answer — including which operations are expected to fail. Each operation runs in its own committed transaction so delete-then-reinsert and version chains have well-defined meaning.',
          'A reference map only checks answers, so a second pass walks the actual pages and asserts the engine’s real invariants: every root-to-leaf path is the same length, keys stay within the bounds their separators imply, a key’s versions are grouped together, the chain of right-links visits exactly the leaves reachable from the top with none left half-split, and every page is reachable. The same walker is reused on a recovered tree.',
        ]},
        { h: 'Crash testing', body: [
          'Recovery is only trustworthy once it is exercised at many crash points, not one. A harness will do this in-process and deterministically: run a workload, simulate a crash by dropping the engine so unflushed pages vanish while the durable log remains, reopen — which runs recovery — and assert the outcome. After each reopen it checks three things: committed data reads back and uncommitted data does not, the recovered tree passes the structural walker, and a second reopen changes nothing (replay is idempotent).',
          'Crashing partway through an operation is simulated by truncating the log at a record boundary — for example after a split is logged but before its downlink — to confirm the right-link keeps search working and the next insert completes the split. The catalogue covers single inserts, splits, deletes, updates, crash victims, mid-split crashes, double recovery, and torn tails.',
        ]},
        { h: 'Concurrency and the lock-manager decision', body: [
          'Two kinds of concurrency bug need two tools. Isolation anomalies depend only on snapshot and commit-log state, not thread timing, so they can be replayed deterministically — one test per anomaly, asserting snapshot isolation prevents the ones it should and documenting the write-skew it does not. Genuine data races in the latches, the buffer pool, and the log buffer need exhaustive interleaving search on small cases. A separate pass hardens the lock-poisoning behaviour on the durability path so one panicked operation cannot cascade into a dead engine.',
          'One decision is recorded deliberately here: a traditional lock manager is not planned. For a multi-version engine, snapshots, first-writer-wins, and short-lived page latches already cover concurrency; a transaction-scoped lock manager would be redundant. If full serializability is ever wanted, it would come from tracking read/write dependencies on top of MVCC, not from locks.',
        ]},
      ],
    },
    {
      id: 'performance', num: '09', title: 'Performance', status: 'planned',
      summary: 'Make improvements that are measured, not guessed — establish baselines first, then target the real bottlenecks.',
      points: [
        { text: 'Lightweight counters — buffer-pool hit rate, sync frequency — and a benchmark suite will set honest baselines before any tuning.' },
        { text: 'Group commit will batch disk syncs across concurrent committers, and the log buffer will move to a lock-free design to lift the single-writer ceiling.' },
        { text: 'Cache-friendly page layout and allocation cleanups follow, each change validated against the baseline it is meant to beat.' },
      ],
      detail: [
        { h: 'Measure before tuning', body: [
          'Cache and access-pattern work is exactly where intuition misleads, so measurement comes first as a stage, not an afterthought. Lightweight always-on counters answer which component is hot — a low buffer-pool hit rate points at eviction churn, a high sync-to-commit ratio points at the commit path — and per-component spans answer where the latency in a single operation went.',
          'A benchmark suite then makes "faster" a number rather than a memory. Its central discipline is to separate CPU cost from disk-sync cost: a commit’s `fsync` is measured in milliseconds and would otherwise drown every sub-microsecond signal, so CPU-bound and durability-bound workloads are benchmarked apart, against saved baselines, with the build configured the way it ships.',
        ]},
        { h: 'The scaling bottleneck', body: [
          'The dominant write cost is the commit sync, and the single biggest structural limit is that every log append and every flush serialises on one lock. The fix that changes the scaling story is group commit — batching the disk syncs of many concurrent committers into one — paired with a log buffer that reserves space without a global lock. This is the one performance item worth doing before the smaller wins, because it is the only one that changes behaviour at high core counts.',
        ]},
        { h: 'The hot path', body: [
          'On the single-thread side, the index search is the hot loop, and its main cost is chasing pointers across a page for each comparison. Storing a few leading key bytes inline in the slot directory lets most comparisons stay within a dense, cache-friendly array and only touch the full record on a tie. Smaller allocation cleanups — reusing scratch buffers on the flush and split paths instead of allocating per operation — follow behind the same benchmark gate, each change kept only if the measurement says it earned its place.',
        ]},
      ],
    },
    {
      id: 'query', num: '10', title: 'Query layer', status: 'future',
      summary: 'Grow from a storage engine into a database you can query in SQL.',
      points: [
        { text: 'A parser, a schema catalog, and an executor will sit on top of the transactional core.' },
        { text: 'The programmatic transaction API the engine exposes today is the same foundation that layer would build on.' },
      ],
      detail: [
        { h: 'From engine to database', body: [
          'Everything so far is the storage engine — the part that stores, indexes, and protects data transactionally. A database adds a language on top: a parser to turn SQL into an internal form, a schema catalog to describe tables and types, and an executor to run queries against the index.',
          'This is a large, separate effort and deliberately last. The point of building durability and concurrency first is that they are exactly the foundation a query layer needs underneath it — the programmatic transaction API the engine exposes today is the same surface that layer would be written against.',
        ]},
      ],
    },
  ],
};

// Point-wise implementation plans, keyed by phase id. For shipped phases this is
// the build order it came together in; for the rest, a rough get-started guide
// drawn from the design notes. Each step: { s: lead, d: detail }.
window.FLUXDB.plans = {
  foundation: [
    { s: 'Disk manager', d: 'Read and write 4 KB pages at exact byte offsets; the file grows by writing past its end. Expose `fsync`, a combined file-and-directory sync, and an atomic whole-file write (temp → sync → rename).' },
    { s: 'Page checksums', d: 'Stamp a `CRC32` over each page on the way out and verify it on the way in, so torn writes and bit-rot surface at read time.' },
    { s: 'Sharded buffer pool', d: 'Select a shard by `page_id & mask`; give each shard its own frame table and a clock replacer with a per-frame reference bit. Pin while in use, set a dirty bit on mutation.' },
    { s: 'Load coordination', d: 'Guard the load of a not-yet-resident page with a condition variable so concurrent fetches read it from disk exactly once.' },
    { s: 'Superblock', d: 'Reserve page 0 for a magic marker, format version, and the root page id; route every update through the atomic write path.' },
    { s: 'Write-ahead gate', d: 'Stamp each page with the sequence number of its last change, and flush the log up to that number before the eviction path writes the page out.' },
  ],
  index: [
    { s: 'Page layouts', d: 'Lay out leaf and internal nodes with a slot directory, and add a right-link and a stored high key to every node.' },
    { s: 'Search with correction', d: 'Descend taking shared latches only; if a node’s high key shows the target moved right, follow the right-link instead of restarting.' },
    { s: 'Single-leaf operations', d: 'Implement insert / get / delete / update against one leaf, writing the MVCC version fields per slot and keeping a key’s versions contiguous.' },
    { s: 'Leaf split', d: 'Split on a key boundary, build both halves, and post the separator to the parent through an ancestor stack collected on the way down.' },
    { s: 'Internal split & new root', d: 'Propagate separators recursively, growing a new root when the split reaches the top.' },
    { s: 'Self-heal & scans', d: 'Mark a node mid-split with the incomplete-split flag and finish the posting on the next traversal; iterate range scans across the right-links, visibility-filtered.' },
  ],
  mvcc: [
    { s: 'Transaction manager', d: 'Hand out monotonic transaction ids; on begin, lock the active set before reading the next id so a concurrent start cannot be missed.' },
    { s: 'Snapshots', d: 'Capture xmin, xmax, and the active set; make the snapshot authoritative over the live commit log so reads stay repeatable after others commit.' },
    { s: 'Visibility', d: 'Decide a version visible when its creator committed as of the snapshot and its deleter had not, read straight from the tuple’s xmin/xmax.' },
    { s: 'Write conflicts', d: 'In the index, enforce first-writer-wins on a contended row — return a write-conflict or a wait, never a lost update.' },
    { s: 'Wait-Die', d: 'Order waits by age: an older transaction may wait, a younger one asked to wait dies and retries, so no cycle can form.' },
    { s: 'Commit log', d: 'Track outcomes in a compact map with the presumed-commit rule; add a read-only fast path for transactions that wrote nothing.' },
  ],
  wal: [
    { s: 'Record framing', d: 'Define a header (sequence number, type, block count, transaction, payload length), block references with an FPI flag, and a trailing `CRC32`.' },
    { s: 'Sequence numbers', d: 'Allocate from one counter that resumes at max-seen-plus-one on open, and track how far the log has been flushed.' },
    { s: 'Durable flush', d: 'Implement flush-up-to as `fsync` on both the log file and its directory; make commit wait on it before acknowledging.' },
    { s: 'Emit at mutation sites', d: 'Log each insert, delete-mark, split, downlink, new root, and compaction where it happens, and stamp the affected page’s sequence number.' },
    { s: 'Full-page images', d: 'Capture a whole-page image the first time a page is dirtied after a checkpoint, for torn-write protection.' },
    { s: 'Torn-tail handling', d: 'On open, truncate a torn final record (a checksum failure at the very tail) and resume; refuse to open on corruption mid-log.' },
  ],
  recovery: [
    { s: 'Scan & rebuild only', d: 'Stand up the recovery manager that iterates the log and rebuilds the commit log first — no page changes yet — to validate the scan and crash-victim logic.' },
    { s: 'Materialize missing pages', d: 'Add an ensure-page primitive that zero-extends the file for any page id the log references beyond the current end.' },
    { s: 'Full-page-image redo', d: 'For split, new-root, and compaction records, copy the image onto the page, restamp the record’s sequence number, and gate on the page’s current number for idempotence.' },
    { s: 'Compact redo', d: 'Re-apply insert and delete-mark records at their recorded slot, and make the downlink insert-if-absent so a partly-flushed parent is safe.' },
    { s: 'Victims & counters', d: 'Mark every seen-but-unsettled transaction aborted, and restore the transaction and page id counters from the maximum seen across all records.' },
    { s: 'Wire into open', d: 'Run recovery during engine open, before the index opens, and persist the rebuilt pages until checkpoints batch that flush.' },
  ],
  checkpointing: [
    { s: 'recLSN + first-dirty hook', d: 'Add a `rec_lsn` to each frame, set on the clean→dirty edge from the mutation site where the log position is known. The same hook decides whether to log a full-page image by comparing the page’s old number to the redo point. Expose the minimum recLSN over dirty frames.' },
    { s: 'Status guard', d: 'Add one read/write lock that commit and abort hold shared across their two steps (log append, then commit-log update) and the checkpoint holds exclusive while it picks the redo point and snapshots.' },
    { s: 'Checkpoint record', d: 'Define and emit a record carrying the redo point, the next transaction and page ids, the active set, the durable aborted set, the vacuum horizon, and the root pointer.' },
    { s: 'Seed recovery from it', d: 'Start replay at the checkpoint’s redo point and prime the commit log from its sets, keeping replay-from-the-start as the no-checkpoint fallback.' },
    { s: 'The fuzzy loop', d: 'Snapshot under the guard, append and flush the record, write dirty pages then one batched `fsync`, atomically repoint the superblock, and discard log older than the redo point. Run it on a background thread, plus once on clean shutdown.' },
    { s: 'Collapse the eviction sync', d: 'Once the batched flush exists, drop the per-eviction `fsync`; warn if the redo point fails to advance across consecutive checkpoints.' },
  ],
  reclamation: [
    { s: 'One status function', d: 'Centralize committed / aborted / in-progress in a single `settled_status`, and define vacuumable as "creator aborted, or deleter committed below the horizon". Route visibility, conflict, and vacuum checks through it — no bare commit-log lookups.' },
    { s: 'The sweep', d: 'Walk the leaves along their right-links and compact each in turn, holding one leaf latch at a time; publish the vacuum horizon only when a full sweep completes.' },
    { s: 'Two-tier log trimming', d: 'Drop committed commit-log entries below the oldest snapshot at any time; drop aborted entries only below the vacuum horizon.' },
    { s: 'Empty-page deletion', d: 'Add half-dead and unlink records — splice a fully empty leaf out of the sibling chain and remove its parent downlink — leaf-only at first, with matching redo arms. Delay recycle until no snapshot can reach the page.' },
    { s: 'Free space', d: 'Track free pages with a crash-safe bitmap page (covered by the normal redo path); until it lands, leak emptied pages rather than reuse them unsafely.' },
    { s: 'Autovacuum', d: 'Drive the sweep from a dead-version threshold on a background thread, with cost-based throttling and a skip bit for clean pages.' },
  ],
  verification: [
    { s: 'Type round-trips', d: 'Property-test the key/value codecs first — decode∘encode is identity, and the encoded order matches the natural order — as a no-harness warm-up on fixed-width keys.' },
    { s: 'Index invariants', d: 'Add a non-leaking test harness, then properties that inserting N keys makes them all readable and a full range scan is strictly ascending with N entries.' },
    { s: 'Reference-map oracle', d: 'Replay random insert / delete / update / get sequences against the index and a reference ordered map, each operation in its own committed transaction, asserting both answers and errors agree.' },
    { s: 'Structural walker', d: 'Walk the real pages and assert the engine’s invariants — balanced paths, keys within bounds, versions grouped, right-link set equals the top-down set, no flag at rest, full page accounting. Reuse it on recovered trees.' },
    { s: 'Crash harness', d: 'Build crash-and-reopen (drop unflushed pages, run recovery) and log-truncation helpers, then assert data survival, structure, and idempotent re-recovery across the scenario catalogue — split, delete, update, crash-victim, mid-split, double recovery, torn tail.' },
    { s: 'Concurrency', d: 'Replay isolation anomalies deterministically (they depend on snapshot and commit-log state, not timing); model-check the latches, buffer pool, and log buffer with exhaustive interleaving search; audit lock-poisoning on the durability path.' },
  ],
  performance: [
    { s: 'Counters first', d: 'Add buffer-pool hit rate and sync-to-commit ratio — the two numbers that classify a slowdown as IO-, CPU-, or lock-bound — per shard and aggregated on read; then splits, compactions, conflicts, and active-set size.' },
    { s: 'Benchmark harness', d: 'Stand up a benchmark suite on fixed-width keys, separating CPU-bound runs (in-memory, no sync) from durability runs (real `fsync`), built with the shipping optimization profile and compared against saved baselines.' },
    { s: 'Tracing spans', d: 'Behind a feature gate, span the operations and the log/pool internals so a single slow commit can be attributed to where its time went.' },
    { s: 'Group commit', d: 'Batch the disk syncs of concurrent committers into one and move the log buffer to lock-free slot reservation — the change that lifts the single-writer ceiling, and the one to do before the smaller wins.' },
    { s: 'Allocation cleanups', d: 'Reuse scratch buffers on the flush, eviction, and split paths instead of allocating per operation; use stack buffers for small payloads.' },
    { s: 'Hot-path layout', d: 'Cache derived header values in the page accessor (free), then store a few leading key bytes inline in the slot directory so most comparisons stay in cache — kept only if the benchmark says it earned its place.' },
  ],
  query: [
    { s: 'Parser', d: 'Tokenize and parse a SQL subset into an abstract syntax tree.' },
    { s: 'Schema & catalog', d: 'Describe tables, columns, and types, persisted as system metadata in the engine itself.' },
    { s: 'Planner & executor', d: 'Translate statements into operations over the index, opening a transaction per statement through the existing transactional API.' },
    { s: 'In-process SQL', d: 'Expose the executor through the same in-process API the engine offers today, so an application can run SQL embedded — no server required. A command-line shell or a network layer for remote clients stays an optional, later step, in keeping with the embedded-first scope.' },
  ],
};

