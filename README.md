# FluxDB

**An embedded, concurrent, transactional database engine built from scratch in Rust.**

FluxDB brings PostgreSQL-style MVCC concurrency into an embedded, in-process
engine: many transactions read and write at once, each on a consistent snapshot,
with no server, no network, and no single-writer bottleneck.

> **Status:** a working transactional *storage engine*. You can open a database,
> run transactions (insert, read, update, delete, scan), close it, reopen it, and
> your committed data is there including after an unclean shutdown. There is no
> SQL layer yet; you talk to it through a typed transaction API.

---

## The problem we're chasing

Embedded databases — the kind that run *inside* your application instead of as a
separate server are everywhere. SQLite alone ships on billions of devices.
They're loved for being simple, fast, and zero-setup.

But the popular ones share a well-known limit: **only one writer at a time.** In
SQLite a write takes a database-wide lock; even in WAL mode, two writers can't
proceed at once they serialize. For read-heavy apps that's fine; for anything
write-concurrent it's a real ceiling.

Server databases like PostgreSQL get past this with **MVCC** (Multi-Version
Concurrency Control): readers never block writers, writers never block readers,
and transactions touching different data proceed concurrently, each on a
consistent snapshot. MVCC has mostly lived in server-shaped databases.

**FluxDB sits between them** — the embedded simplicity of SQLite, the concurrent
write model of Postgres.

## What FluxDB is

- **Embedded** — runs inside your program. No server, no network, no ops.
- **MVCC** — many transactions read and write at once, each on a consistent
  snapshot, PostgreSQL-style.
- **OLTP** — tuned for transactional workloads: lots of small reads and writes,
  not giant analytical scans.
- **Durable** — committed data survives crashes, via write-ahead logging and
  crash recovery.

It's the kind of engine that sits *underneath* a database — the part that
actually stores, indexes, and protects your data transactionally.

## What's inside

FluxDB is a Cargo workspace of small, focused crates:

| Crate | Role |
|---|---|
| `storage` | Disk manager, sharded buffer pool, B+Tree index (MVCC + Lehman-Yao), segmented WAL, crash recovery |
| `db-core` | `TransactionManager` — the MVCC coordinator; transactions, snapshots, CLOG |
| `engine` | Top-level facade: autocommit + transaction API over the storage/MVCC layers |
| `common` | Shared error types, constants, and the `Key`/`Value` trait system |


The engine's core, all real and running:

- an **in-memory page cache** (sharded buffer pool with clock replacement)
  moving 4 KB pages between RAM and disk;
- an **ordered, indexed store** — a concurrent B+Tree (Lehman-Yao latching) with
  point lookups and range scans;
- **MVCC transactions** with snapshot isolation and first-writer-wins conflict
  handling — concurrent reads and writes that don't corrupt one another;
- a **segmented write-ahead log** for durability; and
- a **crash-recovery layer** that replays the WAL and rebuilds the database after
  an unclean shutdown.

## Building & testing

Requires a recent Rust toolchain (workspace uses **edition 2024**).

```bash
cargo build                    # build all crates
cargo test                     # run all tests
cargo test -p storage          # test one crate
cargo clippy -- -D warnings    # lint

# Benchmarks (Criterion; CPU benches use a RAM disk, durability benches a real disk)
cargo bench --bench btree -p engine
```

## Roadmap

In priority order: durability first, because a database is only trustworthy if
it survives failure:

1. **Recovery & durability.** Prove recovery is correct at *every* crash point,
   not just the easy ones. In flight: checkpointing (bounding replay + batched
   page flush) and WAL segment reclamation; then vacuum / space reclamation.
2. **Benchmarking & observability.** You can't improve what you can't measure.
   Honest baselines, a `tracing`-based observability layer, and before/after
   numbers for each bottleneck fixed — not a race against mature engines.
3. **SQL compatibility.** The layer that turns a storage engine into a database
   people can query: parser, planner, execution engine, relational model, and
   secondary indexes. A large, separate effort the transactional foundation is
   built to support.

## Design docs

The internals are documented alongside the code:

- [`DESIGN.md`](DESIGN.md) — durability, recovery, checkpoints, and the
  cross-cutting invariants (WAL-before-page, MVCC visibility, split protocol).

## Why we're building it

The best way to learn systems is to build them. Books explain *how a B+Tree
works* or *what MVCC is*, but theory only goes so far and you can't really learn
an engine by staring at a 100k-line production codebase. FluxDB aims to be small
enough to actually read, understand, and contribute to, while working through the
same problems a production engine solves: page caches, snapshot isolation,
write-ahead logging, crash recovery, and honest measurement.

## Contributing

FluxDB is open to contributors, and you don't need to already know how databases
work. Figuring that out together is the whole point. Open an issue, ask a
question, or just poke around the code; all of it is welcome.

- **Code & issues:** <https://github.com/BlocSoc-iitr/FluxDB>

