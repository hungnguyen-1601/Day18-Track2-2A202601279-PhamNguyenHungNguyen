# Measured results — every rubric criterion, with the number

Run on 2026-08-19, lightweight path, Windows 11 / Python 3.11.9,
`deltalake` 1.6.2 · `pyiceberg` 0.11.1 · `duckdb` 1.5.5 · `polars` 1.43.2 ·
`pyarrow` 25.0.1.

Raw console output for every line below: [`screenshots/05_notebook_outputs_all.txt`](screenshots/05_notebook_outputs_all.txt).

---

## Part A — Foundations (44 pts)

| NB | Criterion | Target | **Measured** |
|---|---|---|---|
| 1 | `_delta_log/` JSON commits visible | present | **2 commits**, dumped in full in [`screenshots/07_delta_log_contents.txt`](screenshots/07_delta_log_contents.txt) |
| 1 | Schema enforcement blocks `age="thirty"` | blocked | **blocked** — `Cast error: Cannot cast string 'thirty' to value of Int64 type` |
| 1 | `schema_mode="merge"` adds `tier` | added | **added in commit 1**; the 3 pre-existing rows read back `tier=NULL`, no file rewritten |
| 2 | Small-file problem reproduced | ≥ 100 files | **200 files** before OPTIMIZE |
| 2 | Speedup **or** files-pruned | ≥ 3× / ≥ 10× | **7.3× speedup** *and* **55× files-pruned** — both gates cleared |
| 2 | `numFiles` drops after OPTIMIZE | meaningful | **200 → 55** |
| 3 | `history()` ≥ 5 versions incl. RESTORE | ≥ 5 | **5** — v0 WRITE, v1 WRITE, v2 MERGE, v3 WRITE, **v4 RESTORE** |
| 3 | MERGE upsert 100K rows | succeeds | **0.15 s** — 50,000 updated + 50,000 inserted, 150,000 output rows |
| 3 | RESTORE rolls back; `score < 0` = 0 | 0 | **0 rows**, restore took **0.03 s** |
| 4 | Bronze / Silver / Gold on storage | all present | all three under `_lakehouse/{bronze,silver,gold}/` |
| 4 | Silver < Bronze (dedup) | fewer | **200,000 → 190,052** (9,948 retry duplicates removed) |
| 4 | Gold ≥ 7 dates × 3 models | ≥ 7 | **8 dates × 3 models = 24 rows**, with p50/p95/cost_usd/error_rate populated |

## Part B — Lakehouse 2026 (50 pts)

| NB | Criterion | Target | **Measured** |
|---|---|---|---|
| 5 | Table created through the catalog, `day(ts)` spec | yes | created via `SqlCatalog`; spec `ts_day: day(ts)` — no path ever chosen by hand |
| 5 | Hidden-partition pruning on `ts` | ≥ 5× | **10×** — 10 files → 1, filtering `ts` and never `ts_day` |
| 5 | Three-tier metadata walked; metadata:data ratio | reported | 1 metadata.json → 10 manifest lists → 10 manifests → 10 data files; **metadata 138.3 KB vs data 47.3 KB = 292.4% of table size** at 500 rows/file |
| 5 | Rename keeps `field_id`; ≥ 2 specs coexist | yes | `latency_ms → latency_millis` kept **field_id=4**; `spec_id` **{1, 2}** both live, all **5,500 rows** still readable |
| 6 | Job 1 Compaction | ≥ 10× fewer | **200 → 11 files (18×)**; data bytes went *up* 10.1 → 16.1 MB first — you pay twice, briefly |
| 6 | Job 2 Clustering, from min/max stats | ≥ 50% skip | **11/11 files → 1/10 files = 90% skipped** |
| 6 | Job 3 Expiry | bytes / 3 snaps | Delta VACUUM reclaimed **16.1 MB**; Iceberg **20 → 3 snapshots** |
| 6 | Job 4 Orphans | 3 found + swept | **3 planted orphans found and removed (21.2 KB)**; **17 stranded Iceberg manifest lists swept (37.3 KB)** |
| 6 | Job 5 Checkpoint | written | `00000000000000000099.checkpoint.parquet` + `_last_checkpoint` — a cold reader replays 1 checkpoint instead of **204** JSONs |
| 7 | Random-access amplification | ≥ 5× | **200×** — one 64 KB frame costs a 12.5 MB row-group read |
| 7 | int8 ≥ 3× smaller; recall **and** topic fidelity | ≥ 3× | **5.8× smaller**; **recall@10 = 0.904**, **topic fidelity = 1.000** |
| 7 | Semantic search as SQL, on-topic | yes | brute force over 2,000 × 256-dim vectors in **14.9 ms**; top-5 all share the query's topic |
| 7 | Lifecycle bug reproduced | 0 in / >0 ext | **0 in-table, 8 in the stale external index**; CDF emitted **8 delete events** |
| 8 | Trajectories through medallion | partitioned | Silver partitioned `agent_version={policy-v2, policy-v3}`; Gold covers **both** policies |
| 8 | Training run pins the table version | exact match | pinned **v0 / 1,578 steps**; after 400 more rollouts landed (v1, 1,978 steps), replay at v0 still returns **1,578** |
| 8 | MCP: cacheable list, `input_required`, task poll | all three | **5 turns → 1 catalog read**; `delete_rows` returned `input_required` until `confirmed`; `submit_scan` polled to `completed` |
| 8 | 4 Art. 10 buckets as partitions; UNCLASSIFIED excluded | 4 | **4 buckets** on disk + UNCLASSIFIED; **1,666 / 2,000 defensible**, **334 excluded (16.7%)** |

## Part C — Reproducibility (6 pts)

| Criterion | **Result** |
|---|---|
| `make test` green | **24 passed in 5.22 s** — [`screenshots/03_make_test.txt`](screenshots/03_make_test.txt) |
| `make run-all` green from a clean setup | **8/8 passed in 64.5 s** — [`screenshots/04_make_run_all.txt`](screenshots/04_make_run_all.txt) |
| `make smoke` | **9/9 checks** — [`screenshots/01_make_smoke.txt`](screenshots/01_make_smoke.txt) |

---

## The two findings that contradict a common belief

The rubric singles these out, so here is the reading, not just the number.

**1. `VACUUM` does not remove orphans (NB6, Job 4).**
Three orphan files were planted with an mtime 30 days in the past — older than
any retention window — and the table was vacuumed with
`retention_hours=0, enforce_retention_duration=False`, the most aggressive
setting available. The dry run listed **211 files** and **not one of the three
orphans**. Files on disk: 15. Files in the log: 10.

The mechanism: `deltalake` (delta-rs) reclaims files the transaction log has
**tombstoned**. A file written by a job that crashed before committing was never
added to the log, so it was never tombstoned, so the log does not know it exists.
Spark's VACUUM additionally lists the table directory, which is why the slide
files VACUUM under orphan removal — but that is an engine-specific behaviour, not
a property of the format. Verify it on your engine or run the set difference
yourself: *files on disk − files referenced by live metadata*, with an age guard.

Why it matters: our "storage under control" dashboards are derived from the log,
which is precisely the source that cannot see this garbage. The failure is
silent and the bill is real.

**2. `expire_snapshots` deletes nothing (NB6, Job 3).**
Iceberg dropped from **20 snapshots to 3** — and **0 of 40** avro files were
deleted. Metadata on disk *grew*, 347.3 KB → 355.6 KB, because expiry writes a
new `metadata.json`.

This is not a bug: expiry's contract is to make files *unreferenced*. Deleting
them is Job 4. In Spark/Java Iceberg the two are usually chained, so people
assume expiry reclaims space. On the Python path you must chain them yourself.
Running Job 3 without Job 4 is the exact reason teams report *"we expire
snapshots every night but the S3 bill never drops."* Chaining the sweep here
reclaimed **37.3 KB across 17 stranded manifest lists**, with all 2,000 rows
still intact.

Both behaviours are pinned by canary tests in `tests/test_lab18.py`
(`test_vacuum_does_not_see_uncommitted_orphans`,
`test_expire_snapshots_is_metadata_only`), so if a library version changes them,
the suite goes red and the notebooks' claims must be revised.

---

## Two more readings worth the marks

**NB5 — why the pruning ratio is 10× and what it is worth.** The filter is
`ts >= '2026-08-05' and ts < '2026-08-06'`; `ts_day` is never mentioned, because
it is not a column — it is a transform Iceberg stored in metadata and derives at
plan time. A Hive user who forgot the partition predicate would read all 10
files. Scaled to 512 MB files and $5/TB scanned, that forgotten predicate is
**$0.022 wasted per query — $220/day at 10,000 queries/day**. Hidden
partitioning does not make the query faster so much as it removes the
*opportunity to forget*.

**NB7 — the "never put blobs in your table" advice is wrong half the time.**
For an analytical scan (`SELECT topic, count(*) GROUP BY topic`), the inline-blob
layout read **1.2 KB of a 12.5 MB table**: projection pushdown means the blob
column costs the scan essentially nothing. The layout only breaks on *random
single-row* access, where Parquet's unit of I/O is the row group — 12.5 MB read
to return one 64 KB frame, **200× amplification**. So the rule is not "blobs are
bad"; it is "inline is fine for scans, fatal for point lookups," which is exactly
the workload split between analytics and GPU feeding.
