# LLM Observability at 1B req/day — Architecture Brief

**Topic A** · Author: Phạm Nguyễn Hưng Nguyên · Day 18 Track 2 bonus · 2026-08-19

---

## 1. Problem statement

A foundation-model API team logs every request/response: **1B req/day, ~5 KB each
→ ~5 TB/day raw, ~1.8 PB/year** if nothing expires. Four constraints, and they
fight each other:

1. Per-tenant cost & latency dashboards refreshed every **5 minutes**.
2. Full prompt/response retained **7 days** for incident review; aggregates only
   after that, for **1 year**.
3. **PII redacted before any human can read it** — including the on-call engineer
   debugging at 3 AM.
4. Total storage spend **≤ $5,000/month**.

Why it is hard: (2) and (4) are the same constraint in disguise. 5 TB/day × 7 days
= 35 TB of hot prompt text; at S3 Standard list price that alone is ~$800/mo, and
that is *before* the year of aggregates, before replicas, and before the
write/compaction traffic. Meanwhile (1) demands a 5-minute freshness SLA, which
pushes toward small, frequent commits — the exact ingestion pattern that produces
the small-file problem and makes both query cost and compaction cost explode
(NB6 measured this: at 2M files the per-object component of managed
compaction was $240 of a $990/mo bill — driven by file **count**, not volume).

The architecture is therefore a negotiation between freshness, retention and file
count, with redaction as a non-negotiable ingest-time gate.

---

## 2. Architecture

```
            ┌──────────────────── INGESTION PATH ────────────────────┐
 API pods   │                                                        │
 (1B/day)   │  gateway  ──▶ Kafka  ──▶ Flink redaction job           │
    │       │  emits        (raw,      · NER + regex → tokenize      │
    │       │  span         24h         · sha256(salt‖pii) → token   │
    └───────┼─▶ per          retention) · salt in KMS, rotated 90d   │
            │  request                  · drops raw text on the floor│
            │                            (never lands unredacted)    │
            │                                   │                    │
            │                                   ▼                    │
            │        ┌──────────── BRONZE ────────────┐              │
            │        │ delta: bronze.llm_calls_raw    │              │
            │        │ redacted JSON + envelope cols  │              │
            │        │ partition: date, hour          │              │
            │        │ commit every 60s (Flink)       │              │
            │        │ RETENTION 7 DAYS (hard)        │              │
            │        └────────────────┬───────────────┘              │
            └─────────────────────────┼──────────────────────────────┘
                                      │ streaming MERGE, dedup on request_id
                                      ▼
                     ┌──────────── SILVER ────────────┐
                     │ delta: silver.llm_calls        │
                     │ typed cols, 1 row = 1 call     │
                     │ partition: date                │
                     │ ZORDER (tenant_id, model)      │
                     │ prompt/response = POINTERS to  │
                     │   blob store, not inline       │
                     │ RETENTION 7 DAYS (text) /      │
                     │   400 DAYS (metadata only)     │
                     └────────────────┬───────────────┘
                                      │ 5-min incremental agg (CDF-driven)
                                      ▼
                     ┌──────────── GOLD ──────────────┐
                     │ delta: gold.tenant_5min        │  ← dashboard hot path
                     │ gold.tenant_daily              │  ← 1-year retention
                     │ (tenant, model, bucket) →      │
                     │  p50/p95/p99, tokens, cost,    │
                     │  error_rate, req_count         │
                     │ partition: date · ZORDER tenant│
                     └────────────────┬───────────────┘
                                      │
   QUERY PATH:   Trino / DuckDB ──────┤   dashboards hit GOLD only (≤ 2 GB/day)
                 incident review ─────┘   hits SILVER + blob pointers, and is
                                          audit-logged per PII-token resolution

   CONTROL PLANE: Apache Polaris (REST catalog) — vends credentials, enforces the
                  row filter `tenant_id = current_tenant()`, plans scans.
```

---

## 3. Key decisions, with rejected alternatives

### D1 — Table format: **Delta Lake**

I chose **Delta**. I rejected **Iceberg** because the hot path here is a
streaming `MERGE` for dedup plus a **Change Data Feed** consumer that drives the
5-minute Gold refresh; Delta's CDF is a first-class, engine-agnostic contract,
whereas on Iceberg I would be reading `_changes` metadata tables or re-deriving
incrementals myself. I rejected **plain Parquet + Hive partitions** because
without ACID commits the 60-second ingestion job's partial writes are visible to
dashboards, and because the retention story (D5) requires deletes that Hive
cannot do transactionally.

*Concession:* Iceberg's hidden partitioning is genuinely better ergonomics — NB5
measured 10× pruning (10 files → 1) from a filter on `ts` that a Hive user would have had to
write against `ts_day` by hand. I mitigate the Delta gap by making `date` a
generated column and enforcing the predicate in the Polaris view, so an analyst
cannot forget it — buying back most of the benefit at the cost of one governance
rule.

### D2 — Catalog: **Apache Polaris (REST)**

I chose **Polaris**. I rejected **Unity Catalog** because tenant isolation here is
a *product* requirement (customers are tenants), and I will not put my
multi-tenant security boundary inside a single vendor's control plane that I
cannot self-host during an incident. I rejected **AWS Glue** because it has no
credential vending and no row-level filter — I would have to reimplement
per-tenant isolation in every engine (Trino, DuckDB, the notebook a data
scientist runs on their laptop), and the laptop is exactly where it would be
forgotten.

The catalog is the security boundary, not a name→path map. That is the 2026
shift NB5 makes concrete.

### D3 — Prompt/response bytes: **pointer, not inline**

I chose to store redacted prompt/response as **objects in a separate bucket**,
referenced by `blob_uri` in Silver. I rejected **inline `BINARY` columns** because
NB7 measured the failure mode directly: Parquet's unit of I/O is the row group,
so a single-row fetch (`WHERE request_id = …` — exactly what incident review
does) reads the whole row group. On the lab corpus the amplification was
**200× more bytes than needed** (a 12.5 MB row group read to fetch one 64 KB frame). Incident review is a random-access workload
wearing an analytical schema's clothes.

I rejected **a separate document store (Elasticsearch/Mongo)** because it doubles
the retention machinery — and NB7's lifecycle bug shows what happens next: the
erasure request deletes from the lakehouse and the copy keeps serving the erased
content.

*Mitigation for the pointer split:* the blob bucket has a **7-day S3 lifecycle
rule** on the same `date=` prefix scheme, so expiry is enforced by the object
store, not by a job that can silently fail. The lakehouse row outlives the blob
and the URI 404s — which is the correct, auditable behaviour, not a bug.

### D4 — Partitioning & clustering: **partition by `date`, Z-ORDER by `(tenant_id, model)`**

I chose date partitions with clustering on tenant. I rejected **partitioning by
`tenant_id`** because the tenant distribution is a power law — the top tenant is
~30% of traffic and the tail is thousands of tenants with <1 MB/day, which
manufactures millions of tiny partitions: the small-file problem by design.
I rejected **partitioning by `(date, hour)` at Silver** because 24× more
partitions × 365 days is 8,760 partitions/year for a marginal pruning gain over
Z-order — and NB6 showed clustering alone took a point query from **11 of 11 files down to
1 of 10 — a 90% skip rate**, proven from min/max stats rather than a stopwatch.

Bronze *does* keep `(date, hour)` because its only reads are "replay the last N
hours" and its lifetime is 7 days — the partition count never accumulates.

### D5 — Retention: **two clocks, written down, enforced by two mechanisms**

I chose **7 days for text** (Bronze + blobs + Silver text pointers) and
**400 days for metadata/aggregates** (Silver envelope columns + Gold). I rejected
**one uniform retention** because the $5K cap makes 365 days of raw text
arithmetically impossible (§5). I rejected **"delete on request only"** because
GDPR / Vietnam PDPL erasure interacts with time travel: as NB8 makes explicit, a
`DELETE` does not remove the row from version *v−1*. Erasure is only complete once
`VACUUM RETAIN 168 HOURS` has expired the versions that still contain it.

So the retention window is a **deliberate, written decision**: 7 days of time
travel on text tables, 30 days on Gold. Anyone who widens it is widening the
erasure SLA, and the runbook says so in those words.

### D6 — Compaction cadence: **hourly on Bronze, daily on Silver, self-managed**

I chose to run OPTIMIZE myself on a schedule. I rejected **managed
auto-compaction** because NB6's cost model showed the per-object component
(`$0.004 / 1K objects × file count × runs`) dominates for exactly the
pathological-small-file table you would most want auto-compacted — the pricing is
anti-correlated with need. I rejected **compacting on every commit** because
compaction writes new files *before* the old ones are reclaimed; you pay for both
copies during the window, and at 60-second commits you would pay that tax 1,440
times a day.

Instead: fix the writer. Flink commits every 60 s (not every 5 s), Bronze compacts
hourly to a 512 MB target, and Job 3 (`VACUUM RETAIN 168h`) is **chained** to
Job 4 (orphan sweep) — because NB6 measured that expiry alone reclaimed *nothing*:
`expire_snapshots` dropped 20 snapshots to 3 and deleted **0 of 40** avro files, while
metadata on disk actually *grew* (347.3 KB → 355.6 KB).

---

## 4. Failure modes

### F1 — 3 AM: dashboards freeze; Gold is 40 minutes stale

**Cause.** The CDF-driven 5-minute aggregation job crashed mid-batch. Its
partially-written Parquet files are on disk but were never committed — **orphans**.
They are invisible to `history()` and to `file_uris()`; you are billed for them and
no dashboard shows them.

**Detect.** A canary comparing `count of *.parquet on disk` against
`len(DeltaTable(p).file_uris())` per table, alerting above a threshold. NB6 proved
you cannot rely on VACUUM for this: with three 30-day-old planted orphans,
`vacuum(retention_hours=0, dry_run=True)` listed 211 tombstoned files and **not one
of the orphans** — delta-rs
reclaims only what the log has *tombstoned*, and a file that was never committed
was never tombstoned.

**Rollback.** None needed for correctness — the commit never landed, so Gold is
stale, not wrong. Restart from the last CDF offset; sweep the orphans with the
set-difference job (files on disk − files referenced by live metadata) with a
**24-hour age guard**, so an in-flight writer's uncommitted files are never
deleted. Without that guard the sweep corrupts the table it was meant to clean.

### F2 — 3 AM: a schema change from the API team poisons Bronze

**Cause.** The gateway team adds `usage.cache_read_tokens` and, three days later,
changes `latency_ms` from int to `"1043ms"`. The first is benign; the second
silently nulls the column the cost model divides by.

**Detect.** Delta **schema enforcement** blocks the type change at the write —
this is the failure being loud instead of silent, and it is why `schema_mode=
"merge"` (additive, opt-in) is enabled on Bronze while **type changes are never
allowed**. The Flink job dead-letters the batch and pages.

**Rollback.** None of the bad data lands. The dead-letter topic replays after the
gateway is fixed. Had a bad batch landed via a manual backfill, `RESTORE` to the
pre-backfill version is one transaction that is itself recorded in `history()` —
NB3's measured behaviour: a 100K-row MERGE plus a poisoned append rolled back to a
known-good version in 0.03 s, leaving `score < 0` at exactly 0 rows and 5 versions
in `history()` *including the RESTORE row*.

### F3 — 3 AM: a right-to-erasure request arrives, and the copy keeps answering

**Cause.** Someone stood up an Elasticsearch mirror of Silver for "faster incident
search". The erasure `DELETE` runs against Delta. The mirror is a one-way upsert
sync — and **deletes are the operation sync pipelines forget**.

**Detect.** A compliance canary that, after every erasure, queries *every*
registered derived store for the erased subject's IDs and fails the run if any
returns > 0. NB7 reproduced exactly this: after the erasure, 0 hits in-table and **8 hits in the
stale external index**.

**Rollback.** There is no rollback for content already served into a RAG prompt —
which is why the design forbids unregistered copies. Any derived index must
subscribe to the **Change Data Feed** so deletes propagate as first-class events,
and must be listed in the catalog with a named owner. An unlisted mirror is an
incident, not config drift.

### F4 — 3 AM: one tenant's query reads another tenant's rows

**Cause.** An analyst points DuckDB straight at the S3 path, bypassing Trino and
therefore bypassing the row filter.

**Detect / prevent.** Storage credentials are **only** vended by Polaris, scoped
per table and per tenant; there are no long-lived keys with bucket-wide read.
Direct-path access fails at the credential, not at the filter — the catalog is the
security boundary (D2). Detection is the audit log of credential vends with no
matching Trino query id.

---

## 5. Cost back-of-envelope

Assumptions: S3 Standard $0.023/GB-mo, S3 IA $0.0125/GB-mo, PUT $0.005/1K,
GET $0.0004/1K. Redaction + Zstd gives ~6× on structured envelopes, ~3.5× on text.

**Raw volume:** 1B × 5 KB = **5 TB/day**.

| Layer | Live volume | Tier | $/mo |
|---|---:|---|---:|
| Bronze (7 d × 5 TB ÷ 6) | 5.8 TB | Standard | 5,800 × $0.023 = **$133** |
| Blob store, prompt+response (7 d × 3.6 TB ÷ 3.5) | 7.2 TB | Standard | 7,200 × $0.023 = **$166** |
| Silver metadata, 400 d (1B × 250 B ÷ 4 = 62 GB/day) | 24.8 TB | 30 d Std + 370 d IA | 1.9 TB×$0.023 + 22.9 TB×$0.0125 = $45 + $293 = **$338** |
| Gold 5-min (≈2M rows/day × 120 B = 240 MB/day, 30 d) | 7 GB | Standard | **$0.2** |
| Gold daily, 400 d | 3 GB | Standard | **$0.1** |
| **Storage subtotal** | | | **≈ $637/mo** |

**Request charges** — the line item people forget:

* **PUT:** Flink commits every 60 s × 24 h × ~8 partitions ≈ 11.5K files/day, plus
  compaction rewrites ≈ 12K/day → ~24K PUT/day = 0.7M/mo × $0.005/1K ≈ **$4/mo**.
* **GET:** dashboards read **Gold only** — 288 refreshes/day × ~40 files ≈ 11.5K
  GET/day; budget 2M GET/day for ad-hoc and incident review → 60M/mo ×
  $0.0004/1K ≈ **$24/mo**.
* **The counterfactual is the whole argument for Gold.** If dashboards hit Silver
  unclustered at ~200K files per day-partition, the same 288 refreshes would be
  57.6M GET/day → **$691/mo in requests alone**, more than the entire storage
  bill. This is the non-linear cost NB6 quantifies: object storage charges per
  *request*. NB6's own toy case — 200 files × 50K queries/day = $4.00/day, versus
  $0.08/day for the same data in 4 files — is the same 50× ratio at lab scale.

**Compaction compute:** ~5 TB/day rewritten hourly at Bronze + 62 GB/day at Silver.
On spot instances at ~6 vCPU-hours/TB → ~32 vCPU-h/day ≈ 960/mo × $0.02 ≈
**$19/mo**. Managed auto-compaction, for comparison, would meter
500 GB × $0.05 × 30 = **$750/mo** on the per-GB component alone — decision D6.

**Total ≈ $680/mo** (storage + requests + compaction) = **~14% of the $5K cap**.

The headroom is not luck; it is D5 (7-day text retention) and D4 (Gold as the
dashboard hot path). Remove either and the number lands at $4–6K/mo, then goes
over the cap on the next quarter of traffic growth.

*Sanity check on the rejected alternative:* keeping raw text for 365 days is
1.8 PB ÷ 3.5 = 514 TB × $0.0125 (IA) = **$6,425/mo** — over budget on storage
alone, with zero compute. The retention decision **is** the architecture.

---

## 6. What I would build first (one-week MVP)

**The slice: one tenant, one model, ingest → Gold, with redaction on.**

**Day 1–2.** Kafka topic + Flink job that does *only* redaction and the Bronze
append, committing every 60 s. Assert the invariant that matters: **no unredacted
byte is ever written to durable storage.** Test it by planting a synthetic
national-ID string in a payload and grepping the Bronze files for it. If this
invariant is not provable in week 1 it will never be provable.

**Day 3.** Silver MERGE with `request_id` dedup and the pointer split. Prove dedup
actually drops rows (Silver < Bronze), the way NB4 does — a dedup step that
changes no row count is a dedup step that is not running.

**Day 4.** Gold 5-minute aggregation driven by CDF, not by a full re-scan. Measure
end-to-end freshness with a timestamped canary request; the number to beat is
5 minutes at p95.

**Day 5.** Register the tables in Polaris with the per-tenant row filter and prove
a second tenant's credentials cannot read the first tenant's rows. Run OPTIMIZE +
`VACUUM RETAIN 168h` + the orphan sweep once, and record before/after **file
counts** — not bytes.

**Why this slice.** It exercises every load-bearing decision (D1 CDF, D2 catalog
boundary, D3 pointer split, D5 retention, D6 self-managed compaction) at a scale
where a mistake costs an afternoon instead of a quarter.

**What it deliberately does not prove** is behaviour at 1B/day. That is week 3: a
replay of one real production hour at 10× speed, and the metric to watch is file
count, not latency. Latency degrades gracefully and visibly; file count degrades
silently and then all at once.
