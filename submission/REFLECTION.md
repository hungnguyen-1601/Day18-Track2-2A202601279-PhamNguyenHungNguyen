# Reflection — which anti-pattern is our data most at risk of?

**"We run maintenance" — when Job 3 and Job 4 are run in isolation.**

Our team streams LLM traces with a short trigger interval and has a nightly
VACUUM on the calendar. On paper that is the small-file anti-pattern, already
solved. NB6 measured two things that say otherwise.

First, `expire_snapshots` dropped Iceberg from 20 snapshots to 3 and deleted
**0 of 40** avro files — metadata on disk actually *grew*, 347.3 KB → 355.6 KB.
Expiry only makes files unreferenced; deleting them is a different job.

Second, three planted orphans aged 30 days stayed invisible: the VACUUM dry-run
listed 211 tombstoned files and **not one orphan**. `deltalake` reclaims what the
log has tombstoned, and a file from a crashed writer was never committed, so it
was never tombstoned.

Our failed jobs write exactly those files. We have been paying for garbage that
`history()`, `file_uris()` and every dashboard derived from them cannot see — so
our "storage is under control" report is confidently wrong.

Fix: ship expiry and orphan sweep as a pair, plus a canary diffing files-on-disk
against `file_uris()`, with a 24-hour age guard so an in-flight writer is never
deleted.
