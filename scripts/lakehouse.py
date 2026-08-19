"""Tiny helper module for the lightweight (delta-rs + DuckDB) path.

Used by all notebooks/*.py — keeps imports + paths consistent.

The Delta tables on disk are the *same format* Spark/Databricks would write,
so a student can later point Spark at `_lakehouse/silver/llm_calls` and get
the same data. This is the value of an open table format.
"""
from __future__ import annotations

import os
from pathlib import Path

# Repo-local lakehouse — easy to inspect, easy to wipe.
ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", Path(__file__).resolve().parents[1] / "_lakehouse"))


def path(layer: str, table: str) -> str:
    """Return absolute path to a table inside a medallion layer.

    layer ∈ {"bronze", "silver", "gold", "scratch"}.
    """
    p = ROOT / layer / table
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def reset(*paths: str) -> None:
    """Delete tables (idempotent rerun support). No-op if missing."""
    import shutil
    for p in paths:
        shutil.rmtree(p, ignore_errors=True)


# ── Convenience: swap to S3 / MinIO with one env var ──
# To target s3://bucket/key instead of local disk, set:
#   LAKEHOUSE_ROOT=s3://my-bucket/lakehouse
#   AWS_* env vars per usual
# delta-rs handles the s3:// scheme natively (no Hadoop, no JVM).


# ══════════════════════════════════════════════════════════════════════
# Iceberg catalog (NB5, NB6, NB8)
# ══════════════════════════════════════════════════════════════════════
# A *catalog* is what turns a pile of Parquet into a table people can find.
# Production uses a REST catalog (Polaris, Unity, Lakekeeper, Glue). Locally
# we use pyiceberg's SqlCatalog on SQLite: same Python API, same on-disk
# Iceberg metadata, no server to run. Swapping to a real REST catalog is a
# config change, not a code change — that's the whole point of the REST spec.

ICEBERG_ROOT = ROOT / "iceberg"


def _catalog_dir(name: str) -> Path:
    """Each caller gets its OWN catalog directory.

    Why not one shared catalog? Because notebooks reset their catalog on rerun,
    and students keep several notebooks open in Jupyter Lab at once. A shared
    directory means NB6 starting up deletes the catalog NB5 is mid-way through
    — an intermittent failure that looks like a corrupt install, not a race.
    Isolation by name makes cross-notebook interference structurally impossible.
    """
    return ICEBERG_ROOT / name


def catalog(name: str = "lab"):
    """Return a local Iceberg catalog, isolated under its own directory.

    Production equivalent (one-line swap — note the identical call site):

        from pyiceberg.catalog import load_catalog
        cat = load_catalog("prod", **{
            "type": "rest",
            "uri": "https://polaris.example.com/api/catalog",
            "credential": "<client_id>:<client_secret>",
        })
    """
    from pyiceberg.catalog.sql import SqlCatalog

    d = _catalog_dir(name)
    (d / "warehouse").mkdir(parents=True, exist_ok=True)
    cat = SqlCatalog(
        name,
        uri=f"sqlite:///{d / 'catalog.db'}",
        warehouse=f"file://{d / 'warehouse'}",
    )
    # Remember the engine so reset_catalog() can close it — see there for why.
    _OPEN_ENGINES.setdefault(name, []).append(cat.engine)
    return cat


# name → SQLAlchemy engines this process has opened against that catalog.
_OPEN_ENGINES: dict[str, list] = {}


def reset_catalog(name: str = "lab") -> None:
    """Drop ONE catalog so a notebook rerun starts clean.

    Scoped to `name` on purpose — see `_catalog_dir`.

    The engine dispose() is not cosmetic. POSIX lets you unlink a file that is
    still open, so `rmtree` alone works there. Windows refuses (WinError 32)
    and — because we pass `ignore_errors=True` — the delete would fail
    *silently*, leaving the old catalog in place. The next `create_table()`
    then raises TableAlreadyExistsError, which reads like a corrupt install.
    That is exactly what a student hits re-running a notebook cell in a live
    Jupyter kernel, where the previous SqlCatalog is still alive.
    """
    import shutil

    for engine in _OPEN_ENGINES.pop(name, []):
        try:
            engine.dispose()
        except Exception:  # noqa: BLE001 — a dead engine is already "disposed"
            pass

    shutil.rmtree(_catalog_dir(name), ignore_errors=True)


def namespace(cat, ns: str = "lake"):
    """Idempotently ensure a namespace exists, then return its name."""
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    try:
        cat.create_namespace(ns)
    except NamespaceAlreadyExistsError:
        pass
    return ns


# ══════════════════════════════════════════════════════════════════════
# Measurement helpers — every mission has to *show a number*
# ══════════════════════════════════════════════════════════════════════

def du(target: str | Path) -> int:
    """Total bytes under a path (data + metadata). Missing path → 0."""
    p = Path(target)
    if not p.exists():
        return 0
    if p.is_file():
        return p.stat().st_size
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def human(n_bytes: float) -> str:
    """1234567 → '1.2 MB'. Keeps notebook output readable."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n_bytes) < 1024 or unit == "GB":
            return f"{n_bytes:.1f} {unit}" if unit != "B" else f"{int(n_bytes)} B"
        n_bytes /= 1024
    return f"{n_bytes:.1f} GB"


def count_files(target: str | Path, suffix: str = ".parquet") -> int:
    """How many data files back this table? The small-file problem, quantified."""
    return len(list(Path(target).rglob(f"*{suffix}")))


def to_arrow(relation):
    """Materialise a DuckDB relation as a pyarrow.Table, across DuckDB versions.

    DuckDB ≤1.2 returned a `pa.Table` from `.arrow()`; ≥1.3 returns a
    streaming `RecordBatchReader`. Notebooks want a Table (they call
    `.num_rows`, `.to_pandas()`), so normalise here instead of pinning DuckDB.
    """
    out = relation.arrow()
    return out.read_all() if hasattr(out, "read_all") else out
