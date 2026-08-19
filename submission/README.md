# Day 18 — Lakehouse Lab submission

**Phạm Nguyễn Hùng Nguyên** · Track 2 · lightweight path · run 2026-08-19

## What is here

| Path | What it is |
|---|---|
| [`notebooks/`](notebooks/) | The eight executed notebooks, **output cells preserved** (`jupytext` → `nbconvert --execute`). Identical copies live in the repo's `notebooks/` — that directory is `.gitignore`d for `*.ipynb`, so these are the committed ones. |
| [`RESULTS.md`](RESULTS.md) | Every rubric criterion with the number actually measured, plus the reading of the two findings that contradict a common belief. **Start here.** |
| [`REFLECTION.md`](REFLECTION.md) | The ≤ 200-word anti-pattern reflection (191 words). |
| [`screenshots/`](screenshots/) | Console evidence — see below. |
| [`bonus/ARCHITECTURE.md`](bonus/ARCHITECTURE.md) | Optional bonus challenge: Topic A, LLM observability at 1B req/day. |

### Evidence in `screenshots/`

The rubric allows either MinIO console captures (Spark path) or
`tree _lakehouse/` plus the contents of a `_delta_log/*.json` (lightweight path).
This submission is the lightweight path, so the evidence is text:

| File | Contents |
|---|---|
| `01_make_smoke.txt` | `make smoke` — 9/9 offline checks |
| `02_make_data.txt` | `make data` + `make data-ai` |
| `03_make_test.txt` | `make test` — **24 passed in 5.22 s** |
| `04_make_run_all.txt` | `make run-all` — **8/8 passed in 64.5 s** |
| `05_notebook_outputs_all.txt` | Every printed line from all eight executed notebooks |
| `06_tree_lakehouse.txt` | `tree _lakehouse/` — 1,153 files, 98.4 MB, Bronze/Silver/Gold + Iceberg warehouses |
| `07_delta_log_contents.txt` | Full `_delta_log/*.json` for NB1 (both commits) and the NB2 Z-ORDER commit, annotated — this is where the `minValues`/`maxValues` that drive file skipping live |

## How it was run

```bash
python -m venv .venv                       # Python 3.11.9
.venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python scripts/verify_lite.py        # = make smoke
.venv/Scripts/python scripts/generate_data_lite.py # = make data
.venv/Scripts/python scripts/generate_ai_data.py   # = make data-ai
.venv/Scripts/python -m pytest                     # = make test    → 24 passed
.venv/Scripts/python scripts/run_all.py            # = make run-all → 8/8 passed

# then, for the executed .ipynb deliverables:
.venv/Scripts/jupytext --to notebook notebooks/0*.py
.venv/Scripts/python -m jupyter nbconvert --to notebook --execute --inplace notebooks/0*.ipynb
```

`make` targets were invoked as their underlying commands because the `Makefile`
hardcodes POSIX venv paths (`.venv/bin/python`); on Windows the interpreter is at
`.venv/Scripts/python.exe`. Same commands, same gates.

## Two environment notes (Windows)

**1. Console encoding.** The notebooks and `verify_lite.py` print `✓`, `→`, `⚠️`
and box-drawing characters. Windows' default console codepage is cp1252, so the
first `make smoke` died on `UnicodeEncodeError: 'charmap' codec can't encode
character '✓'` — before running a single lakehouse check. Fixed by setting
`PYTHONUTF8=1` / `PYTHONIOENCODING=utf-8` in the environment. No source change.

**2. One source fix — `scripts/lakehouse.py::reset_catalog`.** This is the only
change made to the lab outside `submission/`, and it is disclosed here because it
touches a graded file.

`reset_catalog()` did `shutil.rmtree(dir, ignore_errors=True)` while the
`SqlCatalog`'s SQLite connection was still open. POSIX lets you unlink an open
file, so this works on Linux/macOS. Windows raises `WinError 32` — and because of
`ignore_errors=True`, **the delete failed silently** and the stale catalog stayed
on disk.

Two consequences:

* `tests/test_lab18.py::test_reset_catalog_does_not_touch_siblings` failed
  (23/24). The assertion `not (tmp_path/"iceberg"/"drop").exists()` was correct —
  the directory genuinely had not been deleted.
* More importantly for a student: re-running a notebook cell in a **live Jupyter
  kernel** (the normal way to work) keeps the previous `SqlCatalog` alive, so
  NB5/NB6/NB8's `reset_catalog()` would no-op and the next `create_table()` would
  raise `TableAlreadyExistsError` — which reads like a corrupt install, not a
  Windows file lock.

The fix disposes the SQLAlchemy engines this process opened for that catalog name
before the `rmtree`. It is platform-neutral (a no-op cost on POSIX) and scoped to
the one catalog name, preserving the per-notebook isolation the module is built
around. With it, `make test` is **24/24** and same-kernel reruns work.

Diff is one function plus an engine registry in `scripts/lakehouse.py`.

## Result

* `make smoke` — 9/9
* `make test` — 24/24
* `make run-all` — **8/8**, every notebook's own `assert` block green

Per-criterion numbers: [`RESULTS.md`](RESULTS.md).
