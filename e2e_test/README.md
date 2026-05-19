# e2e_test — End-to-end Rust vs R operator comparison

Driver that runs the **local Rust dev binaries** against real Tercen
workflows, downloads the corresponding **R operator output tables**
from the same Tercen instance, and **diffs them column-by-column**
across multiple datasets.

The point: iterate on operator changes in seconds (no Docker build,
no operator-reinstall) and get quantitative parity numbers across a
realistic dataset suite.

## What it tests

Phase 1 (this version) — two **isolated** comparisons per workflow:

1. **Grid**: R-grid (from Tercen) vs Rust-grid (local `dev` binary,
   reading the same workflow's Grid step input).
2. **QT**: R-QT (from Tercen) vs Rust-QT (local `dev` binary, reading
   the same workflow's QT step input — which itself is fed by R-grid).

Phase 2 (future) — **compound Rust→Rust**: feed the Rust-grid output
back through Gather and into Rust-QT, compare the chained pipeline
against R end-to-end. Requires pushing intermediate results back to
Tercen (or simulating Gather locally) — deferred.

## How it works

```
For each workflow in <project folder>:
    For each (stage in {grid, qt}):
        # R side: download the step's output relation from Tercen.
        r_csv = tercen.fetch_step_output(workflow_id, step_id)

        # Rust side: run the local dev binary with OUTPUT_CSV set, so
        # the result DataFrame is dumped to a local file instead of
        # being save_table'd back to Tercen.
        rust_csv = run_dev_binary(
            workflow_id, step_id,
            bin_path=<grid_or_qt_dev_bin>,
            output=rust_csv_path,
        )

        # Diff: per-column stats (max abs diff, % NaN-mismatch, etc.)
        report.add(workflow_id, stage, diff(r_csv, rust_csv))

write_summary(report)
```

The Rust `dev` binary connects to Tercen normally (so it sees the same
input table the R operator saw). The only change vs production is
that it dumps its output DataFrame as CSV instead of round-tripping
through `save_table`.

## Setup

```bash
cd e2e_test
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set environment:

```bash
export TERCEN_URI=https://pamgene.tercen.com:443
export TERCEN_TOKEN=<your token>      # OR TERCEN_USERNAME / TERCEN_PASSWORD
```

## Running

```bash
python run.py \
    --project-id <project ID containing test workflows> \
    --grid-operator-dir ../../pamsoft_grid_rust_operator \
    --qt-operator-dir .. \
    --output-dir reports/
```

CLI flags:

- `--project-id` — Tercen project ID to scan. The driver iterates over
  every DataStep in every workflow under that project, picking out the
  ones whose operator name matches `pamsoft_grid_operator` or
  `pamsoft_grid_qt_operator` (R) — those are the steps we compare
  against the Rust dev binaries.
- `--workflow-ids w1,w2,w3` — instead of scanning the project, test
  exactly these workflows. Useful when you want to focus on a known
  problem case.
- `--grid-operator-dir`, `--qt-operator-dir` — paths to the Rust
  operator source repos. The driver `cargo run --bin dev`s from
  there.
- `--output-dir` — where to write per-workflow diffs and the summary.

## Output

Per workflow + stage, two files:

- `reports/<workflow>_<stage>_r.csv`  — R operator output.
- `reports/<workflow>_<stage>_rust.csv` — Rust dev binary output.
- `reports/<workflow>_<stage>_diff.csv` — per-column statistics.

Plus an aggregate `reports/summary.csv` with one row per
`(workflow × stage × column)`:

| field | description |
|---|---|
| workflow_id | source workflow |
| stage | `grid` or `qt` |
| column | column name (namespace prefix stripped) |
| dtype | numeric / string / bool |
| n_rows | row count (must match between R and Rust) |
| n_diff | rows where the values differ |
| max_abs_diff | numeric only |
| max_rel_diff | numeric only |
| nan_mismatch | rows where one side is NaN and the other isn't |

## Diff semantics

- **Joining R and Rust outputs**: by `.ci` and `.ri` (the row indices
  Tercen uses to align rows of the same relation).
- **Numeric columns**: `abs(r - rust)` and `abs(r - rust) / abs(r)`.
  Both NaN → counts as equal. One NaN, the other not → counts as
  mismatch.
- **String / bool columns**: equality. Case-sensitive for strings.

## Caveats

- The diff is not exact — the algorithms have known parity differences
  (~4% median for QT, ~0.4 px for Grid). The driver reports the
  numbers; tolerance interpretation is up to you.
- The Rust dev binary needs Tercen credentials. Same `TERCEN_URI` /
  `TERCEN_TOKEN` as the driver.
- Phase 1 doesn't test the compound Rust→Rust pipeline. Each stage is
  tested in isolation, fed by the R operator's upstream output.
