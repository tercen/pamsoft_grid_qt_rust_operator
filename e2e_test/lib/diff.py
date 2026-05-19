"""Column-wise diff between an R operator output table and a Rust dev
binary CSV dump.

Strategy: join the two frames on ``(.ci, .ri)`` and then compute
per-column statistics. We strip namespace prefixes (``dsN.foo`` →
``foo``) before joining columns by name — that's what makes the diff
robust to either operator emitting a different namespace.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional

import polars as pl


log = logging.getLogger(__name__)


@dataclass
class ColumnDiff:
    """Per-column summary statistics, one row in the final report."""
    workflow_id: str
    stage: str          # "grid" or "qt"
    column: str
    dtype: str
    n_rows: int
    n_diff: int
    max_abs_diff: Optional[float]
    max_rel_diff: Optional[float]
    nan_mismatch: int


# Columns that index the table — never get diffed, only used to join.
INDEX_COLUMNS = (".ci", ".ri")


def diff_csvs(
    r_csv: Path,
    rust_csv: Path,
    workflow_id: str,
    stage: str,
) -> List[ColumnDiff]:
    """Diff two CSVs by ``(.ci, .ri)`` and return one ``ColumnDiff`` per
    shared data column."""
    r_df = _read(r_csv)
    rust_df = _read(rust_csv)

    r_df = _strip_namespace(r_df)
    rust_df = _strip_namespace(rust_df)

    r_shared, rust_shared = _common_columns(r_df, rust_df)
    log.info(
        "%s/%s: r=%d×%d  rust=%d×%d  shared_cols=%s",
        workflow_id,
        stage,
        r_df.height,
        r_df.width,
        rust_df.height,
        rust_df.width,
        len(r_shared),
    )

    # Inner-join on the index columns. If indices don't align, the
    # joined frame loses rows and that's the loudest possible signal
    # of a row-count mismatch.
    join_keys = [c for c in INDEX_COLUMNS if c in r_shared and c in rust_shared]
    if not join_keys:
        raise RuntimeError(
            f"{workflow_id}/{stage}: neither .ci nor .ri is present in both "
            f"CSVs. R has {list(r_df.columns)}; Rust has {list(rust_df.columns)}"
        )

    joined = r_df.join(
        rust_df,
        on=join_keys,
        how="inner",
        suffix="__rust",
    )
    n_rows = joined.height

    results: List[ColumnDiff] = []
    for col in r_shared:
        if col in INDEX_COLUMNS or col not in rust_shared:
            continue
        results.append(_diff_column(joined, col, workflow_id, stage, n_rows))
    return results


def write_summary(diffs: Iterable[ColumnDiff], path: Path) -> None:
    """Write the per-column diffs as a CSV summary."""
    rows = [asdict(d) for d in diffs]
    if not rows:
        log.warning("write_summary: no diffs to write — empty report")
        pl.DataFrame().write_csv(path)
        return
    pl.DataFrame(rows).write_csv(path)


# ---------------------------------------------------------------------- helpers


def _read(p: Path) -> pl.DataFrame:
    """Read a CSV with conservative type inference. The Rust dev
    binary's CsvWriter and Tercen's table export both produce
    Polars-friendly CSVs, so we let polars infer.

    NaN: polars reads literal ``NaN`` as null by default, but we want
    NaN values to compare equal across both sides — read them as nulls
    consistently and let the per-column comparison decide what to do.
    """
    return pl.read_csv(
        p,
        try_parse_dates=False,
        null_values=["", "NaN", "nan", "NA"],
        infer_schema_length=2000,
    )


def _strip_namespace(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns ``dsN.foo`` → ``foo``. Leave index columns
    (``.ci``, ``.ri``) untouched (they have a leading dot but are
    not namespaced)."""
    mapping = {}
    for col in df.columns:
        if col in INDEX_COLUMNS:
            continue
        if "." in col and not col.startswith("."):
            mapping[col] = col.split(".", 1)[1]
    if mapping:
        df = df.rename(mapping)
    return df


def _common_columns(a: pl.DataFrame, b: pl.DataFrame):
    """Return the columns each frame should keep so they share the
    same set. Order preserved from ``a``."""
    a_set = set(a.columns)
    b_set = set(b.columns)
    shared = [c for c in a.columns if c in b_set]
    # `b` may have columns not in `a`; we drop those silently — the
    # summary captures column-set differences via missing rows.
    return shared, shared


def _diff_column(
    joined: pl.DataFrame,
    col: str,
    workflow_id: str,
    stage: str,
    n_rows: int,
) -> ColumnDiff:
    """One column's worth of stats."""
    r_col = joined[col]
    rust_col = joined[f"{col}__rust"]
    dtype = str(r_col.dtype)

    if r_col.dtype.is_numeric():
        # Promote both sides to f64 for stable arithmetic regardless of
        # the source dtype (i32 / i64 / f32 all compare fine).
        r = r_col.cast(pl.Float64)
        u = rust_col.cast(pl.Float64)
        diff = (r - u).abs()
        rel = (diff / r.abs()).fill_nan(None).fill_null(None)
        # NaN mismatch: exactly one side is null (after the read coerces NaN→null)
        nan_mismatch = int(((r.is_null() & u.is_not_null()) | (r.is_not_null() & u.is_null())).sum())
        # Differ: non-null on both sides AND nonzero diff.
        n_diff = int((diff.fill_null(0.0) > 0.0).sum())
        max_abs = diff.max() if diff.len() > 0 else None
        max_rel = rel.max() if rel.len() > 0 else None
        return ColumnDiff(
            workflow_id=workflow_id,
            stage=stage,
            column=col,
            dtype=dtype,
            n_rows=n_rows,
            n_diff=n_diff,
            max_abs_diff=_safe_f(max_abs),
            max_rel_diff=_safe_f(max_rel),
            nan_mismatch=nan_mismatch,
        )

    # Non-numeric: pure equality.
    equal = r_col == rust_col
    n_diff = int((~equal).sum())
    return ColumnDiff(
        workflow_id=workflow_id,
        stage=stage,
        column=col,
        dtype=dtype,
        n_rows=n_rows,
        n_diff=n_diff,
        max_abs_diff=None,
        max_rel_diff=None,
        nan_mismatch=0,
    )


def _safe_f(v) -> Optional[float]:
    """Coerce a polars max() result (might be ``None``, ``NaN``, or a
    finite f64) to a plain Python float or ``None``."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f
