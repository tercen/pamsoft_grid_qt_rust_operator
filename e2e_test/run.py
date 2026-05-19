#!/usr/bin/env python3
"""End-to-end Rust vs R operator comparison driver.

Usage:

    python run.py \
        --project-id <PROJECT> \
        --grid-operator-dir ../../pamsoft_grid_rust_operator \
        --qt-operator-dir ..

See README.md for the full design.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

# Make `lib/` importable when run from the e2e_test/ dir.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.client import TercenAuth, connect, list_workflows_in_project
from lib.diff import ColumnDiff, diff_csvs, write_summary
from lib.results import fetch_step_output_csv
from lib.runner import run_dev
from lib.workflow import StepRef, find_target_steps


def main(argv=None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("e2e_test")

    auth = TercenAuth.from_env()
    client = connect(auth)
    log.info("Connected to %s", auth.service_uri)

    targets = _collect_targets(client, args)
    if not targets:
        log.error("No Grid / QT steps found. Specify --workflow-ids or check --project-id.")
        return 2

    log.info("Comparing %d target steps:", len(targets))
    for t in targets:
        log.info("  - %s / %s (%s) op=%s", t.workflow_name, t.step_name, t.stage, t.operator_name)

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    all_diffs: List[ColumnDiff] = []
    for t in targets:
        op_dir = Path(args.grid_operator_dir if t.stage == "grid" else args.qt_operator_dir).resolve()
        if not op_dir.exists():
            log.error("Operator dir missing for stage %s: %s", t.stage, op_dir)
            return 2

        slug = f"{t.workflow_id[:12]}_{t.step_id[:12]}_{t.stage}"
        r_csv = output_dir / f"{slug}_r.csv"
        rust_csv = output_dir / f"{slug}_rust.csv"
        rust_log = output_dir / f"{slug}_rust.log"
        diff_csv = output_dir / f"{slug}_diff.csv"

        # 1) R side: download the step's output from Tercen.
        try:
            fetch_step_output_csv(client, t.workflow_id, t.step_id, r_csv)
        except Exception:  # noqa: BLE001
            log.exception("R-side fetch failed for %s; skipping", slug)
            continue

        # 2) Rust side: run the local dev binary.
        try:
            run_dev(
                op_dir,
                t.workflow_id,
                t.step_id,
                rust_csv,
                tercen_uri=auth.service_uri,
                tercen_token=auth.token,
                tercen_username=auth.username,
                tercen_password=auth.password,
                log_path=rust_log,
            )
        except Exception:  # noqa: BLE001
            log.exception("Rust dev binary failed for %s; see %s", slug, rust_log)
            continue

        # 3) Diff the two CSVs.
        try:
            diffs = diff_csvs(r_csv, rust_csv, t.workflow_id, t.stage)
        except Exception:  # noqa: BLE001
            log.exception("Diff failed for %s", slug)
            continue
        write_summary(diffs, diff_csv)
        all_diffs.extend(diffs)
        log.info("✓ %s — %d columns diffed → %s", slug, len(diffs), diff_csv)

    summary_path = output_dir / "summary.csv"
    write_summary(all_diffs, summary_path)
    log.info("=== summary written to %s (%d rows) ===", summary_path, len(all_diffs))
    _print_top_offenders(all_diffs, log)
    return 0


def _collect_targets(client, args) -> List[StepRef]:
    """Resolve --workflow-ids or --project-id into a flat list of
    (workflow, grid/QT step) pairs."""
    targets: List[StepRef] = []
    if args.workflow_ids:
        for wf_id in args.workflow_ids:
            wf = client.workflowService.get(wf_id)
            targets.extend(find_target_steps(wf))
    elif args.project_id:
        for wf in list_workflows_in_project(client, args.project_id):
            targets.extend(find_target_steps(wf))
    return targets


def _print_top_offenders(diffs: List[ColumnDiff], log) -> None:
    """Spot-check: log the 5 columns with the highest max_abs_diff
    across all workflows. Quick eyeball-check after a run."""
    numeric = [d for d in diffs if d.max_abs_diff is not None]
    numeric.sort(key=lambda d: (d.max_abs_diff or 0), reverse=True)
    if not numeric:
        return
    log.info("Top diff offenders (max_abs_diff):")
    for d in numeric[:5]:
        log.info(
            "  %s %s %-24s  abs=%.3g  rel=%.3g  ndiff=%d/%d",
            d.workflow_id[:12],
            d.stage,
            d.column,
            d.max_abs_diff or 0,
            d.max_rel_diff or 0,
            d.n_diff,
            d.n_rows,
        )


def _parse_args(argv):
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--project-id",
        help="Tercen project ID to scan for workflows containing grid/QT steps.",
    )
    src.add_argument(
        "--workflow-ids",
        type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
        help="Comma-separated list of workflow IDs to test, instead of scanning a project.",
    )
    p.add_argument(
        "--grid-operator-dir",
        default="../../pamsoft_grid_rust_operator",
        help="Path to pamsoft_grid_rust_operator (where `cargo run --bin dev` lives).",
    )
    p.add_argument(
        "--qt-operator-dir",
        default="..",
        help="Path to pamsoft_grid_qt_rust_operator (this repo's root).",
    )
    p.add_argument(
        "--output-dir",
        default="reports",
        help="Where to write per-stage R/Rust CSVs, the diffs, and the summary.",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
