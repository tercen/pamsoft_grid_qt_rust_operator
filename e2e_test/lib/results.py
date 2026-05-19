"""Fetch the R operator's output table from Tercen as a Polars
DataFrame, in the same shape the Rust dev binary's ``OUTPUT_CSV``
emits.

Tercen stores a Step's output as a Relation; the SDK's relation /
table services let us materialize it into a DataFrame. We then write
it to CSV alongside the Rust output so the diff stage doesn't care
where each side came from.

The exact SDK method names vary by version. We try a few candidates
in order and raise a clear error if none work — easier to fix one
shim here than refactor the whole driver.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional

import polars as pl


log = logging.getLogger(__name__)


def fetch_step_output_csv(
    client,
    workflow_id: str,
    step_id: str,
    output_csv: Path,
) -> pl.DataFrame:
    """Resolve the step's output relation, stream every column into a
    DataFrame, write it to ``output_csv``, and return the frame.

    The "right" Tercen SDK entry point depends on version. We try in
    order:

    1. ``client.workflowService.get(workflow_id)`` then walk
       ``.steps[]`` for the matching ``step_id`` and pull its
       ``computedRelation`` / ``relation`` field.
    2. If that field gives us a relation ID, ``client.tableService
       .getRelation`` / ``client.relationService.get`` returns the
       relation metadata (column names + table id).
    3. ``client.tableSchemaService.streamTable`` / ``.exportTable``
       streams the rows into a polars frame.

    On SDK mismatch this raises ``RuntimeError`` with a description
    of what was tried — the caller is expected to adjust the shim.
    """
    output_csv = Path(output_csv).resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    workflow = client.workflowService.get(workflow_id)
    step = _find_step(workflow, step_id)
    if step is None:
        raise RuntimeError(
            f"workflow {workflow_id} has no step {step_id}. "
            f"Available steps: {[_id(s) for s in _attr(workflow, 'steps') or []]}"
        )

    table_id = _relation_table_id(client, step)
    if not table_id:
        raise RuntimeError(
            f"step {step_id} has no resolvable output table. The step may "
            f"not have been run yet (reset / re-run in Tercen first), or "
            f"the SDK version doesn't expose computedRelation on this step type."
        )

    df = _stream_table_to_frame(client, table_id)
    df.write_csv(output_csv)
    log.info(
        "Fetched R output: workflow=%s step=%s -> %s (%d rows × %d cols)",
        workflow_id,
        step_id,
        output_csv,
        df.height,
        df.width,
    )
    return df


def _find_step(workflow, step_id: str):
    for step in _attr(workflow, "steps") or []:
        if _id(step) == step_id:
            return step
    return None


def _relation_table_id(client, step) -> Optional[str]:
    """Pull the output table's hash out of the step's relation."""
    # The DataStep model carries either `computedRelation` (after a
    # successful run) or `relation`. Both can be an ID string or a
    # nested object with `.id`.
    for attr in ("computedRelation", "relation"):
        rel = _attr(step, attr)
        if not rel:
            continue
        if isinstance(rel, str):
            return rel
        rel_id = _attr(rel, "id")
        if rel_id:
            # If the relation is already a full object, see if it
            # carries the table hash directly.
            table_hash = _attr(rel, "tableHash") or _attr(rel, "qtHash")
            if table_hash:
                return table_hash
            # Otherwise look up the full relation by ID.
            full = _service_get(
                client,
                ["relationService", "tableSchemaService", "objectService"],
                rel_id,
            )
            for h in ("tableHash", "qtHash"):
                v = _attr(full, h)
                if v:
                    return v
            return rel_id  # best-effort fallback
    return None


def _stream_table_to_frame(client, table_id: str) -> pl.DataFrame:
    """Stream all rows of ``table_id`` into a polars DataFrame.

    The SDK historically exposes a few shapes for this; try them in
    order until one returns rows.
    """
    # Path 1: SDK helper that already builds a polars/pandas frame.
    for fn_name in ("getTable", "streamTable", "exportTable"):
        fn = getattr(client.tableSchemaService, fn_name, None) if hasattr(
            client, "tableSchemaService"
        ) else None
        if fn is None:
            continue
        try:
            result = fn(table_id)
        except Exception as e:  # noqa: BLE001
            log.debug("tableSchemaService.%s(%s) failed: %s", fn_name, table_id, e)
            continue
        df = _coerce_to_polars(result)
        if df is not None:
            return df

    # Path 2: utility helper bundled with the SDK.
    try:
        from tercen.util import helper_functions as utl  # type: ignore

        for fn_name in ("get_table_as_polars", "get_table_as_pandas", "get_table"):
            fn = getattr(utl, fn_name, None)
            if fn is None:
                continue
            try:
                result = fn(client, table_id)
            except Exception as e:  # noqa: BLE001
                log.debug("utl.%s failed: %s", fn_name, e)
                continue
            df = _coerce_to_polars(result)
            if df is not None:
                return df
    except ImportError:
        pass

    raise RuntimeError(
        "Could not stream Tercen table into a DataFrame. Tried "
        "client.tableSchemaService.{getTable,streamTable,exportTable} and "
        "tercen.util.helper_functions.{get_table_as_polars,"
        "get_table_as_pandas,get_table}. Update results.py with the "
        "method your SDK version exposes."
    )


def _coerce_to_polars(obj) -> Optional[pl.DataFrame]:
    """Best-effort conversion of whatever the SDK returned into a
    polars DataFrame. Handles polars, pandas, and dict-of-list."""
    if obj is None:
        return None
    if isinstance(obj, pl.DataFrame):
        return obj
    # pandas
    try:
        import pandas as pd  # type: ignore

        if isinstance(obj, pd.DataFrame):
            return pl.from_pandas(obj)
    except ImportError:
        pass
    if isinstance(obj, dict) and obj:
        return pl.DataFrame(obj)
    return None


def _service_get(client, service_names: Iterable[str], obj_id: str):
    for name in service_names:
        svc = getattr(client, name, None)
        if svc is None:
            continue
        getter = getattr(svc, "get", None)
        if getter is None:
            continue
        try:
            return getter(obj_id)
        except Exception:  # noqa: BLE001
            continue
    return None


def _attr(obj, name):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _id(obj) -> Optional[str]:
    return _attr(obj, "id")
