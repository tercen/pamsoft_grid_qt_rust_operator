"""Workflow inspection: find Grid and QT steps inside a Tercen workflow.

We identify steps by their operator's name/URL. The R operators we
compare against are:

- Grid:  ``pamsoft_grid_operator`` (github.com/pamgene/pamsoft_grid_operator)
- QT:    ``pamsoft_grid_qt_operator`` (github.com/pamgene/pamsoft_grid_qt_operator)

For each matching step we return its ``step_id`` (so the Rust dev
binary can re-fetch the same input from Tercen) plus the
``output_relation_id`` (so we can pull the R operator's result table
for diffing).

This is the only place that knows about the Tercen workflow model
JSON. Other modules take ``StepRef`` objects.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


GRID_OPERATOR_HINTS = ("pamsoft_grid_operator",)
QT_OPERATOR_HINTS = ("pamsoft_grid_qt_operator",)


@dataclass
class StepRef:
    """All the IDs the rest of the driver needs to pull data for a step."""
    workflow_id: str
    workflow_name: str
    step_id: str
    step_name: str
    stage: str  # "grid" or "qt"
    operator_name: Optional[str]
    operator_url: Optional[str]


def find_target_steps(workflow) -> List[StepRef]:
    """Return one ``StepRef`` per Grid / QT step in the workflow.

    The workflow model is the same JSON-ish structure ``execute_jq``
    exposes via the Tercen MCP — ``workflow.steps[*].model.
    operatorSettings.operatorRef.{name,url.uri}``.
    """
    out: List[StepRef] = []
    workflow_id = getattr(workflow, "id", None) or workflow.get("id")
    workflow_name = getattr(workflow, "name", None) or workflow.get("name", "")
    steps = getattr(workflow, "steps", None) or workflow.get("steps", [])
    for step in steps:
        step_kind = _get(step, "kind") or ""
        if step_kind != "DataStep":
            continue
        op = _get_op_ref(step)
        if op is None:
            continue
        op_name = op.get("name") or ""
        op_url = (op.get("url") or {}).get("uri") or ""
        stage = _classify(op_name, op_url)
        if stage is None:
            continue
        out.append(
            StepRef(
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                step_id=_get(step, "id"),
                step_name=_get(step, "name") or "",
                stage=stage,
                operator_name=op_name,
                operator_url=op_url,
            )
        )
    return out


def _classify(op_name: str, op_url: str) -> Optional[str]:
    """Decide which stage a step belongs to — or None if it's neither.

    QT is checked first because ``pamsoft_grid_qt_operator`` contains
    the ``pamsoft_grid_operator`` substring; check the more specific
    name first to avoid misclassification.
    """
    haystack = f"{op_name} {op_url}".lower()
    if any(h in haystack for h in QT_OPERATOR_HINTS):
        return "qt"
    if any(h in haystack for h in GRID_OPERATOR_HINTS):
        return "grid"
    return None


def _get(obj, attr):
    """Tiny shim: support both SDK objects (attribute access) and raw
    dicts (the kind ``execute_jq`` returns). The SDK historically goes
    back and forth on this — be permissive."""
    if isinstance(obj, dict):
        return obj.get(attr)
    return getattr(obj, attr, None)


def _get_op_ref(step):
    """Pull ``operatorRef`` out of ``step.model.operatorSettings``. Tolerate
    missing intermediate fields — non-Data steps and table steps return
    None at various levels."""
    model = _get(step, "model")
    if model is None:
        return None
    settings = _get(model, "operatorSettings")
    if settings is None:
        return None
    ref = _get(settings, "operatorRef")
    if ref is None:
        return None
    # Normalize to dict for downstream code.
    if isinstance(ref, dict):
        return ref
    return {
        "name": getattr(ref, "name", None),
        "url": {"uri": _get(_get(ref, "url"), "uri")},
    }
