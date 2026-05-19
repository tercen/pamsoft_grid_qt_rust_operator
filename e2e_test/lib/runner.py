"""Run the local Rust ``dev`` binary against a Tercen workflow/step.

The dev binary already accepts ``WORKFLOW_ID`` and ``STEP_ID`` from the
environment and connects to Tercen via ``TERCEN_URI`` / ``TERCEN_TOKEN``.
We pass ``OUTPUT_CSV`` as well, which the operator's ``lib.rs::execute``
honours by dumping its result DataFrame to a local CSV (instead of
skipping ``save_table``).

Compile-on-first-use: the dev binary is built via ``cargo run --bin
dev``, so the first call per operator dir pays the build cost (~10 min
cold for the OpenCV transitive deps). Subsequent calls in the same
session are seconds.
"""
from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional


log = logging.getLogger(__name__)


def run_dev(
    operator_dir: Path,
    workflow_id: str,
    step_id: str,
    output_csv: Path,
    *,
    tercen_uri: str,
    tercen_token: Optional[str] = None,
    tercen_username: Optional[str] = None,
    tercen_password: Optional[str] = None,
    extra_env: Optional[dict] = None,
    log_path: Optional[Path] = None,
    timeout_seconds: int = 30 * 60,
) -> None:
    """Invoke ``cargo run --bin dev`` in ``operator_dir`` against the
    given workflow + step, dumping the result DataFrame to
    ``output_csv``.

    Raises ``subprocess.CalledProcessError`` if the binary exits
    non-zero.

    ``log_path`` captures stdout + stderr; if not set, output is
    streamed to the parent process's stderr (so failures don't go
    silently to /dev/null).
    """
    output_csv = Path(output_csv).resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["WORKFLOW_ID"] = workflow_id
    env["STEP_ID"] = step_id
    env["OUTPUT_CSV"] = str(output_csv)
    env["TERCEN_URI"] = tercen_uri
    if tercen_token:
        env["TERCEN_TOKEN"] = tercen_token
    if tercen_username and tercen_password:
        env["TERCEN_USERNAME"] = tercen_username
        env["TERCEN_PASSWORD"] = tercen_password
    if extra_env:
        env.update(extra_env)

    cmd = ["cargo", "run", "--release", "--bin", "dev"]
    log.info(
        "Running %s in %s (workflow=%s step=%s out=%s)",
        " ".join(cmd),
        operator_dir,
        workflow_id,
        step_id,
        output_csv,
    )
    if log_path:
        with open(log_path, "wb") as f:
            res = subprocess.run(
                cmd,
                cwd=operator_dir,
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT,
                timeout=timeout_seconds,
            )
    else:
        res = subprocess.run(
            cmd,
            cwd=operator_dir,
            env=env,
            timeout=timeout_seconds,
        )
    if res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode,
            cmd,
            output=f"see log at {log_path}" if log_path else None,
        )
    if not output_csv.exists():
        raise RuntimeError(
            f"dev binary exited 0 but {output_csv} was not written. "
            f"Did OUTPUT_CSV reach the operator? Check {log_path}."
        )
