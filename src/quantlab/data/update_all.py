"""Unified data update entrypoint for UI.

This module updates raw CSV data by invoking scripts/update_data.py,
then builds curated parquet data in-process.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict

from loguru import logger

from quantlab.data.curate import CuratedDataBuilder


def update_all(config_path: str | Path = "config/data_sources.yaml", force: bool = False) -> Dict[str, Any]:
    """Update raw data then rebuild curated parquet.

    Args:
        config_path: path to data source config for scripts/update_data.py
        force: whether to force full refresh for raw data

    Returns:
        Structured result dict:
            {
                "ok": bool,
                "raw_updated_ok": bool,
                "curated_built_count": int,
                "errors": list[str],
                "elapsed_seconds": float,
            }
    """

    started_at = time.time()
    errors: list[str] = []
    raw_updated_ok = False
    curated_built_count = 0

    repo_root = Path(__file__).resolve().parents[3]
    src_path = str(repo_root / "src")
    env = os.environ.copy()
    old_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{old_pythonpath}" if old_pythonpath else src_path

    cmd = [sys.executable, "scripts/update_data.py", "--config", str(config_path)]
    if force:
        cmd.append("--force")

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            raw_updated_ok = True
        else:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            errors.append(
                "raw update failed"
                + (f" | stderr: {stderr}" if stderr else "")
                + (f" | stdout: {stdout}" if stdout else "")
            )
    except Exception as e:
        errors.append(f"raw update exception: {e}")

    if raw_updated_ok:
        try:
            builder = CuratedDataBuilder()
            built = builder.build_all(validate=True)
            curated_built_count = len(built)
        except Exception as e:
            errors.append(f"curated build exception: {e}")

    elapsed_seconds = round(time.time() - started_at, 3)
    ok = raw_updated_ok and not errors

    result = {
        "ok": ok,
        "raw_updated_ok": raw_updated_ok,
        "curated_built_count": curated_built_count,
        "errors": errors,
        "elapsed_seconds": elapsed_seconds,
    }

    if ok:
        logger.info(f"update_all finished: {result}")
    else:
        logger.error(f"update_all failed: {result}")

    return result
