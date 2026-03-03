"""Unified data update entrypoint for UI.

This module updates raw CSV data by invoking scripts/update_data.py,
then builds curated parquet data in-process.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterator

from loguru import logger


def update_all_stream(force: bool = False, config_path: str = "config/data_sources.yaml") -> Iterator[Dict[str, Any]]:
    """Run raw + curated updates and yield stream events."""
    raw_ok = True
    curated_ok = True
    raw_error_count = 0
    curated_error_count = 0

    repo_root = Path(__file__).resolve().parents[3]
    src_path = str(repo_root / "src")
    env = os.environ.copy()
    old_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{old_pythonpath}" if old_pythonpath else src_path

    cmd = [sys.executable, "scripts/update_data.py", "--config", str(config_path)]
    if force:
        cmd.append("--force")

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        assert proc.stdout is not None
        for line in proc.stdout:
            payload = line.strip()
            if not payload:
                continue
            try:
                ev = json.loads(payload)
            except json.JSONDecodeError:
                yield {"type": "log", "stage": "raw", "message": payload}
                continue

            if ev.get("type") == "error" and ev.get("stage") == "raw":
                raw_error_count += 1
                raw_ok = False
            elif ev.get("type") == "done" and ev.get("stage") == "raw":
                raw_ok = bool(ev.get("ok", raw_ok))
            yield ev

        stderr_text = ""
        if proc.stderr is not None:
            stderr_text = proc.stderr.read().strip()
        rc = proc.wait()
        if rc != 0:
            raw_ok = False
            yield {
                "type": "error",
                "stage": "raw",
                "message": f"update_data exited with code {rc}" + (f" | {stderr_text[:400]}" if stderr_text else ""),
            }
            raw_error_count += 1
    except Exception as e:
        raw_ok = False
        raw_error_count += 1
        yield {"type": "error", "stage": "raw", "message": str(e)}

    try:
        from quantlab.data.curate import CuratedDataBuilder

        builder = CuratedDataBuilder()
        for ev in builder.build_all_iter(validate=True):
            if ev.get("type") == "error" and ev.get("stage") == "curated":
                curated_ok = False
                curated_error_count += 1
            elif ev.get("type") == "done" and ev.get("stage") == "curated":
                curated_ok = bool(ev.get("ok", curated_ok))
            yield ev
    except Exception as e:
        curated_ok = False
        curated_error_count += 1
        yield {"type": "error", "stage": "curated", "message": str(e)}
        yield {"type": "done", "stage": "curated", "ok": False, "done": 0, "total": 0}

    yield {
        "type": "done",
        "ok": raw_ok and curated_ok,
        "raw_ok": raw_ok,
        "curated_ok": curated_ok,
        "raw_error_count": raw_error_count,
        "curated_error_count": curated_error_count,
    }


def update_all(config_path: str | Path = "config/data_sources.yaml", force: bool = False) -> Dict[str, Any]:
    """Compatibility wrapper around update_all_stream."""
    started_at = time.time()
    raw_ok = True
    curated_ok = True
    raw_error_count = 0
    curated_error_count = 0
    curated_built_count = 0
    errors: list[str] = []

    for ev in update_all_stream(force=force, config_path=str(config_path)):
        ev_type = ev.get("type")
        if ev_type == "error":
            stage = ev.get("stage", "unknown")
            errors.append(f"{stage}: {ev.get('message', '')}")
        elif ev_type == "progress" and ev.get("stage") == "curated" and ev.get("ok"):
            curated_built_count += 1
        elif ev_type == "done" and "stage" not in ev:
            raw_ok = bool(ev.get("raw_ok", raw_ok))
            curated_ok = bool(ev.get("curated_ok", curated_ok))
            raw_error_count = int(ev.get("raw_error_count", raw_error_count))
            curated_error_count = int(ev.get("curated_error_count", curated_error_count))

    result = {
        "ok": raw_ok and curated_ok,
        "raw_updated_ok": raw_ok,
        "curated_built_count": curated_built_count,
        "raw_error_count": raw_error_count,
        "curated_error_count": curated_error_count,
        "errors": errors,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }

    if result["ok"]:
        logger.info(f"update_all finished: {result}")
    else:
        logger.error(f"update_all failed: {result}")
    return result
