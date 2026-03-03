"""Unified data update entrypoint for UI.

This module updates raw CSV data by invoking scripts/update_data.py,
then builds curated parquet data in-process.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
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
            # Merge stderr into stdout to avoid deadlocks when updater logs heavily
            # via stderr (default logger sink) while UI is only streaming stdout.
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert proc.stdout is not None
        yield {
            "type": "start",
            "stage": "raw",
            "pid": proc.pid,
            "cmd": " ".join(cmd),
        }

        line_queue: queue.Queue[str | None] = queue.Queue()

        def _drain_stdout() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                line_queue.put(line)
            line_queue.put(None)

        reader = threading.Thread(target=_drain_stdout, daemon=True)
        reader.start()

        heartbeat_interval = 2.0
        started_at = time.time()
        last_heartbeat = 0.0

        while True:
            try:
                line = line_queue.get(timeout=0.5)
            except queue.Empty:
                if proc.poll() is None:
                    now = time.time()
                    if now - last_heartbeat >= heartbeat_interval:
                        last_heartbeat = now
                        yield {
                            "type": "heartbeat",
                            "stage": "raw",
                            "pid": proc.pid,
                            "elapsed": round(now - started_at, 1),
                        }
                elif not reader.is_alive():
                    break
                continue

            if line is None:
                break

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

        reader.join(timeout=1.0)

        rc = proc.wait()
        if rc != 0:
            raw_ok = False
            yield {
                "type": "error",
                "stage": "raw",
                "message": f"update_data exited with code {rc}",
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
