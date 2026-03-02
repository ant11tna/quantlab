"""Build data version metadata from curated dataset files.

Scans data/curated (or a custom directory), computes a deterministic fingerprint from
relative path + mtime + size for each file, and writes data/data_version.json.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _to_iso8601(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _scan_curated_files(curated_dir: Path) -> List[Path]:
    if not curated_dir.exists():
        return []
    return sorted(p for p in curated_dir.rglob("*") if p.is_file())


def _build_fingerprint(files: List[Path], curated_dir: Path) -> str:
    hasher = hashlib.sha256()
    for path in files:
        stat = path.stat()
        rel = path.relative_to(curated_dir).as_posix()
        line = f"{rel}|{stat.st_mtime_ns}|{stat.st_size}\n"
        hasher.update(line.encode("utf-8"))
    return hasher.hexdigest()


def build_data_version(curated_dir: Path, out_file: Path) -> Dict[str, object]:
    files = _scan_curated_files(curated_dir)

    latest_ts: Optional[float] = None
    if files:
        latest_ts = max(path.stat().st_mtime for path in files)

    payload: Dict[str, object] = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "curated_latest_ts": _to_iso8601(latest_ts) if latest_ts is not None else None,
        "file_count": len(files),
        "fingerprint": _build_fingerprint(files, curated_dir),
    }

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build data/data_version.json from curated data")
    parser.add_argument("--curated-dir", type=Path, default=Path("data/curated"), help="Curated data root")
    parser.add_argument("--out", type=Path, default=Path("data/data_version.json"), help="Output json path")
    args = parser.parse_args()

    payload = build_data_version(args.curated_dir, args.out)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
