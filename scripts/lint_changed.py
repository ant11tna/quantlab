#!/usr/bin/env python3
"""Run Ruff on changed Python files or explicit targets."""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Sequence

DIFF_FILTER = "ACMRTUXB"


def _run(cmd: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True)


def _changed_files(base_range: str) -> list[str]:
    result = _run([
        "git",
        "diff",
        "--name-only",
        f"--diff-filter={DIFF_FILTER}",
        base_range,
    ])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git diff failed for {base_range}")

    return [line.strip() for line in result.stdout.splitlines() if line.strip().endswith(".py")]


def _run_ruff(targets: Sequence[str]) -> int:
    cmd = [sys.executable, "-m", "ruff", "check", *targets]
    print("+", " ".join(cmd))
    return subprocess.run(cmd).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Ruff on changed Python files.")
    parser.add_argument("targets", nargs="*", help="Optional explicit files/directories to lint")
    args = parser.parse_args()

    if args.targets:
        return _run_ruff(args.targets)

    ranges = ["origin/main...HEAD", "HEAD~1...HEAD"]
    changed: list[str] = []

    for base_range in ranges:
        try:
            changed = _changed_files(base_range)
            break
        except RuntimeError:
            continue

    if not changed:
        print("no changed python files")
        return 0

    return _run_ruff(changed)


if __name__ == "__main__":
    raise SystemExit(main())
