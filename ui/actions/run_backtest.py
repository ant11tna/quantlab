import os
import subprocess
import sys
from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    """Find repository root by walking upward from ``start`` or this file location."""
    current = (start or Path(__file__)).resolve()
    if current.is_file():
        current = current.parent

    for directory in [current, *current.parents]:
        if (directory / "pyproject.toml").exists():
            return directory
        if (directory / ".git").exists():
            return directory
        if (directory / "README.md").exists():
            return directory

    # Fallback: if markers are missing, assume this file lives in <repo>/ui/actions/.
    return Path(__file__).resolve().parent.parent


def iter_backtest_output():
    repo_root = find_repo_root()
    env = os.environ.copy()
    src = str(repo_root / "src")
    sep = ";" if os.name == "nt" else ":"
    env["PYTHONPATH"] = src + sep + env.get("PYTHONPATH", "")

    cmd = [sys.executable, str(repo_root / "examples" / "run_ashare_backtest.py")]
    p = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    assert p.stdout is not None
    for line in p.stdout:
        yield line.rstrip("\n")

    rc = p.wait()
    yield f"[process-exit] returncode={rc}"
    return rc


def run_backtest_and_collect() -> tuple[int, list[str]]:
    """Run backtest and collect all output lines for non-streaming callers."""
    lines = list(iter_backtest_output())

    rc = 0
    if lines:
        prefix = "[process-exit] returncode="
        last = lines[-1]
        if isinstance(last, str) and last.startswith(prefix):
            try:
                rc = int(last[len(prefix) :])
            except ValueError:
                rc = 1

    return rc, lines
