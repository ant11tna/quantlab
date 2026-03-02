"""CLI package compatibility helpers.

`quantlab` console script points to `quantlab.cli:main`.
This package-level `main` forwards to legacy module `src/quantlab/cli.py`.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def main() -> int:
    legacy_cli_path = Path(__file__).resolve().parents[1] / "cli.py"
    spec = importlib.util.spec_from_file_location("quantlab_legacy_cli", legacy_cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load CLI module from {legacy_cli_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main()


__all__ = ["main"]
