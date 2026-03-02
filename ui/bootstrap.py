"""UI bootstrap for src-layout imports.

Ensure ``repo_root/src`` is available on ``sys.path`` so ``import quantlab``
works reliably across all launch methods.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

src_path = str(SRC)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

