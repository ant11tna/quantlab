from __future__ import annotations

import importlib.util
from pathlib import Path


CLI_PATH = Path(__file__).resolve().parents[1] / "src" / "quantlab" / "cli.py"
spec = importlib.util.spec_from_file_location("quantlab_cli_module", CLI_PATH)
module = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(module)


def test_iter_grid_combinations() -> None:
    grid = {
        "rebalance": ["monthly", "weekly"],
        "momentum_window": [60, 120, 240],
    }

    combos = list(module._iter_grid_combinations(grid))
    assert len(combos) == 6
    assert {c["rebalance"] for c in combos} == {"monthly", "weekly"}
    assert {c["momentum_window"] for c in combos} == {60, 120, 240}


def test_grid_summary_tokens() -> None:
    summary = module._summarize_grid_params({"rebalance": "monthly", "momentum_window": 120})
    assert summary == "w120_monthly"
