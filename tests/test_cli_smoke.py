from __future__ import annotations

import json
import subprocess
from pathlib import Path


def test_cli_backtest_smoke() -> None:
    runs_dir = Path("runs")
    runs_dir.mkdir(exist_ok=True)

    before = {p.name for p in runs_dir.iterdir() if p.is_dir()}

    cmd = [
        "uv",
        "run",
        "quantlab",
        "backtest",
        "--config",
        "config/backtest.yaml",
        "--profile",
        "china_ashare",
    ]
    subprocess.run(cmd, check=True)

    after = {p.name for p in runs_dir.iterdir() if p.is_dir()}
    new_runs = sorted(after - before)
    assert new_runs, "expected a newly created run directory under runs/"

    run_dir = runs_dir / new_runs[-1]
    results_dir = run_dir / "results"

    metrics_path = results_dir / "metrics.json"
    equity_path = results_dir / "equity_curve.parquet"
    weights_path = results_dir / "weights.parquet"

    assert metrics_path.exists(), f"missing {metrics_path}"
    assert equity_path.exists(), f"missing {equity_path}"
    assert weights_path.exists(), f"missing {weights_path}"

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    for key in ["total_return", "max_drawdown", "sharpe"]:
        assert key in metrics.get("summary", {}) or key in metrics, f"missing metric key: {key}"
