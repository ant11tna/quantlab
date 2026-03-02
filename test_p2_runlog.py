"""Test P2 runlog functionality"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from quantlab.core.runlog import (
    create_run_dir,
    write_run_metadata,
    finalize_run,
    list_runs,
    load_run_metrics,
)
import json

print("=" * 60)
print("P2 Runlog Test")
print("=" * 60)

# Test 1: Create run dir
config_text = """default:
  execution:
    participation_rate: 0.2
    min_lot: 1
"""

run_dir = create_run_dir(base='runs_test', name='test_strategy', config_text=config_text)
print(f"\n[1] Created run dir: {run_dir}")
print(f"    Run ID: {run_dir.name}")

# Test 2: Write metadata
write_run_metadata(
    run_dir=run_dir,
    config_path=None,
    config_text=config_text
)
print("\n[2] Metadata written")

# Check files
print("\n[3] Directory structure:")
for item in sorted(run_dir.rglob("*")):
    rel = item.relative_to(run_dir)
    if item.is_file():
        size = item.stat().st_size
        print(f"    {rel} ({size} bytes)")

# Test 3: Finalize with mock artifacts
equity_file = run_dir / "equity_curve.parquet"
equity_file.touch()  # Create dummy file

artifacts = {"equity_curve": equity_file}
metrics = {
    "summary": {
        "total_return": 0.15,
        "sharpe_ratio": 1.2,
        "max_drawdown": -0.08
    }
}

finalize_run(run_dir, artifacts, metrics)
print("\n[4] Run finalized")

# Test 4: List runs
runs = list_runs(base='runs_test')
print(f"\n[5] Found {len(runs)} runs:")
for r in runs:
    print(f"    {r.name}")

# Test 5: Load metrics
loaded = load_run_metrics(run_dir)
print(f"\n[6] Loaded metrics:")
print(f"    total_return: {loaded['summary']['total_return']}")

print("\n" + "=" * 60)
print("All tests passed!")
print("=" * 60)
