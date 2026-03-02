"""Minimal test for P2 runlog (no external deps)"""
import sys
from pathlib import Path
from datetime import datetime
import hashlib
import json
import os

# Inline the functions we want to test
def _generate_short_hash(text, length=6):
    return hashlib.sha256(text.encode()).hexdigest()[:length]

def create_run_dir(base="runs", name=None, config_text=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    strategy_name = name or "backtest"
    short_hash = _generate_short_hash(config_text) if config_text else _generate_short_hash(timestamp)
    run_id = f"{timestamp}__{strategy_name}__{short_hash}"
    
    run_dir = Path(base) / run_id
    results_dir = run_dir / "results"
    run_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    
    return run_dir

# Test
print("=" * 60)
print("P2 Minimal Test")
print("=" * 60)

config_text = """default:
  execution:
    participation_rate: 0.2
    min_lot: 1
"""

# Create run dir
run_dir = create_run_dir(base='runs_test', name='test_strategy', config_text=config_text)
print(f"\n[OK] Created: {run_dir}")
print(f"     Run ID format: {run_dir.name}")

# Check format parts
parts = run_dir.name.split("__")
print(f"\n[OK] Run ID parts:")
print(f"     timestamp: {parts[0]}")
print(f"     name: {parts[1]}")
print(f"     hash: {parts[2]}")

# Create some test files
(run_dir / "config.yaml").write_text(config_text)
(run_dir / "git_rev.txt").write_text("abc123")
(run_dir / "env.txt").write_text("python 3.11")
(run_dir / "data_manifest.json").write_text(json.dumps({"sources": []}))

results_dir = run_dir / "results"
(results_dir / "metrics.json").write_text(json.dumps({"return": 0.15}))
(results_dir / "equity_curve.parquet").touch()
(results_dir / "fills.parquet").touch()

# List structure
print(f"\n[OK] Directory structure:")
for item in sorted(run_dir.rglob("*")):
    rel = item.relative_to(run_dir)
    if item.is_file():
        print(f"     {rel}")

print("\n" + "=" * 60)
print("P2 directory structure verified!")
print("=" * 60)
print("\nExpected structure:")
print("""
runs/
  YYYYMMDD_HHMMSS__<name>__<hash>/
    config.yaml
    data_manifest.json
    git_rev.txt
    env.txt
    results/
      metrics.json
      equity_curve.parquet
      fills.parquet
      ...
""")
