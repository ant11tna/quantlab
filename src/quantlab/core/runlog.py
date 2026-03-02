"""Run tracking and metadata management for reproducibility.

Provides utilities to create, track, and manage experiment runs
with full metadata for reproducibility.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger




def _to_jsonable(obj: Any) -> Any:
    """Recursively convert objects into JSON-serializable built-in types."""
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(v) for v in obj]
    if hasattr(obj, "to_dict"):
        return _to_jsonable(obj.to_dict())
    if isinstance(obj, (datetime, Path)):
        return str(obj)
    # numpy/pandas scalars
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return obj
def _generate_short_hash(text: str, length: int = 6) -> str:
    """Generate short hash from text."""
    return hashlib.sha256(text.encode()).hexdigest()[:length]


def _get_git_rev() -> str:
    """Get current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "N/A"


def _get_git_commit() -> Optional[str]:
    """Get current short git commit hash for metrics metadata."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        git_commit = result.stdout.strip()
        return git_commit or None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _to_decimal(value: Any) -> Optional[float]:
    """Convert numeric-like values to decimal float for JSON output."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_metrics_v1(
    metrics_payload: Dict[str, Any],
    runtime_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalize metrics payload to standard structure v1.0 while keeping legacy keys."""
    summary = metrics_payload.get("summary", {}) if isinstance(metrics_payload.get("summary"), dict) else {}
    risk = metrics_payload.get("risk", {}) if isinstance(metrics_payload.get("risk"), dict) else {}
    trading = metrics_payload.get("trading", {}) if isinstance(metrics_payload.get("trading"), dict) else {}
    benchmark = metrics_payload.get("benchmark", {}) if isinstance(metrics_payload.get("benchmark"), dict) else {}

    config_source: Dict[str, Any] = runtime_config if isinstance(runtime_config, dict) else {}
    strategy_value = config_source.get("strategy")
    strategy_name = strategy_value.get("name") if isinstance(strategy_value, dict) else strategy_value

    config_payload = {
        "symbols": config_source.get("symbols"),
        "start": config_source.get("start"),
        "end": config_source.get("end"),
        "initial_cash": _to_decimal(config_source.get("initial_cash")),
        "rebalance": config_source.get("rebalance"),
        "fee": _to_decimal(config_source.get("fee")),
        "slippage": _to_decimal(config_source.get("slippage")),
        "strategy": strategy_name,
        "strategy_params": config_source.get("strategy_params"),
    }

    normalized = {
        "meta": {
            "schema_version": "1.0",
            "created_at": datetime.now().isoformat(),
            "git_commit": _get_git_commit(),
        },
        "config": config_payload,
        "performance": {
            "total_return": _to_decimal(summary.get("total_return")),
            "annualized_return": _to_decimal(summary.get("annualized_return")),
            "volatility": _to_decimal(summary.get("volatility")),
            "sharpe_ratio": _to_decimal(summary.get("sharpe_ratio", summary.get("sharpe"))),
            "max_drawdown": _to_decimal(summary.get("max_drawdown")),
            "calmar_ratio": _to_decimal(summary.get("calmar_ratio")),
        },
        "risk": {
            "var_95": _to_decimal(risk.get("var_95")),
            "cvar_95": _to_decimal(risk.get("cvar_95")),
            "beta": _to_decimal(risk.get("beta")),
            "alpha": _to_decimal(risk.get("alpha")),
            "skewness": _to_decimal(risk.get("skewness")),
            "kurtosis": _to_decimal(risk.get("kurtosis")),
            "max_drawdown": _to_decimal(risk.get("max_drawdown", summary.get("max_drawdown"))),
            "annualized_volatility": _to_decimal(risk.get("annualized_volatility", summary.get("volatility"))),
        },
        "trade": {
            "total_trades": trading.get("total_trades"),
            "buy_trades": trading.get("buy_trades"),
            "sell_trades": trading.get("sell_trades"),
            "total_volume": _to_decimal(trading.get("total_volume")),
            "avg_trade_size": _to_decimal(trading.get("avg_trade_size")),
            "total_fees": _to_decimal(trading.get("total_fees")),
            "total_impact_cost": _to_decimal(trading.get("total_impact_cost")),
            "impact_cost_ratio": _to_decimal(trading.get("impact_cost_ratio")),
            "turnover": _to_decimal(trading.get("turnover")),
            "avg_impact_bps": _to_decimal(trading.get("avg_impact_bps")),
        },
        "benchmark": {
            "total_return": _to_decimal(benchmark.get("total_return")),
            "annualized_return": _to_decimal(benchmark.get("annualized_return")),
            "volatility": _to_decimal(benchmark.get("volatility")),
            "max_drawdown": _to_decimal(benchmark.get("max_drawdown")),
            "tracking_error": _to_decimal(benchmark.get("tracking_error")),
            "information_ratio": _to_decimal(benchmark.get("information_ratio")),
        },
    }

    for key, value in metrics_payload.items():
        if key not in normalized:
            normalized[key] = value

    return normalized


def _get_env_info() -> Dict[str, str]:
    """Get environment information."""
    info = {
        "python_version": sys.version,
        "platform": sys.platform,
        "timestamp": datetime.now().isoformat(),
    }
    
    # Try to get pip freeze
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "freeze"],
            capture_output=True,
            text=True,
            timeout=30
        )
        info["pip_freeze"] = result.stdout if result.returncode == 0 else "N/A"
    except Exception:
        info["pip_freeze"] = "N/A"
    
    return info


def _calculate_file_hash(filepath: Path, algorithm: str = "sha256") -> str:
    """Calculate file hash for integrity verification.
    
    Args:
        filepath: Path to file
        algorithm: Hash algorithm (default: sha256)
        
    Returns:
        Hex digest of file hash
    """
    import hashlib
    
    hasher = hashlib.new(algorithm)
    
    # Read in chunks to handle large files
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):  # 8KB chunks
                hasher.update(chunk)
        return hasher.hexdigest()
    except (OSError, IOError):
        return "N/A"


def _scan_data_sources(data_dir: Path = Path("data"), include_hash: bool = True) -> List[Dict[str, Any]]:
    """Scan data directory and create manifest with optional hash.
    
    Records file info with sha256 hash for strong reproducibility.
    """
    sources = []
    
    if not data_dir.exists():
        return sources
    
    for root, dirs, files in os.walk(data_dir):
        # Skip hidden dirs and common non-data dirs
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'snapshots', 'manifests']]
        
        for file in files:
            if file.startswith('.') or file.endswith(('.py', '.pyc', '.log', '.tmp')):
                continue
            
            filepath = Path(root) / file
            try:
                stat = filepath.stat()
                entry = {
                    "path": str(filepath.relative_to(Path.cwd())),
                    "size_bytes": stat.st_size,
                    "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "ext": filepath.suffix,
                }
                
                # Calculate hash for reproducibility (P2.4)
                if include_hash and stat.st_size < 100 * 1024 * 1024:  # Skip files > 100MB
                    entry["sha256"] = _calculate_file_hash(filepath)
                
                sources.append(entry)
            except OSError:
                continue
    
    return sorted(sources, key=lambda x: x["path"])


def create_run_dir(
    base: str = "runs",
    name: Optional[str] = None,
    config_text: Optional[str] = None
) -> Path:
    """Create a new run directory with standardized structure.
    
    Args:
        base: Base directory for runs
        name: Optional strategy/job name
        config_text: Config file content for hash generation
        
    Returns:
        Path to created run directory
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Strategy name
    strategy_name = name or "backtest"
    
    # Generate short hash from config
    if config_text:
        short_hash = _generate_short_hash(config_text)
    else:
        short_hash = _generate_short_hash(timestamp)
    
    # Run ID: YYYYMMDD_HHMMSS__<name>__<hash>
    run_id = f"{timestamp}__{strategy_name}__{short_hash}"
    
    # Create directory structure
    run_dir = Path(base) / run_id
    results_dir = run_dir / "results"
    
    run_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Created run directory: {run_dir}")
    return run_dir


def write_run_metadata(
    run_dir: Path,
    config_path: Optional[Path] = None,
    config_text: Optional[str] = None,
    data_manifest: Optional[Dict] = None,
    git_rev: Optional[str] = None,
    env_info: Optional[Dict] = None
) -> None:
    """Write all run metadata files.
    
    Args:
        run_dir: Run directory path
        config_path: Path to original config file
        config_text: Config file content (alternative to config_path)
        data_manifest: Data source manifest
        git_rev: Git revision
        env_info: Environment info
    """
    # 1. Copy config.yaml (original, preserving format)
    if config_path and config_path.exists():
        config_dest = run_dir / "config.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            config_content = f.read()
        with open(config_dest, 'w', encoding='utf-8') as f:
            f.write(config_content)
        logger.info(f"Copied config to {config_dest}")
    elif config_text:
        config_dest = run_dir / "config.yaml"
        with open(config_dest, 'w', encoding='utf-8') as f:
            f.write(config_text)
        logger.info(f"Wrote config to {config_dest}")
    
    # 2. Write data_manifest.json
    if data_manifest is None:
        data_manifest = {
            "generated_at": datetime.now().isoformat(),
            "sources": _scan_data_sources()
        }
    
    manifest_path = run_dir / "data_manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(data_manifest, f, indent=2, default=str)
    logger.info(f"Wrote data manifest to {manifest_path}")
    
    # 3. Write git_rev.txt
    if git_rev is None:
        git_rev = _get_git_rev()
    
    git_path = run_dir / "git_rev.txt"
    with open(git_path, 'w', encoding="utf-8") as f:
        f.write(git_rev)
        if git_rev != "N/A":
            f.write(f"\n# {datetime.now().isoformat()}\n")
    logger.info(f"Wrote git revision to {git_path}")
    
    # 4. Write env.txt
    if env_info is None:
        env_info = _get_env_info()
    
    env_path = run_dir / "env.txt"
    with open(env_path, 'w', encoding="utf-8") as f:
        f.write(f"Python: {env_info.get('python_version', 'N/A')}\n")
        f.write(f"Platform: {env_info.get('platform', 'N/A')}\n")
        f.write(f"Timestamp: {env_info.get('timestamp', 'N/A')}\n")
        f.write("\n--- pip freeze ---\n")
        f.write(env_info.get('pip_freeze', 'N/A'))
    logger.info(f"Wrote environment info to {env_path}")
    
    # 5. Copy uv.lock if exists
    uv_lock = Path("uv.lock")
    if uv_lock.exists():
        import shutil
        shutil.copy(uv_lock, run_dir / "uv.lock")
        logger.info(f"Copied uv.lock to {run_dir}")


def finalize_run(
    run_dir: Path,
    artifacts: Dict[str, Path],
    metrics: Optional[Dict[str, Any]] = None,
    runtime_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Finalize run by writing artifacts and metrics.
    
    Args:
        run_dir: Run directory path
        artifacts: Dict of artifact name -> file path
        metrics: Optional metrics dict to write as metrics.json
        runtime_config: Optional actual runtime config to persist in metrics.json
    """
    results_dir = run_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy artifacts to results directory
    for name, source_path in artifacts.items():
        if not source_path.exists():
            logger.warning(f"Artifact not found: {source_path}")
            continue
        
        # Use original extension
        dest_path = results_dir / source_path.name
        
        # Copy file
        import shutil
        shutil.copy2(source_path, dest_path)
        logger.info(f"Copied artifact {name} to {dest_path}")
    
    # Write metrics.json
    if metrics:
        raw_payload = _to_jsonable(metrics)
        metrics_payload = _build_metrics_v1(
            raw_payload if isinstance(raw_payload, dict) else {},
            runtime_config=runtime_config,
        )

        metrics_path = results_dir / "metrics.json"
        with open(metrics_path, 'w', encoding="utf-8") as f:
            json.dump(metrics_payload, f, indent=2, ensure_ascii=False)
        logger.info(f"Wrote metrics to {metrics_path}")
    
    # Write completion marker
    completion_path = run_dir / "completed"
    with open(completion_path, 'w', encoding="utf-8") as f:
        f.write(datetime.now().isoformat())
    logger.info(f"Run finalized: {run_dir}")


def list_runs(base: str = "runs") -> List[Path]:
    """List all run directories.
    
    Args:
        base: Base directory for runs
        
    Returns:
        List of run directory paths, sorted by name (newest first)
    """
    base_path = Path(base)
    if not base_path.exists():
        return []
    
    runs = [d for d in base_path.iterdir() if d.is_dir()]
    return sorted(runs, reverse=True)


def load_run_metrics(run_dir: Path) -> Optional[Dict[str, Any]]:
    """Load metrics from a run directory.
    
    Args:
        run_dir: Run directory path
        
    Returns:
        Metrics dict or None if not found
    """
    metrics_path = run_dir / "results" / "metrics.json"
    if not metrics_path.exists():
        # Try legacy location
        metrics_path = run_dir / "metrics.json"
    
    if metrics_path.exists():
        with open(metrics_path, 'r', encoding="utf-8") as f:
            return json.load(f)
    return None


def compare_runs(run_dirs: List[Path]) -> Dict[str, Dict[str, Any]]:
    """Compare metrics across multiple runs.
    
    Args:
        run_dirs: List of run directories to compare
        
    Returns:
        Dict mapping metric name -> {run_id: value}
    """
    comparison = {}
    
    for run_dir in run_dirs:
        run_id = run_dir.name
        metrics = load_run_metrics(run_dir)
        
        if metrics and "summary" in metrics:
            for key, value in metrics["summary"].items():
                if key not in comparison:
                    comparison[key] = {}
                comparison[key][run_id] = value
    
    return comparison


def verify_data_manifest(run_dir: Path) -> Dict[str, Any]:
    """Verify data integrity against manifest.
    
    Compares current file hashes with recorded hashes in manifest.
    
    Args:
        run_dir: Run directory containing data_manifest.json
        
    Returns:
        Verification report dict
    """
    manifest_path = run_dir / "data_manifest.json"
    
    if not manifest_path.exists():
        return {"status": "error", "message": "data_manifest.json not found"}
    
    with open(manifest_path, 'r', encoding="utf-8") as f:
        manifest = json.load(f)
    
    sources = manifest.get("sources", [])
    mismatches = []
    verified = []
    missing = []
    
    for entry in sources:
        filepath = Path(entry["path"])
        expected_hash = entry.get("sha256")
        
        if not filepath.exists():
            missing.append(entry["path"])
            continue
        
        if expected_hash and expected_hash != "N/A":
            current_hash = _calculate_file_hash(filepath)
            if current_hash != expected_hash:
                mismatches.append({
                    "path": str(filepath),
                    "expected": expected_hash[:16] + "...",
                    "current": current_hash[:16] + "..."
                })
            else:
                verified.append(str(filepath))
        else:
            # No hash to verify, just check existence
            verified.append(str(filepath))
    
    return {
        "status": "ok" if not mismatches and not missing else "warning",
        "total_files": len(sources),
        "verified": len(verified),
        "mismatches": mismatches,
        "missing": missing
    }


def format_verification_report(report: Dict[str, Any]) -> str:
    """Format verification report for display.
    
    Args:
        report: Verification report from verify_data_manifest()
        
    Returns:
        Formatted string
    """
    lines = [
        "=" * 60,
        "Data Integrity Verification",
        "=" * 60,
        f"Status: {report['status'].upper()}",
        f"Total files: {report['total_files']}",
        f"Verified: {report['verified']}",
    ]
    
    if report['mismatches']:
        lines.append("\nHash Mismatches (DATA CHANGED):")
        for m in report['mismatches']:
            lines.append(f"  ⚠ {m['path']}")
    
    if report['missing']:
        lines.append("\nMissing Files:")
        for f in report['missing']:
            lines.append(f"  ✗ {f}")
    
    if not report['mismatches'] and not report['missing']:
        lines.append("\n✓ All files verified successfully!")
    
    lines.append("=" * 60)
    return "\n".join(lines)
