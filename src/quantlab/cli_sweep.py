"""Parameter Sweep Module for QuantLab

Enables grid search over strategy parameters.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger


@dataclass
class ParameterGrid:
    """Defines a parameter grid for sweep."""
    
    @staticmethod
    def from_dict(grid_dict: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        """Generate parameter combinations from grid dict.
        
        Example:
            grid = {
                "participation_rate": [0.1, 0.2, 0.5, 1.0],
                "impact_k_bps": [0, 10, 20, 30],
                "momentum_window": [20, 60, 120]
            }
        
        Returns:
            List of parameter dicts (cartesian product)
        """
        keys = list(grid_dict.keys())
        values = [grid_dict[k] for k in keys]
        
        combinations = []
        for combo in itertools.product(*values):
            param_set = dict(zip(keys, combo))
            combinations.append(param_set)
        
        return combinations
    
    @staticmethod
    def generate_sweep_configs(
        base_config: Dict[str, Any],
        param_grid: Dict[str, List[Any]]
    ) -> List[Dict[str, Any]]:
        """Generate full configs for each parameter combination.
        
        Args:
            base_config: Base configuration dict
            param_grid: Parameter grid to sweep over
            
        Returns:
            List of config dicts (one per parameter combination)
        """
        param_combinations = ParameterGrid.from_dict(param_grid)
        configs = []
        
        for params in param_combinations:
            config = json.loads(json.dumps(base_config))  # Deep copy
            
            # Update config with sweep parameters
            for key, value in params.items():
                # Handle nested keys (e.g., "execution.participation_rate")
                keys = key.split(".")
                target = config
                for k in keys[:-1]:
                    if k not in target:
                        target[k] = {}
                    target = target[k]
                target[keys[-1]] = value
            
            # Add sweep metadata
            config["_sweep"] = {
                "parameters": params,
                "run_id_suffix": "_".join(f"{k}={v}" for k, v in params.items())
            }
            
            configs.append(config)
        
        return configs


class SweepRunner:
    """Run parameter sweeps."""
    
    def __init__(self, base_config_path: str, grid_config_path: str):
        """Initialize sweep runner.
        
        Args:
            base_config_path: Path to base config YAML
            grid_config_path: Path to parameter grid YAML
        """
        self.base_config_path = Path(base_config_path)
        self.grid_config_path = Path(grid_config_path)
        
        # Load configs
        with open(self.base_config_path, "r", encoding="utf-8-sig") as f:
            self.base_config = yaml.safe_load(f)
        
        with open(self.grid_config_path, "r", encoding="utf-8-sig") as f:
            self.grid_config = yaml.safe_load(f)
    
    def generate_runs(self) -> List[Dict[str, Any]]:
        """Generate all run configurations."""
        return ParameterGrid.generate_sweep_configs(
            self.base_config,
            self.grid_config
        )
    
    def run_all(
        self,
        parallel: bool = False,
        max_workers: int = 4
    ) -> List[str]:
        """Run all parameter combinations.
        
        Returns:
            List of run directory paths
        """
        configs = self.generate_runs()
        run_dirs = []
        
        logger.info(f"Starting sweep with {len(configs)} parameter combinations")
        
        for i, config in enumerate(configs):
            logger.info(f"Run {i+1}/{len(configs)}: {config['_sweep']['run_id_suffix']}")
            
            # Save temp config
            temp_config_path = Path(f"config/temp_sweep_{i}.yaml")
            temp_config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(temp_config_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f)
            
            # Run backtest
            try:
                run_dir = self._run_single(temp_config_path, config)
                run_dirs.append(run_dir)
            except Exception as e:
                logger.error(f"Run {i+1} failed: {e}")
            finally:
                # Cleanup temp config
                temp_config_path.unlink(missing_ok=True)
        
        # Generate comparison
        self._generate_comparison(run_dirs)
        
        return run_dirs
    
    def _run_single(
        self,
        config_path: Path,
        config: Dict[str, Any]
    ) -> str:
        """Run single backtest."""
        from quantlab.cli import cmd_backtest
        import argparse
        
        args = argparse.Namespace(
            config=str(config_path),
            verbose=False
        )
        
        # This would need to be modified to return run_dir
        # For now, just a placeholder
        cmd_backtest(args)
        
        return "runs/latest"  # Placeholder
    
    def _generate_comparison(self, run_dirs: List[str]):
        """Generate comparison report."""
        from quantlab.core.runlog import compare_runs, load_run_metrics
        
        comparison = {}
        for run_dir in run_dirs:
            metrics = load_run_metrics(Path(run_dir))
            if metrics and "summary" in metrics:
                comparison[Path(run_dir).name] = metrics["summary"]
        
        # Save comparison
        comparison_path = Path("runs/sweep_comparison.json")
        with open(comparison_path, "w", encoding="utf-8") as f:
            json.dump(comparison, f, indent=2)
        
        logger.info(f"Sweep comparison saved to {comparison_path}")


def create_example_grid():
    """Create example parameter grid YAML."""
    grid = {
        # Execution parameters
        "execution.participation_rate": [0.1, 0.2, 0.5],
        "execution.impact_k_bps": [0, 10, 20],
        
        # Strategy parameters
        "strategy.top_k": [2, 3, 5],
        "strategy.momentum_window": [60, 120],
        
        # Risk parameters
        "risk.max_weight_per_asset": [0.25, 0.30, 0.40],
    }
    
    return grid


if __name__ == "__main__":
    # Example usage
    grid = create_example_grid()
    print("Example parameter grid:")
    print(yaml.dump(grid))
    
    combinations = ParameterGrid.from_dict(grid)
    print(f"\nTotal combinations: {len(combinations)}")
    print("\nFirst 3 combinations:")
    for i, combo in enumerate(combinations[:3]):
        print(f"  {i+1}. {combo}")
