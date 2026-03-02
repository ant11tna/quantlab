"""Backtest engine.

Event-driven backtesting with realistic execution simulation.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from quantlab.core.time import TradingCalendar
from quantlab.core.types import (
    Bar,
    Order,
    OrderIntent,
    OrderType,
    PortfolioState,
    RunConfig,
    Side,
    TargetWeight,
)
from quantlab.backtest.broker_sim import SimulatedBroker, load_fee_model, ExecutionConfig
from quantlab.backtest.metrics import MetricsCalculator
from quantlab.execution.router import ExecutionRouter
from quantlab.research.portfolio import PortfolioBuilder, WeightOptimizer, WeightConstraints
from quantlab.research.risk_constraints import RiskConstraintChecker, RiskConstraintConfig
from quantlab.research.strategies.base import Strategy


class BacktestEngine:
    """Event-driven backtest engine."""
    
    def __init__(
        self,
        strategy: Strategy,
        initial_cash: float = 1_000_000.0,
        fee_model: str = "us_etfs",
        exec_config: Optional[Dict] = None,
        calendar: str = "XNYS",
        risk_config: Optional[RiskConstraintConfig] = None,
        constraint_mode: str = "clip"
    ) -> None:
        """Initialize backtest engine.
        
        Args:
            strategy: Trading strategy
            initial_cash: Initial capital
            fee_model: Fee model name
            exec_config: Execution config dict with participation_rate, lot_size, min_trade_qty
            calendar: Trading calendar
            risk_config: Risk constraint configuration
            constraint_mode: "clip" to auto-correct weights, "strict" to raise on violation
        """
        self.strategy = strategy
        self.initial_cash = Decimal(str(initial_cash))
        self.fee_model = fee_model
        self.exec_config = exec_config
        self.calendar = TradingCalendar(calendar)
        self.constraint_mode = constraint_mode
        
        # Initialize broker and router (will be created in run())
        self.broker: Optional[SimulatedBroker] = None
        self.router: Optional[ExecutionRouter] = None
        
        # Initialize PortfolioBuilder with risk constraints
        self.builder = self._create_portfolio_builder(risk_config)
        self.metrics_calc = MetricsCalculator()
        
        # Results storage
        self.equity_curve: List[Dict] = []
        self.weights_history: List[Dict] = []
        self.rebalance_events: List[datetime] = []
        
        # P2: Three-way reconciliation data
        self.targets_history: List[Dict] = []  # TargetWeight records
        self.orders_history: List[Dict] = []   # Order records
        
        # P3: Data contract tracking
        self.data_contract: str = "unknown"  # Set in run() via validate_bars_df()
        
        # Run metadata for reproducibility
        self.risk_config = risk_config
    
    def _create_portfolio_builder(
        self, 
        risk_config: Optional[RiskConstraintConfig]
    ) -> PortfolioBuilder:
        """Create PortfolioBuilder with risk constraints integrated.
        
        Args:
            risk_config: Risk constraint configuration
            
        Returns:
            Configured PortfolioBuilder
        """
        # Create risk checker if config provided
        risk_checker = None
        if risk_config:
            # Set strict_mode based on constraint_mode
            config = RiskConstraintConfig(
                max_weight_per_asset=risk_config.max_weight_per_asset,
                min_weight_per_asset=risk_config.min_weight_per_asset,
                max_leverage=risk_config.max_leverage,
                min_cash_ratio=risk_config.min_cash_ratio,
                max_cash_ratio=risk_config.max_cash_ratio,
                max_turnover=risk_config.max_turnover,
                max_annual_turnover=risk_config.max_annual_turnover,
                max_sector_weight=risk_config.max_sector_weight,
                enabled=risk_config.enabled,
                strict_mode=(self.constraint_mode == "strict")
            )
            risk_checker = RiskConstraintChecker(config)
        
        # Create optimizer with turnover constraint
        weight_constraints = None
        if risk_config and risk_config.max_turnover:
            weight_constraints = WeightConstraints(
                max_turnover=risk_config.max_turnover
            )
        optimizer = WeightOptimizer(constraints=weight_constraints)
        
        return PortfolioBuilder(
            optimizer=optimizer,
            risk_checker=risk_checker
        )
    
    def run(
        self,
        data: pd.DataFrame,
        rebalance_freq: Optional[str] = "M",
        rebalance_threshold: Optional[float] = None,
        progress: bool = True
    ) -> Dict:
        """Run backtest.
        
        Args:
            data: Price data DataFrame (columns: ts, symbol, open, high, low, close, volume)
            rebalance_freq: Rebalance frequency (M=monthly, Q=quarterly)
            rebalance_threshold: Deviation threshold for rebalancing
            progress: Show progress bar
            
        Returns:
            Backtest results dictionary
        """
        logger.info("Starting backtest...")
        
        # P3: Data contract validation
        from quantlab.data.schema import validate_bars_df, SCHEMA_RAW_MINIMAL, SCHEMA_CURATED_V1
        is_valid, schema_version, message = validate_bars_df(data, strict=False)
        
        if not is_valid:
            raise ValueError(f"Invalid data schema: {message}")
        
        self.data_contract = schema_version
        logger.info(f"Data contract: {schema_version} - {message}")
        
        # Warn if using raw_minimal (not curated)
        if schema_version == SCHEMA_RAW_MINIMAL:
            logger.warning("Using raw_minimal data contract. Trading regime constraints may not work.")
        
        # Initialize broker and router with execution config
        fee_config = load_fee_model(self.fee_model)
        exec_conf = ExecutionConfig.from_dict(self.exec_config)
        self.broker = SimulatedBroker(self.initial_cash, fee_config, exec_conf)
        self.router = ExecutionRouter(self.broker)
        
        # Prepare data
        data = data.copy()
        data["ts"] = pd.to_datetime(data["ts"])
        data = data.sort_values(["ts", "symbol"])
        
        # Get unique dates and symbols
        dates = data["ts"].unique()
        symbols = data["symbol"].unique().tolist()
        
        # Get rebalance dates
        if rebalance_freq:
            start = pd.Timestamp(dates[0])
            end = pd.Timestamp(dates[-1])
            rebalance_dates = set(
                self.calendar.get_rebalance_dates(start, end, rebalance_freq)
            )
        else:
            rebalance_dates = set()
        
        logger.info(f"Running from {dates[0]} to {dates[-1]}")
        logger.info(f"Rebalance dates: {len(rebalance_dates)}")
        
        # Main loop
        current_weights: Dict[str, float] = {}
        last_rebalance: Optional[datetime] = None
        
        iterator = dates
        if progress:
            from tqdm import tqdm
            iterator = tqdm(dates, desc="Backtesting")
        
        for ts in iterator:
            ts = pd.Timestamp(ts)
            
            # Get today's bars
            day_data = data[data["ts"] == ts]
            bars_with_rows = self._create_bars(day_data)
            bars = {s: bar for s, (bar, _) in bars_with_rows.items()}
            prices = {s: float(bar.close) for s, (bar, _) in bars_with_rows.items()}
            
            # Process any pending orders (pass full rows with regime fields)
            if self.broker:
                self.broker.process_orders(ts, bars_with_rows)
            
            # Check rebalance
            needs_rebalance = False
            
            # Time-based rebalance
            if ts in rebalance_dates:
                needs_rebalance = True
            
            # Threshold-based rebalance
            if rebalance_threshold is not None and last_rebalance is not None:
                current_portfolio = self.broker.get_portfolio_state(ts)
                nav = float(current_portfolio.nav)
                
                if nav > 0:
                    deviation = self._calculate_weight_deviation(
                        current_portfolio, prices, current_weights, nav
                    )
                    if deviation > rebalance_threshold:
                        needs_rebalance = True
                        logger.debug(f"Threshold rebalance triggered: {deviation:.2%}")
            
            # Execute rebalance if needed
            if needs_rebalance or (rebalance_dates and ts == min(rebalance_dates)):
                targets = self._rebalance(ts, data, symbols, prices)
                current_weights = {t.symbol: float(t.target_weight) for t in targets}
                last_rebalance = ts
                self.rebalance_events.append(ts)
            
            # Record state
            self._record_state(ts, prices)
        
        logger.info("Backtest completed")
        
        return self._compile_results()
    
    def _create_bars(self, day_data: pd.DataFrame) -> Dict[str, tuple[Bar, pd.Series]]:
        """Create Bar objects and preserve full row data from DataFrame.
        
        Returns dict of symbol -> (Bar, full_row_series).
        The full_row_series contains all curated fields (is_suspended, can_buy, etc.)
        """
        bars = {}
        for _, row in day_data.iterrows():
            bar = Bar(
                ts=row["ts"],
                symbol=row["symbol"],
                open_=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=Decimal(str(row.get("volume", 0)))
            )
            # Pass full row series for constraint checking (includes regime fields)
            bars[row["symbol"]] = (bar, row)
        return bars
    
    def _rebalance(
        self,
        ts: datetime,
        data: pd.DataFrame,
        symbols: List[str],
        prices: Dict[str, float]
    ) -> List[TargetWeight]:
        """Execute portfolio rebalance and record targets/orders for reconciliation."""
        # Get historical data up to current time
        hist_data = data[data["ts"] <= ts]
        
        # Get current state
        current_state = self.broker.get_portfolio_state(ts)
        
        # Generate targets from strategy
        targets = self.strategy.on_rebalance(hist_data, ts)
        
        if not targets:
            return []
        
        # P2: Record targets for reconciliation
        for t in targets:
            self.targets_history.append({
                "ts": ts,
                "symbol": t.symbol,
                "target_weight": float(t.target_weight),
                "source": t.source,
            })
        
        # Build portfolio
        quantities = self.builder.build_from_targets(
            targets, prices, float(current_state.nav), current_state
        )
        
        # Calculate orders
        orders = self.builder.calculate_orders(quantities, current_state)
        
        # Submit orders and record for reconciliation
        for symbol, qty in orders:
            side = Side.BUY if qty > 0 else Side.SELL
            order = Order(
                id="",  # Will be assigned by broker
                ts=ts,
                symbol=symbol,
                side=side,
                qty=Decimal(str(abs(qty))),
                order_type=OrderType.MARKET,
                strategy_id=self.strategy.name
            )
            self.router.submit_order(order)
            
            # P2: Record order for reconciliation
            self.orders_history.append({
                "ts": ts,
                "symbol": symbol,
                "side": side.name,
                "order_qty": abs(qty),
                "strategy_id": self.strategy.name,
            })
        
        logger.info(f"Rebalance at {ts}: {len(orders)} orders")
        
        return targets
    
    def _calculate_weight_deviation(
        self,
        portfolio: PortfolioState,
        prices: Dict[str, float],
        target_weights: Dict[str, float],
        nav: float
    ) -> float:
        """Calculate maximum weight deviation."""
        max_deviation = 0.0
        
        all_symbols = set(target_weights.keys()) | set(portfolio.positions.keys())
        
        for symbol in all_symbols:
            position = portfolio.positions.get(symbol)
            current_qty = float(position.qty) if position else 0.0
            current_value = current_qty * prices.get(symbol, 0.0)
            current_weight = current_value / nav if nav > 0 else 0.0
            
            target = target_weights.get(symbol, 0.0)
            deviation = abs(current_weight - target)
            max_deviation = max(max_deviation, deviation)
        
        return max_deviation
    
    def _record_state(self, ts: datetime, prices: Dict[str, float]) -> None:
        """Record portfolio state."""
        state = self.broker.get_portfolio_state(ts)
        
        # Calculate weights
        nav = float(state.nav)
        weights = {}
        for symbol, pos in state.positions.items():
            if symbol in prices and nav > 0:
                weights[symbol] = float(pos.qty) * prices[symbol] / nav
        
        # Equity curve
        self.equity_curve.append({
            "ts": ts,
            "nav": float(state.nav),
            "cash": float(state.cash),
            "positions_value": float(state.nav - state.cash),
        })
        
        # Weights
        weight_record = {"ts": ts}
        weight_record.update(weights)
        self.weights_history.append(weight_record)
    
    def _compile_results(self) -> Dict:
        """Compile backtest results including targets/orders for reconciliation."""
        equity_df = pd.DataFrame(self.equity_curve)
        weights_df = pd.DataFrame(self.weights_history)
        trades_df = self.broker.get_trades_df()
        
        # P2: Compile reconciliation data
        targets_df = pd.DataFrame(self.targets_history) if self.targets_history else pd.DataFrame()
        orders_df = pd.DataFrame(self.orders_history) if self.orders_history else pd.DataFrame()
        
        # Calculate metrics
        metrics = self.metrics_calc.calculate(equity_df, trades_df)
        
        results = {
            "equity_curve": equity_df,
            "weights": weights_df,
            "trades": trades_df,
            "targets": targets_df,      # P2: For reconciliation
            "orders": orders_df,        # P2: For reconciliation
            "metrics": metrics,
            "rebalance_events": self.rebalance_events,
            "final_nav": equity_df["nav"].iloc[-1] if len(equity_df) > 0 else self.initial_cash,
            "data_contract": self.data_contract,  # P3: Data schema version
        }
        
        # Attach risk config for save_run to persist
        results["_risk_config"] = self.risk_config
        results["_constraint_mode"] = self.constraint_mode
        
        return results
    
    def save_run(
        self,
        results: Dict,
        run_id: Optional[str] = None,
        output_dir: Optional[Path] = None,
        backtest_config: Optional[Dict] = None,
        data: Optional[pd.DataFrame] = None,
    ) -> Path:
        """Save run results to disk with full configuration for reproducibility.
        
        Args:
            results: Backtest results
            run_id: Optional run ID
            output_dir: Output directory
            backtest_config: Full backtest configuration dict (for YAML persistence)
            data: Optional OHLCV DataFrame used in backtest (to save symbol bars)
            
        Returns:
            Path to run directory
        """
        import re
        
        if run_id is None:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        
        if output_dir is None:
            output_dir = Path("runs") / run_id
        else:
            output_dir = Path(output_dir) / run_id
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create results subdirectory
        results_dir = output_dir / "results"
        results_dir.mkdir(exist_ok=True)
        
        # Save equity curve
        results["equity_curve"].to_parquet(results_dir / "equity_curve.parquet", index=False)
        
        # Save weights
        results["weights"].to_parquet(results_dir / "weights.parquet", index=False)
        
        # Save trades
        if not results["trades"].empty:
            results["trades"].to_parquet(results_dir / "trades.parquet", index=False)
        
        # P2: Save targets and orders for reconciliation
        if not results["targets"].empty:
            results["targets"].to_parquet(results_dir / "targets.parquet", index=False)
        if not results["orders"].empty:
            results["orders"].to_parquet(results_dir / "orders.parquet", index=False)
        
        # Save rejected orders for reconciliation (zero-fill rejections)
        rejected_df = self.broker.get_rejected_orders_df() if self.broker else pd.DataFrame()
        if not rejected_df.empty:
            rejected_df.to_parquet(results_dir / "rejected_orders.parquet", index=False)
        
        # Save metrics.json for UI and reproducibility
        import json
        metrics_path = results_dir / "metrics.json"
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(results["metrics"], f, indent=2, default=str)
        
        # Save bars if data provided
        if data is not None and not data.empty:
            self._save_bars(data, output_dir)
        
        # Save full configuration for reproducibility
        run_config = self._compile_run_config(backtest_config, results)
        config_path = output_dir / "config.yaml"
        self._save_yaml_config(run_config, config_path)
        
        logger.info(f"Saved run to {output_dir}")
        return output_dir
    
    def _save_bars(self, data: pd.DataFrame, output_dir: Path) -> None:
        """Save OHLCV bars per symbol using optimized groupby.
        
        Args:
            data: DataFrame with OHLCV data (must have 'symbol' column)
            output_dir: Run output directory
        """
        import re
        import json
        
        if "symbol" not in data.columns:
            logger.warning("No 'symbol' column in data, skipping bars save")
            return
        
        # Create bars subdirectory
        bars_dir = output_dir / "bars"
        bars_dir.mkdir(exist_ok=True)
        
        # Build symbol -> filename mapping
        symbol_to_file = {}
        
        # Get unique symbols first to log count
        unique_symbols = data["symbol"].unique()
        logger.info(f"Saving bars for {len(unique_symbols)} symbols to {bars_dir}")
        
        # Use groupby for O(N) performance instead of O(N*S)
        for symbol, bars in data.groupby("symbol", sort=False):
            # Sanitize symbol for safe filename
            sanitized = re.sub(r'[^A-Za-z0-9_-]', '_', str(symbol))
            filename = f"{sanitized}.parquet"
            filepath = bars_dir / filename
            
            # Prepare bars data - only keep OHLCV columns
            bars_clean = bars.copy()
            
            # Normalize column names
            col_mapping = {
                "timestamp": "ts",
                "date": "ts",
                "datetime": "ts",
                "open_": "open",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "vol": "volume",
            }
            bars_clean = bars_clean.rename(columns=col_mapping)
            
            # Ensure required columns exist
            for col in ["open", "high", "low", "close"]:
                if col not in bars_clean.columns:
                    logger.warning(f"Missing {col} for {symbol}, skipping")
                    continue
            
            if "volume" not in bars_clean.columns:
                bars_clean["volume"] = 0
            
            # Select and order columns
            cols_to_save = ["ts", "open", "high", "low", "close", "volume"]
            cols_available = [c for c in cols_to_save if c in bars_clean.columns]
            bars_clean = bars_clean[cols_available].copy()
            
            # Ensure ts is datetime
            if "ts" in bars_clean.columns:
                bars_clean["ts"] = pd.to_datetime(bars_clean["ts"])
            
            # Save with compression
            try:
                bars_clean.to_parquet(
                    filepath,
                    index=False,
                    compression="zstd",  # Good balance of speed and compression
                )
                symbol_to_file[str(symbol)] = filename
                logger.debug(f"Saved {len(bars_clean)} bars for {symbol} -> {filename}")
            except Exception as e:
                logger.error(f"Failed to save bars for {symbol}: {e}")
        
        # Save index mapping
        if symbol_to_file:
            index_path = bars_dir / "bars_index.json"
            try:
                with open(index_path, 'w', encoding="utf-8") as f:
                    json.dump({
                        "symbol_to_file": symbol_to_file,
                        "file_to_symbol": {v: k for k, v in symbol_to_file.items()},
                        "count": len(symbol_to_file),
                    }, f, indent=2)
                logger.info(f"Saved bars_index.json with {len(symbol_to_file)} mappings")
            except Exception as e:
                logger.error(f"Failed to save bars_index.json: {e}")
    
    def _compile_run_config(
        self, 
        backtest_config: Optional[Dict],
        results: Dict
    ) -> Dict:
        """Compile complete run configuration for reproducibility.
        
        Args:
            backtest_config: User-provided backtest config
            results: Backtest results with internal config
            
        Returns:
            Complete configuration dictionary
        """
        import git
        
        config = {
            "run_info": {
                "timestamp": datetime.now().isoformat(),
                "strategy": self.strategy.name if hasattr(self.strategy, 'name') else type(self.strategy).__name__,
                "initial_cash": float(self.initial_cash),
                "fee_model": self.fee_model,
                "data_contract": self.data_contract,  # P3: Record data schema version
            },
            "execution": self.exec_config or {},
            "risk_constraints": {},
            "constraint_enforcement": {}
        }
        
        # Add risk constraints from risk_config
        risk_cfg = results.get("_risk_config") or self.risk_config
        if risk_cfg:
            config["risk_constraints"] = {
                "max_position_weight": risk_cfg.max_weight_per_asset,
                "min_position_weight": risk_cfg.min_weight_per_asset,
                "max_leverage": risk_cfg.max_leverage,
                "min_cash_ratio": risk_cfg.min_cash_ratio,
                "max_cash_ratio": risk_cfg.max_cash_ratio,
                "max_turnover": risk_cfg.max_turnover,
                "max_annual_turnover": risk_cfg.max_annual_turnover,
                "max_sector_weight": risk_cfg.max_sector_weight,
                "enabled": risk_cfg.enabled,
                "strict_mode": risk_cfg.strict_mode if hasattr(risk_cfg, 'strict_mode') else False,
            }
        
        # Add constraint enforcement mode
        config["constraint_enforcement"] = {
            "mode": results.get("_constraint_mode", self.constraint_mode),
            "description": "clip=auto-scale weights, strict=raise exception"
        }
        
        # Add git revision for code reproducibility
        try:
            repo = git.Repo(search_parent_directories=True)
            config["run_info"]["git_revision"] = repo.head.object.hexsha
            config["run_info"]["git_branch"] = repo.active_branch.name
        except Exception:
            config["run_info"]["git_revision"] = "unknown"
        
        # Merge with user-provided config
        if backtest_config:
            config["user_config"] = backtest_config
        
        return config
    
    def _save_yaml_config(self, config: Dict, path: Path) -> None:
        """Save configuration to YAML file.
        
        Args:
            config: Configuration dictionary
            path: Output file path
        """
        import yaml
        
        # Convert dataclasses to dicts for YAML serialization
        def convert(obj):
            if hasattr(obj, '__dataclass_fields__'):
                return {k: convert(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert(v) for v in obj]
            elif isinstance(obj, Decimal):
                return float(obj)
            return obj
        
        config_serializable = convert(config)
        
        with open(path, 'w', encoding="utf-8") as f:
            yaml.dump(config_serializable, f, default_flow_style=False, sort_keys=False)
