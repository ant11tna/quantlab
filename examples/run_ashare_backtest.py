"""A-Share ETF Backtest Example (端到端完整示例)

完整流程:
1. 从 AkShare 获取数据 (如果没有本地数据)
2. 运行 curate_data.py 加工数据
3. 加载 curated 数据运行回测
4. 生成对账报告
"""

from __future__ import annotations

import sys
import subprocess
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from loguru import logger

from quantlab.backtest.engine import BacktestEngine
from quantlab.backtest.broker_sim import ExecutionConfig
from quantlab.research.strategies.base import Strategy
from quantlab.core.types import TargetWeight, Side
from quantlab.research.reconcile import quick_reconcile


class ChinaETFStrategy(Strategy):
    """简单 A 股 ETF 策略: 等权配置主流 ETF"""
    
    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.name = "ChinaETF_EqualWeight"
    
    def on_rebalance(self, data: pd.DataFrame, ts: datetime) -> list[TargetWeight]:
        """每月再平衡为等权"""
        # 过滤当前有数据的标的
        current_symbols = data[data["ts"] == ts]["symbol"].unique()
        available = [s for s in self.symbols if s in current_symbols]
        
        if not available:
            return []
        
        weight = 1.0 / len(available)
        return [
            TargetWeight(ts=ts, symbol=s, target_weight=weight, source=self.name)
            for s in available
        ]


def load_curated_data(symbols: list[str]) -> pd.DataFrame:
    """从 data/curated/bars/ 加载加工后的数据"""
    data_dir = Path("data/curated/bars")
    all_data = []
    
    for symbol in symbols:
        # 尝试不同路径
        safe_name = symbol.replace(":", "_")
        paths = [
            data_dir / "etf" / f"{safe_name}.parquet",
            data_dir / "etf" / f"{symbol}.parquet",
            Path("data/raw/bars/etf") / f"{symbol}.csv",
        ]
        
        df = None
        for p in paths:
            if p.exists():
                if p.suffix == ".parquet":
                    df = pd.read_parquet(p)
                else:
                    df = pd.read_csv(p)
                break
        
        if df is None:
            logger.warning(f"Data not found for {symbol}")
            continue
        
        # 确保格式正确
        df["ts"] = pd.to_datetime(df["ts"])
        all_data.append(df)
        logger.info(f"Loaded {symbol}: {len(df)} rows")
    
    if not all_data:
        raise ValueError("No data loaded! Run: python scripts/curate_data.py --all")
    
    return pd.concat(all_data, ignore_index=True)


def check_and_prepare_data():
    """检查并准备数据"""
    curated_dir = Path("data/curated/bars/etf")
    
    if not curated_dir.exists() or not list(curated_dir.glob("*.parquet")):
        logger.info("Curated data not found, preparing data...")
        
        # 1. 获取原始数据
        logger.info("[1/2] Fetching raw data from AkShare...")
        result = subprocess.run(
            [sys.executable, "scripts/update_data.py", "--type", "etf"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            logger.error(f"Data update failed: {result.stderr}")
            return False
        
        # 2. 加工数据
        logger.info("[2/2] Curating data (add regime fields)...")
        result = subprocess.run(
            [sys.executable, "scripts/curate_data.py", "--all"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            logger.error(f"Data curation failed: {result.stderr}")
            return False
        
        logger.info("Data preparation complete!")
    else:
        logger.info(f"Found curated data: {len(list(curated_dir.glob('*.parquet')))} files")
    
    return True


def main():
    """运行 A 股 ETF 回测"""
    print("=" * 70)
    print("A-Share ETF Backtest (端到端示例)")
    print("=" * 70)
    
    # 配置
    symbols = ["ETF:510300", "ETF:510500", "ETF:518880", "ETF:511010"]
    # 510300: 沪深300ETF, 510500: 中证500ETF, 518880: 黄金ETF, 511010: 国债ETF
    
    initial_cash = 1_000_000.0
    
    # Step 1: 准备数据
    print("\n[Step 1] Data Preparation")
    print("-" * 70)
    if not check_and_prepare_data():
        logger.error("Data preparation failed!")
        return 1
    
    # Step 2: 加载数据
    print("\n[Step 2] Loading Curated Data")
    print("-" * 70)
    try:
        data = load_curated_data(symbols)
        print(f"Loaded {len(data)} total rows")
        print(f"Date range: {data['ts'].min()} to {data['ts'].max()}")
        print(f"Symbols: {data['symbol'].unique().tolist()}")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return 1
    
    # Step 3: 创建策略
    print("\n[Step 3] Creating Strategy")
    print("-" * 70)
    strategy = ChinaETFStrategy(symbols=symbols)
    print(f"Strategy: {strategy.name}")
    print(f"Universe: {symbols}")
    
    # Step 4: 配置 A 股执行参数
    print("\n[Step 4] Running Backtest (A-Share Profile)")
    print("-" * 70)
    print("Config: lot_size=100, enforce_t1=true")
    
    exec_config = {
        "participation_rate": 0.2,
        "lot_size": 100,           # A股一手100股
        "min_trade_qty": 100,
        "enforce_t1": True,        # T+1结算
        "impact_k_bps": 20,
        "impact_alpha": 0.5
    }
    
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=initial_cash,
        fee_model="china_ashares",  # 假设有这个费率模型
        exec_config=exec_config
    )
    
    results = engine.run(
        data=data,
        rebalance_freq="M",  # 月度再平衡
        progress=True
    )
    
    # Step 5: 结果分析
    print("\n[Step 5] Results Analysis")
    print("-" * 70)
    
    equity_df = results["equity_curve"]
    trades_df = results["trades"]
    metrics = results["metrics"]
    
    # 关键指标
    final_nav = equity_df["nav"].iloc[-1]
    total_return = (final_nav - initial_cash) / initial_cash
    
    print(f"\nPerformance:")
    print(f"  Initial NAV:    ${initial_cash:,.2f}")
    print(f"  Final NAV:      ${final_nav:,.2f}")
    print(f"  Total Return:   {total_return:.2%}")
    
    if "risk" in metrics:
        risk = metrics["risk"]
        print(f"  Max Drawdown:   {risk.get('max_drawdown', 0):.2%}")
        print(f"  Sharpe Ratio:   {risk.get('sharpe_ratio', 0):.2f}")
    
    print(f"\nTrading:")
    print(f"  Total Trades:   {len(trades_df)}")
    if "trading" in metrics:
        trading = metrics["trading"]
        print(f"  Total Fees:     ${trading.get('total_fees', 0):,.2f}")
    
    # Step 6: 保存和对账
    print("\n[Step 6] Saving & Reconciliation")
    print("-" * 70)
    
    run_dir = engine.save_run(results, data=data)
    print(f"Results saved to: {run_dir}")
    
    # 运行对账
    summary = quick_reconcile(run_dir)
    print(f"\nReconciliation:")
    print(f"  Targets:  {summary.total_targets}")
    print(f"  Orders:   {summary.total_orders}")
    print(f"  Fills:    {summary.total_fills}")
    print(f"  Rejected: {summary.rejected}")
    
    if summary.reject_by_reason:
        print(f"\n  Rejection breakdown:")
        for reason, count in list(summary.reject_by_reason.items())[:3]:
            print(f"    - {reason}: {count}")
    
    print(f"\n  Report: {run_dir}/results/reconcile.md")
    
    print("\n" + "=" * 70)
    print("Backtest Complete!")
    print("=" * 70)
    print(f"\nView results:")
    print(f"  CLI: cat {run_dir}/results/reconcile.md")
    print(f"  UI:  streamlit run ui/app.py")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
