from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quantlab.analytics import run_portfolio_analytics
from quantlab.market.store import MarketStore
from quantlab.portfolio.store import PortfolioStore


def main() -> int:
    portfolio_store = PortfolioStore(base_dir="data/portfolio")
    market_store = MarketStore(base_dir="data/market", universe_dir="data/universe")

    portfolio_id = "default"
    effective_date = portfolio_store.get_active_effective_date(portfolio_id)

    targets = portfolio_store.load_targets()
    scoped = targets[targets["portfolio_id"].astype(str) == portfolio_id].copy()
    scoped["target_weight"] = pd.to_numeric(scoped.get("target_weight"), errors="coerce").fillna(0.0)

    listing_ids = scoped.loc[scoped["target_weight"] > 0, "listing_id"].astype(str).unique().tolist()
    if not listing_ids:
        print("No positive targets for selected portfolio.")
        return 1

    bars = market_store.get_bars(
        listing_ids=listing_ids,
        start=pd.Timestamp("2000-01-01"),
        end=pd.Timestamp.today(),
        freq="1d",
        fields=["close"],
    )
    if bars.empty:
        print("No bars found for current portfolio listings.")
        return 1

    coverage_end = pd.Timestamp(bars["ts"].max()).normalize()
    start = max(pd.Timestamp(bars["ts"].min()).normalize(), coverage_end - pd.Timedelta(days=120))

    result = run_portfolio_analytics(
        portfolio_id=portfolio_id,
        effective_date=effective_date,
        start=start,
        end=coverage_end,
        portfolio_store=portfolio_store,
        market_store=market_store,
        universe_dir="data/universe",
        freq="1d",
        price_field="close",
        base_nav=1.0,
    )

    print("=== Portfolio Analytics M2 Demo ===")
    print(f"portfolio_id={portfolio_id}, effective_date={effective_date}")
    print("metrics:")
    for k, v in result["metrics"].items():
        print(f"  {k}: {v}")

    print("turnover summary:")
    for k in ["rebalance_count", "total_turnover", "avg_daily_turnover"]:
        print(f"  {k}: {result['metrics'].get(k)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
