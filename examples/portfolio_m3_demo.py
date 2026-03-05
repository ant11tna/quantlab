from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quantlab.analytics import run_portfolio_analytics
from quantlab.market.store import MarketStore
from quantlab.portfolio.store import PortfolioStore
from quantlab.universe.store import UniverseStore


def main() -> int:
    portfolio_store = PortfolioStore(base_dir="data/portfolio")
    market_store = MarketStore(base_dir="data/market", universe_dir="data/universe")
    universe_store = UniverseStore(base_dir="data/universe")

    portfolio_id = "default"
    effective_date = portfolio_store.get_active_effective_date(portfolio_id)

    targets = portfolio_store.load_targets()
    scoped = targets[targets["portfolio_id"].astype(str) == portfolio_id].copy()
    scoped["target_weight"] = pd.to_numeric(scoped.get("target_weight"), errors="coerce").fillna(0.0)

    holding_ids = scoped.loc[scoped["target_weight"] > 0, "listing_id"].astype(str).unique().tolist()
    if not holding_ids:
        print("No positive targets for selected portfolio.")
        return 1

    bars = market_store.get_bars(
        listing_ids=holding_ids,
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

    benchmark_id = None
    listings = universe_store.load_listings()
    if not listings.empty and "listing_id" in listings.columns:
        all_ids = [str(x) for x in listings["listing_id"].dropna().astype(str).tolist()]
        for candidate in all_ids:
            if candidate not in set(holding_ids):
                benchmark_id = candidate
                break
        if benchmark_id is None and len(all_ids) >= 2:
            benchmark_id = all_ids[0]

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
        benchmark_listing_id=benchmark_id,
    )

    print("=== Portfolio Analytics M3 Demo ===")
    print(f"portfolio_id={portfolio_id}, effective_date={effective_date}")
    print(f"benchmark_id={benchmark_id}")
    print("metrics:")
    keys = [
        "total_return",
        "cagr",
        "annual_vol",
        "sharpe",
        "tracking_error",
        "information_ratio",
        "excess_total_return",
        "excess_cagr",
        "max_drawdown",
    ]
    for k in keys:
        print(f"  {k}: {result['metrics'].get(k)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
