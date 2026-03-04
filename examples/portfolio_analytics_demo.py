from __future__ import annotations

import sys
import tempfile
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
    current_targets = targets[
        (targets["portfolio_id"].astype(str) == portfolio_id)
        & (targets["effective_date"].astype(str) == effective_date)
    ].copy()

    current_targets["target_weight"] = pd.to_numeric(current_targets["target_weight"], errors="coerce").fillna(0.0)
    positive_targets = current_targets[current_targets["target_weight"] > 0].copy()

    working_store = portfolio_store
    if positive_targets.empty:
        print("No positive target weights found in active effective_date. Bootstrapping demo weights in a temporary store...")
        listings = market_store.load_universe_listings()
        if listings.empty:
            print("No listings available to bootstrap demo portfolio.")
            return 1
        demo_listing_ids = listings.head(2)["listing_id"].astype(str).tolist()
        if not demo_listing_ids:
            print("No listing_ids found to bootstrap demo portfolio.")
            return 1

        with tempfile.TemporaryDirectory(prefix="quantlab_demo_portfolio_") as tmp_dir:
            tmp_store = PortfolioStore(base_dir=tmp_dir)
            tmp_store.save_portfolios(portfolio_store.load_portfolios())

            seed_targets = targets.copy()
            equal_weight = 1.0 / len(demo_listing_ids)
            for listing_id in demo_listing_ids:
                seed_targets = pd.concat(
                    [
                        seed_targets,
                        pd.DataFrame(
                            [
                                {
                                    "portfolio_id": portfolio_id,
                                    "effective_date": effective_date,
                                    "listing_id": listing_id,
                                    "target_weight": equal_weight,
                                    "added_at": pd.Timestamp.utcnow().isoformat(),
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
            seed_targets = seed_targets.drop_duplicates(
                subset=["portfolio_id", "effective_date", "listing_id"], keep="last"
            )
            tmp_store.save_targets(seed_targets)
            working_store = tmp_store

            return _run_demo(working_store, market_store, portfolio_id, effective_date)

    return _run_demo(working_store, market_store, portfolio_id, effective_date)


def _run_demo(
    portfolio_store: PortfolioStore,
    market_store: MarketStore,
    portfolio_id: str,
    effective_date: str,
) -> int:
    targets = portfolio_store.load_targets()
    current_targets = targets[
        (targets["portfolio_id"].astype(str) == portfolio_id)
        & (targets["effective_date"].astype(str) == effective_date)
    ].copy()
    current_targets["target_weight"] = pd.to_numeric(current_targets["target_weight"], errors="coerce").fillna(0.0)

    listing_ids = current_targets[current_targets["target_weight"] > 0]["listing_id"].astype(str).unique().tolist()
    bars = market_store.get_bars(
        listing_ids=listing_ids,
        start=pd.Timestamp("2000-01-01"),
        end=pd.Timestamp.today(),
        freq="1d",
        fields=["close"],
    )
    if bars.empty:
        print("No market bars found for current target listings.")
        return 1

    coverage_start = pd.Timestamp(bars["ts"].min()).normalize()
    coverage_end = pd.Timestamp(bars["ts"].max()).normalize()
    start = max(coverage_start, coverage_end - pd.Timedelta(days=60))

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

    print("=== Portfolio Analytics Demo ===")
    print(f"portfolio_id={portfolio_id}, effective_date={effective_date}")
    print("metrics:")
    for k, v in result["metrics"].items():
        print(f"  {k}: {v}")

    print("meta:")
    meta = result["meta"]
    for k in [
        "aligned_start",
        "aligned_end",
        "n_assets",
        "n_rows_raw",
        "n_rows_aligned",
        "weight_sum",
    ]:
        print(f"  {k}: {meta.get(k)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
