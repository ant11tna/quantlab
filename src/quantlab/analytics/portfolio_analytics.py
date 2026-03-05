from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from quantlab.analytics.metrics import compute_metrics
from quantlab.market.store import MarketStore
from quantlab.portfolio.rebalance import compute_portfolio_from_weights
from quantlab.portfolio.store import PortfolioStore
from quantlab.portfolio.weights_timeline import build_weights_timeline


def _normalize_day(ts_like: str | datetime) -> pd.Timestamp:
    return pd.Timestamp(ts_like).normalize()


_ONE_DAY = pd.Timedelta(days=1)


def run_portfolio_analytics(
    portfolio_id: str,
    effective_date: str,
    start: str | datetime,
    end: str | datetime,
    portfolio_store: PortfolioStore,
    market_store: MarketStore,
    universe_dir: str = "data/universe",
    freq: str = "1d",
    price_field: str = "close",
    base_nav: float = 1.0,
) -> dict:
    """Run dynamic-weight portfolio analytics with effective_date rebalances."""
    _ = universe_dir
    _ = effective_date

    start_ts = _normalize_day(start)
    end_ts = _normalize_day(end)
    if end_ts < start_ts:
        raise ValueError(f"Invalid range: end ({end_ts.date()}) is before start ({start_ts.date()})")

    targets = portfolio_store.load_targets()
    if targets.empty:
        raise ValueError("No portfolio targets found in PortfolioStore; check Slice2 coverage check")

    targets_for_portfolio = targets[targets["portfolio_id"].astype(str) == str(portfolio_id)].copy()
    if targets_for_portfolio.empty:
        raise ValueError(
            f"No targets found for portfolio_id={portfolio_id}; check Slice2 coverage check and target setup"
        )

    targets_for_portfolio["target_weight"] = (
        pd.to_numeric(targets_for_portfolio["target_weight"], errors="coerce").fillna(0.0)
    )
    targets_for_portfolio["listing_id"] = targets_for_portfolio["listing_id"].astype(str)
    positive_any = targets_for_portfolio.groupby("listing_id")["target_weight"].max()
    listing_ids = positive_any[positive_any > 0].index.astype(str).tolist()
    if not listing_ids:
        raise ValueError("All target weights are <= 0; check Slice2 coverage check and rebalance targets")

    bars = market_store.get_bars(
        listing_ids=listing_ids,
        start=start_ts,
        end=(end_ts + _ONE_DAY - pd.Timedelta(nanoseconds=1)),
        freq=freq,
        fields=[price_field],
    )
    if bars.empty:
        raise ValueError("No market bars found for selected listings/range; check Slice2 coverage check")
    if price_field not in bars.columns:
        raise ValueError(f"Requested price field '{price_field}' is missing from MarketStore result")

    prices = bars.pivot_table(index="ts", columns="listing_id", values=price_field, aggfunc="last").sort_index()
    prices.index = pd.to_datetime(prices.index).normalize()
    prices = prices.reindex(columns=listing_ids)
    prices = prices[~prices.isna().all(axis=1)]
    if prices.empty:
        raise ValueError("Price matrix empty after drop-all-NaN rows; check Slice2 coverage check")

    coverage_start = pd.Timestamp(prices.index.min()).normalize()
    coverage_end = pd.Timestamp(prices.index.max()).normalize()
    if pd.isna(coverage_start) or pd.isna(coverage_end):
        raise ValueError("Unable to infer market coverage bounds; check Slice2 coverage check")

    start_clamped = max(start_ts, coverage_start)
    end_clamped = min(end_ts, coverage_end)
    if end_clamped < start_clamped:
        raise ValueError(
            "Requested range outside market coverage; check Slice2 coverage check and shrink date range"
        )

    weights_timeline = build_weights_timeline(
        targets_df=targets_for_portfolio,
        portfolio_id=portfolio_id,
        start=start_clamped,
        end=end_clamped,
        freq="1d",
    )

    daily_index = pd.date_range(start=weights_timeline.index.min(), end=weights_timeline.index.max(), freq="D")
    prices_daily = prices.reindex(daily_index).ffill().reindex(weights_timeline.index)

    portfolio = compute_portfolio_from_weights(prices=prices_daily, weights_timeline=weights_timeline, base_nav=base_nav)

    returns = portfolio["returns"]
    nav = portfolio["nav"]
    contribution_df = portfolio["contribution"].copy()
    contribution_df.index.name = "ts"
    turnover_df = portfolio["turnover_df"].copy()

    returns_df = returns.to_frame().reset_index().rename(columns={returns.index.name or "index": "ts"})
    nav_df = nav.to_frame().reset_index().rename(columns={nav.index.name or "index": "ts"})

    metrics = compute_metrics(returns, nav)
    metrics.update(portfolio["turnover_summary"])

    aligned_start = pd.Timestamp(returns.index.min()).date().isoformat()
    aligned_end = pd.Timestamp(returns.index.max()).date().isoformat()

    meta = {
        "portfolio_id": str(portfolio_id),
        "effective_date": str(effective_date),
        "start": start_ts.date().isoformat(),
        "end": end_ts.date().isoformat(),
        "coverage_start": coverage_start.date().isoformat(),
        "coverage_end": coverage_end.date().isoformat(),
        "start_clamped": start_clamped.date().isoformat(),
        "end_clamped": end_clamped.date().isoformat(),
        "freq": str(freq),
        "price_field": str(price_field),
        "n_assets": int(len(listing_ids)),
        "n_rows_raw": int(len(prices)),
        "n_rows_aligned": int(len(returns)),
        "aligned_start": aligned_start,
        "aligned_end": aligned_end,
        "n_rows_port_ret": int(len(returns)),
        "note": "dynamic weights timeline + daily forward-fill prices",
    }

    analytics_dir = Path("data/analytics")
    analytics_dir.mkdir(parents=True, exist_ok=True)

    nav_path = analytics_dir / "portfolio_nav.parquet"
    returns_path = analytics_dir / "portfolio_returns.parquet"
    metrics_path = analytics_dir / "portfolio_metrics.json"
    contribution_path = analytics_dir / "portfolio_contribution.parquet"

    nav_df.to_parquet(nav_path, engine="pyarrow", index=False)
    returns_df.to_parquet(returns_path, engine="pyarrow", index=False)
    contribution_df.reset_index().to_parquet(contribution_path, engine="pyarrow", index=False)
    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    meta["cache_paths"] = {
        "nav": str(nav_path),
        "returns": str(returns_path),
        "metrics": str(metrics_path),
        "contribution": str(contribution_path),
    }

    return {
        "nav_df": nav_df,
        "returns_df": returns_df,
        "metrics": metrics,
        "meta": meta,
        "contribution_df": contribution_df,
        "turnover_df": turnover_df,
    }
