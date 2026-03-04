from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from quantlab.analytics.metrics import compute_metrics
from quantlab.market.store import MarketStore
from quantlab.portfolio.store import PortfolioStore


def _normalize_day(ts_like: str | datetime) -> pd.Timestamp:
    return pd.Timestamp(ts_like).normalize()


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
    """Run static-weight portfolio analytics with close-based returns."""
    _ = universe_dir

    start_ts = _normalize_day(start)
    end_ts = _normalize_day(end)
    if end_ts < start_ts:
        raise ValueError(f"Invalid range: end ({end_ts.date()}) is before start ({start_ts.date()})")

    targets = portfolio_store.load_targets()
    if targets.empty:
        raise ValueError("No portfolio targets found in PortfolioStore")

    target_mask = (
        (targets["portfolio_id"].astype(str) == str(portfolio_id))
        & (targets["effective_date"].astype(str) == str(effective_date))
    )
    target_rows = targets.loc[target_mask].copy()
    if target_rows.empty:
        raise ValueError(f"No targets found for portfolio_id={portfolio_id}, effective_date={effective_date}")

    target_rows["target_weight"] = pd.to_numeric(target_rows["target_weight"], errors="coerce").fillna(0.0)
    target_rows = target_rows[target_rows["target_weight"] > 0].copy()
    if target_rows.empty:
        raise ValueError("All target weights are <= 0 after filtering")

    weights = target_rows.set_index("listing_id")["target_weight"].astype(float)
    weight_sum = float(weights.sum())
    listing_ids = weights.index.tolist()

    bars = market_store.get_bars(
        listing_ids=listing_ids,
        start=start_ts,
        end=end_ts,
        freq=freq,
        fields=[price_field],
    )

    if bars.empty:
        raise ValueError("No market bars found for selected listings/range")
    if price_field not in bars.columns:
        raise ValueError(f"Requested price field '{price_field}' is missing from MarketStore result")

    prices = (
        bars.pivot_table(index="ts", columns="listing_id", values=price_field, aggfunc="last")
        .sort_index()
    )
    prices = prices.reindex(columns=listing_ids)

    asset_rets = prices.pct_change()
    aligned_rets = asset_rets.dropna(how="any")

    if aligned_rets.empty:
        raise ValueError(
            "Aligned returns are empty after dropna(how='any'). Coverage gap is too large; "
            "check Slice2 coverage panel or shrink the date range."
        )

    port_ret = aligned_rets.dot(weights)
    nav = float(base_nav) * (1.0 + port_ret).cumprod()

    returns_df = port_ret.rename("ret").to_frame().reset_index()
    nav_df = nav.rename("nav").to_frame().reset_index()

    metrics = compute_metrics(port_ret, nav)
    if abs(weight_sum - 1.0) > 1e-3:
        metrics["weight_sum_note"] = f"weight_sum={weight_sum:.6f}, not close to 1.0"

    meta = {
        "portfolio_id": str(portfolio_id),
        "effective_date": str(effective_date),
        "start": start_ts.date().isoformat(),
        "end": end_ts.date().isoformat(),
        "freq": str(freq),
        "price_field": str(price_field),
        "n_assets": int(len(listing_ids)),
        "n_rows_raw": int(len(asset_rets)),
        "n_rows_aligned": int(len(aligned_rets)),
        "aligned_start": pd.Timestamp(aligned_rets.index.min()).date().isoformat(),
        "aligned_end": pd.Timestamp(aligned_rets.index.max()).date().isoformat(),
        "weight_sum": weight_sum,
        "note": "dropna alignment used; middle gaps not checked",
    }

    analytics_dir = Path("data/analytics")
    analytics_dir.mkdir(parents=True, exist_ok=True)

    nav_path = analytics_dir / "portfolio_nav.parquet"
    returns_path = analytics_dir / "portfolio_returns.parquet"
    metrics_path = analytics_dir / "portfolio_metrics.json"

    nav_df.to_parquet(nav_path, engine="pyarrow", index=False)
    returns_df.to_parquet(returns_path, engine="pyarrow", index=False)
    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    meta["cache_paths"] = {
        "nav": str(nav_path),
        "returns": str(returns_path),
        "metrics": str(metrics_path),
    }

    return {
        "nav_df": nav_df,
        "returns_df": returns_df,
        "metrics": metrics,
        "meta": meta,
    }
