from __future__ import annotations

import pandas as pd


def compute_asset_returns(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        raise ValueError("Prices are empty; check Slice2 coverage check for selected assets/date range")
    return prices.pct_change(fill_method=None)


def compute_portfolio_from_weights(
    prices: pd.DataFrame,
    weights_timeline: pd.DataFrame,
    base_nav: float = 1.0,
) -> dict:
    asset_returns = compute_asset_returns(prices)
    returns = asset_returns.dropna(how="all")
    if returns.empty:
        raise ValueError(
            "No aligned returns after pct_change/dropna; check Slice2 coverage check or shrink date range"
        )

    weights = weights_timeline.reindex(returns.index).ffill().fillna(0.0)
    row_sum = weights.sum(axis=1)
    positive_mask = row_sum > 0
    if positive_mask.any():
        weights.loc[positive_mask] = weights.loc[positive_mask].div(row_sum.loc[positive_mask], axis=0)

    weighted = returns.mul(weights, axis=1)
    portfolio_return = weighted.sum(axis=1).rename("ret")
    nav = (float(base_nav) * (1.0 + portfolio_return).cumprod()).rename("nav")

    contribution = weighted.copy()

    prev_weights = weights.shift(1).fillna(0.0)
    daily_turnover = 0.5 * (weights.sub(prev_weights).abs().sum(axis=1))
    daily_turnover = daily_turnover.where(daily_turnover > 1e-12, 0.0)

    turnover_df = daily_turnover.rename("turnover").reset_index().rename(columns={"index": "ts"})
    if "ts" not in turnover_df.columns:
        turnover_df = turnover_df.rename(columns={turnover_df.columns[0]: "ts"})

    turnover_summary = {
        "total_turnover": float(daily_turnover.sum()),
        "rebalance_count": int((daily_turnover > 1e-12).sum()),
        "avg_daily_turnover": float(daily_turnover.mean()) if len(daily_turnover) > 0 else 0.0,
    }

    return {
        "returns": portfolio_return,
        "nav": nav,
        "contribution": contribution,
        "turnover_df": turnover_df,
        "turnover_summary": turnover_summary,
    }
