from __future__ import annotations

from datetime import datetime

import pandas as pd


def _normalize_day(ts_like: str | datetime | pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts_like).normalize()


def build_weights_timeline(
    targets_df: pd.DataFrame,
    portfolio_id: str,
    start: str | datetime,
    end: str | datetime,
    freq: str = "1d",
) -> pd.DataFrame:
    _ = freq

    start_ts = _normalize_day(start)
    end_ts = _normalize_day(end)
    if end_ts < start_ts:
        raise ValueError(f"Invalid range: end ({end_ts.date()}) is before start ({start_ts.date()})")

    if targets_df.empty:
        raise ValueError("No targets found; check Slice2 coverage check and portfolio target setup")

    scoped = targets_df[targets_df["portfolio_id"].astype(str) == str(portfolio_id)].copy()
    if scoped.empty:
        raise ValueError(
            f"No targets for portfolio_id={portfolio_id}; check Slice2 coverage check and target records"
        )

    scoped["effective_date"] = pd.to_datetime(scoped["effective_date"], errors="coerce").dt.normalize()
    scoped["target_weight"] = pd.to_numeric(scoped["target_weight"], errors="coerce").fillna(0.0)
    scoped["listing_id"] = scoped["listing_id"].astype(str)
    scoped = scoped.dropna(subset=["effective_date", "listing_id"])
    if scoped.empty:
        raise ValueError("No valid target rows after coercion; check Slice2 coverage check")

    weights_wide = scoped.pivot_table(
        index="effective_date",
        columns="listing_id",
        values="target_weight",
        aggfunc="last",
    ).sort_index()

    if weights_wide.empty:
        raise ValueError("Weight timeline is empty after pivot; check Slice2 coverage check")

    anchor_index = weights_wide.index.union(pd.DatetimeIndex([start_ts, end_ts]))
    anchored = weights_wide.reindex(anchor_index).sort_index().ffill().fillna(0.0)

    row_sum = anchored.sum(axis=1)
    positive_mask = row_sum > 0
    if positive_mask.any():
        anchored.loc[positive_mask] = anchored.loc[positive_mask].div(row_sum.loc[positive_mask], axis=0)

    full_index = pd.date_range(start=start_ts, end=end_ts, freq="D")
    timeline = anchored.reindex(full_index).ffill().fillna(0.0)
    timeline.index.name = "ts"
    timeline.columns.name = None
    return timeline
