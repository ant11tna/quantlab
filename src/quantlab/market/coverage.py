from __future__ import annotations

from datetime import datetime

import pandas as pd

from quantlab.market.store import MarketStore
from quantlab.portfolio.store import PortfolioStore


_ONE_DAY = pd.Timedelta(days=1)


def _normalize_date(value: str | datetime) -> pd.Timestamp:
    return pd.to_datetime(value).normalize()


def compute_coverage_for_listings(
    listing_ids: list[str],
    market_store: MarketStore,
    start: str | datetime,
    end: str | datetime,
    freq: str = "1d",
) -> pd.DataFrame:
    start_ts = _normalize_date(start)
    end_ts = _normalize_date(end)

    unique_listing_ids = list(dict.fromkeys(str(x) for x in listing_ids if str(x).strip()))
    metadata = market_store.load_metadata()
    listings = market_store.load_universe_listings()

    rows: list[dict] = []
    for listing_id in unique_listing_ids:
        info_row = listings[listings["listing_id"].astype(str) == listing_id]
        region = str(info_row.iloc[0]["region"]) if not info_row.empty else "UNK"
        exchange = str(info_row.iloc[0]["exchange"]) if not info_row.empty else "UNK"

        meta_row = metadata[
            (metadata["listing_id"].astype(str) == listing_id)
            & (metadata["freq"].astype(str) == freq)
        ]

        min_ts = pd.NaT
        max_ts = pd.NaT
        status = "missing"
        gap_type = "missing_all"
        gap_start = start_ts
        gap_end = end_ts

        if not meta_row.empty:
            rec = meta_row.iloc[-1]
            min_ts = pd.to_datetime(rec.get("min_ts"), errors="coerce")
            max_ts = pd.to_datetime(rec.get("max_ts"), errors="coerce")
            rec_status = str(rec.get("status", "")).strip().lower()
            if rec_status == "ok" and pd.notna(min_ts) and pd.notna(max_ts):
                status = "ok"
                head_missing = min_ts > start_ts
                tail_missing = max_ts < end_ts

                if head_missing and tail_missing:
                    gap_type = "missing_head+missing_tail"
                    gap_start = start_ts
                    gap_end = end_ts
                elif head_missing:
                    gap_type = "missing_head"
                    gap_start = start_ts
                    gap_end = min_ts - _ONE_DAY
                elif tail_missing:
                    gap_type = "missing_tail"
                    gap_start = max_ts + _ONE_DAY
                    gap_end = end_ts
                else:
                    gap_type = "none"
                    gap_start = pd.NaT
                    gap_end = pd.NaT

        rows.append(
            {
                "listing_id": listing_id,
                "freq": freq,
                "region": region,
                "exchange": exchange,
                "min_ts": min_ts,
                "max_ts": max_ts,
                "status": status,
                "gap_start": gap_start,
                "gap_end": gap_end,
                "gap_type": gap_type,
            }
        )

    return pd.DataFrame(rows)


def compute_portfolio_coverage(
    portfolio_id: str,
    effective_date: str,
    portfolio_store: PortfolioStore,
    market_store: MarketStore,
    start: str | datetime,
    end: str | datetime,
    freq: str = "1d",
) -> pd.DataFrame:
    targets = portfolio_store.load_targets()
    if targets.empty:
        return pd.DataFrame(
            columns=[
                "listing_id",
                "freq",
                "region",
                "exchange",
                "min_ts",
                "max_ts",
                "status",
                "gap_start",
                "gap_end",
                "gap_type",
            ]
        )

    subset = targets[
        (targets["portfolio_id"].astype(str) == str(portfolio_id))
        & (targets["effective_date"].astype(str) == str(effective_date))
    ]
    listing_ids = subset["listing_id"].astype(str).dropna().drop_duplicates().tolist()

    return compute_coverage_for_listings(
        listing_ids=listing_ids,
        market_store=market_store,
        start=start,
        end=end,
        freq=freq,
    )
