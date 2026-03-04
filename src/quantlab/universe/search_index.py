from __future__ import annotations

from dataclasses import dataclass
from difflib import get_close_matches
from functools import lru_cache
from pathlib import Path

import pandas as pd

from quantlab.universe.store import UniverseStore
from quantlab.universe.types import Candidate


@dataclass(frozen=True)
class _IndexItem:
    key: str
    listing_id: str
    instrument_id: str
    region: str
    exchange: str
    ticker: str
    name: str
    currency: str


def _file_sig(path: Path) -> str:
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{path}:{stat.st_mtime_ns}:{stat.st_size}"


@lru_cache(maxsize=16)
def _build_index(base_dir: str, listings_sig: str, instruments_sig: str) -> tuple[list[_IndexItem], list[str]]:
    # signatures are used only for cache invalidation
    _ = listings_sig, instruments_sig
    store = UniverseStore(base_dir=base_dir)
    listings = store.load_listings()
    instruments = store.load_instruments()

    if listings.empty:
        return [], []

    merged = listings.merge(
        instruments[["instrument_id", "name"]] if not instruments.empty else pd.DataFrame(columns=["instrument_id", "name"]),
        on="instrument_id",
        how="left",
        suffixes=("", "_inst"),
    )

    items: list[_IndexItem] = []
    keys: list[str] = []

    for _, row in merged.iterrows():
        listing_id = str(row.get("listing_id") or "")
        if not listing_id:
            continue

        item = _IndexItem(
            key="",
            listing_id=listing_id,
            instrument_id=str(row.get("instrument_id") or ""),
            region=str(row.get("region") or ""),
            exchange=str(row.get("exchange") or ""),
            ticker=str(row.get("ticker") or ""),
            name=str(row.get("name") or ""),
            currency=str(row.get("currency") or "unknown"),
        )

        ticker_key = item.ticker.strip().upper()
        if ticker_key:
            keys.append(ticker_key)
            items.append(_IndexItem(**{**item.__dict__, "key": ticker_key}))

        name_key = item.name.strip().lower()
        if name_key:
            keys.append(name_key)
            items.append(_IndexItem(**{**item.__dict__, "key": name_key}))

    return items, keys


def fuzzy_match_candidates(
    query: str,
    store: UniverseStore,
    max_candidates: int = 8,
    region_filter: str = "All",
    exchange_filter: str = "All",
) -> list[Candidate]:
    cleaned = query.strip()
    if not cleaned:
        return []

    base_dir = str(store.base_dir)
    listings_sig = _file_sig(Path(base_dir) / "listings.parquet")
    instruments_sig = _file_sig(Path(base_dir) / "instruments.parquet")
    items, keys = _build_index(base_dir, listings_sig, instruments_sig)
    if not items:
        return []

    query_upper = cleaned.upper()
    query_lower = cleaned.lower()

    hits = set(get_close_matches(query_upper, keys, n=max_candidates * 3, cutoff=0.45))
    hits.update(get_close_matches(query_lower, keys, n=max_candidates * 3, cutoff=0.45))

    # contains fallback to improve Chinese / fragment matching
    for key in keys:
        if query_lower in key.lower() or query_upper in key.upper():
            hits.add(key)

    out: dict[str, Candidate] = {}
    for item in items:
        if item.key not in hits:
            continue
        if region_filter != "All" and item.region != region_filter:
            continue
        if exchange_filter != "All" and item.exchange != exchange_filter:
            continue
        if item.listing_id in out:
            continue

        out[item.listing_id] = Candidate(
            listing_id=item.listing_id,
            instrument_id=item.instrument_id,
            region=item.region,
            exchange=item.exchange,
            ticker=item.ticker,
            name=item.name,
            currency=item.currency,
            confidence="fuzzy",
            rationale="fuzzy-match from universe cache",
        )
        if len(out) >= max_candidates:
            break

    return list(out.values())
