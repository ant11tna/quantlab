from __future__ import annotations

import hashlib
import re

from quantlab.universe.normalizer import normalize_input
from quantlab.universe.store import UniverseStore
from quantlab.universe.types import Candidate


_US_QUALIFIED_RE = re.compile(r"^(NASDAQ|NYSE|AMEX):([A-Z][A-Z0-9]*(?:\.[A-Z0-9]+)?)$")
_CN_STRONG_RE = re.compile(r"^(SH|SZ)(\d{6})$")
_CN_PURE_RE = re.compile(r"^(\d{6})$")
_HK_RE = re.compile(r"^(\d{1,5})(?:\.HK)?$")
_US_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:\.[A-Z0-9]+)?$")


def _build_listing_id(region: str, exchange: str, ticker: str) -> str:
    return f"LISTING:{region}:{exchange}:{ticker}"


def _instrument_id(asset_type: str, region: str, primary: str) -> str:
    canonical = f"{asset_type}|{region}|{primary}"
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()
    return f"INSTR:{digest}"


def _from_listing_row(store: UniverseStore, listing_id: str) -> Candidate | None:
    listing = store.get_listing(listing_id)
    if listing is None:
        return None
    instrument = store.get_instrument(str(listing.get("instrument_id", ""))) or {}
    return Candidate(
        listing_id=str(listing["listing_id"]),
        instrument_id=str(listing["instrument_id"]),
        region=str(listing.get("region", "")),
        exchange=str(listing.get("exchange", "")),
        ticker=str(listing.get("ticker", "")),
        name=str(instrument.get("name", "") or ""),
        asset_type=str(instrument.get("asset_type", "unknown") or "unknown"),
        currency=str(listing.get("currency", "unknown") or "unknown"),
        confidence="exact",
        rationale="alias-hit",
    )


def resolve(query: str, store: UniverseStore, max_candidates: int = 8) -> list[Candidate]:
    normalized = normalize_input(query)

    aliases = store.load_aliases()
    if not aliases.empty:
        hit = aliases[aliases["normalized_input"] == normalized]
        if not hit.empty:
            listing_id = str(hit.iloc[0]["listing_id"])
            from_store = _from_listing_row(store, listing_id)
            if from_store is not None:
                return [from_store]

    scored: list[tuple[int, Candidate]] = []

    us_qualified = _US_QUALIFIED_RE.match(normalized)
    if us_qualified:
        exchange, ticker = us_qualified.groups()
        scored.append(
            (
                0,
                Candidate(
                    listing_id=_build_listing_id("US", exchange, ticker),
                    instrument_id=_instrument_id("unknown", "US", ticker),
                    region="US",
                    exchange=exchange,
                    ticker=ticker,
                    currency="USD",
                    confidence="exact",
                    rationale="US exchange-qualified ticker",
                ),
            )
        )

    cn_strong = _CN_STRONG_RE.match(normalized)
    if cn_strong:
        exchange, ticker = cn_strong.groups()
        scored.append(
            (
                0,
                Candidate(
                    listing_id=_build_listing_id("CN", exchange, ticker),
                    instrument_id=_instrument_id("unknown", "CN", ticker),
                    region="CN",
                    exchange=exchange,
                    ticker=ticker,
                    currency="CNY",
                    confidence="exact",
                    rationale="CN strong format",
                ),
            )
        )

    cn_pure = _CN_PURE_RE.match(normalized)
    if cn_pure:
        ticker = cn_pure.group(1)
        exchange = "SH" if ticker[0] in {"6", "9"} else "SZ"
        scored.append(
            (
                2,
                Candidate(
                    listing_id=_build_listing_id("CN", exchange, ticker),
                    instrument_id=_instrument_id("unknown", "CN", ticker),
                    region="CN",
                    exchange=exchange,
                    ticker=ticker,
                    currency="CNY",
                    confidence="inferred",
                    rationale="CN 6-digit inferred exchange from leading digit",
                ),
            )
        )

    hk_match = _HK_RE.match(normalized)
    if hk_match and not _CN_PURE_RE.match(normalized):
        digits = hk_match.group(1)
        ticker = digits.zfill(5)
        strong = normalized.endswith(".HK")
        scored.append(
            (
                1 if strong else 2,
                Candidate(
                    listing_id=_build_listing_id("HK", "HKEX", ticker),
                    instrument_id=_instrument_id("unknown", "HK", ticker),
                    region="HK",
                    exchange="HKEX",
                    ticker=ticker,
                    currency="HKD",
                    confidence="exact" if strong else "inferred",
                    rationale="HK numeric ticker parsed" + ("" if strong else " and padded to 5 digits"),
                ),
            )
        )

    if _US_RE.match(normalized) and not cn_strong and not us_qualified and not normalized.endswith(".HK"):
        scored.append(
            (
                3,
                Candidate(
                    listing_id=_build_listing_id("US", "AUTO", normalized),
                    instrument_id=_instrument_id("unknown", "US", normalized),
                    region="US",
                    exchange="AUTO",
                    ticker=normalized,
                    currency="USD",
                    confidence="inferred",
                    rationale="US ticker with AUTO exchange",
                ),
            )
        )

    unique: dict[str, tuple[int, Candidate]] = {}
    for score, cand in scored:
        prev = unique.get(cand.listing_id)
        if prev is None or score < prev[0]:
            unique[cand.listing_id] = (score, cand)

    ordered = sorted(unique.values(), key=lambda x: x[0])
    return [cand for _, cand in ordered[:max_candidates]]


def confirm(raw_input: str, candidate: Candidate, store: UniverseStore) -> None:
    raw_trimmed = raw_input.strip()
    normalized = normalize_input(raw_trimmed)

    aliases = store.load_aliases()
    if not aliases.empty:
        existing = aliases[aliases["raw_input"] == raw_trimmed]
        if not existing.empty:
            existing_listing = str(existing.iloc[0]["listing_id"])
            if existing_listing != candidate.listing_id:
                raise ValueError(
                    f"Alias conflict for '{raw_trimmed}': existing listing_id={existing_listing}, "
                    f"new listing_id={candidate.listing_id}."
                )

    if store.get_instrument(candidate.instrument_id) is None:
        store.upsert_instrument(
            {
                "instrument_id": candidate.instrument_id,
                "name": candidate.name,
                "asset_type": candidate.asset_type,
                "base_currency": candidate.currency,
                "country": "",
                "sector": "",
            }
        )

    store.upsert_listing(
        {
            "listing_id": candidate.listing_id,
            "instrument_id": candidate.instrument_id,
            "region": candidate.region,
            "exchange": candidate.exchange,
            "ticker": candidate.ticker,
            "mic": "unknown",
            "currency": candidate.currency,
            "lot_size": None,
            "is_active": True,
            "provider": "",
            "provider_symbol": "",
        }
    )

    store.upsert_alias(
        {
            "raw_input": raw_trimmed,
            "normalized_input": normalized,
            "listing_id": candidate.listing_id,
            "confidence": candidate.confidence,
            "note": candidate.rationale,
        }
    )
