from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Candidate:
    """Resolver candidate for a potential security listing."""

    listing_id: str
    instrument_id: str
    region: str
    exchange: str
    ticker: str
    name: str = ""
    asset_type: str = "unknown"
    currency: str = "unknown"
    confidence: str = "inferred"
    rationale: str = ""
