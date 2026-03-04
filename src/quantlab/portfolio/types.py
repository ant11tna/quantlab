from __future__ import annotations

PORTFOLIO_COLUMNS = [
    "portfolio_id",
    "name",
    "base_currency",
    "created_at",
    "updated_at",
]

TARGET_COLUMNS = [
    "portfolio_id",
    "effective_date",
    "listing_id",
    "target_weight",
    "added_at",
]

DEFAULT_PORTFOLIO_ID = "default"
DEFAULT_PORTFOLIO_NAME = "Default Portfolio"
DEFAULT_BASE_CURRENCY = "CNY"
