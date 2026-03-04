from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


INSTRUMENT_COLUMNS = [
    "instrument_id",
    "name",
    "asset_type",
    "base_currency",
    "country",
    "sector",
    "created_at",
    "updated_at",
]

LISTING_COLUMNS = [
    "listing_id",
    "instrument_id",
    "region",
    "exchange",
    "ticker",
    "mic",
    "currency",
    "lot_size",
    "is_active",
    "provider",
    "provider_symbol",
    "created_at",
    "updated_at",
]

ALIAS_COLUMNS = [
    "raw_input",
    "normalized_input",
    "listing_id",
    "confidence",
    "note",
    "created_at",
    "updated_at",
]


class UniverseStore:
    def __init__(self, base_dir: str = "data/universe") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self._instruments_path = self.base_dir / "instruments.parquet"
        self._listings_path = self.base_dir / "listings.parquet"
        self._aliases_path = self.base_dir / "aliases.parquet"

        self._instruments = self._load_or_empty(self._instruments_path, INSTRUMENT_COLUMNS)
        self._listings = self._load_or_empty(self._listings_path, LISTING_COLUMNS)
        self._aliases = self._load_or_empty(self._aliases_path, ALIAS_COLUMNS)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _load_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
        if path.exists():
            df = pd.read_parquet(path, engine="pyarrow")
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            return df[columns]
        return pd.DataFrame(columns=columns)

    @staticmethod
    def _upsert_df(df: pd.DataFrame, key: str, row: dict[str, Any]) -> pd.DataFrame:
        row_copy = dict(row)
        now = UniverseStore._now_iso()
        row_copy.setdefault("created_at", now)
        row_copy["updated_at"] = now

        if df.empty:
            return pd.DataFrame([row_copy], columns=df.columns)

        mask = df[key] == row_copy[key]
        if mask.any():
            idx = df.index[mask][0]
            existing_created_at = df.at[idx, "created_at"] if "created_at" in df.columns else None
            if existing_created_at:
                row_copy["created_at"] = existing_created_at
            for col in df.columns:
                if col in row_copy:
                    df.at[idx, col] = row_copy[col]
            return df

        return pd.concat([df, pd.DataFrame([row_copy], columns=df.columns)], ignore_index=True)

    def load_instruments(self) -> pd.DataFrame:
        return self._instruments.copy()

    def load_listings(self) -> pd.DataFrame:
        return self._listings.copy()

    def load_aliases(self) -> pd.DataFrame:
        return self._aliases.copy()

    def upsert_instrument(self, row: dict[str, Any]) -> None:
        self._instruments = self._upsert_df(self._instruments, "instrument_id", row)
        self._write_df(self._instruments_path, self._instruments)

    def upsert_listing(self, row: dict[str, Any]) -> None:
        self._listings = self._upsert_df(self._listings, "listing_id", row)
        self._write_df(self._listings_path, self._listings)

    def upsert_alias(self, row: dict[str, Any]) -> None:
        self._aliases = self._upsert_df(self._aliases, "raw_input", row)
        self._write_df(self._aliases_path, self._aliases)

    def get_listing(self, listing_id: str) -> dict[str, Any] | None:
        if self._listings.empty:
            return None
        rows = self._listings[self._listings["listing_id"] == listing_id]
        if rows.empty:
            return None
        return rows.iloc[0].to_dict()

    def get_instrument(self, instrument_id: str) -> dict[str, Any] | None:
        if self._instruments.empty:
            return None
        rows = self._instruments[self._instruments["instrument_id"] == instrument_id]
        if rows.empty:
            return None
        return rows.iloc[0].to_dict()

    def save_all(self) -> None:
        self._write_df(self._instruments_path, self._instruments)
        self._write_df(self._listings_path, self._listings)
        self._write_df(self._aliases_path, self._aliases)

    @staticmethod
    def _write_df(path: Path, df: pd.DataFrame) -> None:
        df.to_parquet(path, engine="pyarrow", index=False)
