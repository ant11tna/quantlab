from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .types import (
    DEFAULT_BASE_CURRENCY,
    DEFAULT_PORTFOLIO_ID,
    DEFAULT_PORTFOLIO_NAME,
    PORTFOLIO_COLUMNS,
    TARGET_COLUMNS,
)


class PortfolioStore:
    def __init__(self, base_dir: str = "data/portfolio") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._portfolios_path = self.base_dir / "portfolios.parquet"
        self._targets_path = self.base_dir / "targets.parquet"

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _today_utc_date() -> str:
        return datetime.now(timezone.utc).date().isoformat()

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
    def _write_df(path: Path, df: pd.DataFrame, columns: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        out = df.copy()
        for col in columns:
            if col not in out.columns:
                out[col] = None
        out[columns].to_parquet(path, engine="pyarrow", index=False)

    def load_portfolios(self) -> pd.DataFrame:
        return self._load_or_empty(self._portfolios_path, PORTFOLIO_COLUMNS)

    def load_targets(self) -> pd.DataFrame:
        df = self._load_or_empty(self._targets_path, TARGET_COLUMNS)
        if not df.empty:
            df["target_weight"] = pd.to_numeric(df["target_weight"], errors="coerce").fillna(0.0)
        return df

    def save_portfolios(self, df: pd.DataFrame) -> None:
        self._write_df(self._portfolios_path, df, PORTFOLIO_COLUMNS)

    def save_targets(self, df: pd.DataFrame) -> None:
        out = df.copy()
        if not out.empty:
            out["target_weight"] = pd.to_numeric(out["target_weight"], errors="coerce").fillna(0.0)
        self._write_df(self._targets_path, out, TARGET_COLUMNS)

    def ensure_default_portfolio(
        self,
        portfolio_id: str = DEFAULT_PORTFOLIO_ID,
        name: str = DEFAULT_PORTFOLIO_NAME,
        base_currency: str = DEFAULT_BASE_CURRENCY,
    ) -> None:
        portfolios = self.load_portfolios()
        now = self._now_iso()

        if portfolios.empty:
            portfolios = pd.DataFrame(
                [
                    {
                        "portfolio_id": portfolio_id,
                        "name": name,
                        "base_currency": base_currency,
                        "created_at": now,
                        "updated_at": now,
                    }
                ],
                columns=PORTFOLIO_COLUMNS,
            )
            self.save_portfolios(portfolios)
            return

        mask = portfolios["portfolio_id"].astype(str) == portfolio_id
        if not mask.any():
            row = pd.DataFrame(
                [
                    {
                        "portfolio_id": portfolio_id,
                        "name": name,
                        "base_currency": base_currency,
                        "created_at": now,
                        "updated_at": now,
                    }
                ],
                columns=PORTFOLIO_COLUMNS,
            )
            portfolios = pd.concat([portfolios, row], ignore_index=True)
            self.save_portfolios(portfolios)

    def get_active_effective_date(self, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> str:
        targets = self.load_targets()
        if targets.empty:
            return self._today_utc_date()

        subset = targets[targets["portfolio_id"].astype(str) == portfolio_id]
        if subset.empty:
            return self._today_utc_date()

        effective_dates = pd.to_datetime(subset["effective_date"], errors="coerce")
        if effective_dates.notna().any():
            return effective_dates.max().date().isoformat()

        raw_max = subset["effective_date"].astype(str).max()
        return raw_max if raw_max else self._today_utc_date()

    def upsert_target(
        self,
        portfolio_id: str,
        effective_date: str,
        listing_id: str,
        target_weight: float,
        added_at: str | None = None,
    ) -> None:
        targets = self.load_targets()
        row = {
            "portfolio_id": str(portfolio_id),
            "effective_date": str(effective_date),
            "listing_id": str(listing_id),
            "target_weight": float(target_weight),
            "added_at": added_at or self._now_iso(),
        }

        if targets.empty:
            targets = pd.DataFrame([row], columns=TARGET_COLUMNS)
            self.save_targets(targets)
            return

        mask = (
            (targets["portfolio_id"].astype(str) == row["portfolio_id"])
            & (targets["effective_date"].astype(str) == row["effective_date"])
            & (targets["listing_id"].astype(str) == row["listing_id"])
        )

        if mask.any():
            idx = targets.index[mask][0]
            targets.at[idx, "target_weight"] = row["target_weight"]
            if not str(targets.at[idx, "added_at"] or "").strip():
                targets.at[idx, "added_at"] = row["added_at"]
        else:
            targets = pd.concat([targets, pd.DataFrame([row], columns=TARGET_COLUMNS)], ignore_index=True)

        self.save_targets(targets)

    def remove_target(self, portfolio_id: str, effective_date: str, listing_id: str) -> None:
        targets = self.load_targets()
        if targets.empty:
            return

        keep_mask = ~(
            (targets["portfolio_id"].astype(str) == str(portfolio_id))
            & (targets["effective_date"].astype(str) == str(effective_date))
            & (targets["listing_id"].astype(str) == str(listing_id))
        )
        self.save_targets(targets.loc[keep_mask].reset_index(drop=True))
