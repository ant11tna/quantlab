from __future__ import annotations

import pandas as pd

from quantlab.universe.store import UniverseStore


def validate_weights(df_targets: pd.DataFrame) -> tuple[float, bool, str]:
    if df_targets.empty or "target_weight" not in df_targets.columns:
        return 0.0, False, "请填写权重"

    weights = pd.to_numeric(df_targets["target_weight"], errors="coerce").fillna(0.0)
    total = float(weights.sum())
    if total == 0:
        return total, False, "请填写权重"

    is_close_to_one = abs(total - 1.0) <= 1e-3
    if is_close_to_one:
        message = "权重合计接近 1.0。"
    else:
        message = "权重合计未接近 1.0，请检查或归一化。"
    return total, is_close_to_one, message


def normalize_weights(df_targets: pd.DataFrame) -> pd.DataFrame:
    if df_targets.empty or "target_weight" not in df_targets.columns:
        return df_targets.copy()

    out = df_targets.copy()
    weights = pd.to_numeric(out["target_weight"], errors="coerce").fillna(0.0)
    total = float(weights.sum())
    if total <= 0:
        out["target_weight"] = 0.0
        return out

    out["target_weight"] = weights / total
    return out


def enrich_targets_with_universe(targets_df: pd.DataFrame) -> pd.DataFrame:
    out = targets_df.copy()
    if "listing_id" not in out.columns:
        out["listing_id"] = ""

    universe_store = UniverseStore(base_dir="data/universe")
    listings = universe_store.load_listings()
    instruments = universe_store.load_instruments()

    listing_cols = ["listing_id", "instrument_id", "region", "exchange", "ticker"]
    if listings.empty:
        for col in ["instrument_id", "region", "exchange", "ticker"]:
            out[col] = ""
    else:
        for col in listing_cols:
            if col not in listings.columns:
                listings[col] = None
        out = out.merge(listings[listing_cols], how="left", on="listing_id")

    instrument_cols = ["instrument_id", "name"]
    if instruments.empty:
        out["name"] = ""
    else:
        for col in instrument_cols:
            if col not in instruments.columns:
                instruments[col] = None
        out = out.merge(instruments[instrument_cols], how="left", on="instrument_id")

    out["region"] = out["region"].fillna("").astype(str)
    out["exchange"] = out["exchange"].fillna("").astype(str)
    out["ticker"] = out["ticker"].fillna("").astype(str).str.strip()
    out["name"] = out["name"].fillna("").astype(str).str.strip()

    missing_listing = out["instrument_id"].isna()
    out.loc[missing_listing, "ticker"] = "(unknown listing)"
    out.loc[missing_listing, "name"] = "(unknown listing)"

    missing_name = (~missing_listing) & (out["name"] == "")
    out.loc[missing_name, "name"] = "(name unknown)"

    return out
