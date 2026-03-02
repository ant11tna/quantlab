"""Asset metadata helpers for class-level exposure analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd
import yaml

VALID_ASSET_CLASSES = {
    "equity_cn",
    "bond_cn",
    "gold",
    "equity_us",
    "bond_us",
    "cash",
    "other",
}


def load_assets_map(path: str = "data/assets.yaml") -> Dict[str, Dict[str, str]]:
    """Load symbol -> metadata map from YAML.

    Returns empty dict when file is missing or malformed.
    """
    p = Path(path)
    if not p.exists():
        return {}

    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        return {}

    out: Dict[str, Dict[str, str]] = {}
    for symbol, meta in data.items():
        if not isinstance(symbol, str):
            continue
        if not isinstance(meta, dict):
            meta = {}

        name = str(meta.get("name", symbol))
        asset_class = str(meta.get("asset_class", "other"))
        if asset_class not in VALID_ASSET_CLASSES:
            asset_class = "other"

        out[symbol] = {
            "name": name,
            "asset_class": asset_class,
        }

    return out


def get_asset_class(symbol: str, assets_map: Dict[str, Dict[str, str]]) -> str:
    """Get asset class for symbol, defaulting to 'other'."""
    meta = assets_map.get(symbol, {}) if isinstance(assets_map, dict) else {}
    asset_class = meta.get("asset_class") if isinstance(meta, dict) else None
    if isinstance(asset_class, str) and asset_class in VALID_ASSET_CLASSES:
        return asset_class
    return "other"


def _weight_columns(weights_df: pd.DataFrame) -> Iterable[str]:
    return [c for c in weights_df.columns if c != "ts"]


def group_weights_by_asset_class(weights_df: pd.DataFrame, assets_map: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """Aggregate symbol weights to asset-class exposure by timestamp.

    Input supports wide format (ts + symbol columns) and long format (ts, symbol, weight).
    Output format: ts + asset_class columns.
    """
    if weights_df is None or weights_df.empty:
        return pd.DataFrame(columns=["ts"])

    df = weights_df.copy()

    if {"ts", "symbol", "weight"}.issubset(df.columns):
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
        df = df.dropna(subset=["ts", "symbol", "weight"])
        if df.empty:
            return pd.DataFrame(columns=["ts"])

        df["asset_class"] = df["symbol"].map(lambda s: get_asset_class(str(s), assets_map))
        grouped = (
            df.groupby(["ts", "asset_class"], as_index=False)["weight"]
            .sum()
            .pivot(index="ts", columns="asset_class", values="weight")
            .fillna(0.0)
            .sort_index()
        )
        return grouped.reset_index()

    if "ts" not in df.columns:
        return pd.DataFrame(columns=["ts"])

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts")
    if df.empty:
        return pd.DataFrame(columns=["ts"])

    for col in _weight_columns(df):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    classes = sorted({get_asset_class(str(sym), assets_map) for sym in _weight_columns(df)} | {"other"})
    out = pd.DataFrame({"ts": df["ts"]})
    for cls in classes:
        out[cls] = 0.0

    for symbol in _weight_columns(df):
        cls = get_asset_class(str(symbol), assets_map)
        out[cls] = out[cls] + df[symbol]

    return out
