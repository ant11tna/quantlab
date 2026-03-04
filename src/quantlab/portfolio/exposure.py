from __future__ import annotations

import pandas as pd


def compute_exposure(enriched_targets_df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if not group_cols:
        raise ValueError("group_cols 不能为空")

    base_cols = ["target_weight", *group_cols]
    for col in base_cols:
        if col not in enriched_targets_df.columns:
            raise ValueError(f"缺少必需列: {col}")

    if enriched_targets_df.empty:
        return pd.DataFrame(columns=["group_key", "weight", "weight_pct"])

    out = enriched_targets_df.copy()
    out["target_weight"] = pd.to_numeric(out["target_weight"], errors="coerce").fillna(0.0)

    for col in group_cols:
        values = out[col].fillna("").astype(str).str.strip()
        out[col] = values.where(values != "", "unknown")

    grouped = (
        out.groupby(group_cols, dropna=False)["target_weight"]
        .sum()
        .reset_index(name="weight")
    )

    grouped["group_key"] = grouped[group_cols].astype(str).agg(" / ".join, axis=1)
    grouped["weight"] = pd.to_numeric(grouped["weight"], errors="coerce").fillna(0.0)
    grouped["weight_pct"] = grouped["weight"] * 100.0

    return grouped[["group_key", "weight", "weight_pct"]].sort_values("weight", ascending=False).reset_index(drop=True)


def compute_concentration(enriched_targets_df: pd.DataFrame) -> dict:
    if "target_weight" not in enriched_targets_df.columns:
        raise ValueError("缺少必需列: target_weight")

    if enriched_targets_df.empty:
        return {
            "count": 0,
            "top1": 0.0,
            "top1_pct": 0.0,
            "top3": 0.0,
            "top3_pct": 0.0,
            "top5": 0.0,
            "top5_pct": 0.0,
            "hhi": 0.0,
        }

    weights = pd.to_numeric(enriched_targets_df["target_weight"], errors="coerce").fillna(0.0)
    weights = weights[weights > 0].sort_values(ascending=False).reset_index(drop=True)

    if weights.empty:
        return {
            "count": 0,
            "top1": 0.0,
            "top1_pct": 0.0,
            "top3": 0.0,
            "top3_pct": 0.0,
            "top5": 0.0,
            "top5_pct": 0.0,
            "hhi": 0.0,
        }

    top1 = float(weights.head(1).sum())
    top3 = float(weights.head(3).sum())
    top5 = float(weights.head(5).sum())
    hhi = float((weights**2).sum())

    return {
        "count": int(weights.shape[0]),
        "top1": top1,
        "top1_pct": top1 * 100.0,
        "top3": top3,
        "top3_pct": top3 * 100.0,
        "top5": top5,
        "top5_pct": top5 * 100.0,
        "hhi": hhi,
    }
