from __future__ import annotations

import pandas as pd


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
