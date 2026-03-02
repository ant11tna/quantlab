"""多实验对比页。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

import ui.bootstrap  # noqa: F401

from components import empty_state, page_header, section
from components.common import info_card
from i18n import t
from quantlab.assets import group_weights_by_asset_class, load_assets_map

st.set_page_config(page_title=t("compare.title"), page_icon="📈", layout="wide")

RUNS_DIR = Path("runs")
ASSETS_PATH = Path("data/assets.yaml")


def _safe_read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _metric_value(metrics: dict[str, Any], *keys: str) -> Any:
    sections = [
        metrics,
        metrics.get("performance", {}) if isinstance(metrics.get("performance"), dict) else {},
        metrics.get("risk", {}) if isinstance(metrics.get("risk"), dict) else {},
        metrics.get("trade", {}) if isinstance(metrics.get("trade"), dict) else {},
        metrics.get("summary", {}) if isinstance(metrics.get("summary"), dict) else {},
    ]
    for key in keys:
        for sec in sections:
            if key in sec:
                return sec.get(key)
    return None


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else float("nan")
    except Exception:
        return float("nan")


def _load_equity_curve(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    if "ts" not in df.columns:
        if "date" in df.columns:
            df = df.rename(columns={"date": "ts"})
        elif "time" in df.columns:
            df = df.rename(columns={"time": "ts"})
    if "nav" not in df.columns:
        for c in ("equity", "portfolio_value", "value"):
            if c in df.columns:
                df = df.rename(columns={c: "nav"})
                break
    if not {"ts", "nav"}.issubset(df.columns):
        return pd.DataFrame()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    return df.dropna(subset=["ts", "nav"]).sort_values("ts")[["ts", "nav"]]


@st.cache_data(show_spinner=False)
def scan_runs(runs_dir: str = "runs") -> pd.DataFrame:
    base = Path(runs_dir)
    columns = ["run_id", "metrics_path", "equity_path", "weights_path"]
    if not base.exists():
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, str]] = []
    for run_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True):
        metrics_path = run_dir / "results" / "metrics.json"
        if metrics_path.exists():
            rows.append({
                "run_id": run_dir.name,
                "metrics_path": str(metrics_path),
                "equity_path": str(run_dir / "results" / "equity_curve.parquet"),
                "weights_path": str(run_dir / "results" / "weights.parquet"),
            })
    return pd.DataFrame(rows, columns=columns)


def _build_compare_metrics(selected_runs: list[str], run_index_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for run_id in selected_runs:
        item = run_index_df[run_index_df["run_id"] == run_id]
        if item.empty:
            continue
        metrics = _safe_read_json(Path(item.iloc[0]["metrics_path"]))
        cagr = _safe_float(_metric_value(metrics, "cagr", "annualized_return", "annual_return"))
        max_dd = _safe_float(_metric_value(metrics, "max_drawdown", "drawdown_max"))
        calmar = cagr / abs(max_dd) if pd.notna(cagr) and pd.notna(max_dd) and max_dd != 0 else float("nan")
        rows.append({
            "run_id": run_id,
            "total_return": _safe_float(_metric_value(metrics, "total_return", "return_total")),
            "cagr": cagr,
            "vol": _safe_float(_metric_value(metrics, "annual_vol", "volatility", "annualized_volatility", "vol")),
            "sharpe": _safe_float(_metric_value(metrics, "sharpe", "sharpe_ratio")),
            "max_drawdown": max_dd,
            "calmar": calmar,
        })
    return pd.DataFrame(rows)


def _build_nav_compare(selected_runs: list[str], run_index_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], dict[str, pd.DataFrame], tuple[pd.Timestamp, pd.Timestamp] | None]:
    warnings: list[str] = []
    run_nav_map: dict[str, pd.DataFrame] = {}

    for run_id in selected_runs:
        item = run_index_df[run_index_df["run_id"] == run_id]
        if item.empty:
            warnings.append(f"{run_id}: missing run")
            continue
        eq = _load_equity_curve(Path(item.iloc[0]["equity_path"]))
        if eq.empty:
            warnings.append(f"{run_id}: {t('compare.no_nav_for_run')}")
            continue
        run_nav_map[run_id] = eq

    if len(run_nav_map) < 2:
        return pd.DataFrame(), warnings, run_nav_map, None

    intersection_start = max(df["ts"].min() for df in run_nav_map.values())
    intersection_end = min(df["ts"].max() for df in run_nav_map.values())
    if pd.isna(intersection_start) or pd.isna(intersection_end) or intersection_start > intersection_end:
        return pd.DataFrame(), warnings, run_nav_map, None

    merged = None
    for run_id, eq in run_nav_map.items():
        truncated = eq[(eq["ts"] >= intersection_start) & (eq["ts"] <= intersection_end)].copy()
        if truncated.empty:
            warnings.append(f"{run_id}: {t('compare.no_overlap')}")
            continue
        base = truncated["nav"].iloc[0]
        if pd.isna(base) or base == 0:
            warnings.append(f"{run_id}: {t('compare.invalid_base_nav')}")
            continue
        if eq["ts"].min() < intersection_start or eq["ts"].max() > intersection_end:
            warnings.append(f"{run_id}: {t('compare.truncated_to_intersection')}")
        s = truncated.assign(**{run_id: truncated["nav"] / base})[["ts", run_id]]
        merged = s if merged is None else pd.merge(merged, s, on="ts", how="inner")

    if merged is None or merged.empty:
        return pd.DataFrame(), warnings, run_nav_map, (intersection_start, intersection_end)
    return merged.sort_values("ts").set_index("ts"), warnings, run_nav_map, (intersection_start, intersection_end)


def _drawdown_df(nav_df: pd.DataFrame) -> pd.DataFrame:
    out = {}
    for col in nav_df.columns:
        s = pd.to_numeric(nav_df[col], errors="coerce")
        out[col] = s / s.cummax() - 1
    return pd.DataFrame(out, index=nav_df.index)


def _build_asset_compare(selected_runs: list[str], run_index_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    if not ASSETS_PATH.exists():
        return pd.DataFrame(), [t("compare.no_assets_meta")]
    assets_map = load_assets_map(str(ASSETS_PATH))
    if not assets_map:
        return pd.DataFrame(), [t("compare.no_assets_meta")]
    rows = []
    for run_id in selected_runs:
        item = run_index_df[run_index_df["run_id"] == run_id]
        if item.empty:
            continue
        wp = Path(item.iloc[0]["weights_path"])
        if not wp.exists():
            warnings.append(f"{run_id}: {t('compare.no_weights_for_run')}")
            continue
        try:
            grouped = group_weights_by_asset_class(pd.read_parquet(wp), assets_map)
        except Exception:
            warnings.append(f"{run_id}: {t('compare.no_weights_for_run')}")
            continue
        if grouped.empty or "ts" not in grouped.columns:
            warnings.append(f"{run_id}: {t('compare.no_weights_for_run')}")
            continue
        last = grouped.sort_values("ts").iloc[-1]
        row = {"run_id": run_id}
        for c in grouped.columns:
            if c != "ts":
                row[c] = _safe_float(last[c])
        rows.append(row)
    if not rows:
        return pd.DataFrame(), warnings
    df = pd.DataFrame(rows).fillna(0.0)
    cols = ["run_id"] + sorted([c for c in df.columns if c != "run_id"])
    return df[cols], warnings


def main() -> None:
    page_header(t("compare.title"), t("compare.subtitle"))
    runs_df = scan_runs(str(RUNS_DIR))
    if runs_df.empty:
        empty_state(t("compare.empty"))
        return

    options = runs_df["run_id"].tolist()
    preselected = st.query_params.get("run_ids", "")
    default_selected = [rid for rid in str(preselected).split(",") if rid in options] if preselected else options[:2]
    if len(default_selected) < 2:
        default_selected = options[:2]
    selected_runs = st.multiselect(t("compare.select_runs"), options=options, default=default_selected)
    if len(selected_runs) < 2:
        st.warning(t("compare.need_two"))
        return

    nav_df, nav_warnings, _, intersection = _build_nav_compare(selected_runs, runs_df)
    if intersection is not None:
        start, end = intersection
        points = len(nav_df) if not nav_df.empty else 0
        info_card(
            t("compare.intersection_card_title"),
            t("compare.intersection", start=start.date(), end=end.date(), points=points),
        )
    for msg in nav_warnings:
        st.warning(msg)
    if nav_df.empty or nav_df.shape[1] < 2:
        empty_state(t("compare.no_overlap"))
        return

    section(t("compare.metrics_section"))
    metrics_df = _build_compare_metrics(selected_runs, runs_df)
    if metrics_df.empty:
        empty_state(t("compare.no_metrics"))
    else:
        disp = metrics_df.copy()
        for c in ["total_return", "cagr", "vol", "max_drawdown"]:
            disp[c] = pd.to_numeric(disp[c], errors="coerce") * 100
        st.dataframe(
            disp,
            use_container_width=True,
            hide_index=True,
            column_config={
                "total_return": st.column_config.NumberColumn("total_return", format="%.2f%%"),
                "cagr": st.column_config.NumberColumn("cagr", format="%.2f%%"),
                "vol": st.column_config.NumberColumn("vol", format="%.2f%%"),
                "sharpe": st.column_config.NumberColumn("sharpe", format="%.3f"),
                "max_drawdown": st.column_config.NumberColumn("max_drawdown", format="%.2f%%"),
                "calmar": st.column_config.NumberColumn("calmar", format="%.3f"),
            },
        )

    section(t("compare.nav_section"))
    st.line_chart(nav_df)

    section(t("compare.drawdown_section"))
    st.line_chart(_drawdown_df(nav_df))

    section(t("compare.asset_section"))
    class_df, class_warnings = _build_asset_compare(selected_runs, runs_df)
    for msg in class_warnings:
        st.warning(msg)
    if class_df.empty:
        empty_state(t("compare.no_asset_data"))
    else:
        st.dataframe(class_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
