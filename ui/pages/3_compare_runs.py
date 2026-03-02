"""多实验对比页。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Compare Runs", page_icon="📈", layout="wide")

RUNS_DIR = Path("runs")


def _safe_read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _metric_value(metrics: dict[str, Any], *keys: str) -> Any:
    """兼容 metrics 新旧结构，按优先级读取指标。"""
    sections: list[dict[str, Any]] = [
        metrics,
        metrics.get("performance", {}) if isinstance(metrics.get("performance"), dict) else {},
        metrics.get("risk", {}) if isinstance(metrics.get("risk"), dict) else {},
        metrics.get("trade", {}) if isinstance(metrics.get("trade"), dict) else {},
        metrics.get("summary", {}) if isinstance(metrics.get("summary"), dict) else {},
        metrics.get("trading", {}) if isinstance(metrics.get("trading"), dict) else {},
    ]

    for key in keys:
        for section in sections:
            if key in section:
                return section.get(key)
    return None


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _load_equity_curve(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    out = df.copy()
    if "ts" not in out.columns:
        if "date" in out.columns:
            out = out.rename(columns={"date": "ts"})
        elif "time" in out.columns:
            out = out.rename(columns={"time": "ts"})

    if "nav" not in out.columns:
        for candidate in ("equity", "portfolio_value", "value"):
            if candidate in out.columns:
                out = out.rename(columns={candidate: "nav"})
                break

    if not {"ts", "nav"}.issubset(out.columns):
        return pd.DataFrame()

    out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    out["nav"] = pd.to_numeric(out["nav"], errors="coerce")
    out = out.dropna(subset=["ts", "nav"]).sort_values("ts")

    if out.empty:
        return pd.DataFrame()

    return out[["ts", "nav"]]


@st.cache_data(show_spinner=False)
def scan_runs(runs_dir: str = "runs") -> pd.DataFrame:
    base = Path(runs_dir)
    columns = ["run_id", "metrics_path", "equity_path"]
    if not base.exists():
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, str]] = []
    for run_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True):
        metrics_path = run_dir / "results" / "metrics.json"
        if not metrics_path.exists():
            continue
        rows.append(
            {
                "run_id": run_dir.name,
                "metrics_path": str(metrics_path),
                "equity_path": str(run_dir / "results" / "equity_curve.parquet"),
            }
        )

    return pd.DataFrame(rows, columns=columns)


def _build_compare_metrics(selected_runs: list[str], run_index_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for run_id in selected_runs:
        item = run_index_df[run_index_df["run_id"] == run_id]
        if item.empty:
            continue

        metrics_path = Path(item.iloc[0]["metrics_path"])
        metrics = _safe_read_json(metrics_path)

        rows.append(
            {
                "run_id": run_id,
                "total_return": _safe_float(_metric_value(metrics, "total_return", "return_total")),
                "cagr": _safe_float(_metric_value(metrics, "cagr", "annualized_return", "annual_return")),
                "sharpe": _safe_float(_metric_value(metrics, "sharpe", "sharpe_ratio")),
                "max_drawdown": _safe_float(_metric_value(metrics, "max_drawdown", "drawdown_max")),
                "annual_vol": _safe_float(_metric_value(metrics, "annual_vol", "volatility", "annualized_volatility")),
            }
        )

    return pd.DataFrame(rows)


def _build_nav_compare(selected_runs: list[str], run_index_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    merged: pd.DataFrame | None = None
    warnings: list[str] = []

    for run_id in selected_runs:
        item = run_index_df[run_index_df["run_id"] == run_id]
        if item.empty:
            warnings.append(f"{run_id}: run 索引不存在")
            continue

        equity_path = Path(item.iloc[0]["equity_path"])
        if not equity_path.exists():
            warnings.append(f"{run_id}: 缺少 equity_curve.parquet")
            continue

        eq = _load_equity_curve(equity_path)
        if eq.empty:
            warnings.append(f"{run_id}: equity_curve 为空或字段不完整")
            continue

        base = eq["nav"].iloc[0]
        if pd.isna(base) or base == 0:
            warnings.append(f"{run_id}: 初始 NAV 不可用，跳过")
            continue

        series_df = eq.assign(**{run_id: eq["nav"] / base})[["ts", run_id]]
        if merged is None:
            merged = series_df
        else:
            merged = pd.merge(merged, series_df, on="ts", how="inner")

    if merged is None or merged.empty:
        return pd.DataFrame(), warnings

    merged = merged.sort_values("ts").set_index("ts")
    return merged, warnings


def _drawdown_df(nav_df: pd.DataFrame) -> pd.DataFrame:
    drawdowns: dict[str, pd.Series] = {}
    for col in nav_df.columns:
        s = pd.to_numeric(nav_df[col], errors="coerce")
        dd = s / s.cummax() - 1
        drawdowns[col] = dd
    return pd.DataFrame(drawdowns, index=nav_df.index)


def main() -> None:
    st.title("📈 多实验对比")

    runs_df = scan_runs(str(RUNS_DIR))
    if runs_df.empty:
        st.info("runs 目录下暂无可对比实验（缺少 results/metrics.json）。")
        return

    options = runs_df["run_id"].tolist()
    selected_runs = st.multiselect("选择 run_id（可多选）", options=options, default=options[:2])

    if len(selected_runs) < 2:
        st.warning("请至少选择 2 个 run 才能进行对比。")
        return

    st.subheader("指标对比")
    compare_df = _build_compare_metrics(selected_runs, runs_df)
    sort_col = st.selectbox("排序字段", ["total_return", "cagr", "sharpe", "max_drawdown", "annual_vol"], index=0)
    sort_asc = st.toggle("升序", value=False)

    if not compare_df.empty:
        compare_df = compare_df.sort_values(by=sort_col, ascending=sort_asc, na_position="last")
        display_df = compare_df.copy()
        display_df["total_return"] = display_df["total_return"] * 100
        display_df["cagr"] = display_df["cagr"] * 100
        display_df["max_drawdown"] = display_df["max_drawdown"] * 100
        display_df["annual_vol"] = display_df["annual_vol"] * 100

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "total_return": st.column_config.NumberColumn("total_return", format="%.2f%%"),
                "cagr": st.column_config.NumberColumn("cagr", format="%.2f%%"),
                "sharpe": st.column_config.NumberColumn("sharpe", format="%.3f"),
                "max_drawdown": st.column_config.NumberColumn("max_drawdown", format="%.2f%%"),
                "annual_vol": st.column_config.NumberColumn("annual_vol", format="%.2f%%"),
            },
        )
    else:
        st.info("未读取到可展示的指标。")

    st.subheader("NAV 叠加")
    nav_df, nav_warnings = _build_nav_compare(selected_runs, runs_df)
    for msg in nav_warnings:
        st.warning(msg)

    if nav_df.empty or nav_df.shape[1] < 2:
        st.info("可用于叠加的净值曲线不足 2 条（需有交集时间区间）。")
        return

    st.caption(f"交集区间：{nav_df.index.min().date()} ~ {nav_df.index.max().date()}，共 {len(nav_df)} 个点")
    st.line_chart(nav_df)

    show_drawdown = st.toggle("显示回撤对比", value=False)
    if show_drawdown:
        st.subheader("回撤叠加")
        st.line_chart(_drawdown_df(nav_df))


if __name__ == "__main__":
    main()
