"""Runs Dashboard（实验列表页）- 浏览与筛选历史回测 runs。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

# 确保能 import i18n（指向 ui/ 目录）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st
import yaml
from loguru import logger

from i18n import t

RUNS_DIR = Path("runs")


def _safe_float(value: Any) -> float:
    """将任意值安全转成 float，失败时返回 NaN。"""
    try:
        if value is None:
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _created_at_from_run_dir(run_dir: Path) -> datetime:
    """优先从 run_id 解析时间，失败时回退到目录 mtime。"""
    run_id = run_dir.name
    dt_str = run_id.split("__", maxsplit=1)[0]
    try:
        return datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
    except ValueError:
        return datetime.fromtimestamp(run_dir.stat().st_mtime)


def _metric_value(metrics: dict[str, Any], *keys: str) -> Any:
    """兼容不同 metrics 结构，按优先级查找字段。"""
    sections = [
        metrics,
        metrics.get("summary", {}),
        metrics.get("risk", {}),
        metrics.get("trading", {}),
    ]
    for key in keys:
        for section in sections:
            if isinstance(section, dict) and key in section:
                return section.get(key)
    return None


def build_runs_fingerprint(runs_dir: Path = RUNS_DIR) -> str:
    """基于 run 子目录 + metrics/config mtime 生成 fingerprint。"""
    if not runs_dir.exists():
        return "missing_runs_dir"

    parts: list[str] = []
    for run_dir in sorted([p for p in runs_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        metrics_path = run_dir / "results" / "metrics.json"
        config_path = run_dir / "config.yaml"
        metrics_mtime = metrics_path.stat().st_mtime_ns if metrics_path.exists() else 0
        config_mtime = config_path.stat().st_mtime_ns if config_path.exists() else 0
        parts.append(f"{run_dir.name}|{metrics_mtime}|{config_mtime}")

    return hashlib.md5("\n".join(parts).encode("utf-8")).hexdigest()


@st.cache_data(show_spinner=False)
def scan_runs(_fingerprint: str, runs_dir: str = "runs") -> pd.DataFrame:
    """扫描 runs 目录并汇总为 DataFrame（缓存由 fingerprint 驱动失效）。"""
    base_dir = Path(runs_dir)
    columns = [
        "run_id",
        "strategy",
        "total_return",
        "cagr",
        "sharpe",
        "max_drawdown",
        "start",
        "end",
        "created_at",
        "results_dir",
    ]
    if not base_dir.exists():
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    run_dirs = sorted([p for p in base_dir.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)

    for run_dir in run_dirs:
        metrics_path = run_dir / "results" / "metrics.json"
        if not metrics_path.exists():
            continue

        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"Failed to load metrics for {run_dir.name}: {exc}")
            continue

        config: dict[str, Any] = {}
        config_path = run_dir / "config.yaml"
        if config_path.exists():
            try:
                config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            except Exception as exc:
                logger.warning(f"Failed to parse config for {run_dir.name}: {exc}")

        strategy = (
            _metric_value(metrics, "strategy", "strategy_name")
            or config.get("strategy")
            or config.get("name")
            or run_dir.name.split("__")[1] if "__" in run_dir.name else "unknown"
        )

        row = {
            "run_id": run_dir.name,
            "strategy": str(strategy),
            "total_return": _safe_float(_metric_value(metrics, "total_return", "return_total")),
            "cagr": _safe_float(_metric_value(metrics, "cagr", "annual_return")),
            "sharpe": _safe_float(_metric_value(metrics, "sharpe", "sharpe_ratio")),
            "max_drawdown": _safe_float(_metric_value(metrics, "max_drawdown", "drawdown_max")),
            "start": _metric_value(metrics, "start", "start_date") or config.get("start"),
            "end": _metric_value(metrics, "end", "end_date") or config.get("end"),
            "created_at": _created_at_from_run_dir(run_dir),
            "results_dir": str((run_dir / "results").as_posix()),
        }
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df


def main() -> None:
    st.set_page_config(page_title=t("app.title") + " - " + t("app.runs"), page_icon="📊", layout="wide")
    st.title("Runs Dashboard")

    fingerprint = build_runs_fingerprint(RUNS_DIR)
    runs_df = scan_runs(fingerprint, str(RUNS_DIR))

    if runs_df.empty:
        st.info("暂无实验（runs 目录中没有可用 metrics.json）。")
        st.session_state.pop("selected_run_id", None)
        return

    st.caption(f"共 {len(runs_df)} 个 runs")

    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])

    with c1:
        strategies = sorted(runs_df["strategy"].dropna().astype(str).unique().tolist())
        selected_strategies = st.multiselect("Strategy", options=strategies, default=strategies)

    with c2:
        created_series = runs_df["created_at"].dropna()
        min_date = created_series.min().date() if not created_series.empty else datetime.today().date()
        max_date = created_series.max().date() if not created_series.empty else datetime.today().date()
        date_range = st.date_input("Created At 范围", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    with c3:
        sort_field = st.selectbox("排序字段", options=["total_return", "sharpe", "max_drawdown", "created_at"], index=0)

    with c4:
        sort_asc = st.radio("方向", options=["降序", "升序"], index=0) == "升序"

    filtered = runs_df.copy()
    if selected_strategies:
        filtered = filtered[filtered["strategy"].isin(selected_strategies)]

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_dt = pd.to_datetime(date_range[0])
        end_dt = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        filtered = filtered[(filtered["created_at"] >= start_dt) & (filtered["created_at"] <= end_dt)]

    filtered = filtered.sort_values(by=sort_field, ascending=sort_asc, na_position="last")

    if filtered.empty:
        st.warning("筛选后暂无实验。")
        st.session_state.pop("selected_run_id", None)
        return

    display_df = filtered[["run_id", "strategy", "total_return", "max_drawdown", "sharpe", "created_at"]].copy()
    display_df["total_return"] = display_df["total_return"] * 100
    display_df["max_drawdown"] = display_df["max_drawdown"] * 100

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "total_return": st.column_config.NumberColumn("total_return", format="%.2f%%"),
            "max_drawdown": st.column_config.NumberColumn("max_drawdown", format="%.2f%%"),
            "sharpe": st.column_config.NumberColumn("sharpe", format="%.3f"),
            "created_at": st.column_config.DatetimeColumn("created_at", format="YYYY-MM-DD HH:mm:ss"),
        },
    )

    valid_run_ids = filtered["run_id"].tolist()
    previous = st.session_state.get("selected_run_id")
    default_index = valid_run_ids.index(previous) if previous in valid_run_ids else 0

    selected_run_id = st.selectbox("选择当前查看 run", options=valid_run_ids, index=default_index)
    st.session_state["selected_run_id"] = selected_run_id

    selected_row = filtered[filtered["run_id"] == selected_run_id].iloc[0]

    if st.button("打开详情页/查看详情", type="primary"):
        st.query_params["run_id"] = selected_run_id
        st.switch_page("pages/2_run_detail.py")

    st.markdown("---")
    st.write(f"selected_run_id: `{selected_run_id}`")
    st.write(f"results_dir: `{selected_row['results_dir']}`")


if __name__ == "__main__":
    main()
