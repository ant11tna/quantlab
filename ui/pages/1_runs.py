"""Runs Dashboard（实验列表页）- 浏览与筛选历史回测 runs。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

# 确保能 import ui 下模块
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st
import yaml
from loguru import logger

from components import empty_state, page_header, section
from i18n import t

RUNS_DIR = Path("runs")


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _created_at_from_run_dir(run_dir: Path) -> datetime:
    run_id = run_dir.name
    dt_str = run_id.split("__", maxsplit=1)[0]
    try:
        return datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
    except ValueError:
        return datetime.fromtimestamp(run_dir.stat().st_mtime)


def _metric_value(metrics: dict[str, Any], *keys: str) -> Any:
    sections = [metrics, metrics.get("summary", {}), metrics.get("risk", {}), metrics.get("trading", {})]
    for key in keys:
        for section in sections:
            if isinstance(section, dict) and key in section:
                return section.get(key)
    return None


def build_runs_fingerprint(runs_dir: Path = RUNS_DIR) -> str:
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
    base_dir = Path(runs_dir)
    columns = [
        "run_id", "strategy", "total_return", "cagr", "sharpe", "max_drawdown", "start", "end", "created_at", "results_dir"
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

        strategy = _metric_value(metrics, "strategy", "strategy_name") or config.get("strategy") or config.get("name")
        if not strategy:
            strategy = run_dir.name.split("__", 1)[1] if "__" in run_dir.name else "unknown"

        rows.append(
            {
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
        )

    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df


def _fmt_num(value: float, fmt: str) -> str:
    if pd.isna(value):
        return "-"
    return fmt.format(value)


def _apply_filters(runs_df: pd.DataFrame, keyword: str, recent_days: str) -> pd.DataFrame:
    filtered = runs_df.copy()
    if keyword.strip():
        key = keyword.strip().lower()
        mask = pd.Series(False, index=filtered.index)
        for col in ("run_id", "strategy", "name"):
            if col in filtered.columns:
                mask = mask | filtered[col].astype(str).str.lower().str.contains(key, na=False)
        filtered = filtered[mask]

    if recent_days != "all" and "created_at" in filtered.columns:
        days = int(recent_days)
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        filtered = filtered[filtered["created_at"] >= cutoff]

    return filtered.sort_values(by="created_at", ascending=False, na_position="last")


def main() -> None:
    st.set_page_config(page_title=t("app.title") + " - " + t("app.runs"), page_icon="📊", layout="wide")
    page_header(t("runs.title"), t("runs.subtitle"))

    fingerprint = build_runs_fingerprint(RUNS_DIR)
    runs_df = scan_runs(fingerprint, str(RUNS_DIR))
    if runs_df.empty:
        empty_state(t("runs.empty"), t("runs.empty_hint"))
        st.session_state.pop("selected_run_id", None)
        return

    section(t("runs.kpi_section"))
    k1, k2, k3, k4 = st.columns(4)
    k1.metric(t("runs.kpi_count"), str(len(runs_df)))
    k2.metric(t("runs.kpi_best_return"), _fmt_num(runs_df["total_return"].max(), "{:.2%}"))
    k3.metric(t("runs.kpi_best_sharpe"), _fmt_num(runs_df["sharpe"].max(), "{:.3f}"))
    k4.metric(t("runs.kpi_worst_drawdown"), _fmt_num(runs_df["max_drawdown"].min(), "{:.2%}"))

    section(t("runs.filter_section"))
    f1, f2 = st.columns([2, 1])
    keyword = f1.text_input(t("runs.search_keyword"), placeholder=t("runs.search_placeholder"))
    recent_label = f2.selectbox(t("runs.recent_days"), options=["7", "30", "90", "all"], format_func=lambda x: t(f"runs.recent_{x}"))
    st.caption(t("runs.time_source"))

    filtered = _apply_filters(runs_df, keyword, recent_label)
    if filtered.empty:
        empty_state(t("runs.filtered_empty"), t("runs.filtered_empty_hint"))
        st.session_state.pop("selected_run_id", None)
        return

    table_df = filtered[[c for c in ["run_id", "start", "end", "total_return", "cagr", "max_drawdown", "sharpe"] if c in filtered.columns]].copy()
    if "vol" in filtered.columns:
        table_df["vol"] = filtered["vol"]
    table_df["total_return"] = pd.to_numeric(table_df.get("total_return"), errors="coerce") * 100
    table_df["cagr"] = pd.to_numeric(table_df.get("cagr"), errors="coerce") * 100
    table_df["max_drawdown"] = pd.to_numeric(table_df.get("max_drawdown"), errors="coerce") * 100

    section(t("runs.table_section"))
    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "total_return": st.column_config.NumberColumn(t("runs.total_return"), format="%.2f%%"),
            "cagr": st.column_config.NumberColumn(t("runs.cagr"), format="%.2f%%"),
            "max_drawdown": st.column_config.NumberColumn(t("runs.max_drawdown"), format="%.2f%%"),
            "sharpe": st.column_config.NumberColumn(t("runs.sharpe_ratio"), format="%.3f"),
            "vol": st.column_config.NumberColumn(t("runs.vol"), format="%.2f%%"),
        },
    )

    select_options = filtered["run_id"].tolist()
    previous = st.session_state.get("selected_run_id")
    default_id = previous if previous in select_options else select_options[0]
    default_idx = select_options.index(default_id)

    section(t("runs.select_section"))
    selected_run_id = st.selectbox(t("runs.select_run"), options=select_options, index=default_idx)
    st.session_state["selected_run_id"] = selected_run_id

    if st.button(t("runs.view_btn"), type="primary"):
        st.query_params["run_id"] = selected_run_id
        st.switch_page("pages/2_run_detail.py")


if __name__ == "__main__":
    main()
