"""Homepage dashboard components."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json

import pandas as pd
import streamlit as st

from i18n import t
from quantlab.data.update_all import update_all_stream
from ui.data.loader import RUNS_DIR, list_runs, load_equity_curve, load_run


RESULT_FILES = [
    "metrics.json",
    "equity_curve.parquet",
    "weights.parquet",
    "risk_status.json",
    "yearly_stats.parquet",
    "stress_test.json",
]


def _latest_run_id(runs_df: pd.DataFrame) -> str | None:
    if runs_df.empty:
        return None
    if "created_at" in runs_df.columns:
        ordered = runs_df.sort_values("created_at", ascending=False, na_position="last")
        return str(ordered.iloc[0]["run_id"])
    return str(runs_df.iloc[0]["run_id"])


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except Exception:
        return float("nan")




def _pick_metric(metrics: dict, *keys: str):
    for key in keys:
        if key in metrics and metrics[key] is not None:
            return metrics[key]
    for block in ("summary", "risk", "performance", "trading", "trade"):
        sub = metrics.get(block, {}) if isinstance(metrics, dict) else {}
        if isinstance(sub, dict):
            for key in keys:
                if key in sub and sub[key] is not None:
                    return sub[key]
    return None


def _fmt_pct(v: object) -> str:
    x = _safe_float(v)
    return "-" if pd.isna(x) else f"{x:.2%}"


def _fmt_num(v: object, digits: int = 3) -> str:
    x = _safe_float(v)
    return "-" if pd.isna(x) else f"{x:.{digits}f}"


def render_system_status() -> None:
    """Render basic system/run status cards."""
    st.subheader(t("home.system_status.title"))

    def _latest_file_mtime(root: Path) -> datetime | None:
        if not root.exists() or not root.is_dir():
            return None
        latest_ts: float | None = None
        for p in root.rglob("*"):
            if p.is_file():
                try:
                    mtime = p.stat().st_mtime
                except OSError:
                    continue
                latest_ts = mtime if latest_ts is None else max(latest_ts, mtime)
        return datetime.fromtimestamp(latest_ts) if latest_ts is not None else None

    def _has_any_file(root: Path) -> bool:
        if not root.exists() or not root.is_dir():
            return False
        return any(p.is_file() for p in root.rglob("*"))

    def _latest_run() -> str | None:
        if not RUNS_DIR.exists() or not RUNS_DIR.is_dir():
            return None
        latest_name: str | None = None
        latest_mtime: float | None = None
        for p in RUNS_DIR.iterdir():
            if not p.is_dir():
                continue
            try:
                mtime = p.stat().st_mtime
            except OSError:
                continue
            if latest_mtime is None or mtime > latest_mtime:
                latest_mtime = mtime
                latest_name = p.name
        return latest_name

    def _find_last_update_status() -> str:
        """Find update status from known persisted files if any.

        Current repo logic (`update_all_stream`) streams events to UI and does not
        persist a dedicated update status file, so default is Unknown.
        """
        candidates = [
            Path("logs/update_all_last.json"),
            Path("logs/update_all.json"),
            Path("logs/update_all.log"),
            Path("data/update_all_last.json"),
        ]
        existing = [p for p in candidates if p.exists() and p.is_file()]
        if not existing:
            return t("home.system_status.unknown")

        latest_file = max(existing, key=lambda p: p.stat().st_mtime)
        if latest_file.suffix == ".json":
            try:
                payload = json.loads(latest_file.read_text(encoding="utf-8"))
                ok = payload.get("ok")
                raw_err = int(payload.get("raw_error_count", 0) or 0)
                curated_err = int(payload.get("curated_error_count", 0) or 0)
                if ok is True and raw_err == 0 and curated_err == 0:
                    return t("home.system_status.healthy")
                if raw_err > 0 or curated_err > 0 or ok is False:
                    return t("home.system_status.error")
                return t("home.system_status.unknown")
            except Exception:
                return t("home.system_status.unknown")

        try:
            tail = latest_file.read_text(encoding="utf-8", errors="ignore")[-3000:].lower()
        except Exception:
            return t("home.system_status.unknown")
        if "error" in tail or "failed" in tail:
            return t("home.system_status.error")
        if "finished" in tail or "success" in tail or "ok" in tail:
            return t("home.system_status.healthy")
        return t("home.system_status.unknown")

    raw_dir = Path("data/raw")
    curated_dir = Path("data/curated")

    raw_mtime = _latest_file_mtime(raw_dir)
    curated_mtime = _latest_file_mtime(curated_dir)
    latest_data_mtime = max([d for d in [raw_mtime, curated_mtime] if d is not None], default=None)
    data_updated_text = latest_data_mtime.strftime("%Y-%m-%d %H:%M:%S") if latest_data_mtime else t("common.not_available")

    latest_run = _latest_run() or t("common.not_available")

    raw_ok = _has_any_file(raw_dir)
    curated_ok = _has_any_file(curated_dir)
    if raw_ok or curated_ok:
        integrity = t("common.status_ok")
    else:
        integrity = t("common.status_missing")

    system_status = _find_last_update_status()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(t("home.system_status.data_updated"), data_updated_text)
    c2.metric(t("home.system_status.latest_run"), latest_run)
    c3.metric(t("home.system_status.data_integrity"), integrity)
    c4.metric(t("home.system_status.system_health"), system_status)

    runs_df = list_runs()
    if runs_df.empty:
        st.info(t("runs.empty"))


def render_portfolio_snapshot(run_id: str) -> None:
    """Render portfolio snapshot metrics and optional NAV curve."""
    st.subheader(t("home.portfolio_snapshot.title"))

    if not run_id:
        st.info(t("home.portfolio_snapshot.no_run_selected"))
        return

    run_data = load_run(run_id)
    metrics = run_data.get("metrics_dict") or {}

    eq = load_equity_curve(run_id)
    eq_stats: dict[str, object] = {}
    plot_df = pd.DataFrame()
    if not eq.empty and {"ts", "nav"}.issubset(eq.columns):
        plot_df = eq[["ts", "nav"]].dropna().copy()
        if not plot_df.empty:
            plot_df["ts"] = pd.to_datetime(plot_df["ts"], errors="coerce")
            plot_df["nav"] = pd.to_numeric(plot_df["nav"], errors="coerce")
            plot_df = plot_df.dropna(subset=["ts", "nav"]).sort_values("ts")
        if len(plot_df) >= 2:
            ret = plot_df["nav"].pct_change().dropna()
            ann_vol = ret.std() * (252 ** 0.5) if not ret.empty else float("nan")
            dd = plot_df["nav"] / plot_df["nav"].cummax() - 1
            max_dd_eq = dd.min() if not dd.empty else float("nan")
            current_dd_eq = dd.iloc[-1] if not dd.empty else float("nan")

            dd_duration = float("nan")
            if not dd.empty:
                in_dd = dd < 0
                if in_dd.any():
                    groups = (in_dd != in_dd.shift()).cumsum()
                    durations = in_dd.groupby(groups).sum()
                    dd_duration = float(durations.max()) if not durations.empty else float("nan")

            eq_stats = {
                "ann_vol": ann_vol,
                "max_dd": max_dd_eq,
                "dd_duration_days": dd_duration,
                "current_dd": current_dd_eq,
            }

    risk_status = {}
    risk_path = Path("runs") / run_id / "results" / "risk_status.json"
    if risk_path.exists():
        try:
            risk_status = json.loads(risk_path.read_text(encoding="utf-8"))
            if not isinstance(risk_status, dict):
                risk_status = {}
        except Exception:
            risk_status = {}

    total_return = _pick_metric(metrics, "total_return", "return_total")
    cagr = _pick_metric(metrics, "cagr", "annual_return", "annualized_return")
    sharpe = _pick_metric(metrics, "sharpe", "sharpe_ratio")
    max_dd = _pick_metric(metrics, "max_drawdown", "drawdown_max")
    if max_dd is None or pd.isna(_safe_float(max_dd)):
        max_dd = eq_stats.get("max_dd")

    calmar = _pick_metric(metrics, "calmar")
    if calmar is None:
        cagr_v = _safe_float(cagr)
        max_dd_v = _safe_float(max_dd)
        calmar = cagr_v / abs(max_dd_v) if pd.notna(cagr_v) and pd.notna(max_dd_v) and max_dd_v != 0 else float("nan")

    ann_vol = _pick_metric(metrics, "annual_vol", "volatility", "annualized_volatility", "vol")
    if ann_vol is None or pd.isna(_safe_float(ann_vol)):
        ann_vol = eq_stats.get("ann_vol")

    dd_duration_days = eq_stats.get("dd_duration_days")
    current_dd = risk_status.get("current_drawdown")
    if current_dd is None or pd.isna(_safe_float(current_dd)):
        current_dd = eq_stats.get("current_dd")

    row1 = st.columns(4)
    row1[0].metric("Total Return", _fmt_pct(total_return))
    row1[1].metric("CAGR", _fmt_pct(cagr))
    row1[2].metric("Sharpe", _fmt_num(sharpe, 3))
    row1[3].metric("Calmar", _fmt_num(calmar, 3))

    row2 = st.columns(4)
    row2[0].metric("Max Drawdown", _fmt_pct(max_dd))
    row2[1].metric("Ann.Vol", _fmt_pct(ann_vol))
    row2[2].metric("DD Duration Days", _fmt_num(dd_duration_days, 0))
    row2[3].metric("Current Drawdown", _fmt_pct(current_dd))

    if not plot_df.empty:
        st.line_chart(plot_df.set_index("ts")["nav"])
    else:
        st.info(t("home.portfolio_snapshot.equity_unavailable", run_id=run_id))



def _sort_recent_runs(runs_df: pd.DataFrame) -> pd.DataFrame:
    df = runs_df.copy()
    if "created_at" in df.columns:
        dt = pd.to_datetime(df["created_at"], errors="coerce")
    else:
        dt = pd.Series([pd.NaT] * len(df), index=df.index)

    if "run_id" in df.columns:
        mtime_vals: list[pd.Timestamp] = []
        for rid in df["run_id"].astype(str):
            run_dir = RUNS_DIR / rid
            try:
                mtime_vals.append(pd.to_datetime(run_dir.stat().st_mtime, unit="s"))
            except Exception:
                mtime_vals.append(pd.NaT)
        mtime_series = pd.Series(mtime_vals, index=df.index)
    else:
        mtime_series = pd.Series([pd.NaT] * len(df), index=df.index)

    df["_sort_time"] = dt.fillna(mtime_series)
    return df.sort_values("_sort_time", ascending=False, na_position="last")

def render_recent_runs_table(runs_df: pd.DataFrame) -> None:
    """Render recent runs table with quick detail/compare actions."""
    st.subheader(t("home.recent_runs.title"))

    if runs_df.empty or "run_id" not in runs_df.columns:
        st.info(t("runs.empty"))
        return

    ordered = _sort_recent_runs(runs_df)
    view = ordered.head(10).copy()

    name_col = "strategy" if "strategy" in view.columns else ("name" if "name" in view.columns else None)
    show_cols = ["run_id"]
    if name_col:
        show_cols.append(name_col)
    show_cols += [c for c in ["total_return", "sharpe", "max_drawdown", "created_at"] if c in view.columns]

    table_df = view[show_cols].copy()
    if "total_return" in table_df.columns:
        table_df["total_return"] = pd.to_numeric(table_df["total_return"], errors="coerce") * 100
    if "max_drawdown" in table_df.columns:
        table_df["max_drawdown"] = pd.to_numeric(table_df["max_drawdown"], errors="coerce") * 100
    if "created_at" in table_df.columns:
        table_df["created_at"] = pd.to_datetime(table_df["created_at"], errors="coerce")

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "total_return": st.column_config.NumberColumn("total_return", format="%.2f%%"),
            "max_drawdown": st.column_config.NumberColumn("max_drawdown", format="%.2f%%"),
            "sharpe": st.column_config.NumberColumn("sharpe", format="%.3f"),
            "created_at": st.column_config.DatetimeColumn("created_at", format="YYYY-MM-DD HH:mm:ss"),
        },
    )

    options = ordered["run_id"].astype(str).tolist()
    prev_selected = st.session_state.get("selected_run_id")
    default_run_id = prev_selected if isinstance(prev_selected, str) and prev_selected in options else options[0]
    selected_run_id = st.selectbox(
        t("home.recent_runs.run_id"), options=options, index=options.index(default_run_id), key="recent_runs_selector"
    )
    st.session_state["selected_run_id"] = selected_run_id

    c1, c2 = st.columns(2)
    with c1:
        if st.button(t("home.recent_runs.open_detail"), use_container_width=True, key="recent_open_detail"):
            st.query_params["run_id"] = selected_run_id
            st.switch_page("pages/2_run_detail.py")
    with c2:
        if st.button(t("home.recent_runs.add_to_compare"), use_container_width=True, key="recent_add_compare"):
            basket = st.session_state.get("compare_basket", [])
            if not isinstance(basket, list):
                basket = []
            basket = [str(x) for x in basket]
            if selected_run_id not in basket:
                basket.append(selected_run_id)
                st.session_state["compare_basket"] = basket
                st.success(t("home.recent_runs.added_to_compare", run_id=selected_run_id))
            else:
                st.info(t("home.recent_runs.already_in_compare", run_id=selected_run_id))


def render_data_health() -> None:
    """Lightweight data health monitoring for core data directories."""
    st.subheader(t("home.data_health.title"))

    def _scan_dir(root: Path) -> dict[str, object]:
        if not root.exists() or not root.is_dir():
            return {
                "directory": root.as_posix(),
                "status": t("common.status_missing_text"),
                "last_updated": t("common.not_available"),
                "file_count": 0,
            }

        file_count = 0
        latest_mtime: float | None = None
        for fp in root.rglob("*"):
            if not fp.is_file():
                continue
            file_count += 1
            try:
                mtime = fp.stat().st_mtime
            except OSError:
                continue
            latest_mtime = mtime if latest_mtime is None else max(latest_mtime, mtime)

        if file_count == 0:
            status = t("common.status_empty")
            last_updated = t("common.not_available")
        else:
            status = t("common.status_ok_text")
            last_updated = datetime.fromtimestamp(latest_mtime).strftime("%Y-%m-%d %H:%M:%S") if latest_mtime else t("common.not_available")

        return {
            "directory": root.as_posix(),
            "status": status,
            "last_updated": last_updated,
            "file_count": file_count,
        }

    # Lightweight directory-level checks only; no symbol-specific assumptions.
    targets = [Path("data/raw"), Path("data/curated"), Path("data/features")]
    rows = [_scan_dir(d) for d in targets]

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)



def _run_update_stream_ui(force_update_all: bool, *, sidebar: bool = False) -> None:
    """Run update_all_stream and render progress/log/error/done in current container."""
    target = st.sidebar if sidebar else st
    progress = target.progress(0)
    status = target.empty()
    detail = target.empty()
    backend = target.empty()
    err = target.empty()

    current_stage = None
    for ev in update_all_stream(force=force_update_all):
        ev_type = ev.get("type")
        if ev_type == "progress":
            stage = ev.get("stage", "unknown")
            if stage != current_stage:
                current_stage = stage
                progress.progress(0)

            done = ev.get("done", 0) or 0
            total = ev.get("total", 0) or 0
            pct = done / max(total, 1)
            progress.progress(min(max(pct, 0.0), 1.0))
            status.info(f"{str(stage).upper()} {done}/{total}")
            detail.write(f"{ev.get('symbol', '')}")
        elif ev_type == "start" and ev.get("stage") == "raw":
            backend.info(f"后台进程 PID={ev.get('pid')}\n{ev.get('cmd', '')}")
        elif ev_type == "heartbeat" and ev.get("stage") == "raw":
            backend.info(f"后台进程运行中 PID={ev.get('pid')} · 已运行 {ev.get('elapsed', 0)}s")
        elif ev_type == "log" and ev.get("stage") == "raw":
            detail.write(str(ev.get("message", ""))[:200])
        elif ev_type == "error":
            stage = str(ev.get("stage", "unknown")).upper()
            symbol = ev.get("symbol", "")
            message = str(ev.get("message", ""))[:120]
            err.warning(f"{stage} {symbol} 失败：{message}")
        elif ev_type == "done" and "stage" not in ev:
            if ev.get("ok"):
                target.success("数据更新完成")
            else:
                target.error(
                    f"更新完成但有错误: raw={ev.get('raw_error_count', 0)}, curated={ev.get('curated_error_count', 0)}"
                )

    st.cache_data.clear()
    st.rerun()

def render_quick_actions() -> None:
    """Render quick-action buttons and homepage update trigger."""
    st.subheader(t("home.quick_actions.title"))

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button(t("home.quick_actions.view_runs"), use_container_width=True, key="qa_view_runs"):
            st.switch_page("pages/1_runs.py")
    with c2:
        if st.button(t("home.quick_actions.compare_runs"), use_container_width=True, key="qa_compare_runs"):
            st.switch_page("pages/3_compare_runs.py")
    with c3:
        force_update_all = bool(st.session_state.get("force_update_all", False))
        if st.button(t("home.quick_actions.update_all"), use_container_width=True, key="qa_update_all"):
            _run_update_stream_ui(force_update_all, sidebar=False)

    st.caption(
        t(
            "home.quick_actions.force_update_status",
            state=t("common.on") if st.session_state.get("force_update_all", False) else t("common.off"),
        )
    )
