from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from quantlab.universe import resolver
from quantlab.universe.search_index import fuzzy_match_candidates
from quantlab.universe.store import UniverseStore
from quantlab.universe.types import Candidate

RECENT_COLUMNS = ["listing_id", "display", "used_at", "count"]
HOLDING_COLUMNS = ["listing_id", "weight", "added_at"]


_CODE_LIKE_RE = re.compile(r"^[A-Za-z0-9:.]+$")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _load_recent(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_parquet(path, engine="pyarrow")
    else:
        df = pd.DataFrame(columns=RECENT_COLUMNS)
    for col in RECENT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[RECENT_COLUMNS]


def _save_recent(path: Path, df: pd.DataFrame) -> None:
    _ensure_parent(path)
    df[RECENT_COLUMNS].to_parquet(path, index=False, engine="pyarrow")


def _load_holdings(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_parquet(path, engine="pyarrow")
    else:
        df = pd.DataFrame(columns=HOLDING_COLUMNS)
    for col in HOLDING_COLUMNS:
        if col not in df.columns:
            df[col] = None
    if not df.empty:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
    return df[HOLDING_COLUMNS]


def _save_holdings(path: Path, df: pd.DataFrame) -> None:
    _ensure_parent(path)
    df[HOLDING_COLUMNS].to_parquet(path, index=False, engine="pyarrow")


def _candidate_display_name(candidate: Candidate) -> str:
    name = (candidate.name or "").strip()
    return name if name else "(name unknown)"


def _candidate_to_dict(candidate: Candidate, in_portfolio: bool) -> dict[str, object]:
    return {
        "pick": False,
        "region/exchange": f"{candidate.region}/{candidate.exchange}",
        "ticker": candidate.ticker,
        "name": _candidate_display_name(candidate),
        "listing_id": candidate.listing_id,
        "confidence": candidate.confidence,
        "rationale": candidate.rationale,
        "status": "in portfolio" if in_portfolio else "new",
    }


def _is_name_like_query(query: str) -> bool:
    q = query.strip()
    if not q:
        return False
    if not _CODE_LIKE_RE.match(q):
        return True
    return len(q) >= 4 and q.isalpha()


def _apply_filters(candidates: list[Candidate], region_filter: str, exchange_filter: str) -> list[Candidate]:
    out = candidates
    if region_filter != "All":
        out = [c for c in out if c.region == region_filter]
    if exchange_filter != "All":
        out = [c for c in out if c.exchange == exchange_filter]
    return out


def _resolve_candidates(
    query: str,
    store: UniverseStore,
    max_candidates: int,
    region_filter: str,
    exchange_filter: str,
    use_fuzzy: bool,
    universe_only: bool,
) -> list[Candidate]:
    base = resolver.resolve(query, store, max_candidates=max_candidates)
    base = _apply_filters(base, region_filter, exchange_filter)

    out: dict[str, Candidate] = {c.listing_id: c for c in base}

    should_fuzzy = use_fuzzy and (len(base) < max_candidates // 2 or _is_name_like_query(query))
    if should_fuzzy:
        fuzzy = fuzzy_match_candidates(
            query=query,
            store=store,
            max_candidates=max_candidates,
            region_filter=region_filter,
            exchange_filter=exchange_filter,
        )
        for cand in fuzzy:
            out.setdefault(cand.listing_id, cand)

    ordered = list(out.values())
    if universe_only:
        listings = store.load_listings()
        if listings.empty:
            ordered = []
        else:
            listing_set = set(listings["listing_id"].astype(str).tolist())
            ordered = [c for c in ordered if c.listing_id in listing_set]

    return ordered[:max_candidates]


def _recent_display(candidate: Candidate) -> str:
    name = (candidate.name or "").strip()
    parts = [candidate.region, candidate.exchange, candidate.ticker]
    if name:
        parts.append(name)
    return " ".join([p for p in parts if p])


def _upsert_recent(recent_path: Path, candidate: Candidate) -> None:
    recent = _load_recent(recent_path)
    now = _now_iso()
    display = _recent_display(candidate)

    if recent.empty:
        recent = pd.DataFrame([{"listing_id": candidate.listing_id, "display": display, "used_at": now, "count": 1}], columns=RECENT_COLUMNS)
    else:
        mask = recent["listing_id"].astype(str) == candidate.listing_id
        if mask.any():
            idx = recent.index[mask][0]
            count = int(pd.to_numeric(recent.at[idx, "count"], errors="coerce") or 0) + 1
            recent.at[idx, "display"] = display
            recent.at[idx, "used_at"] = now
            recent.at[idx, "count"] = count
        else:
            recent = pd.concat(
                [
                    recent,
                    pd.DataFrame(
                        [{"listing_id": candidate.listing_id, "display": display, "used_at": now, "count": 1}],
                        columns=RECENT_COLUMNS,
                    ),
                ],
                ignore_index=True,
            )

    recent = recent.sort_values("used_at", ascending=False, na_position="last")
    _save_recent(recent_path, recent)


def _add_or_replace_holding(path: Path, listing_id: str, replace: bool) -> bool:
    holdings = _load_holdings(path)
    mask = holdings["listing_id"].astype(str) == listing_id if not holdings.empty else pd.Series([], dtype=bool)
    now = _now_iso()

    if mask.any():
        if not replace:
            return False
        idx = holdings.index[mask][0]
        holdings.at[idx, "added_at"] = now
    else:
        holdings = pd.concat(
            [
                holdings,
                pd.DataFrame(
                    [{"listing_id": listing_id, "weight": 0.0, "added_at": now}],
                    columns=HOLDING_COLUMNS,
                ),
            ],
            ignore_index=True,
        )

    _save_holdings(path, holdings)
    return True


def symbol_search_pro_component(
    store: UniverseStore,
    portfolio_path: str = "data/portfolio/holdings.parquet",
    allow_add: bool = True,
) -> None:
    key_prefix = "symbol_search_pro"
    recent_path = Path(store.base_dir) / "recent.parquet"
    holdings_path = Path(portfolio_path)

    st.subheader("Symbol Search Pro")

    recent_df = _load_recent(recent_path)
    if not recent_df.empty:
        st.caption("Recent Top 8")
        top_recent = recent_df.sort_values("used_at", ascending=False, na_position="last").head(8)
        cols = st.columns(4)
        for i, row in enumerate(top_recent.itertuples(index=False)):
            label = str(row.display or row.listing_id)
            if cols[i % 4].button(label, key=f"{key_prefix}_recent_{i}"):
                listing = store.get_listing(str(row.listing_id))
                if listing:
                    instrument = store.get_instrument(str(listing.get("instrument_id") or "")) or {}
                    picked = Candidate(
                        listing_id=str(listing.get("listing_id") or ""),
                        instrument_id=str(listing.get("instrument_id") or ""),
                        region=str(listing.get("region") or ""),
                        exchange=str(listing.get("exchange") or ""),
                        ticker=str(listing.get("ticker") or ""),
                        name=str(instrument.get("name") or ""),
                        currency=str(listing.get("currency") or "unknown"),
                        confidence="recent",
                        rationale="selected from recent",
                    )
                    st.session_state[f"{key_prefix}_selected_candidate"] = picked
                    st.session_state[f"{key_prefix}_query"] = str(row.listing_id)

    q_col, rt_col, market_col, exch_col = st.columns([3, 1, 1, 1])
    query = q_col.text_input("输入代码/符号", key=f"{key_prefix}_query", placeholder="AAPL / 600519 / 00700.HK / Tencent")
    realtime = rt_col.checkbox("实时搜索", value=True, key=f"{key_prefix}_realtime")
    region_filter = market_col.selectbox("市场", options=["All", "CN", "US", "HK"], key=f"{key_prefix}_region")

    max_candidates = st.slider("候选数量", min_value=3, max_value=20, value=8, key=f"{key_prefix}_max_candidates")

    with st.expander("高级选项", expanded=False):
        use_fuzzy = st.checkbox("启用模糊搜索", value=True, key=f"{key_prefix}_fuzzy")
        universe_only = st.checkbox("仅显示已在 Universe 中的标的", value=False, key=f"{key_prefix}_universe_only")

    current = st.session_state.get(f"{key_prefix}_candidates", [])
    dynamic_exchanges = sorted({c.exchange for c in current if getattr(c, "exchange", "")})
    exch_options = ["All", "AUTO", "SH", "SZ", "HKEX", "NASDAQ", "NYSE", "AMEX"]
    for ex in dynamic_exchanges:
        if ex not in exch_options:
            exch_options.append(ex)
    exchange_filter = exch_col.selectbox("交易所", options=exch_options, key=f"{key_prefix}_exchange")

    search_clicked = st.button("匹配/搜索", use_container_width=True)

    now = time.time()
    last_query = st.session_state.get(f"{key_prefix}_last_query", "")
    if query != last_query:
        st.session_state[f"{key_prefix}_last_query"] = query
        st.session_state[f"{key_prefix}_last_update_time"] = now

    trigger_search = False
    if realtime and len(query.strip()) >= 1:
        last_update = float(st.session_state.get(f"{key_prefix}_last_update_time", 0.0))
        last_exec = st.session_state.get(f"{key_prefix}_last_exec_query", "")
        if now - last_update >= 0.3 and query != last_exec:
            trigger_search = True
    if not realtime and search_clicked and len(query.strip()) >= 1:
        trigger_search = True

    if trigger_search:
        candidates = _resolve_candidates(
            query=query,
            store=store,
            max_candidates=max_candidates,
            region_filter=region_filter,
            exchange_filter=exchange_filter,
            use_fuzzy=use_fuzzy,
            universe_only=universe_only,
        )
        st.session_state[f"{key_prefix}_candidates"] = candidates
        st.session_state[f"{key_prefix}_last_exec_query"] = query
        st.session_state.pop(f"{key_prefix}_selected_candidate", None)

    candidates = st.session_state.get(f"{key_prefix}_candidates", [])
    if not candidates:
        st.info("暂无候选，请输入代码后搜索。")
        return

    holdings_df = _load_holdings(holdings_path)
    holding_ids = set(holdings_df["listing_id"].astype(str).tolist()) if not holdings_df.empty else set()

    rows = [_candidate_to_dict(c, c.listing_id in holding_ids) for c in candidates]
    table_df = pd.DataFrame(rows)

    selected_candidate = st.session_state.get(f"{key_prefix}_selected_candidate")
    if isinstance(selected_candidate, Candidate):
        table_df.loc[:, "pick"] = table_df["listing_id"] == selected_candidate.listing_id

    edited = st.data_editor(
        table_df,
        hide_index=True,
        use_container_width=True,
        key=f"{key_prefix}_table",
        column_config={
            "pick": st.column_config.CheckboxColumn("pick"),
            "confidence": st.column_config.TextColumn("confidence"),
            "rationale": st.column_config.TextColumn("rationale", width="large"),
        },
        disabled=["region/exchange", "ticker", "name", "listing_id", "confidence", "rationale", "status"],
    )

    picks = edited[edited["pick"] == True]  # noqa: E712
    if len(picks) > 1:
        st.warning("一次只能选择一个候选，请只保留一个 pick。")
    elif len(picks) == 1:
        picked_id = str(picks.iloc[0]["listing_id"])
        chosen = next((c for c in candidates if c.listing_id == picked_id), None)
        if chosen:
            st.session_state[f"{key_prefix}_selected_candidate"] = chosen

    chosen = st.session_state.get(f"{key_prefix}_selected_candidate")
    if not isinstance(chosen, Candidate):
        st.caption("请在候选列表中选择 1 个标的。")
        return

    if not allow_add:
        return

    pending_replace = st.session_state.get(f"{key_prefix}_pending_replace", False)
    if pending_replace:
        st.warning(f"{chosen.listing_id} 已在组合中。是否替换（保留 weight，仅更新 added_at）？")
        c1, c2 = st.columns(2)
        if c1.button("替换", key=f"{key_prefix}_replace_yes", type="primary"):
            _add_or_replace_holding(holdings_path, chosen.listing_id, replace=True)
            _upsert_recent(recent_path, chosen)
            st.session_state[f"{key_prefix}_pending_replace"] = False
            st.success(f"已替换：{chosen.listing_id}")
        if c2.button("取消", key=f"{key_prefix}_replace_no"):
            st.session_state[f"{key_prefix}_pending_replace"] = False
            st.info("已取消替换。")
        return

    if st.button("确认加入组合", type="primary"):
        try:
            resolver.confirm(query, chosen, store)
        except ValueError as exc:
            st.error(str(exc))
            st.info("请改用不同候选，或清除输入后重新匹配。")
            return

        current_holdings = _load_holdings(holdings_path)
        exists = (not current_holdings.empty) and (current_holdings["listing_id"].astype(str) == chosen.listing_id).any()
        if exists:
            st.session_state[f"{key_prefix}_pending_replace"] = True
            st.warning("该标的已存在于组合，请选择“替换/取消”。")
            return

        _add_or_replace_holding(holdings_path, chosen.listing_id, replace=False)
        _upsert_recent(recent_path, chosen)
        st.success(f"已加入组合：{chosen.listing_id}")
