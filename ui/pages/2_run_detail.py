"""回测详情页 - 深度回测分析

回答5个问题：
1. 收益/回撤结构如何？
2. 哪些时间段贡献最大？
3. 换手与成本控制？成本占比？
4. 权重如何变化？调仓频率与集中度？
5. 具体标的进出时机？

布局：
    - 头部（固定）
    - 第1行：指标卡
    - 第2行：净值曲线 | 回撤（ECharts）
    - 第3行：持仓分配 | 换手与成本（ECharts）
    - 第4行：标的分析（Lightweight K线 + 标记 + 成交表）
"""

from __future__ import annotations

from pathlib import Path
import sys

# 确保能 import i18n
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd

from i18n import t
from ui.data.loader import (
    load_run,
    load_equity_curve,
    load_positions,
    load_fills,
    load_symbol_bars,
    get_default_symbol,
    # Transformers
    equity_and_drawdown_to_echarts,
    positions_to_allocation_series,
    compute_turnover_from_positions,
    aggregate_fills_by_ts,
    turnover_and_cost_to_echarts,
    bars_to_lightweight_ohlcv,
    fills_to_lightweight_markers,
)

# Page config
st.set_page_config(
    page_title=t("app.title") + " - " + t("app.run_detail"),
    page_icon="🔍",
    layout="wide",
)


def format_pct(val, decimals: int = 2) -> str:
    """格式化百分比"""
    if val is None or pd.isna(val):
        return "-"
    return f"{val * 100:.{decimals}f}%"


def format_number(val, decimals: int = 0) -> str:
    """格式化数字（千分位）"""
    if val is None or pd.isna(val):
        return "-"
    return f"{val:,.{decimals}f}"


def render_metric_card(label: str, value: str, subtitle: str = ""):
    """渲染指标卡"""
    st.markdown(f"""
    <div style="
        background: #f8f9fa;
        border-radius: 6px;
        padding: 12px 16px;
        text-align: center;
    ">
        <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">
            {label}
        </div>
        <div style="font-size: 22px; font-weight: 600; color: #333; margin: 4px 0;">
            {value}
        </div>
        {f'<div style="font-size: 11px; color: #888;">{subtitle}</div>' if subtitle else ''}
    </div>
    """, unsafe_allow_html=True)


def render_echarts_line_area(dates, values, title: str, color: str = "#5470c6", show_area: bool = True):
    """渲染 ECharts 面积图"""
    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14}},
        "grid": {"top": 50, "right": 30, "bottom": 60, "left": 50},
        "tooltip": {
            "trigger": "axis",
            "formatter": "{b}<br/>" + title + ": {c}",
        },
        "xAxis": {
            "type": "category",
            "data": dates,
            "axisLine": {"lineStyle": {"color": "#ccc"}},
            "axisLabel": {"color": "#666"},
        },
        "yAxis": {
            "type": "value",
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "splitLine": {"lineStyle": {"color": "#eee"}},
        },
        "dataZoom": [
            {"type": "inside", "start": 0, "end": 100},
            {"type": "slider", "start": 0, "end": 100, "bottom": 10},
        ],
        "series": [{
            "type": "line",
            "data": values,
            "smooth": True,
            "symbol": "none",
            "lineStyle": {"width": 2, "color": color},
            **({"areaStyle": {"color": {"type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                "colorStops": [{"offset": 0, "color": color + "40"}, {"offset": 1, "color": color + "05"}]}}}
               if show_area else {}),
        }],
    }
    
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height="300px")
    except ImportError:
        chart_df = pd.DataFrame({t("chart.date"): dates, title: values})
        st.line_chart(chart_df.set_index(t("chart.date")), height=300)


def render_echarts_equity_drawdown(dates, nav_norm, drawdown):
    """渲染净值与回撤组合图"""
    option = {
        "title": {"text": t("detail.equity_drawdown"), "left": "center", "textStyle": {"fontSize": 14}},
        "grid": {"top": 50, "right": 30, "bottom": 60, "left": 50},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": [t("chart.nav"), t("chart.drawdown")], "top": 25},
        "xAxis": {
            "type": "category",
            "data": dates,
            "axisLine": {"lineStyle": {"color": "#ccc"}},
        },
        "yAxis": [
            {
                "type": "value",
                "name": t("chart.nav"),
                "position": "left",
                "axisLine": {"show": False},
                "splitLine": {"lineStyle": {"color": "#eee"}},
            },
            {
                "type": "value",
                "name": t("chart.drawdown"),
                "position": "right",
                "axisLabel": {"formatter": "{value}%"},
                "splitLine": {"show": False},
            },
        ],
        "dataZoom": [
            {"type": "inside", "start": 0, "end": 100},
            {"type": "slider", "start": 0, "end": 100, "bottom": 10},
        ],
        "series": [
            {
                "name": t("chart.nav"),
                "type": "line",
                "data": nav_norm,
                "smooth": True,
                "symbol": "none",
                "lineStyle": {"width": 2, "color": "#5470c6"},
            },
            {
                "name": t("chart.drawdown"),
                "type": "line",
                "yAxisIndex": 1,
                "data": [d * 100 for d in drawdown],
                "smooth": True,
                "symbol": "none",
                "lineStyle": {"width": 1, "color": "#ee6666"},
                "areaStyle": {"color": "#ee666630"},
            },
        ],
    }
    
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height="350px")
    except ImportError:
        st.write(t("detail.equity_drawdown") + " (fallback)")
        df = pd.DataFrame({
            t("chart.date"): dates,
            t("chart.nav"): nav_norm,
            t("chart.drawdown"): drawdown,
        }).set_index(t("chart.date"))
        st.line_chart(df, height=350)


def render_echarts_allocation(dates, series_list):
    """渲染持仓分配堆叠面积图"""
    colors = [
        "#5470c6", "#91cc75", "#fac858", "#ee6666",
        "#73c0de", "#3ba272", "#fc8452", "#9a60b4",
        "#ea7ccc"
    ]
    
    series = []
    for i, s in enumerate(series_list):
        color = colors[i % len(colors)]
        series.append({
            "name": s["name"],
            "type": "line",
            "stack": "alloc",
            "areaStyle": {"opacity": 0.6},
            "lineStyle": {"width": 1},
            "symbol": "none",
            "data": s["data"],
            "color": color,
        })
    
    option = {
        "title": {"text": t("detail.allocation"), "left": "center", "textStyle": {"fontSize": 14}},
        "grid": {"top": 60, "right": 30, "bottom": 60, "left": 50},
        "tooltip": {"trigger": "axis"},
        "legend": {
            "data": [s["name"] for s in series_list],
            "top": 25,
            "type": "scroll",
        },
        "xAxis": {
            "type": "category",
            "boundaryGap": False,
            "data": dates,
            "axisLine": {"lineStyle": {"color": "#ccc"}},
        },
        "yAxis": {
            "type": "value",
            "max": 1,
            "axisLabel": {"formatter": "{value}"},
            "splitLine": {"lineStyle": {"color": "#eee"}},
        },
        "dataZoom": [
            {"type": "inside", "start": 0, "end": 100},
            {"type": "slider", "start": 0, "end": 100, "bottom": 10},
        ],
        "series": series,
    }
    
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height="350px")
    except ImportError:
        st.write(t("detail.allocation") + " (fallback)")
        df_data = {t("chart.date"): dates}
        for s in series_list:
            df_data[s["name"]] = s["data"]
        st.area_chart(pd.DataFrame(df_data).set_index(t("chart.date")), height=350)


def render_echarts_turnover_cost(dates, turnover, fees, impact, total_cost):
    """渲染换手与成本双轴图"""
    option = {
        "title": {"text": t("detail.turnover_cost"), "left": "center", "textStyle": {"fontSize": 14}},
        "grid": {"top": 50, "right": 60, "bottom": 60, "left": 50},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": [t("chart.turnover"), t("chart.fees"), t("chart.impact"), t("chart.total_cost")], "top": 25},
        "xAxis": {
            "type": "category",
            "data": dates,
            "axisLine": {"lineStyle": {"color": "#ccc"}},
        },
        "yAxis": [
            {
                "type": "value",
                "name": t("chart.turnover"),
                "position": "left",
                "axisLine": {"show": False},
                "splitLine": {"lineStyle": {"color": "#eee"}},
            },
            {
                "type": "value",
                "name": t("chart.cost") + " ($)",
                "position": "right",
                "axisLine": {"show": False},
                "splitLine": {"show": False},
            },
        ],
        "dataZoom": [
            {"type": "inside", "start": 0, "end": 100},
            {"type": "slider", "start": 0, "end": 100, "bottom": 10},
        ],
        "series": [
            {
                "name": t("chart.turnover"),
                "type": "bar",
                "data": turnover,
                "itemStyle": {"color": "#5470c6"},
            },
            {
                "name": t("chart.fees"),
                "type": "line",
                "yAxisIndex": 1,
                "data": fees,
                "lineStyle": {"color": "#91cc75"},
            },
            {
                "name": t("chart.impact"),
                "type": "line",
                "yAxisIndex": 1,
                "data": impact,
                "lineStyle": {"color": "#fac858"},
            },
            {
                "name": t("chart.total_cost"),
                "type": "line",
                "yAxisIndex": 1,
                "data": total_cost,
                "lineStyle": {"color": "#ee6666", "width": 2},
            },
        ],
    }
    
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height="350px")
    except ImportError:
        st.write(t("detail.turnover_cost") + " (fallback)")
        df = pd.DataFrame({
            t("chart.date"): dates,
            t("chart.turnover"): turnover,
            t("chart.total_cost"): total_cost,
        }).set_index(t("chart.date"))
        st.bar_chart(df, height=350)


def render_lightweight_chart(ohlc_data, volume_data, markers, symbol: str):
    """渲染 Lightweight Charts 蜡烛图"""
    # 使用 streamlit 原生图表作为 fallback
    if not ohlc_data:
        st.info(t("demo.no_price_data"))
        return
    
    df = pd.DataFrame(ohlc_data)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df = df.set_index("time")
    
    st.write(f"**{symbol}** " + t("chart.price") + " & " + t("chart.volume"))
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.line_chart(df[["close"]], height=300, use_container_width=True)
    
    with col2:
        st.caption(t("demo.trades"))
        if markers:
            marker_df = pd.DataFrame(markers[:10])
            st.dataframe(marker_df[["text", "position"]], hide_index=True, use_container_width=True)
        else:
            st.info(t("demo.no_fills"))


def main():
    """主函数"""
    
    # 从查询参数或 session state 获取 run_id
    query_params = st.query_params
    run_id = query_params.get("run_id")
    
    if not run_id:
        run_id = st.session_state.get("selected_run_id")
    
    if not run_id:
        st.warning(t("detail.no_run"))
        if st.button(t("common.back_runs")):
            st.switch_page("pages/1_runs.py")
        return
    
    # 加载运行数据
    run_data = load_run(run_id)
    metrics = run_data.get("metrics_dict") or {}
    
    # 解析运行信息
    parts = run_id.split("__")
    name = parts[1] if len(parts) > 1 else run_id
    started = parts[0] if len(parts) > 0 else "unknown"
    
    # 头部
    col_header, col_actions = st.columns([4, 1])
    with col_header:
        st.title(f"🔍 {name}")
        st.caption(t("detail.caption", run_id=run_id, started=started))
    with col_actions:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button(t("common.back_runs"), use_container_width=True):
            st.switch_page("pages/1_runs.py")
    
    st.markdown("---")
    
    # 提取指标
    risk = metrics.get("risk", {})
    trading = metrics.get("trading", {})
    summary = metrics.get("summary", metrics)
    
    # 第1行：指标卡
    st.subheader(t("detail.metrics"))
    
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        ret = summary.get("total_return") or risk.get("total_return")
        color = "#52c41a" if ret and ret > 0 else "#f5222d"
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background: {color}10; border-radius: 8px;">
            <div style="font-size: 12px; color: #666;">{t('detail.total_return')}</div>
            <div style="font-size: 24px; font-weight: 600; color: {color};">{format_pct(ret)}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c2:
        dd = summary.get("max_drawdown") or risk.get("max_drawdown")
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background: #ff4d4f10; border-radius: 8px;">
            <div style="font-size: 12px; color: #666;">{t('detail.max_drawdown')}</div>
            <div style="font-size: 24px; font-weight: 600; color: #ff4d4f;">{format_pct(dd)}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c3:
        sharpe = summary.get("sharpe_ratio") or risk.get("sharpe_ratio")
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background: #f0f0f0; border-radius: 8px;">
            <div style="font-size: 12px; color: #666;">{t('detail.sharpe')}</div>
            <div style="font-size: 24px; font-weight: 600; color: #333;">{f"{sharpe:.2f}" if sharpe else "-"}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c4:
        turnover = summary.get("turnover")
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background: #f0f0f0; border-radius: 8px;">
            <div style="font-size: 12px; color: #666;">{t('detail.turnover')}</div>
            <div style="font-size: 24px; font-weight: 600; color: #333;">{format_pct(turnover) if turnover else "-"}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # 成本指标
    st.markdown("<br>", unsafe_allow_html=True)
    c5, c6, c7, c8 = st.columns(4)
    
    fees = trading.get("total_fees", 0) or 0
    impact = trading.get("total_impact_cost", 0) or 0
    total_cost = fees + impact
    
    gross_return = abs(summary.get("total_return", 0) or 0) * 1e6
    cost_to_return = total_cost / max(1e-9, gross_return)
    
    with c5:
        render_metric_card(t("detail.total_fees"), f"${fees:,.2f}")
    with c6:
        render_metric_card(t("detail.impact_cost"), f"${impact:,.2f}")
    with c7:
        render_metric_card(t("detail.total_cost"), f"${total_cost:,.2f}")
    with c8:
        render_metric_card(t("detail.cost_return"), format_pct(cost_to_return, 4))
    
    st.markdown("---")
    
    # 加载时间序列数据
    equity_df = load_equity_curve(run_id)
    positions_df = load_positions(run_id)
    fills_df = load_fills(run_id)
    
    # 第2行：净值与回撤 | 持仓分配
    col_left, col_right = st.columns(2)
    
    with col_left:
        if not equity_df.empty:
            chart_data = equity_and_drawdown_to_echarts(equity_df)
            render_echarts_equity_drawdown(
                chart_data["dates"],
                chart_data["nav_norm"],
                chart_data["drawdown"]
            )
        else:
            st.info(t("runs.no_equity"))
    
    with col_right:
        if not positions_df.empty:
            alloc_data = positions_to_allocation_series(positions_df, top_n=8)
            if alloc_data["dates"]:
                render_echarts_allocation(alloc_data["dates"], alloc_data["series"])
            else:
                st.info(t("common.no_data"))
        else:
            st.info(t("common.no_data"))
    
    st.markdown("---")
    
    # 第3行：换手与成本
    col_left2, col_right2 = st.columns(2)
    
    with col_left2:
        if not positions_df.empty:
            turnover_df = compute_turnover_from_positions(positions_df)
            cost_df = aggregate_fills_by_ts(fills_df)
            
            if not turnover_df.empty:
                tc_data = turnover_and_cost_to_echarts(turnover_df, cost_df)
                render_echarts_turnover_cost(
                    tc_data["dates"],
                    tc_data["turnover"],
                    tc_data["fees"],
                    tc_data["impact"],
                    tc_data["total_cost"]
                )
            else:
                st.info(t("common.no_data"))
        else:
            st.info(t("common.no_data"))
    
    with col_right2:
        st.subheader(t("detail.cost_analysis"))
        
        if not fills_df.empty:
            daily_cost = aggregate_fills_by_ts(fills_df)
            if not daily_cost.empty:
                st.write(f"**{t('detail.trades_count')}：** {len(daily_cost[daily_cost['total_cost'] > 0])}")
                st.write(f"**最大单日成本：** ${daily_cost['total_cost'].max():,.2f}")
                st.write(f"**平均单日成本：** ${daily_cost['total_cost'].mean():,.2f}")
            
            if "symbol" in fills_df.columns and "impact_cost" in fills_df.columns:
                symbol_cost = fills_df.groupby("symbol").agg({
                    "filled_qty": "sum",
                    "fee": "sum",
                    "impact_cost": "sum",
                }).reset_index()
                symbol_cost["total"] = symbol_cost["fee"] + symbol_cost["impact_cost"]
                symbol_cost = symbol_cost.sort_values("total", ascending=False)
                
                st.write(f"**{t('detail.cost_analysis')}：**")
                st.dataframe(symbol_cost, hide_index=True, use_container_width=True)
        else:
            st.info(t("common.no_data"))
    
    st.markdown("---")
    
    # 第4行：标的分析
    st.subheader(t("detail.symbol_analysis"))
    
    default_symbol = get_default_symbol(run_id)
    
    available_symbols = []
    if not positions_df.empty and "symbol" in positions_df.columns:
        available_symbols.extend(positions_df["symbol"].unique().tolist())
    if not fills_df.empty and "symbol" in fills_df.columns:
        available_symbols.extend(fills_df["symbol"].unique().tolist())
    available_symbols = sorted(list(set(available_symbols)))
    
    if available_symbols:
        symbol = st.selectbox(
            t("detail.select_symbol"),
            options=available_symbols,
            index=available_symbols.index(default_symbol) if default_symbol in available_symbols else 0
        )
        
        bars_df = load_symbol_bars(symbol, run_id=run_id)
        symbol_fills = load_fills(run_id, symbol=symbol)
        
        if not bars_df.empty:
            ohlcv_data = bars_to_lightweight_ohlcv(bars_df)
            markers = fills_to_lightweight_markers(symbol_fills)
            
            render_lightweight_chart(
                ohlcv_data["ohlc"],
                ohlcv_data["volume"],
                markers,
                symbol
            )
            
            st.markdown("<br>", unsafe_allow_html=True)
            st.write(f"**{symbol} {t('demo.trades')}：**")
            if not symbol_fills.empty:
                display_cols = ["ts", "side", "filled_qty", "price"]
                if "fee" in symbol_fills.columns:
                    display_cols.append("fee")
                if "impact_cost" in symbol_fills.columns:
                    display_cols.append("impact_cost")
                
                st.dataframe(
                    symbol_fills[display_cols],
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info(f"{symbol} {t('demo.no_fills')}")
        else:
            st.info(f"{symbol} {t('demo.no_price_data')}")
    else:
        st.info(t("common.no_data"))
    
    st.markdown("---")
    st.caption(f"{t('app.runs')}：{run_id} | {t('status.complete')}：{run_data.get('status', 'unknown')}")


if __name__ == "__main__":
    main()
