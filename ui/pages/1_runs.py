"""实验列表页 - 回测运行记录总览

布局：
    左侧：运行记录表格（支持搜索/排序）
    右侧：选中运行的指标卡片 + 净值曲线

图表：
    1. 净值曲线：最近60个数据点
    2. 成本拆解：手续费/冲击成本/滑点
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Dict

# 确保能 import i18n（指向 ui/ 目录）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd
from loguru import logger

from i18n import t
from ui.data.loader import (
    list_runs, 
    load_run, 
    load_equity_curve,
    equity_curve_to_echarts,
    cost_breakdown_to_echarts,
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


def render_metric_card(label: str, value: str, delta: str = None, color: str = None):
    """渲染指标卡片"""
    if color:
        st.markdown(f"""
        <div style="
            background: {color}15;
            border-left: 4px solid {color};
            padding: 12px 16px;
            border-radius: 4px;
            margin-bottom: 8px;
        ">
            <div style="font-size: 12px; color: #666; margin-bottom: 4px;">{label}</div>
            <div style="font-size: 24px; font-weight: 600; color: {color};">{value}</div>
            {f'<div style="font-size: 12px; color: #888;">{delta}</div>' if delta else ''}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.metric(label, value, delta)


def render_equity_sparkline(equity_data: Dict, height: int = 200):
    """渲染净值曲线迷你图"""
    if not equity_data["dates"]:
        st.info(t("runs.no_equity"))
        return
    
    option = {
        "grid": {"top": 10, "right": 10, "bottom": 20, "left": 40},
        "xAxis": {
            "type": "category",
            "data": equity_data["dates"],
            "show": False,
        },
        "yAxis": {
            "type": "value",
            "scale": True,
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "splitLine": {"lineStyle": {"color": "#eee"}},
        },
        "series": [{
            "type": "line",
            "data": equity_data["values"],
            "smooth": True,
            "symbol": "none",
            "lineStyle": {"width": 2, "color": "#5470c6"},
            "areaStyle": {
                "color": {
                    "type": "linear",
                    "x": 0, "y": 0, "x2": 0, "y2": 1,
                    "colorStops": [
                        {"offset": 0, "color": "#5470c640"},
                        {"offset": 1, "color": "#5470c605"},
                    ]
                }
            },
        }],
        "tooltip": {
            "trigger": "axis",
            "formatter": "{b}<br/>" + t("chart.nav") + ": ${c}",
        },
    }
    
    # Try streamlit-echarts if available
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height=f"{height}px")
    except ImportError:
        # Fallback to simple line chart
        chart_df = pd.DataFrame({
            t("chart.date"): equity_data["dates"],
            t("chart.nav"): equity_data["values"]
        })
        st.line_chart(chart_df.set_index(t("chart.date")), height=height, use_container_width=True)


def render_cost_breakdown_bar(cost_data: Dict, height: int = 200):
    """渲染成本拆解柱状图"""
    if not cost_data["categories"]:
        st.info(t("runs.no_cost"))
        return
    
    option = {
        "grid": {"top": 30, "right": 20, "bottom": 30, "left": 60},
        "xAxis": {
            "type": "category",
            "data": cost_data["categories"],
            "axisLine": {"lineStyle": {"color": "#ccc"}},
            "axisLabel": {"color": "#666"},
        },
        "yAxis": {
            "type": "value",
            "name": t("chart.cost") + " ($)",
            "nameTextStyle": {"color": "#666"},
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "splitLine": {"lineStyle": {"color": "#eee"}},
        },
        "series": [{
            "type": "bar",
            "data": [
                {"value": v, "itemStyle": {"color": "#ee6666" if i == 0 else "#fac858" if i == 1 else "#91cc75"}}
                for i, v in enumerate(cost_data["values"])
            ],
            "barWidth": "50%",
        }],
        "tooltip": {
            "trigger": "item",
            "formatter": "{b}: ${c}",
        },
    }
    
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height=f"{height}px")
    except ImportError:
        # Fallback to bar chart
        chart_df = pd.DataFrame({
            t("chart.date"): cost_data["categories"],
            t("chart.cost"): cost_data["values"]
        })
        st.bar_chart(chart_df.set_index(t("chart.date")), height=height, use_container_width=True)


def main():
    """主函数"""
    st.set_page_config(
        page_title=t("app.title") + " - " + t("app.runs"),
        page_icon="📊",
        layout="wide",
    )
    
    st.title(t("runs.title"))
    
    # 加载运行数据
    runs_df = list_runs()
    
    if runs_df.empty:
        st.warning(t("runs.no_runs"))
        st.info(t("runs.expected_path"))
        return
    
    # 布局：左侧（表格）| 右侧（详情）
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader(f"{t('runs.title')}（{len(runs_df)}）")
        
        # 搜索/筛选
        search = st.text_input(t("runs.search"), "")
        
        # 筛选
        filtered_df = runs_df
        if search:
            mask = (
                runs_df["strategy"].str.contains(search, case=False, na=False) |
                runs_df["run_id"].str.contains(search, case=False, na=False)
            )
            filtered_df = runs_df[mask]
        
        # 准备显示列
        display_df = filtered_df.copy()
        display_df["created_at"] = pd.to_datetime(display_df["created_at"]).dt.strftime("%Y-%m-%d %H:%M")
        display_df["total_return"] = display_df["total_return"].apply(format_pct)
        display_df["max_drawdown"] = display_df["max_drawdown"].apply(format_pct)
        display_df["sharpe"] = display_df["sharpe"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")

        # 显示表格
        st.dataframe(
            display_df[["run_id", "strategy", "total_return", "max_drawdown", "sharpe", "created_at"]],
            column_config={
                "run_id": "run_id",
                "strategy": "strategy",
                "total_return": "total_return",
                "max_drawdown": "max_drawdown",
                "sharpe": "sharpe",
                "created_at": "created_at",
            },
            hide_index=True,
            use_container_width=True,
            height=500,
        )
        
        # 选择运行
        selected_run = st.selectbox(
            t("runs.select_for_detail"),
            options=filtered_df["run_id"].tolist(),
            format_func=lambda x: f"{x} ({filtered_df[filtered_df['run_id']==x]['strategy'].iloc[0]})"
        )
    
    with col_right:
        if selected_run:
            st.subheader(t("runs.overview"))
            
            # 加载完整运行数据
            run_data = load_run(selected_run)
            metrics = run_data.get("metrics_dict", {}) or {}
            
            row = filtered_df[filtered_df["run_id"] == selected_run].iloc[0]
            
            # 状态指示器
            status_labels = {
                "complete": t("status.complete"),
                "incomplete": t("status.incomplete"),
                "error": t("status.error")
            }
            status_color = {
                "complete": "#52c41a",
                "incomplete": "#faad14",
                "error": "#f5222d"
            }.get(row["status"], "#999")
            
            st.markdown(f"""
            <div style="
                background: {status_color}15;
                border: 1px solid {status_color}40;
                border-radius: 8px;
                padding: 16px;
                margin-bottom: 16px;
            ">
                <div style="font-size: 14px; color: {status_color}; font-weight: 600;">
                    {status_labels.get(row['status'], row['status']).upper()}
                </div>
                <div style="font-size: 12px; color: #666; margin-top: 4px;">
                    {row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # 关键指标
            c1, c2 = st.columns(2)
            with c1:
                ret = row["total_return"]
                ret_color = "#52c41a" if ret and ret > 0 else "#f5222d" if ret and ret < 0 else "#666"
                render_metric_card(
                    t("runs.total_return"), 
                    format_pct(ret), 
                    color=ret_color
                )
            with c2:
                dd = row["max_drawdown"]
                render_metric_card(
                    t("runs.max_drawdown"), 
                    format_pct(dd) if dd else "-",
                    color="#f5222d" if dd and dd < -0.1 else "#faad14"
                )
            
            c3, c4 = st.columns(2)
            with c3:
                sharpe = row["sharpe"]
                render_metric_card(
                    t("runs.sharpe"), 
                    f"{sharpe:.2f}" if sharpe else "-"
                )
            with c4:
                turnover = row["turnover"]
                render_metric_card(
                    t("runs.turnover"), 
                    format_pct(turnover) if turnover else "-"
                )
            
            # 成本摘要
            fees = row["total_fees"]
            impact = row["total_impact_cost"]
            if fees or impact:
                st.markdown("---")
                st.caption(t("runs.trading_costs"))
                cost_cols = st.columns(2)
                with cost_cols[0]:
                    st.metric(t("runs.fees"), f"${fees:,.2f}" if fees else "-")
                with cost_cols[1]:
                    st.metric(t("runs.impact"), f"${impact:,.2f}" if impact else "-")
            
            # 净值曲线
            st.markdown("---")
            st.caption(t("runs.equity_last60"))
            
            equity_df = load_equity_curve(selected_run)
            if not equity_df.empty:
                equity_data = equity_curve_to_echarts(equity_df, last_n=60)
                render_equity_sparkline(equity_data, height=180)
            else:
                st.info(t("runs.no_equity"))
            
            # 成本拆解
            st.markdown("---")
            st.caption(t("runs.cost_breakdown"))
            
            cost_data = cost_breakdown_to_echarts(metrics)
            render_cost_breakdown_bar(cost_data, height=180)
            
            # 操作按钮
            st.markdown("---")
            c5, c6 = st.columns(2)
            with c5:
                if st.button(t("runs.view_detail"), use_container_width=True):
                    st.switch_page("pages/2_run_detail.py")
            with c6:
                if st.button(t("runs.view_config"), use_container_width=True):
                    with st.expander(t("runs.config"), expanded=True):
                        if run_data.get("config_text"):
                            st.code(run_data["config_text"], language="yaml")
                        else:
                            st.info(t("runs.no_config"))


if __name__ == "__main__":
    main()
