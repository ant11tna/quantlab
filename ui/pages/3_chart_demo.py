"""图表演示 - ECharts + Lightweight Charts 使用真实数据

演示内容：
1. ECharts 面积图展示净值曲线（从 loader 加载真实数据）
2. Lightweight Charts 蜡烛图 + 成交量 + 交易标记（从 loader 加载真实数据）
"""

from __future__ import annotations

from pathlib import Path
import sys

# 确保能 import i18n
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd

from i18n import t
from ui.data.loader import (
    list_runs,
    load_equity_curve,
    load_positions,
    load_fills,
    load_symbol_bars,
    get_default_symbol,
    # Transformers
    equity_curve_to_echarts,
    bars_to_lightweight_ohlcv,
    fills_to_lightweight_markers,
)

# Page config
st.set_page_config(
    page_title=t("app.title") + " - " + t("app.chart_demo"),
    page_icon="📈",
    layout="wide",
)

st.title(t("demo.title"))
st.caption(t("demo.subtitle"))

# Get available runs
runs_df = list_runs()
if runs_df.empty:
    st.error(t("runs.empty"))
    st.stop()

# Select run
run_id = st.selectbox(
    t("demo.select_run"),
    options=runs_df["run_id"].tolist(),
    format_func=lambda x: f"{x} ({runs_df[runs_df['run_id']==x]['name'].iloc[0]})"
)

if not run_id:
    st.stop()

st.markdown("---")

# ============================================================
# Section 1: ECharts - Equity Curve (Area)
# ============================================================
st.subheader(t("demo.echarts_equity"))

# Load real equity data
equity_df = load_equity_curve(run_id)

if equity_df.empty:
    st.warning(t("runs.no_equity"))
else:
    # Transform to ECharts format
    chart_data = equity_curve_to_echarts(equity_df, last_n=None)  # Show all
    
    # Calculate normalized NAV for better display
    nav_values = chart_data["values"]
    nav_norm = [v / nav_values[0] for v in nav_values] if nav_values[0] != 0 else nav_values
    
    option = {
        "tooltip": {"trigger": "axis"},
        "grid": {"left": "8%", "right": "6%", "top": "10%", "bottom": "15%"},
        "xAxis": {
            "type": "category", 
            "data": chart_data["dates"], 
            "boundaryGap": False,
            "axisLabel": {"rotate": 30}
        },
        "yAxis": {
            "type": "value", 
            "scale": True,
            "name": t("chart.nav")
        },
        "dataZoom": [
            {"type": "inside"}, 
            {"type": "slider", "bottom": 10}
        ],
        "series": [
            {
                "name": t("chart.nav"),
                "type": "line",
                "data": [round(v, 4) for v in nav_norm],
                "smooth": True,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#5470c6"},
                "areaStyle": {
                    "color": {
                        "type": "linear",
                        "x": 0, "y": 0, "x2": 0, "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": "#5470c680"},
                            {"offset": 1, "color": "#5470c610"}
                        ]
                    }
                },
            }
        ],
    }
    
    try:
        from streamlit_echarts import st_echarts
        st_echarts(option, height="300px")
        st.caption(f"{t('demo.data_points')}: {len(chart_data['dates'])} | From {chart_data['dates'][0]} to {chart_data['dates'][-1]}")
    except ImportError:
        # Fallback
        chart_df = pd.DataFrame({
            t("chart.date"): chart_data["dates"],
            t("chart.nav"): nav_norm
        })
        st.line_chart(chart_df.set_index(t("chart.date")), height=300)
        st.info("Install streamlit-echarts for better interactivity: `pip install streamlit-echarts`")

st.markdown("---")

# ============================================================
# Section 2: Lightweight Charts - K-Line + Volume + Markers
# ============================================================
st.subheader(t("demo.lightweight_chart"))

# Symbol selector
positions_df = load_positions(run_id)
fills_df = load_fills(run_id)

if positions_df.empty and fills_df.empty:
    st.warning(t("demo.no_data"))
else:
    # Get available symbols
    available_symbols = []
    if not positions_df.empty and "symbol" in positions_df.columns:
        available_symbols.extend(positions_df["symbol"].unique().tolist())
    if not fills_df.empty and "symbol" in fills_df.columns:
        available_symbols.extend(fills_df["symbol"].unique().tolist())
    available_symbols = sorted(list(set(available_symbols)))
    
    # Default symbol
    default_symbol = get_default_symbol(run_id)
    
    col1, col2 = st.columns([1, 3])
    with col1:
        symbol = st.selectbox(
            t("demo.symbol"),
            options=available_symbols,
            index=available_symbols.index(default_symbol) if default_symbol in available_symbols else 0
        )
        
        # Show symbol stats
        symbol_fills = fills_df[fills_df["symbol"] == symbol] if not fills_df.empty else pd.DataFrame()
        if not symbol_fills.empty:
            st.write(f"**{t('demo.trades')}：**")
            st.write(f"- {t('demo.trades_count')}: {len(symbol_fills)}")
            st.write(f"- {t('demo.buy')}: {len(symbol_fills[symbol_fills['side'] == 'BUY'])}")
            st.write(f"- {t('demo.sell')}: {len(symbol_fills[symbol_fills['side'] == 'SELL'])}")
    
    with col2:
        # Load real data (or synthetic if not available)
        bars_df = load_symbol_bars(symbol, run_id=run_id)
        symbol_fills = load_fills(run_id, symbol=symbol)
        
        use_synthetic = False
        if bars_df.empty:
            # No real data - generate synthetic OHLC based on fills
            st.info(f"{symbol} {t('demo.no_price_data')}")
            use_synthetic = True
            
            # Generate synthetic OHLC around fill times
            import numpy as np
            np.random.seed(42)
            
            if not symbol_fills.empty:
                base_price = symbol_fills["price"].mean()
                dates = pd.date_range(
                    start=symbol_fills["ts"].min() - pd.Timedelta(days=5),
                    end=symbol_fills["ts"].max() + pd.Timedelta(days=5),
                    freq="D"
                )
            else:
                base_price = 100.0
                dates = pd.date_range(end=pd.Timestamp.now(), periods=30, freq="D")
            
            prices = base_price + np.cumsum(np.random.randn(len(dates)) * 2)
            
            bars_df = pd.DataFrame({
                "ts": dates,
                "open": prices * (1 + np.random.randn(len(dates)) * 0.01),
                "high": prices * (1 + abs(np.random.randn(len(dates))) * 0.02 + 0.01),
                "low": prices * (1 - abs(np.random.randn(len(dates))) * 0.02 - 0.01),
                "close": prices,
                "volume": np.random.randint(100000, 1000000, len(dates)),
            })
        
        # Transform to Lightweight format
        ohlcv_data = bars_to_lightweight_ohlcv(bars_df)
        markers = fills_to_lightweight_markers(symbol_fills)
        
        # Filter markers to match OHLC time range
        if ohlcv_data["ohlc"] and markers:
            min_time = min(c["time"] for c in ohlcv_data["ohlc"])
            max_time = max(c["time"] for c in ohlcv_data["ohlc"])
            markers = [m for m in markers if min_time <= m["time"] <= max_time]
        
        # Limit markers density (show max 20)
        if len(markers) > 20:
            step = len(markers) // 20
            markers = markers[::step]
        
        # Generate HTML with real data
        html = f"""
            <div id="tv" style="width:100%; height:460px;"></div>
            <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
            <script>
                const ohlc = {json.dumps(ohlcv_data["ohlc"])};
                const volume = {json.dumps(ohlcv_data["volume"])};
                const markers = {json.dumps(markers)};
                
                const container = document.getElementById('tv');
                const chart = LightweightCharts.createChart(container, {{
                    layout: {{
                        textColor: '#111',
                        background: {{ type: 'solid', color: 'white' }},
                    }},
                    rightPriceScale: {{
                        borderVisible: false
                    }},
                    timeScale: {{
                        borderVisible: false,
                        timeVisible: false,
                        secondsVisible: false
                    }},
                    grid: {{
                        vertLines: {{ visible: true, color: '#f0f0f0' }},
                        horzLines: {{ visible: true, color: '#f0f0f0' }},
                    }},
                    crosshair: {{
                        mode: LightweightCharts.CrosshairMode.Normal,
                    }},
                }});
                
                // Candlestick series
                const candleSeries = chart.addCandlestickSeries({{
                    upColor: '#26a69a',
                    downColor: '#ef5350',
                    borderUpColor: '#26a69a',
                    borderDownColor: '#ef5350',
                    wickUpColor: '#26a69a',
                    wickDownColor: '#ef5350',
                }});
                candleSeries.setData(ohlc);
                
                // Volume histogram (at bottom)
                const volumeSeries = chart.addHistogramSeries({{
                    priceFormat: {{ type: 'volume' }},
                    priceScaleId: '',
                    scaleMargins: {{ top: 0.85, bottom: 0 }},
                }});
                volumeSeries.setData(volume);
                
                // Trade markers
                if (markers.length > 0) {{
                    candleSeries.setMarkers(markers);
                }}
                
                chart.timeScale().fitContent();
                
                // Responsive resize
                const ro = new ResizeObserver(entries => {{
                    for (let entry of entries) {{
                        const cr = entry.contentRect;
                        chart.applyOptions({{ width: cr.width, height: cr.height }});
                    }}
                }});
                ro.observe(container);
            </script>
            """
            
        components.html(html, height=480, scrolling=False)
        
        # Data summary
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.caption(f"{t('demo.ohlc_bars')}: {len(ohlcv_data['ohlc'])}")
        with col_b:
            st.caption(f"{t('demo.volume_points')}: {len(ohlcv_data['volume'])}")
        with col_c:
            st.caption(f"{t('demo.trade_markers')}: {len(markers)}")

st.markdown("---")

# ============================================================
# Section 3: Data Debug
# ============================================================
with st.expander(t("demo.debug")):
    st.write(f"**{t('demo.equity_curve')}：**")
    st.dataframe(equity_df.head() if not equity_df.empty else pd.DataFrame())
    
    st.write(f"**{symbol} {t('demo.ohlc_data')}：**")
    st.dataframe(bars_df.head() if not bars_df.empty else pd.DataFrame())
    
    st.write(f"**{symbol} {t('demo.fills')}：**")
    st.dataframe(symbol_fills if not symbol_fills.empty else pd.DataFrame())
    
    st.write(f"**{t('demo.ohlc_json')}：**")
    if ohlcv_data["ohlc"]:
        st.json(ohlcv_data["ohlc"][:3])
    
    st.write(f"**{t('demo.markers_json')}：**")
    if markers:
        st.json(markers[:10])

st.markdown("---")
st.caption(f"{t('runs.run_id')}: " + run_id)
