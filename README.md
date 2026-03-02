# QuantLab

Quantitative research and backtesting system with clear separation between research and execution.

## 核心理念

- **研究与执行彻底解耦**：研究产生"目标仓位/权重/交易意图"，执行负责转成订单并跟踪状态
- **结果可复现**：每次实验固定数据版本 + 代码版本 + 参数 + 输出报告
- **接口先行**：先定义数据源、回测撮合、broker 执行的抽象层，本地先用 Mock 实现跑通闭环

## 技术栈

- **存储**：Parquet（原始/中间结果）+ DuckDB（分析查询）
- **可视化**：Streamlit + Lightweight Charts
- **工程**：uv + Git + pre-commit + ruff + mypy

## 快速开始

```bash
# 1. 安装依赖
uv sync

# 2. 获取原始数据 (保存到 data/raw/bars/)
python scripts/update_data.py --config config/data_sources.yaml

# 3. 数据精加工: 生成 curated_v1 (保存到 data/curated/bars/)
python scripts/curate_data.py --all

# 4. 运行回测 (使用 curated 数据)
uv run quantlab backtest --config config/backtest.yaml --profile china_ashare

# 5. 启动 UI 查看结果
uv run streamlit run ui/app.py
```

## 目录结构

```
quantlab/
  config/           # 配置文件
  data/             # 数据目录
    raw/bars/       # 原始 CSV (update_data.py 输出)
    curated/bars/   # 精加工 Parquet (curate_data.py 输出)
    manifest/       # 数据版本清单
  db/               # DuckDB 数据库
  src/quantlab/     # 源代码
  scripts/          # 数据脚本
    update_data.py  # 获取原始数据
    curate_data.py  # 数据精加工
  runs/             # 实验运行记录
  ui/               # Streamlit 界面
```

## 数据流程

数据采用分层架构: **Raw → Curated → Engine**

### 1. Raw Layer (data/raw/bars/)
- **来源**: `scripts/update_data.py` 从 AkShare 获取
- **格式**: CSV (OHLCV)
- **特点**: 原始数据，无 regime 字段

### 2. Curated Layer (data/curated/bars/)
- **生成**: `scripts/curate_data.py` 加工
- **格式**: Parquet (zstd 压缩)
- **特点**: 包含 curated_v1 regime 字段:
  - `is_suspended`: 是否停牌
  - `is_limit_up`/`is_limit_down`: 涨跌停标记
  - `can_buy`/`can_sell`: 交易可行性
  - `prev_close`: 昨收 (用于计算涨跌幅)

### 3. 数据加工命令

```bash
# 加工所有原始数据
python scripts/curate_data.py --all

# 加工特定标的
python scripts/curate_data.py --symbol ETF:510300

# 验证但不写入
python scripts/curate_data.py --all --dry-run
```

## 市场配置 (Market Profiles)

根据市场特性选择合适的 execution profile:

| 市场 | lot_size | enforce_t1 | 适用场景 |
|------|----------|------------|----------|
| **US Equity** | 1 | false | 美股/ETF，T+0，可零股 |
| **China A-Share** | 100 | **true** | A股，T+1，一手100股 |
| **Crypto** | 1 | false | 加密货币，24/7，可小数 |

使用 profile 运行回测:
```bash
# A股回测 (自动启用 T+1 和 100股一手)
uv run quantlab backtest --config config/backtest.yaml --profile china_ashare

# 美股回测 (T+0，零股交易)
uv run quantlab backtest --config config/backtest.yaml --profile us_equity
```

## 研究流程

1. **定义实验配置**：在 `config/` 下创建实验配置
2. **运行回测**：生成 `runs/{timestamp}/` 目录
3. **分析结果**：通过 Streamlit UI 查看和对比
4. **记录洞察**：更新 `agents.md` 作为研究 backlog

## 核心概念

- **Bar**: 价格数据 (ts, open, high, low, close, volume)
- **TargetWeight**: 目标权重 (ts, symbol, target_weight)
- **Signal**: 交易信号 (ts, symbol, side, strength, reason)
- **OrderIntent**: 交易意图 (ts, symbol, target_qty/weight, urgency)
- **Order/Fill**: 订单与成交记录
- **PortfolioState**: 组合状态
