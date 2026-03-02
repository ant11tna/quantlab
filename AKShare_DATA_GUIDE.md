# AKShare 数据接入指南

完整的中文市场数据接入方案，支持 ETF（可交易）和指数（基准）数据。

## 目录结构

```
data/
  curated/
    bars/
      etf/              # ETF 日线数据（可交易标的）
        ETF:510300.csv
        ETF:510500.csv
        ...
      index/            # 指数日线数据（基准）
        IDX:000300.csv
        IDX:000905.csv
        ...
  manifest/
    etf.json            # ETF 数据水位线
    index.json          # 指数数据水位线
```

## 统一数据格式

所有数据统一为以下格式：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts | string | YYYY-MM-DD |
| symbol | string | ETF:510300 或 IDX:000300 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| volume | float | 成交量 |
| amount | float | 成交额（可选） |

## 配置说明

### config/data_sources.yaml

```yaml
etf:
  enabled: true
  adjust: "qfq"           # 前复权
  symbols:
    - "510300"            # 沪深300ETF
    - "510500"            # 中证500ETF
    # ... 添加更多

index:
  enabled: true
  symbols:
    - "000300"            # 沪深300指数
    - "000905"            # 中证500指数
    # ... 添加更多
```

### Symbol 命名规则

| 类型 | 格式 | 示例 |
|------|------|------|
| ETF | `ETF:` + 代码 | ETF:510300, ETF:159915 |
| 指数 | `IDX:` + 代码 | IDX:000300, IDX:399006 |

## 常用代码

### ETF 代码

| 代码 | 名称 | 类型 |
|------|------|------|
| 510300 | 华泰柏瑞沪深300ETF | A股大盘 |
| 510500 | 南方中证500ETF | A股中盘 |
| 159915 | 易方达创业板ETF | A股成长 |
| 588000 | 华夏科创50ETF | 科创板 |
| 511010 | 国泰上证10年期国债ETF | 国债 |
| 518880 | 华安黄金ETF | 黄金 |
| 513500 | 博时标普500ETF | QDII美股 |
| 513100 | 国泰纳斯达克100ETF | QDII美股 |
| 159920 | 华夏恒生ETF | QDII港股 |

### 指数代码

| 代码 | 名称 |
|------|------|
| 000300 | 沪深300指数 |
| 000905 | 中证500指数 |
| 000016 | 上证50指数 |
| 399006 | 创业板指数 |
| 000688 | 科创50指数 |

## 使用流程

### 1. 更新数据

```bash
# 全量更新
python scripts/update_data.py

# 仅更新 ETF
python scripts/update_data.py --type etf

# 仅更新指数
python scripts/update_data.py --type index

# 强制刷新（重新拉取全部历史）
python scripts/update_data.py --force

# 指定配置文件
python scripts/update_data.py --config config/data_sources.yaml
```

### 2. 查看数据

```bash
ls data/curated/bars/etf/
head data/curated/bars/etf/ETF:510300.csv
```

### 3. 在回测中使用

```python
import pandas as pd
from pathlib import Path
from quantlab.backtest.engine import BacktestEngine
from quantlab.research.strategies.base import EqualWeightStrategy

def load_etf_data(symbols: list[str]) -> pd.DataFrame:
    """Load ETF data from curated directory."""
    all_data = []
    for symbol in symbols:
        path = Path(f"data/curated/bars/etf/{symbol}.csv")
        if path.exists():
            df = pd.read_csv(path)
            all_data.append(df)
    return pd.concat(all_data, ignore_index=True)

# Define universe
universe = ["ETF:510300", "ETF:510500", "ETF:518880", "ETF:511010"]

# Load data
data = load_etf_data(universe)

# Run backtest
strategy = EqualWeightStrategy(symbols=universe)
engine = BacktestEngine(strategy=strategy, initial_cash=1_000_000)
results = engine.run(data=data, rebalance_freq="M")
```

## 增量更新机制

脚本会自动：
1. 读取本地 CSV 的最后一个日期
2. 只拉取该日期之后的新数据
3. 合并去重、排序
4. 更新水位线文件

这样可以避免每次都拉取全部历史数据，提高效率。

## 数据校验

每次写入前会验证：
- ✅ 时间戳升序且无重复
- ✅ high >= max(open, close)
- ✅ low <= min(open, close)
- ✅ volume >= 0
- ✅ 无 NaN 值

校验失败会记录到日志，不会覆盖旧数据。

## 定时更新（推荐）

### Linux/macOS (crontab)

```bash
# 编辑 crontab
crontab -e

# 添加：每个交易日 18:30 更新
30 18 * * 1-5 cd /path/to/quantlab && python scripts/update_data.py >> data/cron.log 2>&1
```

### Windows (任务计划程序)

1. 打开"任务计划程序"
2. 创建基本任务
3. 触发器：每周一至周五 18:30
4. 操作：启动程序
5. 程序：`python.exe`
6. 参数：`scripts/update_data.py`
7. 起始于：`D:\path\to\quantlab`

## 故障排查

### 网络问题

AkShare 依赖东方财富等数据源，如果遇到连接问题：
- 检查网络连接
- 稍后重试（可能是服务器限制）
- 使用代理（如果需要）

### 数据缺失

某些 QDII ETF 或指数可能没有完整历史：
- 检查 AkShare 文档确认接口支持
- 考虑使用替代数据源

### 复权问题

- `qfq` (前复权)：适合回测（默认）
- `hfq` (后复权)：适合长期分析
- `""` (不复权)：原始价格

## 扩展开发

如需添加更多数据源：

1. 在 `config/data_sources.yaml` 添加配置
2. 在 `scripts/update_data.py` 添加拉取逻辑
3. 保持统一数据格式即可被回测引擎使用
