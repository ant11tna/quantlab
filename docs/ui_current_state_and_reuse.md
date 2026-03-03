# UI 现状结构与可复用函数清单

## 1) 首页（`ui/app.py`）现状

- **页面基础**：设置 Streamlit page config，并应用全局样式。`st.title + st.markdown` 展示首页标题与副标题。  
- **Sidebar 导航**：通过 `st.page_link` 提供 4 个入口：Home / Runs / Run Detail / Compare Runs。  
- **数据更新入口（Update All）**：
  - `force_update_all` 复选框（是否强制更新）。
  - 点击按钮后消费 `update_all_stream(force=...)` 事件流，按事件类型更新 UI：
    - `progress`：更新进度条、阶段状态、当前 symbol；
    - `start/heartbeat/log`（raw 阶段）：展示后台 PID、运行时长、日志片段；
    - `error`：逐条 warning；
    - `done`：显示成功/失败汇总。
  - 更新结束后执行 `st.cache_data.clear()` + `st.rerun()`。

## 2) runs 列表生成逻辑

当前仓库里有两套“runs 索引”逻辑，主 UI 页面各自维护：

- **`ui/pages/1_runs.py::scan_runs`（列表页主用）**
  - 仅纳入存在 `runs/<run_id>/results/metrics.json` 的 run。
  - 读取：
    - `metrics.json`（核心指标）
    - `config.yaml`（strategy/start/end 等补充字段）
  - 生成字段：`run_id/strategy/total_return/cagr/sharpe/max_drawdown/start/end/created_at/results_dir`。
  - 缓存：`@st.cache_data` + `build_runs_fingerprint()`（基于 metrics/config mtime）控制失效。

- **`ui/data/loader.py::list_runs`（通用数据加载层）**
  - 也按 `runs/<run_id>/results/metrics.json` 作为准入条件；
  - 读取 `config.yaml` 补充 strategy 与 created_at；
  - 兼容不同 metrics key（summary/risk/trading/root），返回更“通用”的 DataFrame（含兼容字段）。

- **`ui/pages/3_compare_runs.py::scan_runs`（对比页局部索引）**
  - 只存最小路径索引：`metrics_path/equity_path/weights_path`，后续按需读取。

## 3) 关键结果文件路径与读取方式

- **metrics**
  - 路径：`runs/<run_id>/results/metrics.json`
  - 读取：`json.load` / `Path.read_text()+json.loads`（各页面封装了 `_safe_read_json`）。

- **config**
  - 路径：`runs/<run_id>/config.yaml`
  - 读取：`yaml.safe_load`（列表页/详情页/loader 均有容错）。

- **equity_curve**
  - 主路径：`runs/<run_id>/results/equity_curve.parquet`
  - fallback（loader）：`equity_curve.csv`；并支持 root 目录 fallback（通过 `_get_results_path`）。
  - 规范化：把 `date/time/timestamp` -> `ts`，`equity/portfolio_value/value` -> `nav`，并转 datetime/numeric。

- **weights**
  - 路径：`runs/<run_id>/results/weights.parquet`
  - loader 中还支持 `weights.csv` 以及从 `positions` 兼容读取。

- **risk_status**
  - 路径：`runs/<run_id>/results/risk_status.json`
  - 用于详情页 Risk tab（current_drawdown / rolling 指标）。

- **yearly_stats**
  - 路径：`runs/<run_id>/results/yearly_stats.parquet`
  - 用于详情页 Yearly 分析表格。

- **stress_test**
  - 路径：`runs/<run_id>/results/stress_test.json`
  - 读取后展示 `results` 列表（DataFrame）。

## 4) `data/assets.yaml` 使用方式

- 元数据文件定义 `symbol -> {name, asset_class}`。
- 在详情页与对比页中：
  - 先 `load_assets_map("data/assets.yaml")` 读取映射；
  - 再 `group_weights_by_asset_class(weights_df, assets_map)` 生成按资产类别聚合的权重时序或对比快照；
  - 详情页额外用 `get_asset_class()` 标记未映射 symbol（归入 `other` 并提示）。

## 5) 可复用函数清单（建议作为统一入口）

### A. 直接复用（不改签名）

- `ui/pages/1_runs.py`
  - `build_runs_fingerprint`：runs 列表缓存失效控制。
  - `scan_runs`：主列表页 runs 索引与指标抽取。

- `ui/data/loader.py`
  - `_get_results_path`：结果文件多路径 fallback。
  - `load_run`：单 run 的 config/metrics/paths 汇总读取。
  - `load_equity_curve` / `load_positions` / `load_fills`：通用文件读取与字段归一化。

- `src/quantlab/assets.py`
  - `load_assets_map`：资产元数据读取。
  - `group_weights_by_asset_class`：权重按资产类别聚合。
  - `get_asset_class`：symbol 分类判定。

- `ui/pages/3_compare_runs.py`
  - `_build_compare_metrics`：多 run 指标拼表。
  - `_build_nav_compare`：净值曲线对齐（取时间交集并归一化）。

### B. 拟新增/抽象的入口（本次仅标注，不改代码）

- `quantlab.ui.runs.index.scan_runs_index()`
  - 统一现有 `1_runs.py::scan_runs` 与 `3_compare_runs.py::scan_runs`，输出标准 run 索引（含 metrics/config/equity/weights 路径）。

- `quantlab.ui.runs.results.load_result_bundle(run_id)`
  - 固化详情页所需 `metrics/config/equity/weights/risk_status/yearly_stats/stress_test` 一次性读取与 schema normalize。

- `quantlab.ui.runs.compare.build_compare_payload(run_ids)`
  - 封装 compare 页的 metrics/nav/drawdown/asset_class 数据准备，避免页面层重复逻辑。
