# QuantLab Windows 启动指南

## 快速开始 Quick Start

### 1. 环境检查 Environment Check
双击运行 `check-env.bat` 检查环境是否就绪。

### 2. 安装依赖 Install Dependencies (首次运行)
如果依赖缺失，运行 `install-deps.bat` 安装。

### 3. 启动应用 Launch Application

#### 方式一：交互式启动 (推荐)
双击 `start.bat` → 选择页面 [1-4]

#### 方式二：快速启动
双击 `start-quick.bat` → 直接进入 Chart Demo

---

## Bat 文件说明

| 文件 | 用途 |
|------|------|
| `check-env.bat` | 环境检查，验证 Python、依赖、数据 |
| `install-deps.bat` | 安装/更新依赖 |
| `start.bat` | 交互式启动，可选择页面 |
| `start-quick.bat` | 快速启动 Chart Demo |

---

## 页面说明

| 选项 | 页面 | 说明 |
|------|------|------|
| 1 | Runs | 实验列表，查看所有回测 |
| 2 | Chart Demo | 图表演示，ECharts + Lightweight |
| 3 | Run Detail | 回测详情，深度分析 |
| 4 | Main App | 主入口 |

---

## 生成示例数据

首次使用建议生成示例数据：

```bash
python examples/risk_constraints_demo.py
```

然后运行 `start.bat` 查看结果。

---

## Troubleshooting

### Python 未找到
- 安装 Python 3.10+ 并添加到 PATH
- 或修改 bat 文件中的 `python` 为完整路径

### 依赖缺失
运行 `install-deps.bat` 自动安装。

### 端口冲突
修改 `start.bat` 中的 streamlit 命令：
```
streamlit run ui/app.py --server.port 8502
```
