# QuantLab 三层架构

## 设计原则

**Strategy 不知道 Broker 存在，Broker 不知道策略逻辑**

## 三层分离

### 1. Strategy Layer (策略层)
**职责**: 生成目标权重 (Target Weights)  
**输入**: 市场数据 + 当前权重  
**输出**: `List[TargetWeight]`

```python
class Strategy:
    def generate_targets(
        self, 
        data: pd.DataFrame,
        current_weights: Optional[Dict[str, float]]
    ) -> List[TargetWeight]:
        # 只计算权重，不涉及订单
        pass
```

**禁止**:
- 直接生成订单
- 调用 broker 方法
- 读取文件系统

---

### 2. Portfolio Layer (组合层)
**职责**: 权重优化与风险控制  
**输入**: TargetWeights + 当前状态 + 约束  
**输出**: `Dict[symbol, target_qty]`

```python
class PortfolioBuilder:
    def build(
        self,
        targets: List[TargetWeight],
        current_state: PortfolioState,
        constraints: RiskConstraints
    ) -> Dict[str, int]:
        # 1. 权重归一化
        # 2. 应用风险约束
        # 3. 转换为数量
        pass
```

**包含模块**:
- `WeightOptimizer`: 权重优化
- `RiskConstraints`: 风险约束
  - `max_weight_per_asset`: 单票上限
  - `max_turnover`: 最大换手
  - `leverage_cap`: 杠杆上限
  - `min_cash_ratio`: 最小现金比例

---

### 3. Execution Layer (执行层)
**职责**: 订单执行与撮合  
**输入**: Orders  
**输出**: Fills / PortfolioState

```python
class ExecutionRouter:
    def submit_orders(self, orders: List[Order]) -> List[str]:
        # 只负责订单路由和状态跟踪
        pass
```

**禁止**:
- 修改目标权重
- 了解策略逻辑
- 做交易决策

---

## ETF 轮动策略架构

```
┌─────────────────────────────────────────┐
│  Risk Regime (风险开关)                  │
│  • 权益篮子 200MA 判断                   │
│  • 6-1 动量判断                          │
│  输出: RISK_ON / RISK_OFF                │
└─────────────────────────────────────────┘
                    ↓
        ┌───────────────────────┐
        │   RISK_ON             │
        │  • 计算动量            │
        │  • 选 TopK             │
        │  • 等权配置            │
        └───────────────────────┘
                    ↓
        ┌───────────────────────┐
        │   RISK_OFF            │
        │  • 权益降仓            │
        │  • 黄金+长债增配       │
        └───────────────────────┘
                    ↓
        ┌───────────────────────┐
        │  Portfolio Builder    │
        │  应用风险约束          │
        └───────────────────────┘
                    ↓
        ┌───────────────────────┐
        │  Execution Router     │
        │  订单执行              │
        └───────────────────────┘
```
