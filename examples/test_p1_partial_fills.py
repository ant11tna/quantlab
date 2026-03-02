"""P1 验证脚本：测试部分成交 + 成交量约束

场景：
- 下单 600 股
- bar volume = 1000
- participation_rate = 0.2 (20%)
- 预期：分 3 根 bar 成交，每根 200 股
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from loguru import logger

from quantlab.backtest.broker_sim import SimulatedBroker, FeeConfig, ExecutionConfig
from quantlab.core.types import Order, OrderType, Side, OrderStatus


def create_test_data():
    """创建测试数据：每根 bar volume=1000"""
    dates = pd.date_range(start="2024-01-01", periods=5, freq="D")
    
    data = []
    for i, date in enumerate(dates):
        data.append({
            "ts": date,
            "symbol": "TEST",
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 + i,
            "close": 100.0 + i,
            "volume": 1000,  # 固定成交量
        })
    
    return pd.DataFrame(data)


def test_partial_fills():
    """测试部分成交逻辑"""
    logger.info("=" * 60)
    logger.info("P1 验证：部分成交 + 成交量约束")
    logger.info("=" * 60)
    
    # 配置：20% 参与率，最小 1 股
    exec_config = ExecutionConfig(
        participation_rate=Decimal("0.2"),  # 每根 bar 最多成交 20%
        min_lot=Decimal("1")
    )
    
    # 初始资金充足
    broker = SimulatedBroker(
        initial_cash=Decimal("100000"),
        fee_config=FeeConfig(),
        exec_config=exec_config
    )
    
    # 创建大订单：600 股
    large_order = Order(
        id="",
        ts=datetime.now(),
        symbol="TEST",
        side=Side.BUY,
        qty=Decimal("600"),  # 目标 600 股
        order_type=OrderType.MARKET
    )
    
    order_id = broker.place_order(large_order)
    logger.info(f"下单: {large_order.qty} 股 @ TEST")
    logger.info(f"Bar volume=1000, participation_rate=0.2, 每根 bar 最多成交 200")
    
    # 模拟多根 bar
    test_data = create_test_data()
    
    fill_history = []
    
    for idx, row in test_data.iterrows():
        from quantlab.core.types import Bar
        
        bar = Bar(
            ts=row["ts"],
            symbol=row["symbol"],
            open_=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=Decimal(str(row["volume"]))
        )
        
        # 处理订单
        fills = broker.process_orders(row["ts"], {"TEST": bar})
        
        # 获取订单状态
        order = broker.orders[0]
        
        logger.info(
            f"Bar {idx+1}: volume={bar.volume}, "
            f"本次成交={sum(f.qty for f in fills)}, "
            f"累计成交={order.filled_qty}/{order.qty}, "
            f"状态={order.status.value}"
        )
        
        fill_history.append({
            "bar": idx + 1,
            "fills": fills,
            "filled_qty": order.filled_qty,
            "remaining_qty": order.remaining_qty,
            "status": order.status.value
        })
        
        # 如果已完成，提前结束
        if order.is_filled:
            logger.info(f"✅ 订单在 Bar {idx+1} 完成")
            break
    
    # 验证结果
    logger.info("\n" + "=" * 60)
    logger.info("验证结果")
    logger.info("=" * 60)
    
    order = broker.orders[0]
    
    # 检查 1：是否跨多根 bar
    bars_used = len([h for h in fill_history if h["fills"]])
    logger.info(f"1. 跨 bar 数量: {bars_used} (预期: 3)")
    
    # 检查 2：每根 bar 成交量
    if bars_used >= 3:
        fills_per_bar = [sum(f.qty for f in h["fills"]) for h in fill_history[:3]]
        logger.info(f"2. 每根 bar 成交量: {[float(q) for q in fills_per_bar]} (预期: [200, 200, 200])")
    
    # 检查 3：最终状态
    logger.info(f"3. 最终状态: {order.status.value} (预期: FILLED)")
    logger.info(f"4. 总成交: {order.filled_qty} (预期: 600)")
    
    # 检查 5：现金不为负
    logger.info(f"5. 剩余现金: {broker.cash} (预期: > 0)")
    
    # 断言验证
    assert order.is_filled, "订单应该完成"
    assert order.filled_qty == Decimal("600"), "应该成交 600 股"
    assert broker.cash > 0, "现金不能为负"
    assert bars_used >= 3, "应该跨至少 3 根 bar"
    
    logger.info("\n✅ P1 验证通过！")
    return True


def test_cash_constraint():
    """测试现金约束：资金不足时自动裁剪"""
    logger.info("\n" + "=" * 60)
    logger.info("P1 验证：现金约束（不允许杠杆）")
    logger.info("=" * 60)
    
    exec_config = ExecutionConfig(
        participation_rate=Decimal("1.0"),  # 100%，方便测试
        min_lot=Decimal("1")
    )
    
    # 初始资金只够买 50 股
    broker = SimulatedBroker(
        initial_cash=Decimal("5000"),  # $5000
        fee_config=FeeConfig(),
        exec_config=exec_config
    )
    
    # 想下单 100 股 @ $100 = $10,000（资金不足）
    order = Order(
        id="",
        ts=datetime.now(),
        symbol="TEST",
        side=Side.BUY,
        qty=Decimal("100"),
        order_type=OrderType.MARKET
    )
    
    order_id = broker.place_order(order)
    logger.info(f"下单: 100 股, 可用现金: $5000")
    
    # 模拟一根 bar
    from quantlab.core.types import Bar
    bar = Bar(
        ts=datetime.now(),
        symbol="TEST",
        open_=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("10000")
    )
    
    fills = broker.process_orders(datetime.now(), {"TEST": bar})
    
    final_order = broker.orders[0]
    logger.info(f"实际成交: {final_order.filled_qty} 股")
    logger.info(f"剩余现金: {broker.cash}")
    logger.info(f"订单状态: {final_order.status.value}")
    
    # 验证
    assert broker.cash >= 0, "现金不能为负"
    assert final_order.filled_qty < Decimal("100"), "应该被裁剪"
    assert final_order.status == OrderStatus.PARTIAL_FILLED, "应该是部分成交"
    
    logger.info("✅ 现金约束验证通过！")
    return True


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    
    try:
        test_partial_fills()
        test_cash_constraint()
        logger.info("\n" + "=" * 60)
        logger.info("🎉 所有 P1 测试通过！")
        logger.info("=" * 60)
    except AssertionError as e:
        logger.error(f"❌ 测试失败: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
