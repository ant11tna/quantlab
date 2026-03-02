"""Mock broker adapter for testing.

Simulates a broker without actually connecting to any API.
Useful for testing the execution layer before live deployment.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from loguru import logger

from quantlab.core.types import Order, Fill, Position, PortfolioState, OrderStatus, Side
from quantlab.execution.broker_base import BrokerAdapter


class MockBrokerAdapter(BrokerAdapter):
    """Mock broker for testing.
    
    Simulates realistic order execution with configurable latency
    and fill rates for testing the execution layer.
    """
    
    def __init__(
        self,
        latency_ms: float = 50.0,
        fill_rate: float = 1.0,
        partial_fill_prob: float = 0.0,
        paper_trading: bool = True
    ) -> None:
        """Initialize mock broker.
        
        Args:
            latency_ms: Simulated API latency
            fill_rate: Probability of order fill (0-1)
            partial_fill_prob: Probability of partial fill
            paper_trading: Whether this is paper trading mode
        """
        super().__init__("mock", paper_trading)
        self.latency_ms = latency_ms
        self.fill_rate = fill_rate
        self.partial_fill_prob = partial_fill_prob
        
        self._orders: Dict[str, Order] = {}
        self._fills: Dict[str, List[Fill]] = {}
        self._positions: Dict[str, Position] = {}
        self._cash: float = 1_000_000.0
        self._order_counter = 0
        
        # Simulated market prices
        self._prices: Dict[str, float] = {}
    
    async def connect(self) -> bool:
        """Connect to mock broker."""
        await self._simulate_latency()
        self._connected = True
        logger.info("Mock broker connected")
        return True
    
    async def disconnect(self) -> None:
        """Disconnect from mock broker."""
        await self._simulate_latency()
        self._connected = False
        logger.info("Mock broker disconnected")
    
    async def place_order(self, order: Order) -> str:
        """Place a mock order."""
        if not self._connected:
            raise ConnectionError("Not connected")
        
        await self._simulate_latency()
        
        self._order_counter += 1
        order_id = f"MOCK_{self._order_counter:06d}"
        
        # Create order copy with ID
        submitted_order = Order(
            id=order_id,
            ts=datetime.now(),
            symbol=order.symbol,
            side=order.side,
            qty=order.qty,
            order_type=order.order_type,
            limit_price=order.limit_price,
            status=OrderStatus.SUBMITTED,
            strategy_id=order.strategy_id
        )
        
        self._orders[order_id] = submitted_order
        self._fills[order_id] = []
        
        logger.info(f"Mock order placed: {order_id} {order.symbol} {order.side.name}")
        
        # Simulate fill
        await self._simulate_fill(order_id)
        
        return order_id
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a mock order."""
        await self._simulate_latency()
        
        if order_id not in self._orders:
            return False
        
        order = self._orders[order_id]
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
            return False
        
        order.status = OrderStatus.CANCELLED
        return True
    
    async def get_order_status(self, order_id: str) -> Dict:
        """Get mock order status."""
        await self._simulate_latency()
        
        if order_id not in self._orders:
            return {"error": "Order not found"}
        
        order = self._orders[order_id]
        fills = self._fills.get(order_id, [])
        
        filled_qty = sum(float(f.qty) for f in fills)
        avg_price = (
            sum(float(f.qty * f.price) for f in fills) / filled_qty
            if filled_qty > 0 else 0
        )
        
        return {
            "order_id": order_id,
            "status": order.status.name,
            "symbol": order.symbol,
            "side": order.side.name,
            "ordered_qty": float(order.qty),
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "fills": len(fills)
        }
    
    async def list_open_orders(self) -> List[Order]:
        """List open mock orders."""
        await self._simulate_latency()
        
        return [
            o for o in self._orders.values()
            if o.status in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED)
        ]
    
    async def get_positions(self) -> Dict[str, Position]:
        """Get mock positions."""
        await self._simulate_latency()
        return self._positions.copy()
    
    async def get_cash(self) -> float:
        """Get mock cash."""
        await self._simulate_latency()
        return self._cash
    
    async def get_portfolio_state(self) -> PortfolioState:
        """Get mock portfolio state."""
        await self._simulate_latency()
        
        positions_value = sum(
            float(p.qty) * self._prices.get(s, 0)
            for s, p in self._positions.items()
        )
        
        return PortfolioState(
            ts=datetime.now(),
            cash=Decimal(str(self._cash)),
            positions=self._positions.copy(),
            nav=Decimal(str(self._cash + positions_value)),
            exposures={
                s: Decimal(str(float(p.qty) * self._prices.get(s, 0)))
                for s, p in self._positions.items()
            }
        )
    
    async def get_quote(self, symbol: str) -> Dict:
        """Get mock quote."""
        await self._simulate_latency()
        
        base_price = self._prices.get(symbol, 100.0)
        spread = 0.01  # 1 cent spread
        
        return {
            "symbol": symbol,
            "bid": base_price - spread / 2,
            "ask": base_price + spread / 2,
            "last": base_price,
            "timestamp": datetime.now().isoformat()
        }
    
    # -------------------------------------------------------------------------
    # Mock-specific methods
    # -------------------------------------------------------------------------
    
    def set_price(self, symbol: str, price: float) -> None:
        """Set mock price for symbol."""
        self._prices[symbol] = price
    
    def set_cash(self, amount: float) -> None:
        """Set mock cash balance."""
        self._cash = amount
    
    async def _simulate_latency(self) -> None:
        """Simulate network latency."""
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000)
    
    async def _simulate_fill(self, order_id: str) -> None:
        """Simulate order fill."""
        order = self._orders[order_id]
        
        # Check if we fill
        if random.random() > self.fill_rate:
            order.status = OrderStatus.REJECTED
            logger.warning(f"Mock order rejected: {order_id}")
            return
        
        # Determine fill quantity
        fill_qty = float(order.qty)
        if random.random() < self.partial_fill_prob:
            fill_qty = fill_qty * random.uniform(0.3, 0.7)
        
        fill_qty = Decimal(str(round(fill_qty)))
        
        if fill_qty <= 0:
            order.status = OrderStatus.CANCELLED
            return
        
        # Get price
        price = Decimal(str(self._prices.get(order.symbol, 100.0)))
        
        # Create fill
        fill = Fill(
            order_id=order_id,
            ts=datetime.now(),
            qty=fill_qty,
            price=price,
            fee=fill_qty * price * Decimal("0.001"),  # 10 bps fee
            slippage=Decimal("0")
        )
        
        self._fills[order_id].append(fill)
        
        # Update order status
        if fill_qty >= order.qty:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIAL_FILLED
        
        # Update position
        if order.symbol not in self._positions:
            self._positions[order.symbol] = Position(symbol=order.symbol)
        
        pos = self._positions[order.symbol]
        
        if order.side == Side.BUY:
            total_cost = float(pos.qty) * float(pos.avg_price) + float(fill.qty * fill.price)
            pos.qty += fill.qty
            if pos.qty > 0:
                pos.avg_price = Decimal(str(total_cost / float(pos.qty)))
            self._cash -= float(fill.qty * fill.price + fill.fee)
        else:
            pos.qty -= fill.qty
            if pos.qty == 0:
                pos.avg_price = Decimal("0")
            self._cash += float(fill.qty * fill.price - fill.fee)
        
        logger.info(f"Mock order filled: {order_id} {fill_qty} @ {price}")
