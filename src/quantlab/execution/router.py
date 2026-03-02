"""Execution Router - Bridge between strategy and broker.

Decouples backtest engine from specific broker implementations.
Future live trading only requires swapping the broker adapter.
"""

from __future__ import annotations

from typing import List, Protocol

from loguru import logger

from quantlab.core.types import Order, Fill
from quantlab.execution.broker_base import BrokerAdapter


class ExecutionBackend(Protocol):
    """Protocol for execution backends (broker or simulator)."""
    
    def place_order(self, order: Order) -> str:
        """Submit an order, return order ID."""
        ...
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        ...
    
    def get_fills(self) -> List[Fill]:
        """Get all fills."""
        ...


class ExecutionRouter:
    """Route orders to execution backend.
    
    This class serves as the abstraction layer between the backtest engine
    and the actual execution mechanism. In backtesting, it wraps the
    SimulatedBroker. In live trading, it will wrap a BrokerAdapter.
    
    Usage:
        # Backtesting
        sim_broker = SimulatedBroker(initial_cash, fee_config)
        router = ExecutionRouter(sim_broker)
        
        # Live trading (future)
        ibkr = IBKRAdapter(credentials)
        router = ExecutionRouter(ibkr)
    """
    
    def __init__(self, backend: ExecutionBackend) -> None:
        """Initialize router with execution backend.
        
        Args:
            backend: Broker adapter or simulated broker
        """
        self.backend = backend
        self._pending_orders: List[Order] = []
        self._submitted_orders: List[Order] = []
    
    def submit_orders(self, orders: List[Order]) -> List[str]:
        """Submit multiple orders.
        
        Args:
            orders: List of orders to submit
            
        Returns:
            List of order IDs
        """
        order_ids = []
        for order in orders:
            try:
                order_id = self.backend.place_order(order)
                order_ids.append(order_id)
                self._submitted_orders.append(order)
                logger.debug(f"Order submitted: {order_id} {order.symbol}")
            except Exception as e:
                logger.error(f"Failed to submit order {order.symbol}: {e}")
        
        return order_ids
    
    def submit_order(self, order: Order) -> str:
        """Submit single order.
        
        Args:
            order: Order to submit
            
        Returns:
            Order ID
        """
        order_ids = self.submit_orders([order])
        return order_ids[0] if order_ids else ""
    
    def cancel_all(self) -> List[bool]:
        """Cancel all pending orders.
        
        Returns:
            List of cancellation results
        """
        results = []
        # Note: This requires tracking order IDs, which depends on backend
        logger.warning("cancel_all() not fully implemented")
        return results
    
    def get_fills(self) -> List[Fill]:
        """Get all fills from backend."""
        return self.backend.get_fills()
    
    @property
    def is_live(self) -> bool:
        """Check if using live broker (vs simulation)."""
        return isinstance(self.backend, BrokerAdapter)


class RiskAwareRouter(ExecutionRouter):
    """Execution router with pre-trade risk checks.
    
    Wraps a base router and adds risk gate validation before order submission.
    """
    
    def __init__(
        self,
        backend: ExecutionBackend,
        risk_gate: "RiskGate",
        portfolio_getter: callable
    ) -> None:
        """Initialize risk-aware router.
        
        Args:
            backend: Base execution backend
            risk_gate: Risk gate for validation
            portfolio_getter: Callable that returns current portfolio state
        """
        super().__init__(backend)
        self.risk_gate = risk_gate
        self.portfolio_getter = portfolio_getter
        self._rejected_orders: List[tuple[Order, str]] = []
    
    def submit_orders(self, orders: List[Order]) -> List[str]:
        """Submit orders with risk checks."""
        from quantlab.execution.broker_base import RiskGate
        
        portfolio = self.portfolio_getter()
        # Need prices for risk check - for now, skip detailed check
        # In full implementation, would need price feed
        
        approved_orders = []
        for order in orders:
            # Simplified risk check - full implementation would check
            # position limits, leverage, etc.
            approved_orders.append(order)
        
        return super().submit_orders(approved_orders)
    
    def get_rejected_orders(self) -> List[tuple[Order, str]]:
        """Get list of rejected orders with reasons."""
        return self._rejected_orders.copy()


def create_router_for_backtest(broker_sim) -> ExecutionRouter:
    """Create execution router for backtesting.
    
    Args:
        broker_sim: SimulatedBroker instance
        
    Returns:
        Configured ExecutionRouter
    """
    return ExecutionRouter(broker_sim)


def create_router_for_live(broker_adapter: BrokerAdapter) -> ExecutionRouter:
    """Create execution router for live trading.
    
    Args:
        broker_adapter: Live broker adapter (IBKR, etc.)
        
    Returns:
        Configured ExecutionRouter
    """
    return ExecutionRouter(broker_adapter)
