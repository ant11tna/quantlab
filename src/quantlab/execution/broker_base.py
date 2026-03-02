"""Base broker adapter interface.

Abstract interface for live broker adapters.
Research outputs target weights/intents, execution converts to orders.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Protocol

from quantlab.core.types import Order, Fill, Position, PortfolioState, OrderIntent


class BrokerAdapter(ABC):
    """Abstract base class for broker adapters.
    
    This interface is designed for future live trading integration.
    During backtesting, use SimulatedBroker instead.
    
    Design principles:
    1. Research layer outputs OrderIntent (what to achieve)
    2. Execution layer converts to Order (how to achieve it)
    3. Broker adapter handles order lifecycle
    4. All operations return immediately with local confirmation
    5. State changes arrive asynchronously via callbacks/polling
    """
    
    def __init__(self, name: str, paper_trading: bool = True) -> None:
        """Initialize broker adapter.
        
        Args:
            name: Broker name
            paper_trading: Whether using paper trading
        """
        self.name = name
        self.paper_trading = paper_trading
        self._connected = False
    
    # -------------------------------------------------------------------------
    # Connection & Lifecycle
    # -------------------------------------------------------------------------
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to broker API.
        
        Returns:
            True if connection successful
        """
        raise NotImplementedError
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from broker API."""
        raise NotImplementedError
    
    @property
    def is_connected(self) -> bool:
        """Check if connected to broker."""
        return self._connected
    
    # -------------------------------------------------------------------------
    # Order Management
    # -------------------------------------------------------------------------
    
    @abstractmethod
    async def place_order(self, order: Order) -> str:
        """Submit an order to the broker.
        
        Args:
            order: Order to submit
            
        Returns:
            Broker order ID
            
        Raises:
            ConnectionError: If not connected
            OrderRejected: If order is rejected
        """
        raise NotImplementedError
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order.
        
        Args:
            order_id: Broker order ID
            
        Returns:
            True if cancellation request accepted
        """
        raise NotImplementedError
    
    @abstractmethod
    async def get_order_status(self, order_id: str) -> Dict:
        """Get current order status.
        
        Args:
            order_id: Broker order ID
            
        Returns:
            Order status dictionary
        """
        raise NotImplementedError
    
    @abstractmethod
    async def list_open_orders(self) -> List[Order]:
        """List all open orders.
        
        Returns:
            List of open orders
        """
        raise NotImplementedError
    
    # -------------------------------------------------------------------------
    # Portfolio & Account
    # -------------------------------------------------------------------------
    
    @abstractmethod
    async def get_positions(self) -> Dict[str, Position]:
        """Get current positions.
        
        Returns:
            Dict of symbol -> Position
        """
        raise NotImplementedError
    
    @abstractmethod
    async def get_cash(self) -> float:
        """Get available cash.
        
        Returns:
            Available cash balance
        """
        raise NotImplementedError
    
    @abstractmethod
    async def get_portfolio_state(self) -> PortfolioState:
        """Get complete portfolio state.
        
        Returns:
            PortfolioState object
        """
        raise NotImplementedError
    
    # -------------------------------------------------------------------------
    # Market Data
    # -------------------------------------------------------------------------
    
    @abstractmethod
    async def get_quote(self, symbol: str) -> Dict:
        """Get current market quote.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Quote dictionary with bid/ask/last
        """
        raise NotImplementedError
    
    # -------------------------------------------------------------------------
    # Research -> Execution Bridge
    # -------------------------------------------------------------------------
    
    async def execute_intent(
        self,
        intent: OrderIntent,
        current_prices: Dict[str, float]
    ) -> str:
        """Execute an order intent.
        
        This is the bridge between research and execution.
        Converts OrderIntent to Order and submits.
        
        Args:
            intent: Order intent from research
            current_prices: Current market prices
            
        Returns:
            Order ID
        """
        # Convert intent to order
        from quantlab.core.types import OrderType, Side
        from decimal import Decimal
        from datetime import datetime
        
        # Determine quantity
        if intent.target_qty is not None:
            qty = intent.target_qty
        elif intent.target_weight is not None:
            # Calculate quantity from weight
            # This requires knowing portfolio NAV
            portfolio = await self.get_portfolio_state()
            nav = float(portfolio.nav)
            target_value = nav * float(intent.target_weight)
            price = current_prices.get(intent.symbol, 0)
            if price > 0:
                qty = Decimal(str(target_value / price))
            else:
                raise ValueError(f"No price available for {intent.symbol}")
        else:
            raise ValueError("Intent must have target_qty or target_weight")
        
        # Determine side from quantity change
        current_pos = await self.get_positions()
        current_qty = current_pos.get(intent.symbol, Position(intent.symbol)).qty
        
        if qty > current_qty:
            side = Side.BUY
            order_qty = qty - current_qty
        elif qty < current_qty:
            side = Side.SELL
            order_qty = current_qty - qty
        else:
            return ""  # No action needed
        
        # Create order
        order = Order(
            id="",  # Will be assigned by broker
            ts=datetime.now(),
            symbol=intent.symbol,
            side=side,
            qty=order_qty,
            order_type=OrderType.MARKET,  # Could be LIMIT based on urgency
            strategy_id=intent.reason
        )
        
        return await self.place_order(order)


class OrderRouter:
    """Route orders with position sizing and constraints.
    
    This is a helper class that sits between research and broker adapter.
    It handles:
    - Target weight -> quantity conversion
    - Lot size rounding
    - Risk gate checks
    """
    
    def __init__(
        self,
        broker: BrokerAdapter,
        min_order_size: float = 100.0,
        lot_sizes: Optional[Dict[str, int]] = None
    ) -> None:
        """Initialize order router.
        
        Args:
            broker: Broker adapter
            min_order_size: Minimum order notional value
            lot_sizes: Symbol -> lot size mapping
        """
        self.broker = broker
        self.min_order_size = min_order_size
        self.lot_sizes = lot_sizes or {}
    
    async def route_targets(
        self,
        target_weights: Dict[str, float],
        current_prices: Dict[str, float],
        nav: float
    ) -> List[str]:
        """Route target weights to orders.
        
        Args:
            target_weights: Target portfolio weights
            current_prices: Current prices
            nav: Portfolio NAV
            
        Returns:
            List of order IDs
        """
        order_ids = []
        
        # Get current positions
        positions = await self.broker.get_positions()
        current_quantities = {s: float(p.qty) for s, p in positions.items()}
        
        for symbol, weight in target_weights.items():
            if symbol not in current_prices:
                continue
            
            price = current_prices[symbol]
            target_value = nav * weight
            target_qty = target_value / price
            
            # Apply lot size
            lot_size = self.lot_sizes.get(symbol, 1)
            target_qty = round(target_qty / lot_size) * lot_size
            
            # Check minimum size
            if target_value < self.min_order_size:
                continue
            
            # Calculate order quantity
            current_qty = current_quantities.get(symbol, 0)
            order_qty = target_qty - current_qty
            
            if abs(order_qty) * price < self.min_order_size:
                continue  # Skip small orders
            
            # Create and submit order
            from quantlab.core.types import Order, Side, OrderType
            from decimal import Decimal
            from datetime import datetime
            
            order = Order(
                id="",
                ts=datetime.now(),
                symbol=symbol,
                side=Side.BUY if order_qty > 0 else Side.SELL,
                qty=Decimal(str(abs(order_qty))),
                order_type=OrderType.MARKET
            )
            
            order_id = await self.broker.place_order(order)
            order_ids.append(order_id)
        
        return order_ids


class RiskGate:
    """Risk gate for pre-trade checks.
    
    Validates orders against risk constraints before submission.
    Can be used in both research and live trading.
    """
    
    def __init__(
        self,
        max_position_weight: float = 0.50,
        max_sector_weight: float = 0.60,
        max_leverage: float = 1.5,
        max_drawdown_stop: Optional[float] = None
    ) -> None:
        """Initialize risk gate.
        
        Args:
            max_position_weight: Maximum single position weight
            max_sector_weight: Maximum sector weight
            max_leverage: Maximum portfolio leverage
            max_drawdown_stop: Stop trading if drawdown exceeds this
        """
        self.max_position_weight = max_position_weight
        self.max_sector_weight = max_sector_weight
        self.max_leverage = max_leverage
        self.max_drawdown_stop = max_drawdown_stop
        
        self._stopped = False
        self._peak_nav: Optional[float] = None
    
    def check_order(
        self,
        order: Order,
        portfolio: PortfolioState,
        prices: Dict[str, float]
    ) -> tuple[bool, str]:
        """Check if order passes risk checks.
        
        Args:
            order: Order to check
            portfolio: Current portfolio state
            prices: Current prices
            
        Returns:
            (passed, reason) tuple
        """
        if self._stopped:
            return False, "Risk gate is stopped"
        
        nav = float(portfolio.nav)
        if nav == 0:
            return False, "Zero NAV"
        
        # Check drawdown stop
        if self.max_drawdown_stop is not None:
            if self._peak_nav is None or nav > self._peak_nav:
                self._peak_nav = nav
            
            drawdown = (self._peak_nav - nav) / self._peak_nav
            if drawdown > self.max_drawdown_stop:
                self._stopped = True
                return False, f"Max drawdown exceeded: {drawdown:.2%}"
        
        # Check position size
        order_notional = float(order.qty) * prices.get(order.symbol, 0)
        new_weight = order_notional / nav
        
        if new_weight > self.max_position_weight:
            return False, f"Position weight {new_weight:.2%} exceeds max {self.max_position_weight:.2%}"
        
        # Check leverage
        new_exposure = sum(
            abs(float(p.qty) * prices.get(s, 0))
            for s, p in portfolio.positions.items()
        ) + order_notional
        
        new_leverage = new_exposure / nav
        if new_leverage > self.max_leverage:
            return False, f"Leverage {new_leverage:.2f}x exceeds max {self.max_leverage:.2f}x"
        
        return True, ""
    
    def reset(self) -> None:
        """Reset risk gate."""
        self._stopped = False
        self._peak_nav = None
