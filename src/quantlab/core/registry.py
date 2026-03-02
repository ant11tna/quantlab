"""Registry for strategies, data sources, and backtest engines.

Provides a central registry pattern for extensible components.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Generic, Type, TypeVar
from functools import wraps

T = TypeVar("T")


class Registry(Generic[T]):
    """Generic registry for named components."""
    
    def __init__(self, name: str) -> None:
        self.name = name
        self._items: Dict[str, T] = {}
    
    def register(self, name: str, item: T) -> T:
        """Register an item.
        
        Can be used as a decorator:
            @registry.register("my_name")
            def my_func(): ...
        """
        if name in self._items:
            raise KeyError(f"{self.name} '{name}' already registered")
        self._items[name] = item
        return item
    
    def get(self, name: str) -> T:
        """Get registered item by name."""
        if name not in self._items:
            available = list(self._items.keys())
            raise KeyError(f"{self.name} '{name}' not found. Available: {available}")
        return self._items[name]
    
    def list(self) -> list[str]:
        """List all registered names."""
        return list(self._items.keys())
    
    def has(self, name: str) -> bool:
        """Check if name is registered."""
        return name in self._items
    
    def __call__(self, name: str) -> Callable[[T], T]:
        """Decorator registration."""
        def decorator(item: T) -> T:
            return self.register(name, item)
        return decorator


# Global registries
strategies = Registry[Any]("strategy")
data_sources = Registry[Any]("data_source")
backtest_engines = Registry[Any]("backtest_engine")
broker_adapters = Registry[Any]("broker_adapter")
fee_models = Registry[Any]("fee_model")


def register_strategy(name: str):
    """Decorator to register a strategy."""
    return strategies(name)


def register_data_source(name: str):
    """Decorator to register a data source."""
    return data_sources(name)


def register_backtest_engine(name: str):
    """Decorator to register a backtest engine."""
    return backtest_engines(name)


def register_broker_adapter(name: str):
    """Decorator to register a broker adapter."""
    return broker_adapters(name)


def register_fee_model(name: str):
    """Decorator to register a fee model."""
    return fee_models(name)
