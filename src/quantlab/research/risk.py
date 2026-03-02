"""Risk analytics and metrics.

Portfolio risk decomposition and analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger


@dataclass
class RiskMetrics:
    """Portfolio risk metrics."""
    
    # Return metrics
    total_return: float = 0.0
    annualized_return: float = 0.0
    volatility: float = 0.0
    annualized_volatility: float = 0.0
    
    # Risk-adjusted returns
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    
    # Drawdown metrics
    max_drawdown: float = 0.0
    max_drawdown_duration: int = 0
    avg_drawdown: float = 0.0
    
    # Tail risk
    var_95: float = 0.0
    var_99: float = 0.0
    cvar_95: float = 0.0
    skewness: float = 0.0
    kurtosis: float = 0.0
    
    # Other
    beta: Optional[float] = None
    alpha: Optional[float] = None
    information_ratio: Optional[float] = None


@dataclass
class RiskDecomposition:
    """Risk contribution decomposition."""
    
    symbol: str
    weight: float
    volatility: float
    marginal_contribution: float
    contribution: float
    percent_contribution: float


class RiskAnalyzer:
    """Analyze portfolio risk."""
    
    def __init__(self, risk_free_rate: float = 0.02) -> None:
        """Initialize risk analyzer.
        
        Args:
            risk_free_rate: Annual risk-free rate
        """
        self.risk_free_rate = risk_free_rate
    
    def calculate_metrics(
        self,
        returns: pd.Series,
        benchmark_returns: Optional[pd.Series] = None,
        frequency: int = 252
    ) -> RiskMetrics:
        """Calculate risk metrics from return series.
        
        Args:
            returns: Portfolio returns series
            benchmark_returns: Optional benchmark returns
            frequency: Number of periods per year
            
        Returns:
            RiskMetrics object
        """
        metrics = RiskMetrics()
        
        if len(returns) == 0:
            return metrics
        
        # Basic return metrics
        metrics.total_return = (1 + returns).prod() - 1
        metrics.annualized_return = (1 + metrics.total_return) ** (frequency / len(returns)) - 1
        
        # Volatility
        metrics.volatility = returns.std()
        metrics.annualized_volatility = metrics.volatility * np.sqrt(frequency)
        
        # Sharpe ratio
        excess_returns = returns - self.risk_free_rate / frequency
        if metrics.volatility > 0:
            metrics.sharpe_ratio = excess_returns.mean() / metrics.volatility * np.sqrt(frequency)
        
        # Sortino ratio (downside deviation)
        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() if len(downside_returns) > 0 else 0
        if downside_std > 0:
            metrics.sortino_ratio = returns.mean() / downside_std * np.sqrt(frequency)
        
        # Drawdown
        metrics.max_drawdown, metrics.max_drawdown_duration = self._calculate_drawdown(returns)
        
        # VaR and CVaR
        metrics.var_95 = np.percentile(returns, 5)
        metrics.var_99 = np.percentile(returns, 1)
        metrics.cvar_95 = returns[returns <= metrics.var_95].mean() if len(returns) > 0 else 0
        
        # Higher moments
        metrics.skewness = returns.skew()
        metrics.kurtosis = returns.kurtosis()
        
        # Calmar ratio
        if abs(metrics.max_drawdown) > 0:
            metrics.calmar_ratio = metrics.annualized_return / abs(metrics.max_drawdown)
        
        # Beta and alpha if benchmark provided
        if benchmark_returns is not None and len(benchmark_returns) == len(returns):
            metrics.beta, metrics.alpha = self._calculate_beta_alpha(
                returns, benchmark_returns, frequency
            )
            
            # Information ratio
            tracking_error = (returns - benchmark_returns).std() * np.sqrt(frequency)
            if tracking_error > 0:
                metrics.information_ratio = (
                    (metrics.annualized_return - self._annualized_return(benchmark_returns, frequency))
                    / tracking_error
                )
        
        return metrics
    
    def _calculate_drawdown(
        self,
        returns: pd.Series
    ) -> Tuple[float, int]:
        """Calculate maximum drawdown and duration."""
        # Cumulative returns
        cum_returns = (1 + returns).cumprod()
        
        # Running maximum
        running_max = cum_returns.expanding().max()
        
        # Drawdown
        drawdown = (cum_returns - running_max) / running_max
        
        # Max drawdown
        max_dd = drawdown.min()
        
        # Max drawdown duration
        # Find longest period where drawdown < 0
        is_in_dd = drawdown < 0
        max_duration = 0
        current_duration = 0
        
        for in_dd in is_in_dd:
            if in_dd:
                current_duration += 1
                max_duration = max(max_duration, current_duration)
            else:
                current_duration = 0
        
        return max_dd, max_duration
    
    def _calculate_beta_alpha(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series,
        frequency: int
    ) -> Tuple[float, float]:
        """Calculate beta and alpha relative to benchmark."""
        # Align series
        aligned = pd.concat([returns, benchmark_returns], axis=1).dropna()
        if len(aligned) == 0:
            return 0.0, 0.0
        
        port_ret = aligned.iloc[:, 0]
        bench_ret = aligned.iloc[:, 1]
        
        # Calculate beta
        covariance = port_ret.cov(bench_ret)
        benchmark_variance = bench_ret.var()
        
        beta = covariance / benchmark_variance if benchmark_variance > 0 else 0
        
        # Calculate alpha
        port_annual = self._annualized_return(port_ret, frequency)
        bench_annual = self._annualized_return(bench_ret, frequency)
        alpha = port_annual - (self.risk_free_rate + beta * (bench_annual - self.risk_free_rate))
        
        return beta, alpha
    
    def _annualized_return(self, returns: pd.Series, frequency: int) -> float:
        """Calculate annualized return."""
        total = (1 + returns).prod() - 1
        n = len(returns)
        return (1 + total) ** (frequency / n) - 1 if n > 0 else 0
    
    def decompose_risk(
        self,
        weights: np.ndarray,
        returns: pd.DataFrame
    ) -> List[RiskDecomposition]:
        """Decompose portfolio risk by asset.
        
        Args:
            weights: Portfolio weights array
            returns: Returns DataFrame (assets as columns)
            
        Returns:
            List of RiskDecomposition objects
        """
        symbols = returns.columns.tolist()
        
        # Calculate covariance matrix
        cov_matrix = returns.cov().values
        
        # Portfolio variance
        port_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
        port_vol = np.sqrt(port_variance)
        
        # Marginal contributions
        marginal = np.dot(cov_matrix, weights) / port_vol if port_vol > 0 else np.zeros(len(weights))
        
        # Component contributions
        contributions = weights * marginal
        
        # Percentage contributions
        total_contribution = contributions.sum()
        percent_contributions = (
            contributions / total_contribution if total_contribution > 0 else np.zeros(len(weights))
        )
        
        # Individual volatilities
        individual_vols = np.sqrt(np.diag(cov_matrix))
        
        # Create decompositions
        decompositions = []
        for i, symbol in enumerate(symbols):
            decompositions.append(RiskDecomposition(
                symbol=symbol,
                weight=weights[i],
                volatility=individual_vols[i],
                marginal_contribution=marginal[i],
                contribution=contributions[i],
                percent_contribution=percent_contributions[i]
            ))
        
        return decompositions
    
    def rolling_metrics(
        self,
        returns: pd.Series,
        window: int = 252,
        frequency: int = 252
    ) -> pd.DataFrame:
        """Calculate rolling risk metrics.
        
        Args:
            returns: Returns series
            window: Rolling window size
            frequency: Periods per year
            
        Returns:
            DataFrame with rolling metrics
        """
        rolling = pd.DataFrame(index=returns.index)
        
        # Rolling returns
        rolling["return"] = returns.rolling(window).apply(
            lambda x: (1 + x).prod() - 1
        )
        
        # Rolling volatility (annualized)
        rolling["volatility"] = returns.rolling(window).std() * np.sqrt(frequency)
        
        # Rolling Sharpe
        excess_returns = returns - self.risk_free_rate / frequency
        rolling["sharpe"] = (
            excess_returns.rolling(window).mean() / 
            returns.rolling(window).std() * np.sqrt(frequency)
        )
        
        # Rolling max drawdown
        def rolling_max_dd(x):
            cum = (1 + x).cumprod()
            running_max = cum.expanding().max()
            dd = (cum - running_max) / running_max
            return dd.min()
        
        rolling["max_drawdown"] = returns.rolling(window).apply(rolling_max_dd)
        
        return rolling.dropna()


class CorrelationAnalyzer:
    """Analyze correlations between assets."""
    
    def __init__(self) -> None:
        pass
    
    def correlation_matrix(self, returns: pd.DataFrame) -> pd.DataFrame:
        """Calculate correlation matrix."""
        return returns.corr()
    
    def rolling_correlation(
        self,
        returns: pd.DataFrame,
        window: int = 63  # ~3 months
    ) -> pd.DataFrame:
        """Calculate rolling average correlation."""
        # Calculate rolling correlation for each pair
        corr_series = []
        
        symbols = returns.columns
        for i, s1 in enumerate(symbols):
            for s2 in symbols[i+1:]:
                rolling_corr = returns[s1].rolling(window).corr(returns[s2])
                corr_series.append(rolling_corr)
        
        # Average across all pairs
        avg_corr = pd.concat(corr_series, axis=1).mean(axis=1)
        return avg_corr
    
    def correlation_regime(
        self,
        returns: pd.DataFrame,
        high_threshold: float = 0.7,
        low_threshold: float = 0.3
    ) -> pd.Series:
        """Classify correlation regime."""
        avg_corr = self.rolling_correlation(returns)
        
        regime = pd.Series("normal", index=avg_corr.index)
        regime[avg_corr > high_threshold] = "high"
        regime[avg_corr < low_threshold] = "low"
        
        return regime
