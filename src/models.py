"""Core typed interfaces for the NIFTY scalping platform.

All domain objects are defined here as frozen dataclasses for immutability
and clear contracts between layers.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class UnderlyingBar:
    """5-second OHLCV bar for an underlying instrument (index or equity)."""

    symbol: str
    timestamp: int  # unix epoch seconds
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True, slots=True)
class OptionBar:
    """5-second OHLCV bar for a specific option contract."""

    underlying: str
    expiry: str  # YYYY-MM-DD
    strike: float
    option_type: str  # "CE" or "PE"
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int = 0


@dataclass(frozen=True, slots=True)
class OptionChainSnapshot:
    """Point-in-time snapshot of the option chain for contract selection."""

    underlying: str
    timestamp: int
    expiry: str
    entries: tuple[OptionChainEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class OptionChainEntry:
    """Single strike row in an option chain snapshot."""

    strike: float
    option_type: str
    ltp: float
    bid: float
    ask: float
    volume: int
    oi: int
    iv: float = 0.0


# ---------------------------------------------------------------------------
# Feature / regime layer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FeatureRow:
    """Computed feature vector for a single 5s timestamp."""

    symbol: str
    timestamp: int

    # Multi-horizon returns
    ret_1: float = 0.0  # 1-bar (5s)
    ret_3: float = 0.0  # 3-bar (15s)
    ret_6: float = 0.0  # 6-bar (30s)
    ret_12: float = 0.0  # 12-bar (60s)
    ret_60: float = 0.0  # 60-bar (5m)

    # Volatility
    realized_vol_60: float = 0.0  # 5-min realized vol
    realized_vol_180: float = 0.0  # 15-min realized vol
    range_expansion: float = 0.0  # current range vs trailing avg range

    # VWAP
    vwap: float = 0.0
    vwap_deviation: float = 0.0  # (close - vwap) / vwap

    # Opening range
    or_high: float = 0.0
    or_low: float = 0.0
    or_breakout_up: bool = False
    or_breakout_down: bool = False
    or_minutes_elapsed: int = 0

    # Trend
    trend_slope_60: float = 0.0  # 5-min linear slope
    trend_slope_180: float = 0.0  # 15-min linear slope
    acceleration: float = 0.0  # slope change rate

    # Breadth (from NIFTY50 constituents)
    breadth_advancing_pct: float = 0.0
    breadth_dispersion: float = 0.0  # cross-sectional vol of constituent returns

    # Option context (populated only when option data available)
    atm_iv: float = 0.0
    atm_spread_pct: float = 0.0
    put_call_oi_ratio: float = 0.0
    atm_volume: int = 0


class RegimeLabel(enum.Enum):
    """Market regime for gating strategy signals."""

    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    NO_TRADE = "no_trade"


# ---------------------------------------------------------------------------
# Strategy / signal layer
# ---------------------------------------------------------------------------


class SignalDirection(enum.Enum):
    LONG_CE = "long_ce"  # bullish → buy call
    LONG_PE = "long_pe"  # bearish → buy put


@dataclass(frozen=True, slots=True)
class StrategySignal:
    """Raw signal emitted by a strategy before contract selection."""

    strategy_name: str
    timestamp: int
    direction: SignalDirection
    conviction: float  # 0..1
    regime: RegimeLabel
    features: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeIntent:
    """Signal enriched with a specific contract to trade."""

    signal: StrategySignal
    expiry: str
    strike: float
    option_type: str  # "CE" or "PE"
    entry_premium: float
    estimated_spread: float
    estimated_slippage: float
    hard_stop_premium: float
    time_stop_seconds: int


# ---------------------------------------------------------------------------
# Execution / fill layer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FillEvent:
    """Simulated or real fill for an order."""

    intent: TradeIntent
    fill_timestamp: int
    fill_price: float
    fees: float
    slippage: float


class ExitReason(enum.Enum):
    TARGET = "target"
    HARD_STOP = "hard_stop"
    TIME_STOP = "time_stop"
    SESSION_END = "session_end"
    KILL_SWITCH = "kill_switch"
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class PositionState:
    """Tracks an open position from entry to exit."""

    entry: FillEvent
    current_premium: float = 0.0
    unrealized_pnl: float = 0.0
    bars_held: int = 0
    exit: Optional[FillEvent] = None
    exit_reason: Optional[ExitReason] = None

    @property
    def is_open(self) -> bool:
        return self.exit is None

    @property
    def realized_pnl(self) -> float:
        if self.exit is None:
            return 0.0
        direction = 1.0  # long premium only
        gross = direction * (self.exit.fill_price - self.entry.fill_price)
        return gross - self.entry.fees - self.exit.fees - self.entry.slippage - self.exit.slippage


# ---------------------------------------------------------------------------
# Risk / config layer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Hard risk limits enforced by the simulator and live loop."""

    max_positions: int = 1
    max_daily_loss: float = 5000.0
    max_consecutive_losses: int = 3
    hold_min_seconds: int = 30
    hold_max_seconds: int = 300
    session_start_hour: int = 9
    session_start_minute: int = 15
    session_end_hour: int = 15
    session_end_minute: int = 30
    cooldown_after_loss_seconds: int = 60
    no_entry_after_hour: int = 15
    no_entry_after_minute: int = 15


@dataclass(frozen=True, slots=True)
class ExperimentConfig:
    """Configuration for a backtest or paper-trade run."""

    name: str
    risk: RiskLimits = field(default_factory=RiskLimits)
    strategies: tuple[str, ...] = ("momentum_breakout", "mean_reversion_snap")
    strike_search_range: int = 6  # ATM ± N strikes
    min_contract_volume: int = 100
    min_contract_oi: int = 500
    max_spread_pct: float = 0.02
    max_premium: float = 200.0
    min_premium: float = 5.0
    fee_per_lot: float = 40.0  # brokerage + STT + charges
    slippage_pct: float = 0.005  # 0.5% of premium
    walk_forward_window_days: int = 5


# ---------------------------------------------------------------------------
# Broker adapter interface
# ---------------------------------------------------------------------------


class BrokerAdapter:
    """Abstract broker interface. Subclass for Upstox, Fyers, etc."""

    async def get_ltp(self, symbol: str) -> float:
        raise NotImplementedError

    async def get_option_chain(
        self, underlying: str, expiry: str
    ) -> OptionChainSnapshot:
        raise NotImplementedError

    async def place_order(
        self, symbol: str, qty: int, side: str, order_type: str, price: float = 0.0
    ) -> str:
        raise NotImplementedError

    async def get_order_status(self, order_id: str) -> dict:
        raise NotImplementedError

    async def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    async def subscribe_market_data(self, symbols: list[str], callback) -> None:
        raise NotImplementedError

    async def unsubscribe_market_data(self, symbols: list[str]) -> None:
        raise NotImplementedError
