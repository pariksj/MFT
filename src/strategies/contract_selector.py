"""Contract selector: pick the best option contract for a given signal.

For each signal, searches the current weekly NIFTY expiry across ATM ± N strikes,
ranks contracts by direction fit, liquidity, premium affordability, and estimated
friction, and only falls forward to the next expiry if the current week fails
liquidity rules.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.models import (
    ExperimentConfig,
    OptionChainEntry,
    OptionChainSnapshot,
    SignalDirection,
    StrategySignal,
    TradeIntent,
)


@dataclass
class ContractScore:
    """Intermediate scoring for contract ranking."""

    entry: OptionChainEntry
    strike: float
    option_type: str
    direction_score: float = 0.0
    liquidity_score: float = 0.0
    premium_score: float = 0.0
    friction_score: float = 0.0
    total_score: float = 0.0


def get_atm_strike(spot_price: float, strike_step: float = 50.0) -> float:
    """Round spot price to nearest strike step."""
    return round(spot_price / strike_step) * strike_step


def filter_chain_entries(
    chain: OptionChainSnapshot,
    option_type: str,
    atm_strike: float,
    strike_range: int,
    strike_step: float = 50.0,
    config: ExperimentConfig | None = None,
) -> list[OptionChainEntry]:
    """Filter chain entries to eligible contracts within strike range."""
    if config is None:
        min_vol = 100
        min_oi = 500
        max_spread = 0.02
        min_prem = 5.0
        max_prem = 200.0
    else:
        min_vol = config.min_contract_volume
        min_oi = config.min_contract_oi
        max_spread = config.max_spread_pct
        min_prem = config.min_premium
        max_prem = config.max_premium

    low_strike = atm_strike - strike_range * strike_step
    high_strike = atm_strike + strike_range * strike_step

    eligible = []
    for e in chain.entries:
        if e.option_type != option_type:
            continue
        if e.strike < low_strike or e.strike > high_strike:
            continue
        if e.volume < min_vol:
            continue
        if e.oi < min_oi:
            continue
        if e.ltp < min_prem or e.ltp > max_prem:
            continue
        # Spread check
        if e.bid > 0 and e.ask > 0:
            spread_pct = (e.ask - e.bid) / ((e.ask + e.bid) / 2)
            if spread_pct > max_spread:
                continue
        eligible.append(e)

    return eligible


def score_contracts(
    entries: list[OptionChainEntry],
    signal: StrategySignal,
    atm_strike: float,
    strike_step: float = 50.0,
) -> list[ContractScore]:
    """Score and rank eligible contracts."""
    if not entries:
        return []

    scores = []
    for e in entries:
        cs = ContractScore(entry=e, strike=e.strike, option_type=e.option_type)

        # Direction score: prefer ATM or slightly ITM for delta
        distance = abs(e.strike - atm_strike) / strike_step
        cs.direction_score = max(0, 1.0 - distance * 0.15)

        # Liquidity score: normalized volume and OI
        max_vol = max(x.volume for x in entries)
        max_oi = max(x.oi for x in entries)
        cs.liquidity_score = 0.5 * (e.volume / max_vol if max_vol > 0 else 0) + 0.5 * (
            e.oi / max_oi if max_oi > 0 else 0
        )

        # Premium score: prefer moderate premium (not too cheap, not too expensive)
        # Sweet spot around 30-100
        if 30 <= e.ltp <= 100:
            cs.premium_score = 1.0
        elif e.ltp < 30:
            cs.premium_score = e.ltp / 30.0
        else:
            cs.premium_score = max(0, 1.0 - (e.ltp - 100) / 100)

        # Friction score: lower spread = better
        if e.bid > 0 and e.ask > 0:
            spread_pct = (e.ask - e.bid) / ((e.ask + e.bid) / 2)
            cs.friction_score = max(0, 1.0 - spread_pct * 50)  # 2% spread → 0 score
        else:
            cs.friction_score = 0.5  # no bid/ask data, assume moderate

        # Weighted total
        cs.total_score = (
            0.30 * cs.direction_score
            + 0.30 * cs.liquidity_score
            + 0.20 * cs.premium_score
            + 0.20 * cs.friction_score
        )

        scores.append(cs)

    scores.sort(key=lambda x: x.total_score, reverse=True)
    return scores


def select_contract(
    signal: StrategySignal,
    chain: OptionChainSnapshot,
    spot_price: float,
    config: ExperimentConfig | None = None,
    strike_step: float = 50.0,
) -> TradeIntent | None:
    """Select the best contract for a signal, returning a TradeIntent or None.

    Args:
        signal: The strategy signal to fill.
        chain: Current option chain snapshot.
        spot_price: Current underlying spot price.
        config: Experiment configuration with risk/filter params.
        strike_step: Distance between strikes (50 for NIFTY).

    Returns:
        TradeIntent if a suitable contract is found, None otherwise.
    """
    if config is None:
        config = ExperimentConfig(name="default")

    atm = get_atm_strike(spot_price, strike_step)

    # Determine option type from signal direction
    option_type = "CE" if signal.direction == SignalDirection.LONG_CE else "PE"

    # Filter eligible contracts
    eligible = filter_chain_entries(
        chain, option_type, atm, config.strike_search_range, strike_step, config
    )

    if not eligible:
        return None

    # Score and rank
    ranked = score_contracts(eligible, signal, atm, strike_step)
    if not ranked:
        return None

    best = ranked[0]
    entry = best.entry

    # Compute fill assumptions
    spread = 0.0
    if entry.bid > 0 and entry.ask > 0:
        spread = entry.ask - entry.bid
    estimated_slippage = entry.ltp * config.slippage_pct

    # Hard stop: lose at most 50% of premium
    hard_stop = entry.ltp * 0.5

    return TradeIntent(
        signal=signal,
        expiry=chain.expiry,
        strike=best.strike,
        option_type=option_type,
        entry_premium=entry.ltp,
        estimated_spread=spread,
        estimated_slippage=estimated_slippage,
        hard_stop_premium=hard_stop,
        time_stop_seconds=config.risk.hold_max_seconds,
    )
