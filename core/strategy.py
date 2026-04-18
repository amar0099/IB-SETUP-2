"""
core/strategy.py
Inside Bar Breakout – strategy logic (index-level, timeframe-agnostic).
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, time
import pytz

IST = pytz.timezone("Asia/Kolkata")

NO_ENTRY_AFTER = time(15, 0)
FORCE_EXIT_AT  = time(15, 10)
MIN_MOTHER_PTS = 100
MAX_MOTHER_PCT = 0.005          # 0.5 % of entry


@dataclass
class Setup:
    index: str
    mother_high: float
    mother_low: float
    baby_close_time: datetime      # 15-min candle that completed the baby
    range_pts: float = field(init=False)

    def __post_init__(self):
        self.range_pts = round(self.mother_high - self.mother_low, 2)


@dataclass
class Signal:
    setup: Setup
    direction: str                 # "LONG" | "SHORT"
    confirmed_at: datetime         # 1-min candle close that fired the breakout
    entry_price: Optional[float] = None   # filled on next 1-min open


@dataclass
class Trade:
    signal: Signal
    index: str
    entry_price: float
    sl: float
    target: float
    risk: float
    option_symbol: str
    option_order_id: Optional[str] = None
    option_entry_price: Optional[float] = None
    status: str = "OPEN"           # OPEN | SL | TARGET | TIME | MANUAL
    pnl: float = 0.0


# ─── EMA helpers ───────────────────────────────────────────────────────────────

def compute_ema20(daily_closes: pd.Series) -> float:
    """Return the latest 20-day EMA value."""
    if len(daily_closes) < 20:
        raise ValueError("Need at least 20 daily closes for EMA20.")
    return float(daily_closes.ewm(span=20, adjust=False).mean().iloc[-1])


# ─── Setup detection (runs on completed 15-min candles) ────────────────────────

def detect_setups(candles_15m: pd.DataFrame, index: str) -> list[Setup]:
    """
    candles_15m columns: open, high, low, close, datetime (IST-aware).
    Returns a deduplicated list of valid inside-bar setups.
    """
    df = candles_15m.copy().reset_index(drop=True)
    setups: list[Setup] = []

    if len(df) < 2:
        return setups

    for i in range(1, len(df)):
        mother = df.iloc[i - 1]
        baby   = df.iloc[i]

        m_time = _to_ist(mother["datetime"])
        b_time = _to_ist(baby["datetime"])

        # skip first candle of day
        if m_time.time() == time(9, 15) or b_time.time() == time(9, 15):
            continue
        # skip if baby is the last candle (15:15)
        if b_time.time() == time(15, 15):
            continue

        # inside-bar check
        if not (baby["high"] < mother["high"] and baby["low"] > mother["low"]):
            continue

        rng = mother["high"] - mother["low"]
        if rng < MIN_MOTHER_PTS:
            continue

        setups.append(Setup(
            index=index,
            mother_high=round(mother["high"], 2),
            mother_low=round(mother["low"], 2),
            baby_close_time=b_time,
        ))

    # dedup: same baby candle → keep widest mother
    setups = _dedup_setups(setups)
    return setups


def _dedup_setups(setups: list[Setup]) -> list[Setup]:
    seen: dict[datetime, Setup] = {}
    for s in setups:
        key = s.baby_close_time
        if key not in seen or s.range_pts > seen[key].range_pts:
            seen[key] = s
    return list(seen.values())


# ─── Signal confirmation (called on each new 1-min candle close) ───────────────

def check_breakout(
    candle_1m: dict,           # {high, low, close, open, datetime}
    setup: Setup,
    ema20: float,
    already_signalled: bool,
) -> Optional[Signal]:
    """
    Returns a Signal if the 1-min candle confirms a breakout, else None.
    already_signalled: prevents double-firing on the same setup.
    """
    if already_signalled:
        return None

    now = _to_ist(candle_1m["datetime"])
    if now.time() >= NO_ENTRY_AFTER:
        return None

    # window: up to 30 min after baby close
    elapsed = (now - setup.baby_close_time).total_seconds() / 60
    if elapsed > 30:
        return None

    close  = candle_1m["close"]
    broke_up   = close > setup.mother_high
    broke_down = close < setup.mother_low

    # both sides in same candle → dead
    if broke_up and broke_down:
        return None

    direction = None
    if broke_up:
        direction = "LONG"
    elif broke_down:
        direction = "SHORT"
    else:
        return None

    # EMA trend filter
    if direction == "LONG"  and close < ema20:
        return None
    if direction == "SHORT" and close > ema20:
        return None

    return Signal(setup=setup, direction=direction, confirmed_at=now)


# ─── Risk sizing ───────────────────────────────────────────────────────────────

def build_trade_params(signal: Signal, entry_price: float):
    """Validate max-risk filter and return sl/target/risk."""
    setup = signal.setup
    if signal.direction == "LONG":
        sl     = setup.mother_low
        target = entry_price + 2 * (entry_price - sl)
    else:
        sl     = setup.mother_high
        target = entry_price - 2 * (sl - entry_price)

    risk = abs(entry_price - sl)

    # max-risk filter: mother range ≤ 0.5 % of entry
    if setup.range_pts > MAX_MOTHER_PCT * entry_price:
        return None, None, None, None   # signal dead

    return round(sl, 2), round(target, 2), round(risk, 2), entry_price


# ─── Exit logic ────────────────────────────────────────────────────────────────

def check_exit(trade: Trade, candle_1m: dict) -> Optional[str]:
    """
    Returns exit reason string or None if still in trade.
    Checks SL, target, and time exit.
    """
    now   = _to_ist(candle_1m["datetime"])
    high  = candle_1m["high"]
    low   = candle_1m["low"]
    open_ = candle_1m["open"]

    # time exit
    if now.time() >= FORCE_EXIT_AT:
        return "TIME"

    if trade.signal.direction == "LONG":
        if low  <= trade.sl:     return "SL"
        if high >= trade.target: return "TARGET"
    else:
        if high >= trade.sl:     return "SL"
        if low  <= trade.target: return "TARGET"

    return None


# ─── ATM strike calculation ────────────────────────────────────────────────────

INDEX_STEP = {
    "NIFTY":    50,
    "BANKNIFTY": 100,
}

def atm_strike(spot: float, index: str, offset: int = 0) -> int:
    """
    offset in points (e.g. +100, -200).
    Returns the nearest valid strike + offset, rounded to the index step.
    """
    step = INDEX_STEP.get(index, 50)
    base = round(spot / step) * step
    raw  = base + offset
    return int(round(raw / step) * step)


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _to_ist(dt) -> datetime:
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.astimezone(IST)
    return IST.localize(dt)
