"""
core/engine.py  (v2)

Data  → Fyers WebSocket (live ticks → candles built in-memory)
Orders → Zerodha KiteConnect

The engine runs in a background thread and polls candle snapshots
from FyersFeed every POLL_INTERVAL seconds.
"""

import threading
import time as time_mod
from datetime import datetime, time
from typing import Optional
import pytz

from .strategy import (
    detect_setups, check_breakout, build_trade_params,
    check_exit, atm_strike, compute_ema20,
    Setup, Signal, Trade,
    NO_ENTRY_AFTER, FORCE_EXIT_AT,
)
from .broker     import ZerodhaClient, LOT_SIZE, EXCHANGE
from .fyers_feed import FyersFeed

IST          = pytz.timezone("Asia/Kolkata")
MARKET_OPEN  = time(9, 15)
MARKET_CLOSE = time(15, 30)
POLL_INTERVAL = 10   # seconds between engine ticks


class AlgoEngine:
    """
    Data source : FyersFeed  (WebSocket -> in-memory candles)
    Order source: ZerodhaClient (KiteConnect REST)
    """

    def __init__(self, fyers: FyersFeed, broker: ZerodhaClient):
        self.fyers  = fyers
        self.broker = broker

        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

        # configurable
        self.index      = "NIFTY"
        self.expiry     = None
        self.lots       = 1
        self.pe_offset  = 0
        self.ce_offset  = 0
        self.paper_mode = False

        # runtime state
        self.active_setup:    Optional[Setup]  = None
        self.active_trade:    Optional[Trade]  = None
        self.sl_hits_today:   int              = 0
        self._last_signal_id: Optional[str]   = None
        self._last_day_reset: Optional[int]   = None
        self._ema20:          Optional[float]  = None
        self._ema20_date:     Optional[int]   = None

        self.log: list[dict] = []
        self._lock = threading.Lock()

    # ── Public controls ──────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self.fyers.start_feed([self.index])
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="AlgoEngine"
        )
        self._thread.start()
        self._log("INFO",
            f"Algo started | data: Fyers WS | orders: Zerodha | {self.index}")

    def stop(self):
        self._stop_flag.set()
        self._log("INFO", "Algo stopped by user.")

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Main loop ────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_flag.is_set():
            try:
                now = datetime.now(IST)
                doy = now.timetuple().tm_yday
                if self._last_day_reset != doy:
                    self._daily_reset(now)

                if not (MARKET_OPEN <= now.time() <= MARKET_CLOSE):
                    time_mod.sleep(30)
                    continue

                if not self.fyers.connected:
                    self._log("INFO", "Waiting for Fyers WS feed…")
                    time_mod.sleep(5)
                    continue

                self._tick(now)
            except Exception as e:
                self._log("ERROR", str(e))

            time_mod.sleep(POLL_INTERVAL)

    def _tick(self, now: datetime):
        if self.active_trade:
            self._monitor_trade()
            return

        if self.sl_hits_today >= 2:
            return

        self._refresh_ema(now)
        if self._ema20 is None:
            return

        df_1m = self.fyers.get_candles(self.index, 1, include_partial=True)
        if df_1m.empty:
            return
        latest_1m = df_1m.iloc[-1].to_dict()

        self._refresh_setup()
        if self.active_setup is None:
            return

        already = self._last_signal_id == _setup_id(self.active_setup)
        signal  = check_breakout(latest_1m, self.active_setup, self._ema20, already)
        if signal is None:
            return

        self._last_signal_id = _setup_id(self.active_setup)
        self._log("SIGNAL", (
            f"{signal.direction} | close {round(latest_1m['close'],2)} | "
            f"mother {self.active_setup.mother_low}–{self.active_setup.mother_high}"
        ))
        self._enter_trade(signal, latest_1m)

    # ── Setup refresh ────────────────────────────────────────────

    def _refresh_setup(self):
        df_15m = self.fyers.get_candles(self.index, 15)
        if df_15m.empty:
            return
        setups = detect_setups(df_15m, self.index)
        if not setups:
            return
        candidate = setups[-1]
        if (self.active_setup is None or
                _setup_id(candidate) != _setup_id(self.active_setup)):
            self.active_setup = candidate
            self._log("SETUP", (
                f"Inside bar | mother {candidate.mother_low}–"
                f"{candidate.mother_high} ({candidate.range_pts} pts)"
            ))

    # ── EMA refresh ──────────────────────────────────────────────

    def _refresh_ema(self, now: datetime):
        doy = now.timetuple().tm_yday
        if self._ema20_date == doy and self._ema20 is not None:
            return
        try:
            closes = self.fyers.get_daily_closes(self.index, days=30)
            if len(closes) >= 20:
                self._ema20      = compute_ema20(closes)
                self._ema20_date = doy
                self._log("INFO", f"EMA20: {round(self._ema20, 2)}")
        except Exception as e:
            self._log("ERROR", f"EMA20 fetch: {e}")

    # ── Entry ─────────────────────────────────────────────────────

    def _enter_trade(self, signal: Signal, latest_1m: dict):
        entry_price = latest_1m["close"]
        sl, target, risk, _ = build_trade_params(signal, entry_price)
        if sl is None:
            self._log("FILTER", "Max-risk filter rejected setup.")
            return

        spot     = self.fyers.get_ltp(self.index) or entry_price
        opt_type = "PE" if signal.direction == "LONG" else "CE"
        offset   = self.pe_offset if opt_type == "PE" else self.ce_offset
        strike   = atm_strike(spot, self.index, offset)

        symbol = self.broker.get_option_symbol(
            self.index, self.expiry, strike, opt_type
        )
        if symbol is None:
            self._log("ERROR",
                f"Symbol lookup failed: {self.index} {strike}{opt_type} {self.expiry}")
            return

        qty      = LOT_SIZE[self.index] * self.lots
        order_id = None

        if not self.paper_mode:
            try:
                order_id = self.broker.sell_option(
                    symbol, EXCHANGE[self.index], qty
                )
                self._log("ORDER", f"Sell order → Zerodha | ID: {order_id}")
            except Exception as e:
                self._log("ERROR", f"Zerodha order failed: {e}")
                return

        self.active_trade = Trade(
            signal=signal,
            index=self.index,
            entry_price=entry_price,
            sl=sl,
            target=target,
            risk=risk,
            option_symbol=symbol,
            option_order_id=order_id,
        )
        tag = "[PAPER] " if self.paper_mode else ""
        self._log("ENTRY", (
            f"{tag}{signal.direction} | SELL {symbol} x{qty} | "
            f"spot ~{round(spot,2)} | SL {sl} | Target {target}"
        ))

    # ── Monitor & exit ────────────────────────────────────────────

    def _monitor_trade(self):
        df_1m = self.fyers.get_candles(self.index, 1, include_partial=True)
        if df_1m.empty:
            return
        latest_1m = df_1m.iloc[-1].to_dict()
        reason    = check_exit(self.active_trade, latest_1m)
        if reason:
            self._close_trade(reason)

    def _close_trade(self, reason: str):
        trade = self.active_trade
        qty   = LOT_SIZE[self.index] * self.lots

        if not self.paper_mode:
            try:
                oid = self.broker.buy_option(
                    trade.option_symbol, EXCHANGE[self.index], qty
                )
                self._log("ORDER", f"Cover order → Zerodha | ID: {oid}")
            except Exception as e:
                self._log("ERROR", f"Exit order failed: {e}")

        if reason == "TARGET":
            trade.pnl = round(trade.risk * 2 * qty, 2)
        elif reason == "SL":
            trade.pnl = round(-trade.risk * qty, 2)
        else:
            trade.pnl = 0

        trade.status = reason
        if reason == "SL":
            self.sl_hits_today += 1
            if self.sl_hits_today >= 2:
                self._log("RISK", "2 SL hits today — trading halted.")

        sign = "+" if trade.pnl >= 0 else ""
        self._log("EXIT",
            f"{reason} | {trade.option_symbol} | ~P&L {sign}{trade.pnl}")

        self.active_trade    = None
        self.active_setup    = None
        self._last_signal_id = None

    # ── Daily reset ───────────────────────────────────────────────

    def _daily_reset(self, now: datetime):
        self.sl_hits_today   = 0
        self.active_setup    = None
        self.active_trade    = None
        self._last_signal_id = None
        self._last_day_reset = now.timetuple().tm_yday
        self._log("INFO", f"Daily reset — {now.strftime('%d %b %Y')}")

    # ── Log ───────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        ts = datetime.now(IST).strftime("%H:%M:%S")
        with self._lock:
            self.log.append({"time": ts, "level": level, "msg": msg})
            if len(self.log) > 200:
                self.log = self.log[-200:]

    # ── UI summary ────────────────────────────────────────────────

    def status_summary(self) -> dict:
        signal   = "—"
        position = "—"
        if self.active_trade:
            signal   = self.active_trade.signal.direction
            position = self.active_trade.option_symbol
        elif self.active_setup:
            signal = "Watching"
        return {
            "signal":   signal,
            "position": position,
            "sl_hits":  self.sl_hits_today,
            "ema20":    round(self._ema20, 2) if self._ema20 else "—",
            "ltp":      self.fyers.get_ltp(self.index) or "—",
        }


def _setup_id(s: Setup) -> str:
    return f"{s.baby_close_time.isoformat()}_{s.mother_high}_{s.mother_low}"
