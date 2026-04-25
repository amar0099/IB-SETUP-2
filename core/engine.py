"""
core/engine.py (refactored)
Inside Bar Breakout – 15-min setup detection, then 1-min breakout confirmation.
"""

import time as time_mod
from datetime import datetime, time, timedelta
import pytz

from core.strategy import (
    detect_setups, check_breakout, build_trade_params,
    compute_ema20, Setup, Signal, Trade,
)
from core.broker import INDEX_NFO_ROOT, EXCHANGE, LOT_SIZE

IST = pytz.timezone("Asia/Kolkata")
POLL_INTERVAL = 0.5  # seconds between _tick calls (fast response once setup found)

# ── Constants ──────────────────────────────────────────────────────────────────

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)
NO_ENTRY_AFTER = time(15, 0)
FORCE_EXIT_AT = time(15, 10)
MIN_MOTHER_PTS = 100
MAX_MOTHER_PCT = 0.005
BREAKOUT_WINDOW = 30  # minutes after baby close to catch breakout


def _setup_id(s: Setup) -> tuple:
    """Unique ID for a setup (to avoid duplicate signals)."""
    return (s.mother_high, s.mother_low, s.baby_close_time)


class AlgoEngine:
    """
    State machine:
      MONITOR_15M → (inside bar found) → MONITOR_1M → (entry or expires) → MONITOR_15M
    """

    def __init__(self, fyers, broker):
        self.index = None  # set by app
        self.fyers = fyers
        self.broker = broker

        # State
        self.active_setup: Setup = None
        self.active_trade = None
        self.mode = "MONITOR_15M"  # or "MONITOR_1M"

        # EMA
        self._ema20 = None
        self._ema20_date = None

        # SL hit tracking
        self.sl_hits_today = 0

        # Config
        self.lots = 1
        self.pe_offset = 0
        self.ce_offset = 0
        self.paper_mode = True
        self.expiry = None

        # Logs
        self.log = []
        self._log_lock = None
        
        # Thread control
        self._running = False
        self._engine_thread = None

    def _log(self, level: str, msg: str):
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self.log.append({"time": ts, "level": level, "msg": msg})
        if len(self.log) > 500:
            self.log = self.log[-500:]
        print(f"{ts} [{level:7}] {msg}")

    @property
    def running(self) -> bool:
        return self._running

    def start(self):
        """Start engine in background thread."""
        if self._running:
            return
        self._running = True
        import threading
        self._engine_thread = threading.Thread(target=self.run, daemon=True, name="AlgoEngine")
        self._engine_thread.start()

    def stop(self):
        """Stop engine."""
        self._running = False

    def status_summary(self) -> dict:
        """Return current status for live dashboard."""
        if self.active_trade:
            signal = self.active_trade.signal.direction
            position = f"{signal} @ {self.active_trade.entry_price}"
        elif self.active_setup:
            signal = "Watching"
            position = f"Setup: {self.active_setup.mother_low}–{self.active_setup.mother_high}"
        else:
            signal = "Idle"
            position = "—"

        try:
            ltp = self.fyers.kite.quote(f"NSE:{self._get_index_symbol()}")["last_price"] if hasattr(self.fyers, 'kite') else "—"
        except:
            ltp = "—"

        return {
            "signal": signal,
            "position": position,
            "ltp": f"{ltp:.2f}" if isinstance(ltp, (int, float)) else ltp,
            "ema20": f"{self._ema20:.2f}" if self._ema20 else "—",
            "sl_hits": self.sl_hits_today,
        }

    def _get_index_symbol(self) -> str:
        """Map index to NSE symbol."""
        mapping = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK"}
        return mapping.get(self.index, "NIFTY 50")

    def run(self):
        """Main loop."""
        if not self.index:
            self._log("ERROR", "Index not set — cannot start")
            return
            
        self._log("INFO", f"Algo started | data: Fyers REST | orders: Zerodha | {self.index}")

        while self._running:
            try:
                now = datetime.now(IST)

                # Market hours check
                if not (MARKET_OPEN <= now.time() < MARKET_CLOSE):
                    time_mod.sleep(5)
                    continue

                # Daily reset
                if now.hour == 9 and now.minute == 15:
                    self.sl_hits_today = 0
                    self.active_trade = None
                    self.active_setup = None
                    self._log("INFO", f"Daily reset — {now.strftime('%d %b %Y')}")

                # State machine
                if self.mode == "MONITOR_15M":
                    self._monitor_15m(now)
                elif self.mode == "MONITOR_1M":
                    self._monitor_1m(now)

                time_mod.sleep(POLL_INTERVAL)

            except Exception as e:
                self._log("ERROR", str(e))
                time_mod.sleep(1)
        
        self._log("INFO", "Algo stopped")
        self._running = False
        self.fyers.stop_feed()

    def _monitor_15m(self, now: datetime):
        """
        Check for inside bar setups every 15 minutes.
        Only check at :16, :31, :46, :01 past the hour (1 min after 15m candle closes).
        """
        # Stop polling during MONITOR_15M to save quota
        if self.fyers._connected:
            self.fyers.stop_feed()

        # Only check every 15 min, 1 min after candle close
        minute = now.minute
        check_minutes = [16, 31, 46, 1]  # (9:31, 9:46, 10:01, 10:16 → checks 9:30, 9:45, 10:00, 10:15 candles)
        if minute not in check_minutes:
            return

        # Avoid double-checking in the same minute
        if not hasattr(self, '_last_15m_check_minute'):
            self._last_15m_check_minute = -1
        if self._last_15m_check_minute == minute:
            return
        self._last_15m_check_minute = minute

        # Get 15m candles
        df_15m = self.fyers.get_candles(self.index, 15)
        if df_15m.empty or len(df_15m) < 2:
            self._log("DEBUG", f"15m monitor: not enough candles ({len(df_15m)})")
            return

        self._log("DEBUG", f"15m check @ {now.strftime('%H:%M')} — {len(df_15m)} candles")

        # Detect setups
        self._refresh_ema(now)
        if self._ema20 is None:
            return

        setups = detect_setups(df_15m, self.index)
        if not setups:
            self._log("DEBUG", f"No inside bars in {len(df_15m)} candles")
            return

        candidate = setups[-1]
        setup_id = _setup_id(candidate)

        # New setup?
        if self.active_setup is None or setup_id != _setup_id(self.active_setup):
            self.active_setup = candidate
            self._last_signal_id = None
            self.mode = "MONITOR_1M"
            # Start REST polling now that setup is detected
            self.fyers.start_feed([self.index])
            self._log("SETUP", (
                f"🔶 Inside bar detected | mother {candidate.mother_low}–"
                f"{candidate.mother_high} ({candidate.range_pts} pts) | "
                f"baby close: {candidate.baby_close_time.strftime('%H:%M')}"
            ))

    def _monitor_1m(self, now: datetime):
        """
        Poll 1m candles and check for breakout.
        Runs every second until breakout or 30 min window expires.
        """
        # Ensure REST polling is running during MONITOR_1M
        if not self.fyers._connected:
            self.fyers.start_feed([self.index])

        # Check if setup window has expired
        if self.active_setup:
            elapsed_min = (now - self.active_setup.baby_close_time).total_seconds() / 60
            if elapsed_min > BREAKOUT_WINDOW:
                self._log("INFO", f"Setup expired (>{BREAKOUT_WINDOW}min) — back to 15m monitor")
                self.active_setup = None
                self.mode = "MONITOR_15M"
                self.fyers.stop_feed()
                return

        # Get latest 1m candle
        df_1m = self.fyers.get_candles(self.index, 1, include_partial=True)
        if df_1m.empty:
            return

        latest_1m = df_1m.iloc[-1].to_dict()

        # Check breakout
        if self.active_trade is None:
            already = hasattr(self, '_last_signal_id') and self._last_signal_id == _setup_id(self.active_setup)
            signal = check_breakout(latest_1m, self.active_setup, self._ema20, already)

            if signal:
                self._last_signal_id = _setup_id(self.active_setup)
                self._log("SIGNAL", (
                    f"🟢 {signal.direction} | close {round(latest_1m['close'], 2)} | "
                    f"mother {self.active_setup.mother_low}–{self.active_setup.mother_high}"
                ))
                self._enter_trade(signal, latest_1m)
        else:
            self._monitor_trade(latest_1m)

    def _refresh_ema(self, now: datetime):
        """EMA20 daily (cached)."""
        doy = now.timetuple().tm_yday
        if self._ema20_date == doy and self._ema20 is not None:
            return
        try:
            closes = self.fyers.get_daily_closes(self.index, days=30)
            if len(closes) >= 20:
                self._ema20 = compute_ema20(closes)
                self._ema20_date = doy
                self._log("INFO", f"EMA20: {round(self._ema20, 2)}")
        except Exception as e:
            self._log("ERROR", f"EMA20 fetch: {e}")

    def _enter_trade(self, signal: Signal, latest_1m: dict):
        """Enter a trade."""
        entry_price = latest_1m["open"]  # next candle open
        sl, target, risk, _ = build_trade_params(signal, entry_price)

        if sl is None:
            self._log("INFO", f"Trade rejected — risk filter")
            return

        strike = self._get_atm_strike(entry_price, signal.direction)
        opt_type = "CE" if signal.direction == "LONG" else "PE"
        
        # Get option symbol from Zerodha instruments
        try:
            expiry = self.expiry
            symbol = self.broker.get_option_symbol(self.index, expiry, strike, opt_type)
            if not symbol:
                self._log("ERROR", f"No option symbol for {self.index} {strike} {opt_type}")
                return
        except Exception as e:
            self._log("ERROR", f"Symbol lookup: {e}")
            return

        exchange = EXCHANGE[self.index]
        qty = LOT_SIZE[self.index] * self.lots

        self._log("ENTRY", f"{signal.direction} @ {entry_price} | SL {sl} | TGT {target} | strike {strike}")

        if not self.paper_mode:
            try:
                order_id = self.broker.sell_option(symbol, exchange, qty)
                self.active_trade = Trade(
                    signal=signal, index=self.index, entry_price=entry_price,
                    sl=sl, target=target, risk=risk, option_symbol=symbol,
                    option_order_id=order_id,
                )
            except Exception as e:
                self._log("ERROR", f"Order placement: {e}")
        else:
            self.active_trade = Trade(
                signal=signal, index=self.index, entry_price=entry_price,
                sl=sl, target=target, risk=risk, option_symbol=symbol,
            )

    def _monitor_trade(self, latest_1m: dict):
        """Monitor open trade for SL/target/exit."""
        from core.strategy import check_exit
        exit_reason = check_exit(self.active_trade, latest_1m)

        if exit_reason:
            self._log("EXIT", f"{exit_reason} | PnL: {self.active_trade.pnl:.2f}")
            if exit_reason == "SL":
                self.sl_hits_today += 1
            self.active_trade = None
            self.active_setup = None
            self.mode = "MONITOR_15M"
            self.fyers.stop_feed()


    def _get_atm_strike(self, spot: float, direction: str) -> int:
        from core.strategy import atm_strike
        offset_pts = self.ce_offset if direction == "LONG" else self.pe_offset
        return atm_strike(spot, self.index, offset=offset_pts)