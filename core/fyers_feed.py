"""
core/fyers_feed.py

Fyers data layer:
  - OAuth2 login (redirect flow, same pattern as Zerodha)
  - WebSocket live tick feed for NIFTY 50 and NIFTY BANK
  - In-memory candle builder: assembles 1-min and 15-min OHLCV bars from ticks
  - Thread-safe: UI and engine read candles without blocking the WS thread
"""

import threading
import time as time_mod
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Callable
import pytz
import pandas as pd
import requests

IST = pytz.timezone("Asia/Kolkata")

# ── Fyers symbol map ──────────────────────────────────────────────────────────
# Fyers uses NSE:NIFTY50-INDEX and NSE:NIFTYBANK-INDEX for indices
FYERS_SYMBOLS = {
    "NIFTY":     "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
}

# ── Candle intervals we track ─────────────────────────────────────────────────
INTERVALS = [1, 15]   # minutes


class CandleBuilder:
    """
    Builds OHLCV candles from a raw tick stream for one symbol.
    Thread-safe (protects _candles with a lock).
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._lock  = threading.Lock()
        # {interval_minutes: [{"datetime", "open", "high", "low", "close", "volume"}]}
        self._candles: dict[int, list[dict]] = {i: [] for i in INTERVALS}
        # {interval_minutes: current partial candle or None}
        self._current: dict[int, Optional[dict]] = {i: None for i in INTERVALS}

    def on_tick(self, ltp: float, ts: datetime):
        """Feed one tick into every interval's candle builder."""
        ts_ist = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
        for interval in INTERVALS:
            self._update_interval(ltp, ts_ist, interval)

    def _update_interval(self, ltp: float, ts: datetime, interval: int):
        bucket = self._bucket_start(ts, interval)
        with self._lock:
            cur = self._current[interval]
            if cur is None or cur["datetime"] != bucket:
                # close the previous candle
                if cur is not None:
                    self._candles[interval].append(cur)
                    # keep only last 200 closed candles
                    if len(self._candles[interval]) > 200:
                        self._candles[interval] = self._candles[interval][-200:]
                # open a new candle
                self._current[interval] = {
                    "datetime": bucket,
                    "open":     ltp,
                    "high":     ltp,
                    "low":      ltp,
                    "close":    ltp,
                    "volume":   0,
                }
            else:
                cur["high"]  = max(cur["high"], ltp)
                cur["low"]   = min(cur["low"],  ltp)
                cur["close"] = ltp

    def get_candles(self, interval: int, include_partial: bool = False) -> pd.DataFrame:
        """
        Returns a DataFrame of closed candles for the given interval.
        If include_partial=True, appends the current unfinished candle.
        """
        with self._lock:
            rows = list(self._candles[interval])
            if include_partial and self._current[interval]:
                rows = rows + [dict(self._current[interval])]
        if not rows:
            return pd.DataFrame(columns=["datetime","open","high","low","close","volume"])
        return pd.DataFrame(rows)

    def latest_ltp(self) -> Optional[float]:
        with self._lock:
            cur = self._current.get(1)
            return cur["close"] if cur else None

    @staticmethod
    def _bucket_start(ts: datetime, interval: int) -> datetime:
        """Floor ts to the nearest interval-minute boundary."""
        total_minutes = ts.hour * 60 + ts.minute
        floored = (total_minutes // interval) * interval
        h, m = divmod(floored, 60)
        return ts.replace(hour=h, minute=m, second=0, microsecond=0)


class FyersFeed:
    """
    Manages Fyers OAuth login and the WebSocket live feed.
    Exposes candle data via get_candles() for any tracked index.
    """

    def __init__(self, app_id: str, secret_key: str, redirect_uri: str = "http://127.0.0.1:8501"):
        self.app_id       = app_id
        self.secret_key   = secret_key
        self.redirect_uri = redirect_uri
        self.access_token: Optional[str] = None
        self._ws          = None
        self._ws_thread: Optional[threading.Thread] = None
        self._stop_flag   = threading.Event()
        self._builders: dict[str, CandleBuilder] = {}
        self._on_tick_callbacks: list[Callable] = []
        self._connected   = False

        # initialise builders for supported indices
        for index in FYERS_SYMBOLS:
            self._builders[index] = CandleBuilder(FYERS_SYMBOLS[index])

    # ── Auth ─────────────────────────────────────────────────────────────────

    def login_url(self, redirect_uri: str = None) -> str:
        """Return the Fyers OAuth URL the user should open in a browser."""
        redir = redirect_uri or self.redirect_uri
        state = "ib_algo"
        url = (
            f"https://api-t1.fyers.in/api/v3/generate-authcode"
            f"?client_id={self.app_id}"
            f"&redirect_uri={redir}"
            f"&response_type=code"
            f"&state={state}"
        )
        return url

    def complete_login(self, auth_code: str) -> str:
        """Exchange auth_code for access_token. Returns access_token."""
        import hashlib
        checksum = hashlib.sha256(
            f"{self.app_id}:{self.secret_key}".encode()
        ).hexdigest()

        payload = {
            "grant_type":  "authorization_code",
            "appIdHash":   checksum,
            "code":        auth_code,
        }
        resp = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-authcode",
            json=payload,
            timeout=10,
        )
        data = resp.json()
        if data.get("s") != "ok":
            raise ValueError(f"Fyers token error: {data.get('message', data)}")
        self.access_token = data["access_token"]
        return self.access_token

    def set_access_token(self, token: str):
        self.access_token = token

    @property
    def connected(self) -> bool:
        return self._connected and (self._ws_thread is not None) and self._ws_thread.is_alive()

    # ── WebSocket feed ────────────────────────────────────────────────────────

    def start_feed(self, indices: list[str]):
        """Start the Fyers WebSocket in a background thread."""
        if self._ws_thread and self._ws_thread.is_alive():
            return
        self._stop_flag.clear()
        self._tracked = [FYERS_SYMBOLS[i] for i in indices if i in FYERS_SYMBOLS]
        self._ws_thread = threading.Thread(
            target=self._run_websocket, daemon=True, name="FyersWS"
        )
        self._ws_thread.start()

    def stop_feed(self):
        self._stop_flag.set()
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception:
                pass
        self._connected = False

    def _run_websocket(self):
        """Internal: connect and keep the WS alive with reconnect logic."""
        from fyers_apiv3.FyersWebsocket import data_ws

        def _on_message(msg):
            # msg is a list of tick dicts from Fyers v3
            if not isinstance(msg, list):
                return
            for tick in msg:
                sym = tick.get("symbol") or tick.get("s", "")
                ltp = tick.get("ltp") or tick.get("last_price")
                ts_raw = tick.get("timestamp") or tick.get("exchange_timestamp")
                if ltp is None:
                    continue
                ts = (
                    datetime.fromtimestamp(ts_raw, tz=IST)
                    if isinstance(ts_raw, (int, float))
                    else datetime.now(IST)
                )
                # route to the matching builder
                for index, fsym in FYERS_SYMBOLS.items():
                    if fsym == sym:
                        self._builders[index].on_tick(float(ltp), ts)
                # fire any registered callbacks
                for cb in self._on_tick_callbacks:
                    try:
                        cb(sym, float(ltp), ts)
                    except Exception:
                        pass

        def _on_error(msg):
            self._connected = False

        def _on_close(msg):
            self._connected = False

        def _on_open():
            self._connected = True
            self._ws.subscribe(symbols=self._tracked, data_type="SymbolUpdate")

        while not self._stop_flag.is_set():
            try:
                self._ws = data_ws.FyersDataSocket(
                    access_token=f"{self.app_id}:{self.access_token}",
                    log_path="",
                    litemode=False,
                    write_to_file=False,
                    reconnect=True,
                    on_connect=_on_open,
                    on_close=_on_close,
                    on_error=_on_error,
                    on_message=_on_message,
                )
                self._ws.connect()
                # connect() blocks until disconnect; loop will reconnect
            except Exception:
                pass
            if not self._stop_flag.is_set():
                time_mod.sleep(5)   # wait before reconnect

    # ── Data access ───────────────────────────────────────────────────────────

    def get_candles(self, index: str, interval_minutes: int, include_partial: bool = False) -> pd.DataFrame:
        """
        Returns closed OHLCV candles for the given index and interval.
        interval_minutes must be 1 or 15.
        """
        builder = self._builders.get(index)
        if builder is None:
            return pd.DataFrame()
        return builder.get_candles(interval_minutes, include_partial=include_partial)

    def get_ltp(self, index: str) -> Optional[float]:
        builder = self._builders.get(index)
        return builder.latest_ltp() if builder else None

    def add_tick_callback(self, fn: Callable):
        """Register fn(symbol, ltp, datetime) called on every tick."""
        self._on_tick_callbacks.append(fn)

    def get_daily_closes(self, index: str, days: int = 30) -> pd.Series:
        """
        Fyers REST historical API — daily closes for EMA20.
        Falls back to what we have from the live feed if REST fails.
        """
        try:
            return self._fetch_daily_closes_rest(index, days)
        except Exception:
            # graceful fallback: use 15-min closes compressed to daily
            df = self.get_candles(index, 15)
            if df.empty:
                return pd.Series(dtype=float)
            return df["close"].reset_index(drop=True)

    def _fetch_daily_closes_rest(self, index: str, days: int) -> pd.Series:
        symbol   = FYERS_SYMBOLS[index]
        to_dt    = datetime.now(IST)
        from_dt  = to_dt - timedelta(days=days + 10)
        payload  = {
            "symbol":      symbol,
            "resolution":  "D",
            "date_format": "1",
            "range_from":  from_dt.strftime("%Y-%m-%d"),
            "range_to":    to_dt.strftime("%Y-%m-%d"),
            "cont_flag":   "1",
        }
        headers = {"Authorization": f"{self.app_id}:{self.access_token}"}
        resp    = requests.get(
            "https://api-t1.fyers.in/api/v3/history",
            params=payload,
            headers=headers,
            timeout=10,
        )
        data = resp.json()
        if data.get("s") != "ok":
            raise ValueError(data.get("message", "Fyers history error"))
        candles = data.get("candles", [])
        # candles: [[epoch, o, h, l, c, v], ...]
        closes = pd.Series([c[4] for c in candles])
        return closes.reset_index(drop=True)
