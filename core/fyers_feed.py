"""
core/fyers_feed.py

Fyers data layer — REST polling edition.
WebSocket is replaced with 1-second REST polling of the /quotes endpoint.
This is required because Streamlit Cloud blocks outbound WebSocket connections.
"""

import threading
import time as time_mod
from datetime import datetime, timedelta
from typing import Optional, Callable
import pytz
import pandas as pd
import requests

IST = pytz.timezone("Asia/Kolkata")

FYERS_SYMBOLS = {
    "NIFTY":     "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
}

INTERVALS = [1, 15]   # minutes


class CandleBuilder:
    def __init__(self, symbol: str):
        self.symbol   = symbol
        self._lock    = threading.Lock()
        self._candles: dict[int, list[dict]] = {i: [] for i in INTERVALS}
        self._current: dict[int, Optional[dict]] = {i: None for i in INTERVALS}

    def on_tick(self, ltp: float, ts: datetime):
        ts_ist = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
        for interval in INTERVALS:
            self._update_interval(ltp, ts_ist, interval)

    def _update_interval(self, ltp: float, ts: datetime, interval: int):
        bucket = self._bucket_start(ts, interval)
        with self._lock:
            cur = self._current[interval]
            if cur is None or cur["datetime"] != bucket:
                if cur is not None:
                    self._candles[interval].append(cur)
                    if len(self._candles[interval]) > 200:
                        self._candles[interval] = self._candles[interval][-200:]
                self._current[interval] = {
                    "datetime": bucket,
                    "open":  ltp, "high": ltp,
                    "low":   ltp, "close": ltp,
                    "volume": 0,
                }
            else:
                cur["high"]  = max(cur["high"], ltp)
                cur["low"]   = min(cur["low"],  ltp)
                cur["close"] = ltp

    def get_candles(self, interval: int, include_partial: bool = False) -> pd.DataFrame:
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
        total_minutes = ts.hour * 60 + ts.minute
        floored = (total_minutes // interval) * interval
        h, m = divmod(floored, 60)
        return ts.replace(hour=h, minute=m, second=0, microsecond=0)


class FyersFeed:
    """
    Fyers data feed using REST polling (/quotes endpoint).
    Polls every 1 second and feeds ticks into CandleBuilder.
    """

    def __init__(self, app_id: str, secret_key: str, redirect_uri: str = "http://127.0.0.1:8501"):
        self.app_id       = app_id
        self.secret_key   = secret_key
        self.redirect_uri = redirect_uri
        self.access_token: Optional[str] = None

        self._poll_thread: Optional[threading.Thread] = None
        self._stop_flag   = threading.Event()
        self._builders: dict[str, CandleBuilder] = {}
        self._on_tick_callbacks: list[Callable] = []
        self._connected   = False
        self._tracked_indices: list[str] = []
        self._log_cb: Optional[Callable] = None   # engine log callback

        for index in FYERS_SYMBOLS:
            self._builders[index] = CandleBuilder(FYERS_SYMBOLS[index])

    # ── Auth ──────────────────────────────────────────────────────────────────

    def login_url(self, redirect_uri: str = None) -> str:
        redir = redirect_uri or self.redirect_uri
        return (
            f"https://api-t1.fyers.in/api/v3/generate-authcode"
            f"?client_id={self.app_id}"
            f"&redirect_uri={redir}"
            f"&response_type=code"
            f"&state=ib_algo"
        )

    def complete_login(self, auth_code: str) -> str:
        import hashlib
        checksum = hashlib.sha256(
            f"{self.app_id}:{self.secret_key}".encode()
        ).hexdigest()
        resp = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-authcode",
            json={"grant_type": "authorization_code", "appIdHash": checksum, "code": auth_code},
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
        return (self._poll_thread is not None) and self._poll_thread.is_alive()

    # ── Feed (REST polling) ───────────────────────────────────────────────────

    def start_feed(self, indices: list[str]):
        """Start REST polling thread. Safe to call multiple times."""
        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._stop_flag.clear()
        self._tracked_indices = [i for i in indices if i in FYERS_SYMBOLS]
        self._poll_thread = threading.Thread(
            target=self._run_rest_poll, daemon=True, name="FyersREST"
        )
        self._poll_thread.start()
        self._connected = True   # Mark connected immediately — thread is running
        print(f"[FYERS REST] Poll thread started for {self._tracked_indices}")

    def stop_feed(self):
        self._stop_flag.set()
        self._connected = False

    def _run_rest_poll(self):
        """Poll quotes every 1 second using the Fyers SDK."""
        self._log("INFO", "REST poll thread running")
        from fyers_apiv3 import fyersModel

        # Diagnostic: log token state
        if not self.access_token:
            self._log("ERROR", "access_token is None — login not completed")
            return
        self._log("INFO", f"Token check: app_id={self.app_id}, token_len={len(self.access_token)}, token_start={self.access_token[:10]}***")

        fyers_client = fyersModel.FyersModel(
            client_id=self.app_id,
            token=self.access_token,
            log_path="",
        )
        consecutive_errors = 0

        while not self._stop_flag.is_set():
            try:
                symbols = ",".join(FYERS_SYMBOLS[i] for i in self._tracked_indices)
                data = fyers_client.quotes({"symbols": symbols})

                if data.get("s") == "ok":
                    if consecutive_errors > 0:
                        self._log("INFO", "REST poll recovered — receiving quotes")
                    consecutive_errors = 0

                    for item in data.get("d", []):
                        sym = item.get("n", "")
                        v   = item.get("v", {})
                        ltp = v.get("lp") or v.get("last_price")
                        if ltp is None:
                            continue
                        ts = datetime.now(IST)
                        for index, fsym in FYERS_SYMBOLS.items():
                            if fsym == sym:
                                self._builders[index].on_tick(float(ltp), ts)
                        for cb in self._on_tick_callbacks:
                            try:
                                cb(sym, float(ltp), ts)
                            except Exception:
                                pass
                else:
                    consecutive_errors += 1
                    msg = data.get("message", str(data))
                    self._log("ERROR", f"Quotes API error: {msg}")
                    time_mod.sleep(min(consecutive_errors * 2, 30))

            except Exception as e:
                consecutive_errors += 1
                self._log("ERROR", f"REST poll exception: {e}")
                time_mod.sleep(min(consecutive_errors * 2, 30))
                continue

            time_mod.sleep(1)

        self._log("INFO", "REST poll thread stopped")
        self._connected = False

    # ── Data access ───────────────────────────────────────────────────────────

    def get_candles(self, index: str, interval_minutes: int, include_partial: bool = False) -> pd.DataFrame:
        builder = self._builders.get(index)
        if builder is None:
            return pd.DataFrame()
        return builder.get_candles(interval_minutes, include_partial=include_partial)

    def get_ltp(self, index: str) -> Optional[float]:
        builder = self._builders.get(index)
        return builder.latest_ltp() if builder else None

    def add_tick_callback(self, fn: Callable):
        self._on_tick_callbacks.append(fn)

    def set_log_callback(self, fn: Callable):
        """Set a callback(level, msg) to surface REST errors in the engine log."""
        self._log_cb = fn

    def _log(self, level: str, msg: str):
        if self._log_cb:
            try:
                self._log_cb(level, msg)
            except Exception:
                pass
        print(f"[FYERS REST] [{level}] {msg}")

    def get_daily_closes(self, index: str, days: int = 30) -> pd.Series:
        try:
            return self._fetch_daily_closes_rest(index, days)
        except Exception:
            df = self.get_candles(index, 15)
            if df.empty:
                return pd.Series(dtype=float)
            return df["close"].reset_index(drop=True)

    def _fetch_daily_closes_rest(self, index: str, days: int) -> pd.Series:
        symbol  = FYERS_SYMBOLS[index]
        to_dt   = datetime.now(IST)
        from_dt = to_dt - timedelta(days=days + 10)
        headers = {"Authorization": f"{self.app_id}:{self.access_token}"}
        resp = requests.get(
            "https://api-t1.fyers.in/api/v3/history",
            params={
                "symbol":      symbol,
                "resolution":  "D",
                "date_format": "1",
                "range_from":  from_dt.strftime("%Y-%m-%d"),
                "range_to":    to_dt.strftime("%Y-%m-%d"),
                "cont_flag":   "1",
            },
            headers=headers,
            timeout=10,
        )
        data = resp.json()
        if data.get("s") != "ok":
            raise ValueError(data.get("message", "Fyers history error"))
        candles = data.get("candles", [])
        return pd.Series([c[4] for c in candles]).reset_index(drop=True)
