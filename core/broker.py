"""
core/broker.py
Zerodha KiteConnect wrapper – auth, data, order management.
"""

import os
import webbrowser
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import pytz
import streamlit as st
from kiteconnect import KiteConnect, KiteTicker

IST = pytz.timezone("Asia/Kolkata")

# NSE/BSE tradingsymbol roots for option chain lookup
INDEX_NFO_ROOT = {
    "NIFTY":     "NIFTY",
    "BANKNIFTY": "BANKNIFTY",
}

EXCHANGE = {
    "NIFTY":     "NFO",
    "BANKNIFTY": "NFO",
}

LOT_SIZE = {
    "NIFTY":     25,
    "BANKNIFTY": 15,
}


class ZerodhaClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.kite: Optional[KiteConnect] = None
        self._instruments_cache: Optional[pd.DataFrame] = None
        self._instruments_date: Optional[datetime] = None

    # ── Auth ────────────────────────────────────────────────────────────────

    def login_url(self) -> str:
        kite = KiteConnect(api_key=self.api_key)
        return kite.login_url()

    def complete_login(self, request_token: str) -> str:
        """Exchange request_token for access_token. Returns access_token."""
        kite = KiteConnect(api_key=self.api_key)
        data = kite.generate_session(request_token, api_secret=self.api_secret)
        access_token = data["access_token"]
        kite.set_access_token(access_token)
        self.kite = kite
        return access_token

    def set_access_token(self, token: str):
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(token)

    @property
    def connected(self) -> bool:
        if self.kite is None:
            return False
        try:
            self.kite.profile()
            return True
        except Exception:
            return False

    # ── Instruments ─────────────────────────────────────────────────────────

    def _load_instruments(self, exchange="NFO"):
        today = datetime.now(IST).date()
        if (
            self._instruments_cache is not None
            and self._instruments_date == today
        ):
            return self._instruments_cache
        df = pd.DataFrame(self.kite.instruments(exchange))
        self._instruments_cache = df
        self._instruments_date  = today
        return df

    def get_expiries(self, index: str) -> list[datetime]:
        df   = self._load_instruments()
        root = INDEX_NFO_ROOT[index]
        sub  = df[(df["name"] == root) & (df["instrument_type"].isin(["CE","PE"]))]
        expiries = sorted(sub["expiry"].dropna().unique())
        return [e for e in expiries if e >= datetime.now(IST).date()]

    def get_option_symbol(
        self,
        index: str,
        expiry,             # datetime.date
        strike: int,
        opt_type: str,      # "CE" | "PE"
    ) -> Optional[str]:
        df   = self._load_instruments()
        root = INDEX_NFO_ROOT[index]
        mask = (
            (df["name"] == root) &
            (df["expiry"] == expiry) &
            (df["strike"] == strike) &
            (df["instrument_type"] == opt_type)
        )
        row = df[mask]
        if row.empty:
            return None
        return row.iloc[0]["tradingsymbol"]

    def get_instrument_token(self, symbol: str, exchange="NFO") -> Optional[int]:
        df   = self._load_instruments()
        row  = df[(df["tradingsymbol"] == symbol) & (df["exchange"] == exchange)]
        if row.empty:
            return None
        return int(row.iloc[0]["instrument_token"])

    # ── Live quote ───────────────────────────────────────────────────────────

    def get_ltp(self, exchange: str, tradingsymbol: str) -> Optional[float]:
        key  = f"{exchange}:{tradingsymbol}"
        data = self.kite.ltp([key])
        return data[key]["last_price"] if key in data else None

    def get_index_ltp(self, index: str) -> Optional[float]:
        mapping = {
            "NIFTY":     ("NSE", "NIFTY 50"),
            "BANKNIFTY": ("NSE", "NIFTY BANK"),
        }
        exch, sym = mapping[index]
        return self.get_ltp(exch, sym)

    # ── Historical candles ───────────────────────────────────────────────────

    def get_candles(
        self,
        index: str,
        interval: str = "15minute",
        days_back: int = 1,
    ) -> pd.DataFrame:
        """Fetch OHLC candles for the index from historical API."""
        mapping = {
            "NIFTY":     ("NSE", "NIFTY 50"),
            "BANKNIFTY": ("NSE", "NIFTY BANK"),
        }
        exch, sym = mapping[index]
        df_inst   = self._load_instruments("NSE")
        row = df_inst[df_inst["tradingsymbol"] == sym]
        if row.empty:
            return pd.DataFrame()
        token     = int(row.iloc[0]["instrument_token"])
        to_date   = datetime.now(IST)
        from_date = to_date - timedelta(days=days_back)
        records   = self.kite.historical_data(token, from_date, to_date, interval)
        df = pd.DataFrame(records)
        if df.empty:
            return df
        df.rename(columns={"date": "datetime"}, inplace=True)
        return df

    def get_daily_closes(self, index: str, days: int = 30) -> pd.Series:
        df = self.get_candles(index, interval="day", days_back=days)
        if df.empty:
            return pd.Series(dtype=float)
        return df["close"].reset_index(drop=True)

    # ── Orders ───────────────────────────────────────────────────────────────

    def sell_option(
        self,
        symbol: str,
        exchange: str,
        qty: int,
        order_type: str = "MARKET",
        price: float = 0.0,
    ) -> str:
        """
        Sell (write) an option — SELL transaction, product=NRML.
        Returns order_id.
        """
        params = dict(
            variety=KiteConnect.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=symbol,
            transaction_type=KiteConnect.TRANSACTION_TYPE_SELL,
            quantity=qty,
            product=KiteConnect.PRODUCT_NRML,
            order_type=(
                KiteConnect.ORDER_TYPE_MARKET if order_type == "MARKET"
                else KiteConnect.ORDER_TYPE_LIMIT
            ),
            price=price if order_type == "LIMIT" else None,
        )
        if params["price"] is None:
            del params["price"]
        resp = self.kite.place_order(**params)
        return resp["order_id"]

    def buy_option(
        self,
        symbol: str,
        exchange: str,
        qty: int,
        order_type: str = "MARKET",
        price: float = 0.0,
    ) -> str:
        """Buy back (cover) a sold option."""
        params = dict(
            variety=KiteConnect.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=symbol,
            transaction_type=KiteConnect.TRANSACTION_TYPE_BUY,
            quantity=qty,
            product=KiteConnect.PRODUCT_NRML,
            order_type=(
                KiteConnect.ORDER_TYPE_MARKET if order_type == "MARKET"
                else KiteConnect.ORDER_TYPE_LIMIT
            ),
            price=price if order_type == "LIMIT" else None,
        )
        if params["price"] is None:
            del params["price"]
        resp = self.kite.place_order(**params)
        return resp["order_id"]

    def get_order_status(self, order_id: str) -> dict:
        orders = self.kite.orders()
        for o in orders:
            if str(o["order_id"]) == str(order_id):
                return o
        return {}

    def get_positions(self) -> list[dict]:
        pos = self.kite.positions()
        return pos.get("net", [])
