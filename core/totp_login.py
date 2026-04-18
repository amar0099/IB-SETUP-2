"""
core/totp_login.py
Headless TOTP login for Fyers and Zerodha.
"""

import base64
import hashlib
import os
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import pyotp
import requests
import streamlit as st
from fyers_apiv3 import fyersModel

TOKEN_DIR = Path(".tokens")
TOKEN_DIR.mkdir(exist_ok=True)


def _get_secret(key: str, fallback: str = "") -> str:
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.environ.get(key, fallback)


def _b64(value: str) -> str:
    return base64.b64encode(str(value).encode()).decode()


class _TokenCache:
    def __init__(self, name: str):
        self._file = TOKEN_DIR / f"{name}.txt"
        self._date = TOKEN_DIR / f"{name}_date.txt"

    def load(self) -> Optional[str]:
        try:
            if self._date.read_text().strip() == date.today().isoformat():
                t = self._file.read_text().strip()
                return t if t else None
        except Exception:
            pass
        return None

    def save(self, token: str):
        self._file.write_text(token.strip())
        self._date.write_text(date.today().isoformat())

    def clear(self):
        for f in (self._file, self._date):
            if f.exists():
                f.unlink()


# ══════════════════════════════════════════════════════════════════════════════
# FYERS — 4-step headless login
# Step 4 returns access_token directly in data.auth — no Step 5 needed
# ══════════════════════════════════════════════════════════════════════════════

def _fyers_login(
    client_id:  str,
    secret_key: str,
    username:   str,
    pin:        str,
    totp_key:   str,
    status_cb=None,
) -> tuple[Optional[str], Optional[str]]:

    def _s(msg):
        if status_cb: status_cb(msg)

    redirect_uri = "https://ib-setup.streamlit.app/"

    try:
        sess = requests.Session()

        # Clear any stale session
        try:
            sess.get("https://api-t2.fyers.in/vagator/v2/logout", timeout=5)
        except Exception:
            pass

        # Step 1 — send OTP
        _s("Fyers 1/4 — sending login OTP…")
        r1 = sess.post(
            "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2",
            json={"fy_id": _b64(username), "app_id": "2"}, timeout=10,
        )
        if r1.status_code == 429:
            return None, "Fyers rate-limited (429). Wait ~60s and retry."
        r1d = r1.json()
        if r1d.get("s") != "ok":
            return None, f"Fyers step 1 failed: {r1d}"

        # Step 2 — verify TOTP
        _s("Fyers 2/4 — verifying TOTP…")
        r2 = sess.post(
            "https://api-t2.fyers.in/vagator/v2/verify_otp",
            json={"request_key": r1d["request_key"], "otp": pyotp.TOTP(totp_key).now()},
            timeout=10,
        )
        r2d = r2.json()
        if r2d.get("s") != "ok":
            return None, f"Fyers step 2 failed: {r2d}"

        # Step 3 — verify PIN
        _s("Fyers 3/4 — verifying PIN…")
        r3 = sess.post(
            "https://api-t2.fyers.in/vagator/v2/verify_pin_v2",
            json={"request_key": r2d["request_key"],
                  "identity_type": "pin", "identifier": _b64(pin)},
            timeout=10,
        )
        r3d = r3.json()
        if r3d.get("s") != "ok":
            return None, f"Fyers step 3 failed: {r3d}"

        # Step 4 — get access token
        # appType = suffix of client_id (e.g. "100" from "XXXX-100")
        _s("Fyers 4/4 — fetching access token…")
        app_id   = client_id.split("-")[0]
        app_type = client_id.split("-")[-1]

        r4 = sess.post(
            "https://api-t1.fyers.in/api/v3/token",
            json={
                "fyers_id":       username,
                "app_id":         app_id,
                "redirect_uri":   redirect_uri,
                "appType":        app_type,
                "code_challenge": "",
                "state":          "algo",
                "scope":          "",
                "nonce":          "",
                "response_type":  "code",
                "create_cookie":  True,
            },
            headers={"Authorization": f"Bearer {r3d['data']['access_token']}"},
            timeout=10,
        )
        r4d   = r4.json()
        if r4d.get("s") != "ok":
            return None, f"Fyers step 4 failed: {r4d}"

        # API returns access_token directly in data.auth
        token = r4d.get("data", {}).get("auth")
        if not token:
            return None, f"Fyers step 4: no token in response: {r4d}"

        _s("Fyers login complete.")
        return token, None

    except Exception as e:
        return None, f"Fyers exception: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# ZERODHA — 4-step headless login
# ══════════════════════════════════════════════════════════════════════════════

def _zerodha_login(
    api_key:    str,
    api_secret: str,
    user_id:    str,
    password:   str,
    totp_key:   str,
    status_cb=None,
) -> tuple[Optional[str], Optional[str]]:

    def _s(msg):
        if status_cb: status_cb(msg)

    try:
        sess = requests.Session()
        sess.headers.update({"X-Kite-Version": "3"})

        # Step 1 — password login
        _s("Zerodha 1/4 — logging in…")
        r1 = sess.post(
            "https://kite.zerodha.com/api/login",
            data={"user_id": user_id, "password": password},
            timeout=10,
        )
        r1d = r1.json()
        if r1d.get("status") != "success":
            return None, f"Zerodha step 1 failed: {r1d.get('message', r1d)}"

        # Step 2 — TOTP
        _s("Zerodha 2/4 — verifying TOTP…")
        r2 = sess.post(
            "https://kite.zerodha.com/api/twofa",
            data={
                "user_id":     user_id,
                "request_id":  r1d["data"]["request_id"],
                "twofa_value": pyotp.TOTP(totp_key).now(),
                "twofa_type":  "totp",
                "skip_session": "",
            },
            timeout=10,
        )
        r2d = r2.json()
        if r2d.get("status") != "success":
            return None, f"Zerodha step 2 failed: {r2d.get('message', r2d)}"

        # Step 3 — extract enctoken from cookies (set by Step 2 TOTP)
        _s("Zerodha 3/3 — extracting enctoken…")
        enctoken = sess.cookies.get("enctoken")
        if not enctoken:
            return None, "Zerodha step 3: enctoken cookie not set after TOTP login."

        # enctoken IS the access token for Kite Web API
        token = enctoken
        _s("Zerodha login complete (using enctoken).")
        return token, None


    except Exception as e:
        return None, f"Zerodha exception: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# HIGH-LEVEL WRAPPERS
# ══════════════════════════════════════════════════════════════════════════════

class FyersTOTPLogin:
    _cache = _TokenCache("fyers")

    def __init__(self, client_id="", secret_key="", username="", pin="", totp_key=""):
        self.client_id  = client_id  or _get_secret("FYERS_CLIENT_ID")
        self.secret_key = secret_key or _get_secret("FYERS_SECRET_KEY")
        self.username   = username   or _get_secret("FYERS_USERNAME")
        self.pin        = pin        or _get_secret("FYERS_PIN")
        self.totp_key   = totp_key   or _get_secret("FYERS_TOTP_KEY")

    @property
    def credentials_complete(self) -> bool:
        return all([self.client_id, self.secret_key,
                    self.username, self.pin, self.totp_key])

    def get_access_token(self, force: bool = False, status_cb=None) -> str:
        if not force:
            cached = self._cache.load()
            if cached:
                if status_cb: status_cb("Fyers: reusing today's cached token.")
                return cached
        token, err = _fyers_login(
            self.client_id, self.secret_key,
            self.username, self.pin, self.totp_key,
            status_cb=status_cb,
        )
        if not token:
            raise RuntimeError(err)
        self._cache.save(token)
        return token

    def get_fyers_model(self, **kw) -> fyersModel.FyersModel:
        token = self.get_access_token(**kw)
        return fyersModel.FyersModel(
            client_id=self.client_id, token=token, log_path=""
        )

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


class ZerodhaTOTPLogin:
    _cache = _TokenCache("zerodha")

    def __init__(self, api_key="", api_secret="", user_id="", password="", totp_key=""):
        self.api_key    = api_key    or _get_secret("ZERODHA_API_KEY")
        self.api_secret = api_secret or _get_secret("ZERODHA_SECRET")
        self.user_id    = user_id    or _get_secret("ZERODHA_USER_ID")
        self.password   = password   or _get_secret("ZERODHA_PASSWORD")
        self.totp_key   = totp_key   or _get_secret("ZERODHA_TOTP_KEY")

    @property
    def credentials_complete(self) -> bool:
        return all([self.api_key, self.api_secret,
                    self.user_id, self.password, self.totp_key])

    def get_access_token(self, force: bool = False, status_cb=None) -> str:
        if not force:
            cached = self._cache.load()
            if cached:
                if status_cb: status_cb("Zerodha: reusing today's cached token.")
                return cached
        token, err = _zerodha_login(
            self.api_key, self.api_secret,
            self.user_id, self.password, self.totp_key,
            status_cb=status_cb,
        )
        if not token:
            raise RuntimeError(err)
        self._cache.save(token)
        return token

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


def clear_all_caches():
    FyersTOTPLogin.clear_cache()
    ZerodhaTOTPLogin.clear_cache()
