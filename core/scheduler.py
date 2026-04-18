"""
core/scheduler.py

Daily lifecycle manager:
  09:00 → re-login both brokers via TOTP
  09:15 → start algo engine
  15:30 → stop algo engine

Fixes vs previous version:
  - Login lock (_login_lock) prevents duplicate concurrent logins
  - trigger_login_now() marks today's login as done so the loop doesn't re-fire
  - Retry after failure waits 5 min then retries (max 3 attempts per day)
"""

import threading
import time as time_mod
from datetime import datetime, time
from typing import Callable, Optional
import pytz

IST = pytz.timezone("Asia/Kolkata")

LOGIN_TIME = time(9, 0)
START_TIME = time(9, 15)
STOP_TIME  = time(15, 30)
SLEEP_TICK = 30
MAX_LOGIN_RETRIES = 3


class DailyScheduler:

    def __init__(
        self,
        on_login_success: Callable,
        on_login_failure: Callable,
        on_log: Callable,
    ):
        self._on_login_success = on_login_success
        self._on_login_failure = on_login_failure
        self._on_log           = on_log

        self._thread: Optional[threading.Thread] = None
        self._stop_flag  = threading.Event()
        self._login_lock = threading.Lock()   # prevents duplicate concurrent logins

        self._last_login_day: Optional[int] = None
        self._last_start_day: Optional[int] = None
        self._last_stop_day:  Optional[int] = None
        self._login_retries:  int           = 0

        # Credentials
        self.fy_client_id  = ""
        self.fy_secret_key = ""
        self.fy_username   = ""
        self.fy_pin        = ""
        self.fy_totp_key   = ""
        self.zd_api_key    = ""
        self.zd_secret     = ""
        self.zd_user_id    = ""
        self.zd_password   = ""
        self.zd_totp_key   = ""

        self.engine = None
        self.fyers  = None
        self.broker = None

    # ── Public ────────────────────────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="DailyScheduler"
        )
        self._thread.start()
        self._log("INFO", "Daily scheduler started.")

    def stop(self):
        self._stop_flag.set()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def trigger_login_now(self):
        """
        Force immediate re-login (UI button / mid-day cold start).
        Marks today as already handled so the main loop doesn't fire again.
        Uses the lock so this and the loop can never run simultaneously.
        """
        doy = datetime.now(IST).timetuple().tm_yday
        self._last_login_day  = doy   # prevent loop from double-firing
        self._login_retries   = 0
        threading.Thread(
            target=self._do_login, daemon=True, name="ForceLogin"
        ).start()

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_flag.is_set():
            now = datetime.now(IST)
            doy = now.timetuple().tm_yday
            t   = now.time()

            # 09:00 — re-login (only if not already triggered via trigger_login_now)
            if t >= LOGIN_TIME and self._last_login_day != doy:
                self._last_login_day = doy
                self._login_retries  = 0
                self._do_login()

            # 09:15 — start engine
            if t >= START_TIME and self._last_start_day != doy:
                self._last_start_day = doy
                self._do_start_engine()

            # 15:30 — stop engine
            if t >= STOP_TIME and self._last_stop_day != doy:
                self._last_stop_day = doy
                self._do_stop_engine()

            time_mod.sleep(SLEEP_TICK)

    # ── Login ─────────────────────────────────────────────────────────────────

    def _do_login(self):
        # Lock ensures only one login attempt runs at a time
        if not self._login_lock.acquire(blocking=False):
            self._log("INFO", "Login already in progress — skipping duplicate call.")
            return

        try:
            self._log("INFO", "Daily re-login starting…")
            from .totp_login import FyersTOTPLogin, ZerodhaTOTPLogin, clear_all_caches
            from .fyers_feed import FyersFeed
            from .broker     import ZerodhaClient

            clear_all_caches()

            def _status(msg):
                self._log("INFO", msg)

            # ── Fyers ─────────────────────────────────────────────────────────
            fy = FyersTOTPLogin(
                client_id  = self.fy_client_id,
                secret_key = self.fy_secret_key,
                username   = self.fy_username,
                pin        = self.fy_pin,
                totp_key   = self.fy_totp_key,
            )
            fy_token = fy.get_access_token(force=True, status_cb=_status)

            fyers = FyersFeed(self.fy_client_id, self.fy_secret_key)
            fyers.set_access_token(fy_token)
            self.fyers = fyers

            # ── Zerodha ───────────────────────────────────────────────────────
            zd = ZerodhaTOTPLogin(
                api_key    = self.zd_api_key,
                api_secret = self.zd_secret,
                user_id    = self.zd_user_id,
                password   = self.zd_password,
                totp_key   = self.zd_totp_key,
            )
            zd_token = zd.get_access_token(force=True, status_cb=_status)

            broker = ZerodhaClient(self.zd_api_key, self.zd_secret)
            broker.set_access_token(zd_token)
            self.broker = broker

            self._login_retries = 0
            self.fy_connected = True
            self.zd_connected = True
            self.login_error  = ""
            self._log("INFO", "Both brokers connected successfully.")
            
            # Build engine here in scheduler
            from .engine import AlgoEngine
            engine = AlgoEngine(fyers, broker)
            engine.index      = "NIFTY"
            engine.lots       = 1
            engine.pe_offset  = 0
            engine.ce_offset  = 0
            engine.paper_mode = True
            self.engine = engine
            self._log("INFO", f"Engine created: {engine}")
            
            self._on_login_success(fyers, broker)

        except Exception as e:
            self._login_retries += 1
            self._log("ERROR", f"Login failed (attempt {self._login_retries}/{MAX_LOGIN_RETRIES}): {e}")
            self._on_login_failure(str(e))

            if self._login_retries < MAX_LOGIN_RETRIES:
                self._log("INFO", f"Retrying in 5 minutes…")
                self._login_lock.release()
                time_mod.sleep(300)
                self._do_login()
                return
            else:
                self._log("ERROR", "Max login retries reached. Will try again tomorrow at 09:00.")
                # Reset so tomorrow's schedule fires fresh
                self._last_login_day = None

        finally:
            if self._login_lock.locked():
                self._login_lock.release()

    # ── Engine start / stop ───────────────────────────────────────────────────

    def _do_start_engine(self):
        if self.engine is None:
            self._log("INFO", "Engine not ready at 09:15 — login may still be in progress.")
            return
        if not self.engine.running:
            self.engine.start()
            self._log("INFO", "Engine auto-started at 09:15.")

    def _do_stop_engine(self):
        if self.engine and self.engine.running:
            self.engine.stop()
            self._log("INFO", "Engine auto-stopped at 15:30.")

    # ── Log ───────────────────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        try:
            self._on_log(level, msg)
        except Exception:
            pass
