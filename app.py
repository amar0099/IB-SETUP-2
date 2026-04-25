"""
app.py  –  Inside Bar Breakout Algo v7
White theme · Tabbed layout · Dedicated Log tab
Fully autonomous on Streamlit Cloud.

Every day:
  09:00 → auto re-login both brokers via TOTP
  09:15 → algo engine auto-starts
  15:30 → algo engine auto-stops
"""

import os
import time as _t
from datetime import datetime

import pandas as pd
import pytz
import streamlit as st

IST = pytz.timezone("Asia/Kolkata")

st.set_page_config(
    page_title="IB Algo",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* ── White base ── */
[data-testid="stAppViewContainer"],
[data-testid="stHeader"],
section[data-testid="stSidebar"] { background:#ffffff; }
[data-testid="block-container"]  { padding-top:1.2rem; }

/* ── Typography ── */
body, p, label, div { color:#1e293b; }

/* ── Section heading ── */
.sh {
    font-size:11px; font-weight:700; letter-spacing:.08em;
    text-transform:uppercase; color:#94a3b8;
    margin:1.4rem 0 .5rem; border-bottom:1px solid #f1f5f9;
    padding-bottom:4px;
}

/* ── Stat cards ── */
.scard {
    background:#f8fafc; border:1px solid #e2e8f0;
    border-radius:10px; padding:12px 16px;
}
.slabel { font-size:11px; color:#94a3b8; margin-bottom:3px; font-weight:600; letter-spacing:.04em; text-transform:uppercase; }
.sval   { font-size:20px; font-weight:700; color:#0f172a; }

/* ── Semantic colours ── */
.green  { color:#16a34a!important; }
.red    { color:#dc2626!important; }
.amber  { color:#d97706!important; }
.blue   { color:#2563eb!important; }
.muted  { color:#94a3b8!important; }

/* ── Signal badges ── */
.badge-long  { background:#dcfce7; color:#15803d; padding:3px 12px; border-radius:20px; font-size:12px; font-weight:700; }
.badge-short { background:#fee2e2; color:#b91c1c; padding:3px 12px; border-radius:20px; font-size:12px; font-weight:700; }
.badge-idle  { background:#f1f5f9; color:#64748b; padding:3px 12px; border-radius:20px; font-size:12px; }
.badge-watch { background:#eff6ff; color:#1d4ed8; padding:3px 12px; border-radius:20px; font-size:12px; }

/* ── Connection pills ── */
.pill-ok   { display:inline-block; background:#dcfce7; color:#15803d; border-radius:20px; padding:2px 12px; font-size:12px; font-weight:600; }
.pill-warn { display:inline-block; background:#fef9c3; color:#854d0e; border-radius:20px; padding:2px 12px; font-size:12px; font-weight:600; }
.pill-err  { display:inline-block; background:#fee2e2; color:#b91c1c; border-radius:20px; padding:2px 12px; font-size:12px; font-weight:600; }

/* ── Log table colours ── */
.log-INFO   { color:#64748b; }
.log-SETUP  { color:#6366f1; }
.log-SIGNAL { color:#0284c7; }
.log-ENTRY  { color:#16a34a; }
.log-EXIT   { color:#d97706; }
.log-ORDER  { color:#7c3aed; }
.log-RISK   { color:#dc2626; }
.log-ERROR  { color:#dc2626; }
.log-FILTER { color:#d97706; }

/* ── Cred form box ── */
.cred-box {
    background:#f8fafc; border:1px solid #e2e8f0;
    border-radius:10px; padding:1rem 1.25rem; margin-bottom:8px;
}

/* ── Tab styling ── */
[data-testid="stTabs"] button {
    font-size:13px; font-weight:600; color:#64748b;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color:#0f172a; border-bottom:2px solid #3b82f6;
}

/* ── Info banner ── */
.info-banner {
    background:#eff6ff; border:1px solid #bfdbfe;
    border-radius:8px; padding:10px 16px;
    font-size:13px; color:#1e40af; margin-bottom:12px;
}
</style>
""", unsafe_allow_html=True)

# ── Imports ───────────────────────────────────────────────────────────────────
try:
    from core.broker     import ZerodhaClient
    from core.fyers_feed import FyersFeed
    from core.engine     import AlgoEngine
    from core.totp_login import FyersTOTPLogin, ZerodhaTOTPLogin, clear_all_caches
    from core.scheduler  import DailyScheduler
except ImportError as e:
    st.error(f"Missing dependency: {e}\nRun: pip install -r requirements.txt")
    st.stop()

# ── Secret helper ─────────────────────────────────────────────────────────────
def _sec(key, fb=""):
    try:
        if key in st.secrets: return str(st.secrets[key])
    except Exception: pass
    return os.environ.get(key, fb)

# ── Session-state bootstrap ───────────────────────────────────────────────────
_DEFAULTS = {
    "fy_client_id":  _sec("FYERS_CLIENT_ID"),
    "fy_secret_key": _sec("FYERS_SECRET_KEY"),
    "fy_username":   _sec("FYERS_USERNAME"),
    "fy_pin":        _sec("FYERS_PIN"),
    "fy_totp_key":   _sec("FYERS_TOTP_KEY"),
    "zd_api_key":    _sec("ZERODHA_API_KEY"),
    "zd_secret":     _sec("ZERODHA_SECRET"),
    "zd_user_id":    _sec("ZERODHA_USER_ID"),
    "zd_password":   _sec("ZERODHA_PASSWORD"),
    "zd_totp_key":   _sec("ZERODHA_TOTP_KEY"),
    "algo_index":      "NIFTY",
    "algo_lots":       1,
    "algo_pe_offset":  0,
    "algo_ce_offset":  0,
    "algo_paper_mode": True,
    "algo_expiry":     None,
    "scheduler":     None,
    "engine":        None,
    "fyers":         None,
    "broker":        None,
    "fy_connected":  False,
    "zd_connected":  False,
    "sched_log":     [],
    "login_error":   "",
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Shared log ────────────────────────────────────────────────────────────────
def _append_log(level, msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    try:
        st.session_state.sched_log.append({"time": ts, "level": level, "msg": msg})
        if len(st.session_state.sched_log) > 500:
            st.session_state.sched_log = st.session_state.sched_log[-500:]
    except Exception:
        pass

def _on_login_success(fyers, broker):
    engine = AlgoEngine(fyers, broker)
    engine.index      = "NIFTY"
    engine.lots       = 1
    engine.pe_offset  = 0
    engine.ce_offset  = 0
    engine.paper_mode = True

    # Assign to scheduler - try both session_state and direct reference
    try:
        sched = st.session_state["scheduler"]
        sched.engine = engine
        sched.fyers  = fyers
        sched.broker = broker
    except Exception:
        # Background thread may not have session_state access
        # Scheduler will assign engine in _do_login after this returns
        pass

    # Also try session_state update (may fail from background thread)
    try:
        st.session_state["fyers"]        = fyers
        st.session_state["broker"]       = broker
        st.session_state["engine"]       = engine
        st.session_state["fy_connected"] = True
        st.session_state["zd_connected"] = True
        st.session_state["login_error"]  = ""
    except Exception:
        pass

def _on_login_failure(error):
    st.session_state.login_error  = error
    st.session_state.fy_connected = False
    st.session_state.zd_connected = False

# ── Scheduler bootstrap ───────────────────────────────────────────────────────
def _bootstrap():
    if st.session_state.scheduler is not None:
        return
    creds_ok = all([
        st.session_state.fy_client_id, st.session_state.fy_secret_key,
        st.session_state.fy_username,  st.session_state.fy_pin,
        st.session_state.fy_totp_key,
        st.session_state.zd_api_key,   st.session_state.zd_secret,
        st.session_state.zd_user_id,   st.session_state.zd_password,
        st.session_state.zd_totp_key,
    ])
    if not creds_ok:
        return

    sched = DailyScheduler(
        on_login_success=_on_login_success,
        on_login_failure=_on_login_failure,
        on_log=_append_log,
    )
    sched.fy_client_id  = st.session_state.fy_client_id
    sched.fy_secret_key = st.session_state.fy_secret_key
    sched.fy_username   = st.session_state.fy_username
    sched.fy_pin        = st.session_state.fy_pin
    sched.fy_totp_key   = st.session_state.fy_totp_key
    sched.zd_api_key    = st.session_state.zd_api_key
    sched.zd_secret     = st.session_state.zd_secret
    sched.zd_user_id    = st.session_state.zd_user_id
    sched.zd_password   = st.session_state.zd_password
    sched.zd_totp_key   = st.session_state.zd_totp_key
    st.session_state.scheduler = sched
  
    from core.scheduler import LOGIN_TIME, STOP_TIME
    now = datetime.now(IST)
    if LOGIN_TIME <= now.time() <= STOP_TIME:
        _append_log("INFO", "Mid-day start — triggering immediate login.")
        sched.trigger_login_now()

_bootstrap()

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("## 📈 Inside Bar Breakout Algo")
now_ist = datetime.now(IST)
st.caption(f"{now_ist.strftime('%d %b %Y  %H:%M:%S IST')}  ·  Auto-login 09:00  ·  Auto-start 09:15  ·  Auto-stop 15:30")

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS FORM (only if secrets not configured)
# ─────────────────────────────────────────────────────────────────────────────
creds_ok = all([
    st.session_state.fy_client_id, st.session_state.fy_secret_key,
    st.session_state.fy_username,  st.session_state.fy_pin,
    st.session_state.fy_totp_key,
    st.session_state.zd_api_key,   st.session_state.zd_secret,
    st.session_state.zd_user_id,   st.session_state.zd_password,
    st.session_state.zd_totp_key,
])

if not creds_ok:
    st.markdown("<div class='sh'>One-time credentials setup</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='info-banner'>Fill once. On Streamlit Cloud add to "
        "<b>App Settings → Secrets</b> and this form disappears permanently.</div>",
        unsafe_allow_html=True,
    )
    with st.form("creds"):
        st.markdown("<div class='cred-box'>", unsafe_allow_html=True)
        st.markdown("**Fyers**")
        fc1, fc2 = st.columns(2)
        st.session_state.fy_client_id  = fc1.text_input("Client ID",   value=st.session_state.fy_client_id,  placeholder="XXXX-100")
        st.session_state.fy_secret_key = fc2.text_input("Secret key",  value=st.session_state.fy_secret_key, placeholder="••••••••", type="password")
        fc3, fc4, fc5 = st.columns(3)
        st.session_state.fy_username   = fc3.text_input("Username",    value=st.session_state.fy_username,   placeholder="user@email.com")
        st.session_state.fy_pin        = fc4.text_input("PIN",         value=st.session_state.fy_pin,        placeholder="1234", type="password")
        st.session_state.fy_totp_key   = fc5.text_input("TOTP secret", value=st.session_state.fy_totp_key,  placeholder="BASE32…", type="password")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div class='cred-box' style='margin-top:8px'>", unsafe_allow_html=True)
        st.markdown("**Zerodha**")
        zc1, zc2 = st.columns(2)
        st.session_state.zd_api_key  = zc1.text_input("API key",    value=st.session_state.zd_api_key,  placeholder="api_key_xxx", type="password")
        st.session_state.zd_secret   = zc2.text_input("API secret", value=st.session_state.zd_secret,   placeholder="••••••••",   type="password")
        zc3, zc4, zc5 = st.columns(3)
        st.session_state.zd_user_id  = zc3.text_input("User ID",    value=st.session_state.zd_user_id,  placeholder="AB1234")
        st.session_state.zd_password = zc4.text_input("Password",   value=st.session_state.zd_password, placeholder="••••••••",   type="password")
        st.session_state.zd_totp_key = zc5.text_input("TOTP secret",value=st.session_state.zd_totp_key, placeholder="BASE32…",    type="password")
        st.markdown("</div>", unsafe_allow_html=True)

        if st.form_submit_button("Save & connect", width='stretch', type="primary"):
            _bootstrap()
            st.rerun()
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
tab_dash, tab_config, tab_log, tab_codelog = st.tabs(["Dashboard", "Config", "Log", "Code Log"])

sched = st.session_state.get("scheduler")
if not sched:
    with tab_dash:
        st.info("Scheduler initializing... Refresh page in a moment.")
    st.stop()

engine  = getattr(sched, 'engine', None)
fyers   = getattr(sched, 'fyers', None)
broker  = getattr(sched, 'broker', None)
fy_ok   = getattr(sched, 'fy_connected', False)
zd_ok   = getattr(sched, 'zd_connected', False)
ws_ok   = fyers.connected if fyers else False

# ═════════════════════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════
with tab_dash:

    # ── Connection strip ──────────────────────────────────────────────────────
    st.markdown("<div class='sh'>Connections</div>", unsafe_allow_html=True)
    cc1, cc2, cc3, cc4 = st.columns(4)

    def _pill(col, label, ok, ok_txt, fail_txt="Waiting"):
        cls = "pill-ok" if ok else "pill-warn"
        col.markdown(
            f"<div style='margin-bottom:4px;font-size:11px;color:#94a3b8;font-weight:600;text-transform:uppercase;letter-spacing:.04em'>{label}</div>"
            f"<span class='{cls}'>{'● ' + ok_txt if ok else '○ ' + fail_txt}</span>",
            unsafe_allow_html=True,
        )

    _pill(cc1, "Fyers",       fy_ok, "Connected")
    _pill(cc2, "Zerodha",     zd_ok, "Connected")
    _pill(cc3, "Live feed",   ws_ok, "Live", "Offline")
    _pill(cc4, "Scheduler",   sched is not None, "Running")

    login_err = getattr(sched, 'login_error', '') if sched else ''
    if login_err:
        st.error(f"Login error: {login_err}")

    # Manual buttons
    mb1, mb2, mb3, _ = st.columns([1, 1, 1, 3])
    with mb1:
        if st.button("Re-login now", width='stretch'):
            sched = st.session_state.get("scheduler")
            if sched:
                clear_all_caches()
                sched.trigger_login_now()
                st.toast("Login started (check Code Log in 10s)")
    with mb2:
        if st.button("Clear cache", width='stretch'):
            clear_all_caches()
            st.toast("Cache cleared.")
    with mb3:
        if st.button("↻ Refresh", width='stretch'):
            st.rerun()

    # ── Engine controls ───────────────────────────────────────────────────────
    st.markdown("<div class='sh'>Engine</div>", unsafe_allow_html=True)
    
    # DEBUG: Manual engine creation
    if not engine and broker and fyers:
        if st.button("⚠ Create engine manually", type="secondary"):
            from core.engine import AlgoEngine
            engine = AlgoEngine(fyers, broker)
            engine.index = "NIFTY"
            engine.lots = 1
            sched = st.session_state.get("scheduler")
            if sched:
                sched.engine = engine
            st.success("Engine created")
            st.rerun()

    eng_running = engine and engine.running
    b1, b2, b3, _ = st.columns([1, 1, 1.2, 3])

    with b1:
        if st.button("▶ Start", width='stretch',
                     disabled=(not engine) or eng_running or not st.session_state.algo_expiry):
            import logging
            logging.info(f"START BUTTON CLICKED - engine={engine}, expiry={st.session_state.algo_expiry}")
            engine.expiry     = st.session_state.algo_expiry
            engine.index      = st.session_state.algo_index
            engine.lots       = st.session_state.algo_lots
            engine.pe_offset  = st.session_state.algo_pe_offset
            engine.ce_offset  = st.session_state.algo_ce_offset
            engine.paper_mode = st.session_state.algo_paper_mode
            logging.info("About to call engine.start()")
            engine.start()
            logging.info("engine.start() completed")
            st.rerun()

    with b2:
        if st.button("⏹ Stop", width='stretch',
                     disabled=(not engine) or not eng_running):
            engine.stop()
            st.rerun()

    with b3:
        if st.button("↺ Apply config", width='stretch',
                     help="Push config changes to running engine"):
            if engine:
                engine.index      = st.session_state.algo_index

                # Also update broker connections if they were refreshed
                if fyers:
                    engine.fyers = fyers
                if broker:
                    engine.broker = broker

                engine.lots       = st.session_state.algo_lots
                engine.pe_offset  = st.session_state.algo_pe_offset
                engine.ce_offset  = st.session_state.algo_ce_offset
                engine.paper_mode = st.session_state.algo_paper_mode
                if st.session_state.algo_expiry:
                    engine.expiry = st.session_state.algo_expiry

                # Re-subscribe feed to the (possibly new) symbol
                if fyers:
                    try:
                        fyers.start_feed([engine.index])
                    except Exception as e:
                        st.warning(f"Feed re-subscribe failed: {e}")

                st.toast(f"Config applied — feed now tracking {engine.index}")

    eng_color = "green" if eng_running else "red"
    eng_txt   = "● Running" if eng_running else "○ Stopped"
    paper_tag = "  ·  Paper mode" if (engine and engine.paper_mode) else ""
    st.markdown(
        f"<div style='font-size:12px;color:#94a3b8;margin-top:4px'>"
        f"<span class='{eng_color}'>{eng_txt}</span>{paper_tag}</div>",
        unsafe_allow_html=True,
    )

    # ── Live status cards ─────────────────────────────────────────────────────
    st.markdown("<div class='sh'>Live status</div>", unsafe_allow_html=True)

    summary = engine.status_summary() if engine else {
        "signal": "—", "position": "—", "ltp": "—", "ema20": "—", "sl_hits": 0
    }

    sc1, sc2, sc3, sc4, sc5 = st.columns(5)

    def _stat(col, label, val, css=""):
        col.markdown(
            f"<div class='scard'><div class='slabel'>{label}</div>"
            f"<div class='sval {css}'>{val}</div></div>",
            unsafe_allow_html=True,
        )

    sig = summary["signal"]
    sig_html = (
        "<span class='badge-long'>LONG</span>"     if sig == "LONG"     else
        "<span class='badge-short'>SHORT</span>"   if sig == "SHORT"    else
        "<span class='badge-watch'>Watching</span>"if sig == "Watching" else
        "<span class='badge-idle'>Idle</span>"
    )
    sc1.markdown(
        f"<div class='scard'><div class='slabel'>Signal</div>"
        f"<div style='padding-top:6px'>{sig_html}</div></div>",
        unsafe_allow_html=True,
    )
    _stat(sc2, "Position",    summary["position"])
    _stat(sc3, "LTP (Fyers)", summary["ltp"],    "blue")
    _stat(sc4, "EMA20",       summary["ema20"],  "muted")
    sl = summary["sl_hits"]
    _stat(sc5, "SL hits today", f"{sl} / 2",
          "red" if sl >= 2 else ("amber" if sl == 1 else "green"))

    # ── Live candles ──────────────────────────────────────────────────────────
    st.markdown("<div class='sh'>Live 1-min candles (Fyers)</div>", unsafe_allow_html=True)

    if fyers:
        df_live = fyers.get_candles(st.session_state.algo_index, 1, include_partial=True)
        if not df_live.empty:
            show = df_live.tail(20).copy()
            show["datetime"] = show["datetime"].astype(str)
            for c in ["open","high","low","close"]:
                show[c] = show[c].round(2)
            st.dataframe(show[["datetime","open","high","low","close"]],
                         width='stretch', height=260, hide_index=True)
        else:
            st.caption("Waiting for tick data…")
    else:
        st.caption("Not connected yet. Auto-login at 09:00.")

# ═════════════════════════════════════════════════════════════════════════════
# TAB 2 — CONFIG
# ═════════════════════════════════════════════════════════════════════════════
with tab_config:

    st.markdown("<div class='sh'>Strategy</div>", unsafe_allow_html=True)
    cfg1, cfg2, cfg3, cfg4 = st.columns([1.4, 1.8, 1, 1])

    with cfg1:
        index = st.selectbox("Index", ["NIFTY","BANKNIFTY"],
                             index=["NIFTY","BANKNIFTY"].index(st.session_state.algo_index))
        st.session_state.algo_index = index

    with cfg2:
        if broker:
            try:
                expiries      = broker.get_expiries(index)
                expiry_labels = [str(e) for e in expiries]
                exp_sel       = st.selectbox("Expiry", expiry_labels)
                st.session_state.algo_expiry = expiries[expiry_labels.index(exp_sel)]
            except Exception:
                st.selectbox("Expiry", ["(fetch failed)"])
        else:
            st.selectbox("Expiry", ["(not connected)"])

    with cfg3:
        lots = st.number_input("Lots", min_value=1, max_value=50,
                               value=st.session_state.algo_lots)
        st.session_state.algo_lots = lots

    with cfg4:
        mode_idx = 1 if st.session_state.algo_paper_mode else 0
        mode = st.selectbox("Mode", ["Live","Paper"], index=mode_idx)
        st.session_state.algo_paper_mode = (mode == "Paper")

    st.markdown("<div class='sh'>Strike selection</div>", unsafe_allow_html=True)

    OFFSETS = list(range(-1000, 1001, 100))
    OL = {o: ("ATM" if o == 0 else f"ATM{'+' if o>0 else ''}{o}") for o in OFFSETS}

    sk1, sk2 = st.columns(2)
    with sk1:
        st.markdown("**Long signal → Sell PE at**")
        pe = st.select_slider("PE", options=OFFSETS,
                              value=st.session_state.algo_pe_offset,
                              format_func=lambda x: OL[x],
                              label_visibility="collapsed")
        st.session_state.algo_pe_offset = pe
        st.caption(f"Sells **{OL[pe]}** PE on Zerodha")

    with sk2:
        st.markdown("**Short signal → Sell CE at**")
        ce = st.select_slider("CE", options=OFFSETS,
                              value=st.session_state.algo_ce_offset,
                              format_func=lambda x: OL[x],
                              label_visibility="collapsed")
        st.session_state.algo_ce_offset = ce
        st.caption(f"Sells **{OL[ce]}** CE on Zerodha")

    st.markdown("<div class='sh'>Schedule</div>", unsafe_allow_html=True)
    sc1, sc2, sc3 = st.columns(3)
    sc1.info("**Auto-login:** 09:00 IST daily")
    sc2.info("**Auto-start:** 09:15 IST daily")
    sc3.info("**Auto-stop:** 15:30 IST daily")

    st.markdown("<div class='sh'>Debug</div>", unsafe_allow_html=True)
    with st.expander("Active setup / trade"):
        s = engine.active_setup if engine else None
        t = engine.active_trade if engine else None
        if s:
            st.json({"index": s.index, "mother_high": s.mother_high,
                     "mother_low": s.mother_low, "range_pts": s.range_pts,
                     "baby_close_time": str(s.baby_close_time)})
        if t:
            st.json({"direction": t.signal.direction, "entry": t.entry_price,
                     "sl": t.sl, "target": t.target, "risk": t.risk,
                     "symbol": t.option_symbol, "order_id": t.option_order_id})
        if not s and not t:
            st.caption("No active setup or trade.")

    with st.expander("Last 5 × 15-min candles"):
        if fyers:
            df15 = fyers.get_candles(st.session_state.algo_index, 15)
            if not df15.empty:
                st.dataframe(df15.tail(5).round(2), hide_index=True)
            else:
                st.caption("No data yet.")
        else:
            st.caption("Not connected.")

# ═════════════════════════════════════════════════════════════════════════════
# TAB 3 — LOG
# ═════════════════════════════════════════════════════════════════════════════
with tab_log:

    # Controls row
    lc1, lc2, lc3, _ = st.columns([1, 1, 1, 3])

    LOG_LEVELS = ["ALL", "SIGNAL", "ENTRY", "EXIT", "ORDER", "ERROR", "INFO"]
    level_filter = lc1.selectbox("Level", LOG_LEVELS, label_visibility="collapsed")
    search_term  = lc2.text_input("Search", placeholder="Search messages…",
                                  label_visibility="collapsed")
    if lc3.button("Clear log", width='stretch'):
        st.session_state.sched_log = []
        if engine:
            engine.log = []
        st.rerun()

    # Build unified log
    sched_log  = list(st.session_state.sched_log)
    engine_log = list(engine.log) if engine else []
    all_logs   = sorted(sched_log + engine_log,
                        key=lambda x: x["time"], reverse=True)[:500]

    # Apply filters
    if level_filter != "ALL":
        all_logs = [r for r in all_logs if r["level"] == level_filter]
    if search_term:
        all_logs = [r for r in all_logs if search_term.lower() in r["msg"].lower()]

    st.caption(f"{len(all_logs)} entries")

    LOG_COLORS = {
        "INFO":   "#94a3b8",
        "SETUP":  "#6366f1",
        "SIGNAL": "#0284c7",
        "ENTRY":  "#16a34a",
        "EXIT":   "#d97706",
        "ORDER":  "#7c3aed",
        "RISK":   "#dc2626",
        "FILTER": "#d97706",
        "ERROR":  "#dc2626",
    }

    if all_logs:
        df_log = pd.DataFrame(all_logs)[["time","level","msg"]]
        df_log.columns = ["Time","Level","Message"]

        def _style(row):
            c = LOG_COLORS.get(row["Level"], "#94a3b8")
            return [f"color:{c}; font-size:12px"] * len(row)

        st.dataframe(
            df_log.style.apply(_style, axis=1),
            width='stretch',
            height=520,
            hide_index=True,
        )
    else:
        st.caption("No log entries match the filter.")

# ═════════════════════════════════════════════════════════════════════════════
# TAB 4 — CODE LOG
# ═════════════════════════════════════════════════════════════════════════════
with tab_codelog:
    import pathlib, logging as _logging

    LOG_FILE = pathlib.Path("algo.log")

    # Setup Python file logger once per process
    if not st.session_state.get("_file_logger_ready"):
        _fl = _logging.getLogger("algo")
        _fl.setLevel(_logging.DEBUG)
        if not _fl.handlers:
            _fh = _logging.FileHandler(LOG_FILE, encoding="utf-8")
            _fh.setLevel(_logging.DEBUG)
            _fh.setFormatter(_logging.Formatter(
                "%(asctime)s  [%(levelname)-7s]  %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            _fl.addHandler(_fh)
            import sys as _sys
            def _exc_hook(et, ev, tb):
                if issubclass(et, KeyboardInterrupt):
                    _sys.__excepthook__(et, ev, tb); return
                _fl.critical("Unhandled exception", exc_info=(et, ev, tb))
            _sys.excepthook = _exc_hook
        st.session_state["_file_logger_ready"] = True

    # Mirror _append_log to file
    if not st.session_state.get("_log_file_patched"):
        _fl2 = _logging.getLogger("algo")
        _orig = _append_log
        def _patched(level, msg):
            _orig(level, msg)
            _fl2.log(getattr(_logging, level, _logging.INFO), msg)
        globals()["_append_log"] = _patched
        if sched:
            sched._on_log = _patched
        st.session_state["_log_file_patched"] = True

    # Controls
    cl1, cl2, cl3, cl4 = st.columns([1, 1.5, 1, 1])
    tail_lines  = cl1.number_input("Lines", min_value=50, max_value=5000,
                                   value=200, step=50, label_visibility="collapsed")
    search_code = cl2.text_input("Search", placeholder="filter...",
                                 label_visibility="collapsed", key="code_log_search")
    if cl3.button("Clear", width='stretch'):
        LOG_FILE.write_text("")
        st.toast("Log cleared.")
        st.rerun()
    if LOG_FILE.exists() and LOG_FILE.stat().st_size > 0:
        cl4.download_button("Download", data=LOG_FILE.read_bytes(),
                            file_name="algo.log", mime="text/plain",
                            width='stretch')

    # Display
    if LOG_FILE.exists() and LOG_FILE.stat().st_size > 0:
        raw = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        if search_code:
            raw = [l for l in raw if search_code.lower() in l.lower()]
        show = raw[-int(tail_lines):]
        st.caption(f"{len(raw)} total lines - showing last {len(show)}")
        st.code("\n".join(show), language="log")
    else:
        st.info("algo.log is empty. Errors and events will appear here once the engine runs.")

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh — after all tabs
# ─────────────────────────────────────────────────────────────────────────────
if engine and engine.running:
    _t.sleep(0.5)
    st.rerun()
