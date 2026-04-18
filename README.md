# Inside Bar Breakout Algo v2
**Data: Fyers WebSocket  |  Orders: Zerodha KiteConnect**

## Architecture

```
Fyers WebSocket feed
  └── CandleBuilder (in-memory)
        ├── 1-min candles  ──► breakout signal check (every 10s)
        └── 15-min candles ──► inside bar setup detection
                                       |
                                       v
                             AlgoEngine (background thread)
                                       |
                                       v
                             Zerodha KiteConnect
                               SELL option on entry
                               BUY (cover) on exit
```

## Setup

```bash
pip install -r requirements.txt
streamlit run app.py
```

## API credentials

### Fyers
1. Go to https://myapi.fyers.in -> Create App
2. Set Redirect URI to http://127.0.0.1:8501
3. Note App ID (format: XXXX-100) and Secret key

### Zerodha
1. Go to https://developers.kite.trade -> Create App
2. Set Redirect URL to http://127.0.0.1:8501
3. Note API key and API secret

## Daily login (both tokens expire at midnight)

### Fyers
1. Paste App ID + Secret in the panel
2. Click "Open Fyers login" -> complete login
3. Copy the auth_code from the redirect URL
4. Paste -> "Connect Fyers"

### Zerodha
1. Paste API key + Secret
2. Click "Open Zerodha login" -> complete login
3. Copy the request_token from the redirect URL
4. Paste -> "Connect Zerodha"

## Signal to order mapping

| Index signal   | Action on Zerodha                          |
|----------------|--------------------------------------------|
| LONG breakout  | SELL ATM PE (or offset strike) NRML MARKET |
| SHORT breakout | SELL ATM CE (or offset strike) NRML MARKET |
| SL/Target/Time | BUY back the option NRML MARKET            |

## Strike offset

ATM +/- 100 to 1000 pts in 100-pt steps.
Independent offsets for PE (long signal) and CE (short signal).

## Files

```
ib_algo_v2/
├── app.py
├── requirements.txt
└── core/
    ├── strategy.py     inside-bar detection, EMA, SL/target
    ├── broker.py       Zerodha KiteConnect (orders only)
    ├── fyers_feed.py   Fyers OAuth + WebSocket + candle builder
    └── engine.py       algo loop
```

## Paper mode

Select Paper before starting. Identical logic, zero real orders sent.

Risk warning: This software places real orders. Paper-test first.
Always responsible for your own losses.
