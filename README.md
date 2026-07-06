# DNSE Listener

Standalone Node.js daemon for persisting DNSE market data and order events into Supabase.

Subscribes to two WebSocket channels on a single connection:
- `ohlc_closed` — 1-minute OHLC bars → `m1_intraday_close`
- `order.STOCK.json` — real-time stock order events → `dnse_order_events`

## Files

- `src/index.js`: bootstrap, config, message routing, symbol refresh loop
- `src/ws.js`: DNSE WebSocket lifecycle — transport-only, single `onMessage` callback
- `src/symbols.js`: Supabase RPC fetch for active stock tickers
- `src/subscriptions.js`: desired symbol reconciliation
- `src/m1-ohlc-sink.js`: PostgREST upsert into `m1_intraday_close` (OHLC bars)
- `src/order-sink.js`: PostgREST insert into `dnse_order_events` (order events)
- `dnse-listener.service`: systemd unit for the VPS

## Environment

Copy `.env.example` to `/etc/dnse-listener.env` on the VPS and fill in:

```bash
DNSE_API_KEY=...
DNSE_API_SECRET=...
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=...
OHLC_RESOLUTION=1
REFRESH_MS=300000
HEARTBEAT_MS=25000
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Set permissions with:

```bash
sudo chown root:root /etc/dnse-listener.env
sudo chmod 600 /etc/dnse-listener.env
```

## Deploy

1. Copy this directory to `/opt/dnse-listener`.
2. Run `npm install --omit=dev`.
3. Copy `dnse-listener.service` to `/etc/systemd/system/dnse-listener.service`.
4. Run `sudo systemctl daemon-reload`.
5. Run `sudo systemctl enable --now dnse-listener`.
6. Inspect logs with `journalctl -u dnse-listener -f`.
