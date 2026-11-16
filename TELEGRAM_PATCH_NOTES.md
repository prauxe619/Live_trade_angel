# Unified two-destination Telegram routing patch

## Scope
This incremental patch consolidates the four Telegram sender paths reviewed so far:
- `daily_signal_engine_v11.py`
- `revenue_agent/telegram_vip_alert.py`
- `trading_live/live_bridge.py`
- `scripts/telegram_alert.py`
- `tests/test_two_channel_telegram_routing.py`

## Routing contract
- Bot token: `SIGNAL_BOT_TOKEN` only.
- Destination 1: `CHAT_ID` (personal Telegram chat).
- Destination 2: `LIVE_BOT_CHANNEL_ID` (existing Telegram channel).
- Destinations are deduplicated; exactly two distinct destinations are required.
- Legacy `TRADING_TELEGRAM_CHAT_ID`, `VIP_CHANNEL_ID`, `LIVE_BOT_TOKEN`, and `BOT_TOKEN`
  are not used as delivery credentials or destinations in these sender paths.
- The engine, collector VIP helper, live execution bridge and standalone alert
  script each fan out messages to both canonical destinations.
- A failure delivering to one destination does not prevent an attempt to the other.
- Free/promotional channel posting remains disabled.

## Safety
- No `.env` or systemd environment file is included or modified.
- No broker credentials, Telegram tokens, chat IDs or database URLs are included.
- No strategy thresholds, order logic, trading mode, or paper safeguards are changed.
- Keep `TRADING_MODE=paper`, `PAPER_TRADING=1`, `TRADING_ENABLED=false`,
  `ALLOW_ORDER_SUBMISSION=false`, `LIVE_SHADOW=true`,
  `PAPER_LEDGER_ENABLED=true`.

## Verification and limitation
- The included routing tests check the canonical environment variables and reject
  legacy single-destination credential fallbacks.
- These are the four sender files reviewed in this patch. Before claiming repository-wide
  coverage, search the EC2 repository for any additional direct Telegram API calls,
  `sendMessage` URLs, `send_telegram`, `send_alert`, or bot-token usage.

## Deployment
Back up the existing files, extract this ZIP into the repository root, run the
included tests and syntax checks, then perform a Telegram delivery test to both
destinations before restarting the supervisor. This patch itself does not send
Telegram messages, place orders, or restart services.
