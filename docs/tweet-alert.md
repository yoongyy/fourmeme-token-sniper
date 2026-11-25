Tweet Alert
===========

Trigger condition
- TokenManager createToken tx: `tx.value` converted to USD is greater than `MIN_LIQUIDITY_TWEET`.

Matching flow
- Local substring match (Unicode-aware) against token name/symbol in tweet text or translation.
- Basic tokenization match (Unicode-aware) as fallback.
- Gemini LLM check if enabled; prompt instructs “yes” on exact substring matches.
- If any step matches, a Tweet Alert is queued (and optional tweet trade follows settings).

Notes
- USD conversion uses the current BNB/USD price; if price lookup fails, the alert is skipped.
- Threshold source: `MIN_LIQUIDITY_TWEET` environment variable in `.env`.
- `TWEET_MATCH_DISABLE=1` bypasses matching and returns the first tweet (or requires Gemini when always-query is set).
