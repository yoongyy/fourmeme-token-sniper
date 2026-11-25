Liquidity Alert
===============

Trigger conditions
- TokenManager createToken tx: `tx.value` converted to USD is greater than `MIN_LIQUIDITY`.
- TokenManager buyTokenAMAP tx: `tx.value` converted to USD is greater than `MIN_LIQUIDITY`.
- addLiquidity tx or Pancake pair creation: detected liquidity in USD is greater than `MIN_LIQUIDITY`.

Notes
- USD conversion uses the current BNB/USD price; if price lookup fails, the alert is skipped.
- Deduplication: once an alert is sent for a token, further tx-based alerts are suppressed.
- Threshold source: `MIN_LIQUIDITY` environment variable in `.env`.
