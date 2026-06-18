# PREIPO edge research notes

Window: 2026-06-11 00:00 UTC to 2026-06-18 11:10 UTC.

Data lives under `strategies/preipoarb/research/`. Raw exchange zip files, tick parquet, edge signal parquet, plots, and notes are strategy-local.

## Edge definition

Historical data does not include bid/ask1, so this run uses direct latest tick price ratio:

- `buy_binance_sell_okx = (okx_tick_price - binance_tick_price) / binance_tick_price * 10000 - costs`
- `buy_okx_sell_binance = (binance_tick_price - okx_tick_price) / okx_tick_price * 10000 - costs`
- Each exchange tick price is forward-filled for up to 60 seconds.
- Costs: Binance 5bps + OKX 5bps + slippage 2bps per leg.

This is not order-book replay. It is suitable for regime design and rough backtesting, not exact execution-quality measurement.

## Data shape

60s freshness window:

- ANTHROPIC buy Binance / sell OKX coverage: 78.3% of seconds.
- OPENAI buy Binance / sell OKX coverage: 81.8% of seconds.
- 120s gives more coverage, but adds staler prices; 60s is the current research default.

Edge is not stationary:

- OPENAI median edge is around 312bps, and drifts upward into the 400bps area.
- ANTHROPIC median edge is around 199bps over the full window, but it jumps into a 350-500bps regime after June 15.
- 30m volatility is much smaller than the level drift: median 30m 90-10 range is around 14-16bps, p90 around 26-30bps.

Conclusion: z-score against a long rolling mean/std is fragile. It mixes two different things:

- long-term venue basis drift, which can move from 200bps to 400bps and stay there;
- local jump volatility, which often reverts within minutes to hours.

## Candidate system

Treat edge as a price series and trade local jumps above a short baseline:

1. Build per-asset, per-direction edge from latest tick price ratio.
2. Use `30m median` as local fair value.
3. Use `30m q90-q10 range` as local volatility range, not std.
4. Enter buy Binance / sell OKX when:
   - `edge - median_30m >= max(20bps, 1.5 * range_30m)`
   - `range_30m >= 12bps`
   - edge is positive after costs.
5. Exit when:
   - edge gives back 75% of the entry jump, i.e. `edge <= entry_base + 0.25 * (entry_edge - entry_base)`;
   - or max hold reaches 2h.
6. Cooldown after close: 5m.

This avoids using long-window std and adapts to changing volatility regimes. It does not require the long-term edge mean to be stable.

## Rough backtest

Approximation: PnL per 100 USDT notional is `(entry_edge - close_edge) / 10000 * 100`.

Balanced candidate result:

- trades: 42
- total pnl per 100 USDT leg notional: 10.67 USDT
- win rate: 97.6%
- average hold: about 25 minutes
- no end-open positions in this run

Breakdown:

```text
asset      reason  trades       pnl  win_rate  avg_hold_sec
ANTHROPIC  revert      18  4.171378  100.00%       1348.17
ANTHROPIC  time         3  0.032498   66.67%       7200.00
OPENAI     revert      21  6.466660  100.00%        832.19
```

## Artifacts

- Edge data: `strategies/preipoarb/research/data/signal/PREIPO-BINANCE-OKX-EDGE-APPROX-20260611-20260618-age60.parquet`
- 5m edge bars: `strategies/preipoarb/research/data/signal/PREIPO-BINANCE-OKX-EDGE-5M-20260611-20260618-age60.parquet`
- Candidate trades: `strategies/preipoarb/research/data/signal/PREIPO-LOCAL-JUMP-BALANCED-TRADES-20260611-20260618-age60.parquet`
- Window coverage: `strategies/preipoarb/research/data/signal/PREIPO-EDGE-WINDOW-COVERAGE-20260611-20260618.parquet`
- Main plots:
  - `strategies/preipoarb/research/plots/PREIPO-OPENAI-LOCAL-JUMP-BALANCED-20260611-20260618-age60.png`
  - `strategies/preipoarb/research/plots/PREIPO-ANTHROPIC-LOCAL-JUMP-BALANCED-20260611-20260618-age60.png`
  - `strategies/preipoarb/research/plots/PREIPO-EDGE-DISTRIBUTION-20260611-20260618-age60.png`

## Next iteration

Before changing live strategy code:

- Run the same local-jump rule on a second window when more days are available.
- Compare 60s vs 120s freshness on trade timing and false entries.
- Test a partial-scale-in version: add another lot only if the new jump is above the current entry jump by a separate local range threshold.
- Add live safeguards around stale ticks and execution spread because this research uses tick price ratio, not actual bid/ask1.
