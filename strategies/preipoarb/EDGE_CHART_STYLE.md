# Edge Chart Style

This note records the default chart standard for preipoarb edge research.

## Data

- Use bid/ask1 quote parquet from `strategies/preipoarb/research/`.
- Pair Binance and OKX quotes by timestamp with backward `merge_asof`.
- Research charts may use a wider quote tolerance for visual inspection, but backtest charts must use the strategy `max_quote_age_sec`.
- Use Beijing time on the x-axis.

## Edge Lines

- `long_edge = (okx_ask - binance_bid) / binance_mid * 10000`
- `short_edge = (okx_bid - binance_ask) / binance_mid * 10000`
- Draw quote-level edge as points, not connected lines.
- Draw `long_edge` in blue and `short_edge` in orange.
- Draw the time-weighted 3h mean for each side as a solid line.
- Draw signal lines:
  - Long signal: `long_mean - grid_band_bps`
  - Short signal: `short_mean + grid_band_bps`
- Use different colors for long and short means/signals.

## Trade Markers

- Long open: green upward triangle.
- Short open: red downward triangle.
- Close: black square.
- On dense local charts, annotate action, actual edge, and bps. On full-period charts, markers without text are preferred to keep the chart readable.

## Layout

- Prefer `15 x 7` or `15 x 7.5` inches at `150 dpi`.
- Keep the legend in the upper-left unless it covers trade points.
- Use light grid lines and avoid clipping signal lines.
- For a full-period chart, verify that all signal lines and trade markers are visible.
- For a local event chart, zoom to the relevant seconds/minutes and annotate the exact fills.
