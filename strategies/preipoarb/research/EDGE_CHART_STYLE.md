# PreIPO Edge 作图指导

这个文件记录 `preipoarb` 策略 edge 研究和 live 复盘图的默认规范。以后画 `preipoarb` 的 edge、quote、订单或回测图，先阅读本文件，再写作图代码。

## 数据

- 使用 `strategies/preipoarb/research/` 下的 bid/ask1 quote parquet。
- Binance 和 OKX quote 使用 timestamp 做 backward `merge_asof` 配对。
- 图上可以展示 quote age，但 live 策略不按 quote age 过滤信号。
- x 轴统一使用北京时间。
- 订单标记优先使用策略 snapshot 里的 `action_rows`，因为它有 signal edge、actual edge、方向和 qty。
- 如果 node 停止时产生强平订单，且 snapshot 没有对应 action，可以从 `orders.csv` 合并两腿后补一个 `STOP` 标记。
- 用 `orders.csv` 补 STOP 时，先按同标的、同方向、2 秒内是否已有 snapshot action 去重，避免同一次策略动作被重复标成 STOP。

## Edge 计算

- `binance_mid = (binance_bid + binance_ask) / 2`
- `long_edge = (okx_ask - binance_bid) / binance_mid * 10000`
- `short_edge = (okx_bid - binance_ask) / binance_mid * 10000`

## Edge 点、均线和信号线

- quote 级别 edge 画成点，不要连成线。
- `long_edge` 使用蓝色点。
- `short_edge` 使用橙色点。
- 均线必须画出来，不能只放在 legend 里：
  - 先把 quote edge resample 到 1 分钟。
  - 每分钟取均值。
  - 缺 quote 的分钟用上一分钟 forward fill。
  - 再计算 3h 时间加权均线。
  - 直接用这个分钟级均线序列画线，不要把它 exact merge 回 quote 时间，否则会因为时间戳不相等导致大量 NaN。
- `long 3h mean` 使用深蓝实线。
- `short 3h mean` 使用深橙实线。
- 信号线必须画出来：
  - Long signal: `long_mean - grid_band_bps`
  - Short signal: `short_mean + grid_band_bps`
- `long signal` 使用绿色虚线。
- `short signal` 使用粉色/紫红虚线。
- y 轴范围必须包含 edge 点、均线、信号线和所有订单标记，不能把信号线裁掉。

## 订单标记

- LONG 交易：绿色上三角。
- SHORT 交易：红色下三角。
- 不要使用黑色方块。
- 实心三角表示开仓或 flip 后留下的新方向。
- 空心三角表示平仓或 stop 强平。
- 标签只写实际交易方向、qty 和实际 edge，例如：
  - `LONG qty=0.02 295.2 bps`
  - `SHORT qty=0.01 340.8 bps`
- 全周期图不要在标签里写 `open` / `close` / `flip`，避免读图时混淆；动作细节放在配套 CSV 或文字说明里。
- 对比 live 交易时，每个标的单独出一张完整周期图。
- 全周期图要标注每个订单 edge 值；如果标签距离很近，用 leader line 和上下错位分开。
- 如果全周期图仍然看不清密集交易，另出局部时间窗口图。

## 布局

- 默认图尺寸使用 `15 x 7` 或 `15 x 7.5` inch，`150 dpi`。
- legend 默认放左上角，只有遮挡订单点时才移动。
- 使用浅色 grid。
- x 轴左右留 padding，避免边界订单标签被裁剪。
- 全周期图完成后必须目视检查：
  - edge 点是否是点，不是线。
  - 3h 均线是否连续可见。
  - long/short signal 线是否连续可见。
  - 所有订单标记和标签是否在图内。
  - 标签是否没有严重重叠。
  - y 轴是否没有把信号线或订单点裁掉。
