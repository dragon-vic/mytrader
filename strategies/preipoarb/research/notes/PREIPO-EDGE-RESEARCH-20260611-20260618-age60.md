# PREIPO tick-implied edge research

- window: 2026-06-11 00:00:00+00:00 to 2026-06-18 11:10:37+00:00
- edge definition: direct ratio of latest Binance and OKX tick prices; each exchange price forward-filled up to 60s
- costs in edge: Binance 5.0bps + OKX 5.0bps + slippage 2.0bps per leg
- tick-price rows: 2,580,152
- edge rows: 2,065,716

## Edge distribution

                                   count        mean         std         min          5%         25%         50%         75%         90%         95%         max
asset     direction                                                                                                                                             
ANTHROPIC buy_binance_sell_okx  505247.0  257.110422  111.349079  -12.165344  144.891242  168.839746  198.860684  379.485531  425.935913  466.257997  499.615994
          buy_okx_sell_binance  505247.0 -276.818570  104.769356 -502.524590 -472.250166 -392.588617 -222.424153 -193.556736 -177.641669 -170.406087  -15.834320
OPENAI    buy_binance_sell_okx  527611.0  322.219219   50.750257  178.826343  260.199897  287.527958  311.885978  340.790419  405.744059  439.528052  486.706001
          buy_okx_sell_binance  527611.0 -339.050562   47.258199 -490.830797 -447.851662 -356.634090 -329.600985 -306.702170 -288.424731 -280.881995 -203.178484

## Volatility regimes

                                median_range_30m  p90_range_30m  median_abs_drift_6h  p90_abs_drift_6h
asset     direction                                                                                   
ANTHROPIC buy_binance_sell_okx         15.913173      30.445842             9.190655         27.915052
          buy_okx_sell_binance         15.007078      28.439926             8.749651         26.383844
OPENAI    buy_binance_sell_okx         14.012601      25.974099             8.442721         26.712067
          buy_okx_sell_binance         13.109555      24.317312             7.882894         25.032322