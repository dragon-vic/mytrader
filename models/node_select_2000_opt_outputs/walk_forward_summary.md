# 2000ms Walk Forward

每个月：用更早数据训练，用上个月验证集选阈值，用本月做测试。

## Top1

| variant | score | top_k | months | trades | sum_bps | avg_bps | win_month_pct | min_month_bps | median_month_bps | avg_win_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| xgb_pre_ensemble | ens_avg_adj20 | 1 | 11 | 1346 | 18932.29 | 14.07 | 81.82 | -2172.37 | 1317.80 | 62.99 |
| xgb_pre_ensemble | ens_avg_mul | 1 | 11 | 1372 | 18103.46 | 13.19 | 81.82 | -2100.36 | 1275.99 | 62.08 |
| xgb_pre_ensemble | ens_min_mul | 1 | 11 | 1328 | 18012.64 | 13.56 | 81.82 | -1927.60 | 1273.41 | 62.02 |
| xgb_ensemble | ens_min_mul | 1 | 11 | 1238 | 17690.34 | 14.29 | 81.82 | -2054.65 | 1239.33 | 61.38 |
| xgb_market_pre | score_adj20 | 1 | 11 | 1334 | 17537.37 | 13.15 | 81.82 | -2183.79 | 1249.57 | 62.11 |
| xgb_pre_ensemble_dual_veto | score_mul+market_mul | 1 | 11 | 1319 | 17486.20 | 13.26 | 81.82 | -2048.10 | 1246.58 | 61.80 |
| xgb_pre_move | score_adj20 | 1 | 11 | 1333 | 17441.34 | 13.08 | 81.82 | -2179.40 | 1205.44 | 61.81 |
| xgb_funding_hist | score_mul | 1 | 11 | 1260 | 17260.99 | 13.70 | 81.82 | -2283.78 | 1268.78 | 62.78 |
| xgb_market_pre | score_mul | 1 | 11 | 1306 | 17243.30 | 13.20 | 81.82 | -2052.46 | 1213.61 | 62.01 |
| xgb_pre_move | score_mul | 1 | 11 | 1397 | 17120.06 | 12.25 | 81.82 | -2286.38 | 1157.37 | 60.93 |
| xgb_hist_bucket_threshold | score_adj20 | 1 | 11 | 1378 | 16852.00 | 12.23 | 81.82 | -2356.12 | 1202.47 | 61.38 |
| xgb_dynamic_best | selected_by_valid | 1 | 11 | 1350 | 16778.75 | 12.43 | 81.82 | -2517.40 | 1157.37 | 61.39 |

## Top3

| variant | score | top_k | months | trades | sum_bps | avg_bps | win_month_pct | min_month_bps | median_month_bps | avg_win_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| xgb_pre_ensemble_dual_veto | score_mul+market_mul | 3 | 11 | 2607 | 29283.99 | 11.23 | 81.82 | -3457.21 | 1552.28 | 62.01 |
| xgb_pre_move | score_adj20 | 3 | 11 | 2888 | 29055.81 | 10.06 | 81.82 | -3556.08 | 1751.21 | 60.80 |
| xgb_pre_move | score_mul | 3 | 11 | 2878 | 28813.51 | 10.01 | 81.82 | -3722.69 | 1754.77 | 60.25 |
| xgb_pre_ensemble | ens_avg_mul | 3 | 11 | 2932 | 28531.37 | 9.73 | 81.82 | -3464.86 | 1671.13 | 60.54 |
| xgb_pre_ensemble | ens_avg_adj20 | 3 | 11 | 2882 | 28522.78 | 9.90 | 81.82 | -3576.94 | 1648.77 | 60.52 |
| xgb_market_pre | score_mul | 3 | 11 | 2793 | 28155.23 | 10.08 | 81.82 | -3478.77 | 1922.23 | 61.04 |
| xgb_market_pre | score_adj20 | 3 | 11 | 2789 | 28078.10 | 10.07 | 81.82 | -3610.34 | 1743.50 | 61.12 |
| xgb_pre_ensemble | ens_min_mul | 3 | 11 | 3093 | 27394.59 | 8.86 | 81.82 | -3460.62 | 1917.04 | 60.44 |
| xgb_base_threshold | score_mul | 3 | 11 | 2375 | 26780.75 | 11.28 | 90.91 | -3642.84 | 1509.16 | 61.90 |
| xgb_funding_hist | score_adj20 | 3 | 11 | 2783 | 26324.29 | 9.46 | 81.82 | -4121.66 | 1497.75 | 60.48 |
| xgb_funding_hist | score_mul | 3 | 11 | 2774 | 26217.63 | 9.45 | 81.82 | -3667.84 | 1513.38 | 60.53 |
| xgb_dynamic_best | selected_by_valid | 3 | 11 | 2662 | 26171.84 | 9.83 | 81.82 | -3974.69 | 1626.64 | 61.04 |
