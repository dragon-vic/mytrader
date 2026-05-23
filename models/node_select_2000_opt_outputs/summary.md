# 2000ms Funding Node Selection

口径：t-500ms 开仓，t+2000ms 平仓成交，收益 = abs(funding) - 价格成本 - 10bps。

- 事件池：abs(funding) >= 30bps
- 训练集：2025-01-02 16:00:00+00:00 到 2025-12-02 22:00:00.001000+00:00
- 验证集：2025-12-02 22:00:00.001000+00:00 到 2026-01-01 00:00:00+00:00
- 测试集：2026-01-01 00:00:00+00:00 到 2026-05-23 16:00:00.003000+00:00
- 训练/验证/测试事件数：4077 / 720 / 3726

## Validation Best Single Model

| variant | loss | score | top_k | threshold | valid_trades | valid_win_pct | valid_sum_bps | test_trades | test_win_pct | test_avg_bps | test_sum_bps | test_capture_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| funding_hist | sq | score_net | 1 | 12.50 | 72 | 56.94 | 375.56 | 332 | 73.19 | 28.03 | 9304.84 | 29.90 |

## Baseline Comparison

| variant | loss | score | top_k | threshold | valid_trades | valid_win_pct | valid_sum_bps | test_trades | test_win_pct | test_avg_bps | test_sum_bps | test_capture_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| simple_max_rate | none | abs_rate_bps | 1 | 109.15 | 75 | 48.00 | -296.76 | 366 | 56.56 | 12.36 | 4524.94 | 14.54 |
| simple_max_rate | none | abs_rate_bps | 3 | 72.50 | 205 | 43.41 | -2189.28 | 1014 | 58.78 | 9.88 | 10021.02 | 21.40 |

## Top1 Validation-Selected Results

| variant | loss | score | top_k | threshold | valid_trades | valid_win_pct | valid_sum_bps | test_trades | test_win_pct | test_avg_bps | test_sum_bps | test_capture_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| funding_hist | sq | score_net | 1 | 12.50 | 72 | 56.94 | 375.56 | 332 | 73.19 | 28.03 | 9304.84 | 29.90 |
| funding_hist | sq | score_mul | 1 | 15.00 | 71 | 56.34 | 359.61 | 324 | 73.15 | 28.41 | 9204.95 | 29.58 |
| funding_hist | sq | score_adj10 | 1 | 14.58 | 71 | 56.34 | 359.61 | 323 | 72.76 | 28.12 | 9082.83 | 29.19 |
| funding_hist | sq | score_adj20 | 1 | 15.00 | 78 | 56.41 | 318.26 | 342 | 73.10 | 27.22 | 9308.06 | 29.91 |
| ranked_no_liq | sq | score_net | 1 | 15.00 | 69 | 62.32 | 187.13 | 287 | 72.47 | 31.24 | 8964.72 | 28.81 |
| pre_move | mae | score_adj20 | 1 | 12.20 | 90 | 56.67 | 148.89 | 407 | 72.73 | 26.19 | 10657.82 | 34.25 |
| pre_move | sq | score_mul | 1 | 11.22 | 93 | 56.99 | 117.30 | 370 | 72.43 | 26.22 | 9702.26 | 31.18 |
| pre_move | sq | score_adj10 | 1 | 11.53 | 93 | 56.99 | 117.30 | 366 | 72.40 | 26.43 | 9672.87 | 31.08 |
| pre_move | sq | score_adj20 | 1 | 15.53 | 81 | 56.79 | 102.67 | 320 | 73.75 | 28.94 | 9260.37 | 29.76 |
| pre_move | mae | score_net | 1 | 12.50 | 70 | 58.57 | 99.56 | 354 | 72.60 | 27.68 | 9798.38 | 31.49 |
| pre_move | sq | score_net | 1 | 9.52 | 93 | 56.99 | 83.04 | 381 | 71.65 | 26.00 | 9906.17 | 31.83 |
| thin_no_liq | mae | score_net | 1 | 12.50 | 69 | 56.52 | 58.75 | 426 | 67.14 | 22.84 | 9730.73 | 31.27 |

## Top3 Validation-Selected Results

| variant | loss | score | top_k | threshold | valid_trades | valid_win_pct | valid_sum_bps | test_trades | test_win_pct | test_avg_bps | test_sum_bps | test_capture_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pre_move | mae | score_adj10 | 3 | 5.00 | 206 | 50.49 | -109.34 | 1004 | 70.22 | 18.49 | 18561.56 | 39.64 |
| pre_move | sq | score_adj20 | 3 | 6.44 | 213 | 49.77 | -178.27 | 817 | 71.48 | 20.76 | 16964.73 | 36.23 |
| pre_move | sq | score_adj10 | 3 | 5.22 | 214 | 49.53 | -220.26 | 834 | 71.70 | 20.91 | 17442.58 | 37.25 |
| pre_move | mae | score_net | 3 | 3.44 | 211 | 50.24 | -226.08 | 1060 | 70.19 | 17.83 | 18895.27 | 40.35 |
| pre_move | sq | score_mul | 3 | 4.38 | 214 | 49.53 | -245.66 | 841 | 71.58 | 20.83 | 17517.14 | 37.41 |
| pre_move | mae | score_adj20 | 3 | 5.37 | 212 | 50.00 | -251.86 | 1023 | 70.67 | 18.58 | 19007.84 | 40.59 |
| pre_move | mae | score_mul | 3 | 3.77 | 211 | 49.76 | -255.20 | 1056 | 70.36 | 17.95 | 18955.17 | 40.48 |
| pre_move | sq | score_net | 3 | 3.99 | 214 | 50.00 | -289.88 | 849 | 71.02 | 20.49 | 17393.07 | 37.15 |
| market_pre | sq | score_adj10 | 3 | 6.53 | 210 | 49.05 | -291.47 | 939 | 71.46 | 19.55 | 18359.06 | 39.21 |
| market_hist | sq | score_mul | 3 | 5.44 | 208 | 47.12 | -294.33 | 956 | 67.47 | 16.96 | 16217.94 | 34.64 |
| market_hist | sq | score_net | 3 | 4.81 | 208 | 47.12 | -296.80 | 968 | 67.36 | 16.83 | 16295.13 | 34.80 |
| ranked_no_liq | sq | score_net | 3 | 4.00 | 211 | 50.24 | -311.28 | 894 | 68.68 | 18.62 | 16645.25 | 35.55 |

## Notes

- 表格按验证集收益排序；测试集只用于最后外样本对比。
- `simple_max_rate` 是每个节点按 funding 最大直接选币并只用验证集选择 rate 阈值。
- `saved_node_2000_low_api` 是已有落盘 XGB 模型在同一 2026 测试窗口上的表现。
- 当前实盘配置使用 walk-forward Top1 最优的 `pre_ensemble_avg_adj20`，不是单一静态验证集最优模型。
- top3 在 2026 外样本总收益更高，但月度回撤也更大；默认实盘先使用 top1。