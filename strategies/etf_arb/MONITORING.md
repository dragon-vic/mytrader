# ETF策略监控入口

AWS项目：`/home/ubuntu/pycharm_nt`。此说明供用户和已经具备服务器访问能力的助手使用。

## 查询

```text
/home/ubuntu/miniconda/envs/nt/bin/python /home/ubuntu/pycharm_nt/strategies/etf_arb/monitor.py
```

命令独立运行，读取策略日志与JSON检查点；不导入策略执行组件，不读取密钥，不连接交易所账户，不下单，不修改持仓或策略参数。输出中文摘要，并原子更新以下文件：

- `/home/ubuntu/pycharm_nt/strategies/etf_arb/monitor/status.json`：结构化监控数据，适合GPT读取。
- `/home/ubuntu/pycharm_nt/strategies/etf_arb/monitor/status.md`：人可读摘要。

每次汇报先执行命令刷新，固定文件是上次查询快照。只读取文件时，先检查`generated_at`和`data_asof`。

## 汇报顺序

1. 数据时间、运行状态、是否有异常或心跳过期。
2. 七组合当前可成交偏离、今日采样峰值和对应北京时间，报价是否新鲜。
3. 当前阶段、双腿数量、待处理订单、暂停组合；今日开闭阶段和成交回报数量。
4. 行情延迟、采集是否存活/丢数据、最近节点日志错误数量。
5. 有异常时指出具体组合、时间和事件。用户仅要求查询时保持查询范围。

## 数据口径

- `status=running`表示最新实例有90秒以内的健康心跳且基线已就绪；`stopped`表示日志记录正常停止，`heartbeat_stale`表示心跳超过90秒。
- 偏离是策略基线修正后的双向bid/ask开仓偏离较大值。负数表示盘口价差已经覆盖偏离，不能据此判断ETF涨跌方向。
- `daily_peak`使用30秒心跳中双腿报价新鲜的样本；记录覆盖见`today.coverage`，停机期间没有数据。秒内精确峰值需逐报价累计指标；当前摘要不会把采样峰值标为全量峰值。
- `stage`和`pending_orders`来自策略检查点；`available_usdt`来自最近健康心跳。它们用于策略监控；需要核对交易所真实仓位和挂单时，另行调用已有只读账户接口。
- `order_batches_sent`是双腿发单动作数，`fill_events`是成交回报条数，一个订单可以有多条成交回报。手续费按原币种列出。
- 盈亏字段当前为空并说明来源需求；不要把账户余额变化或订单名义额当成策略净利润。
- `recent_events`包含退出请求和异常事件，正常计划退出也会出现在这里；结合reason判断。
- 每个组合的`baseline`包含原始/有效收盘锚、锚调整幅度、分段修正、正常组合基差、实际收盘参考校验状态和更新时间。`contract_robust`表示使用合约稳健锚，`cash_close_checked`表示已用同日美股ETF/标的实际收盘与正常组合基差修复异常锚；`close_conflict`表示参考要求的调整超过1%，采用稳健合约锚；`publication_pending`表示参考在该分段定稿后才首次获取，等下一分段使用。`reference_anchor`、`reference_gap`展示参考锚及其相对稳健合约锚的差值。基线仅使用实际价格。昨日新基线重算值与旧版实盘日志分别报告，注明分钟价或盘口采样口径。

## 手机端可复用指令

“登录我的AWS服务器，先阅读 `/home/ubuntu/pycharm_nt/strategies/etf_arb/MONITORING.md`，执行其中的监控查询命令，汇报ETF策略运行状态、七组当前偏离与今日采样峰值、持仓和成交、延迟及异常。注明数据时间；只查询，不修改策略、不下单。”

服务器连接通过已配置的服务器访问工具完成；监控文件不保存连接凭据。
