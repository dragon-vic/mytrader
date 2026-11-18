---
name: nt-project-context
description: nt_quant 当前实盘入口、共享模块、策略职责、目录与本地/AWS运行环境。
---

# 当前项目

项目以 Binance U 本位实盘为主。维护三个可发现策略。

- `etf_arb` 杠杆ETF与标的永续的短期偏离回归。
- `sk_adr_arb` SK普通股与ADR永续的溢价分档策略。
- `agent_trading` 保留的内置AI交易与研究模块，含外部JSON、邮件和Codex工具链。

`legacy/` 保存旧策略、旧适配器、旧回测入口、Telegram工具、旧模型及历史说明，脱离活动入口。研究与运行产物继续保存在原策略目录。独立观测台由其他任务维护，访问与发布行为需按当次用户任务授权。

## 环境与入口

- 本地根目录 `D:/project/nt_quant`，Python `D:/app/miniconda/envs/nt/python.exe`，PowerShell 7。
- AWS SSH alias `aws`，生产项目 `/home/ubuntu/pycharm_nt`，Python `/home/ubuntu/miniconda/envs/nt/bin/python`。
- Python依赖 `requirements.txt`，固定NT版本1.230.0。凭据在根目录`.env`，保留本地和服务器独立配置。
- 入口 `python run.py <策略名> live|testnet [background]`，Linux后台使用tmux。
- 配置依次合并 `strategies/global.yaml`、策略`live_config.yaml`，由NT config类验证策略参数。
- 每次运行独立 `strategies/<策略>/report/live-<时间>/`，统一写订单、持仓与摘要。

## 模块职责

| 位置 | 职责 |
|---|---|
| `run.py`、`live.py` | 配置选择、构建Node、停止协议、运行与报告收尾 |
| `utils/live_node.py` | manage_stop策略完成退出后再关闭NT客户端，重复停止请求合并 |
| `adapters/binance.py` | Binance市场、凭据、客户端工厂与市价规格 |
| `adapters/binance_execution.py` | 结果未知保护、原订单核单、真实成交与NT报告链路 |
| `adapters/binance_account.py` | 原生USDT可用保证金、模式、逐合约杠杆与限额 |
| `utils/live_strategy.py` | owner线程归属、跨线程投递与到期计时器取消 |
| `utils/background.py` | 单请求和单结果邮箱的后台读取 |
| `utils/execution.py` | 订单错误分类、确认结果契约与1/2/5秒退避 |
| `utils/state.py`、`utils/runtime_lock.py` | 原子JSON检查点、跨进程排他锁 |
| `utils/execution_lease.py` | 运行模式、venue、账户类型、合约级执行归属锁 |
| `utils/wecom.py` | 有界企业微信队列、后台HTTP、发送记录与失败隔离 |
| `utils/config.py`、`utils/runtime_setup.py` | 配置规范化、运行目录与NT组件规格 |
| `utils/live_control.py`、`utils/reports.py`、`utils/summary.py` | Node停止协议及策略无关报告 |

执行锁按本机同一运行模式、venue和账户类型保守隔离，ETF与SK共享SKHY时会发生冲突。AI执行列表也覆盖多个ETF标的及SKHYNIX，重叠合约同样受执行锁约束。不同凭据使用同一机器也共用此范围。锁覆盖整个Node运行和收尾。跨机器启动需要部署层保证单实例。

ETF和SK配置启用`confirmed_orders`并关闭NT通用inflight合成终态检查，由各自看门狗查询。AI模块继续原工厂和原引擎默认行为。

## ETF策略

九组共17合约，具体范围以`live_config.yaml`为准。开仓和加仓要求可成交盘口偏离在2%–25%之间（含两端），首次100ms复查。同方向严格低于1%重新武装，1%至2%保留许可。开加平仓跟随当期有效基线，持仓对冲斜率独立保存。

每阶段后台读取一次原生账户，90%资金使用率、最高10倍名义预算，按实际杠杆和限额约束。每批每腿最多1000U、80%可见深度、预计价差损耗上限0.1个百分点，全程市价分批。全局同一时刻一批双腿，多组合各自持仓，批次完成后按偏离选择下一组。下一笔开仓/加仓等待候选组合任一腿在上一批完成后新收到L1，再由on_quote_tick检查并执行；按接收序号判定，报价年龄独立于此门槛。退出和风险修复继续按原流程调度。正常退出从0.2%起，每小时增加0.1个百分点，最高0.5%。开盘后5分钟、收盘前30分钟、资金费前后5分钟禁新增风险，收盘前5分钟开始退出。

- `baseline.py`、`close_reference.py` 管理收盘锚、分段前5分钟校准、现金市场参考。
- `market.py` 管理最后有效L1、L5与已消耗容量。L1持续有效，行情连接错误独立处理。
- `capital.py` 管理策略共享资金账与在途预留。
- `special_events.py`和事件YAML管理公司行动、暂停交易、规格检查与单位重建。
- `recorder.py` 用有界内存环形缓存与后台gzip保存事件前10秒、过程及后60秒盘口。
- `runtime/state.json` 保存阶段、虚拟数量、订单、重试、事件边界与暂停状态。
- `etf_events.jsonl` 保存决策和30秒health，成交记录含预计均价、实际价格、滑点与费用。
- `monitor.py` 是独立只读查询入口，使用`MONITORING.md`，运行时主动刷新后汇报。

详细运行规则见`strategies/etf_arb/OPERATIONS.md`，事故机制见`docs/production_incidents.md`。一次性调研与验证产物位于`strategies/etf_arb/research/`，按照当次配置解释历史结果。

## SK策略

保留40%至50%的业务档位，43%起采用较大金额，每腿市价1000U，完整双腿后间隔5秒。35%触发锁定退出，两腿归零后请求停止Node。人工停机保留持仓。

`runtime/state.json`保存已完成档位、进行中轮次、未决订单和仓位核对依据，恢复时优先读取检查点。配置`current_add_position`只用于首次无检查点启动，首次接管历史仓位仍需核实该值。原生资金按每档更新、90%可用保证金约束，批次期间本地预留，企业微信异步通知。待核单/仓位不匹配的检查点保留供人工核对，启动按停止协议结束。

SK明确单腿失败后先核实原单，回退本轮实际成交，保留此前持仓并暂停加仓。回退单仍走原订单核单和减仓退避，未决60秒告警、每5分钟重复提醒。尚在回退的检查点保留原批次归属供重启核对。


## 策略观测台

当前账户源码在 `sites/strategy-observatory-current/`，Sites项目 `appgprj_6afc68ed363481919a96edfb589bf25d`，地址 `https://nt-strategy-observatory.victoricehu.chatgpt.site`。正式入口 `https://kiwi-data.fun`，SK位于`/sk`，九组杠杆ETF位于`/leveraged`。Cloudflare Worker `nt-strategy-observatory` 通过 `gateway/index.ts` 和 `wrangler.gateway.jsonc` 流式转发到当前账户Sites，后端授权凭据保存在Worker私密变量中。界面和D1通过Sites技能发布，域名入口通过Wrangler发布。

AWS源码仍为 `/home/ubuntu/pycharm_nt/sites/strategy-observatory/`。`nt-strategy-site` cron每分钟执行`ops/publish_snapshot.py`，读取既有health和公开K线，通过独立HMAC鉴权推送至kiwi-data.fun，使用flock、50秒超时及低CPU优先级。上传配置`/home/ubuntu/.config/nt-strategy-site/config.json`权限600，日志同目录`publish.log`。

2026-11-17修复原站1102 CPU超限。ETF上传和详情图保留全部30秒health样本，列表小图按像素保留首尾及高低点，连线缺口根据原始相邻样本判断，路径缓存避免悬停重复生成。详情纵轴贴合实际波动并标注最高点和北京时间。SK保留24根小时K和最近一小时分钟K。九组全天快照约2.2至2.6MB，正式域名gzip传输实测约412KB。写入校验并过滤公开字段，读取流式解压已校验快照，正式域名通过12轮全量上传和读取。旧本地源码`sites/strategy-observatory/`保留作迁移前存档。

ETF走势和最高偏离从最近一次XNYS实际收盘开始统计，到下一次实际收盘重置。publisher使用exchange-calendars日历计算`etf.window_start/window_end`，覆盖夏令时、节假日与提前收盘。北京时间零点和策略重启均保留本收盘周期数据，周末连续显示。前端展示“收盘后最高”和周期起点，图形横轴从收盘时刻起，超过24小时显示日期。历史单组上限20000点，上传上限16MB，覆盖连续休市期间的完整30秒样本。
