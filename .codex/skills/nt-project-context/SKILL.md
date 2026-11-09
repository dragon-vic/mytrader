---
name: nt-project-context
description: nt_quant 项目的用途、架构、目录、环境、运行方式和服务器背景。
---

# 项目背景

## 项目用途

本项目是基于 NautilusTrader 的量化交易框架，支持：

- 多策略运行
- 回测
- live 实盘
- testnet 模拟盘
- 多交易所行情和执行 adapter
- 策略运行目录和 artifact
- 订单、持仓、统计摘要和报告
- 外部数据和外部命令输入
- 独立的 Telegram 等工具

框架代码应服务于所有策略，不应针对某一个策略写特殊分支。

## 当前策略

策略位于 `strategies/`：

- `agent_trading`
- `etf_arb`（杠杆 ETF 偏离回归实盘策略）
- `framework_test`
- `pre_ipo`
- `sk_adr_arb`

新增或删除策略时，必须同步更新本文件中的策略列表、目录说明和相关运行背景。

策略自己的配置、运行数据、报告和策略私有工具放在对应策略目录内。
与策略运行无关的研究代码、研究数据、分析图和研究笔记放在对应策略的 `research/` 目录。

`etf_arb` 交易 Binance 杠杆 ETF 永续与对应标的的价差回归。初步研究位于 `strategies/etf_arb/research/20261031/`，包括逐笔成交、深度、资金费、标记价和研究结果。独立逐笔回测位于 `research/backtest_20261031/`，Python下载与核算，Node合并逐笔回放，`run_backtest.ps1`为研究入口。实盘由 `etf_arb.py`、`config.py`、`baseline.py`、`market.py`、`capital.py`、`special_events.py`、`runtime_lock.py` 和 `live_config.yaml` 组成，使用通用入口 `python run.py etf_arb live background`。七组合共享原生可用保证金，每个新阶段异步读取一次，10倍交易所杠杆、90%资金使用率，以双腿合计9倍名义额为目标，并受交易所各腿限额约束。一次一个阶段，2%盘口阈值、100ms快照复查，按深度串行分批，每批每腿最多1000 USDT，批次数由流动性和剩余预算决定。开盘后5分钟、收盘前30分钟及资金费前后5分钟禁开/加仓，收盘前5分钟发起退出。`OPERATIONS.md`记录完整流程和运维方式。运行状态保存在 `runtime/state.json`，每次运行的事件与30秒心跳保存在独立report目录的 `etf_events.jsonl`。相关资源不放在 `sk_adr_arb` 目录。

`research/snxx_full_20261101/` 为 SNXX/SNDK 连续全历史研究，`run_full.ps1` 调用共享逐笔引擎并使用独立研究配置；跳过上线前14天交易，持仓期间的理论价跨每日重置保持连续。完整日成交缓存与前一研究批次共享，所有数据保存在D盘。

`etf_arb/recorder.py` 为实盘事件盘口采集模块：13合约有界字节环形缓存，2%触发保存前10秒、偏离/持仓全过程和结束后60秒，记录L1、L5、标记价资金费与策略决策状态。后台批量gzip写入当前report目录的`capture/`，逐次跨阈值独立标记。每合约20000条缓存、1024批队列、5GiB运行磁盘预算及2GiB剩余空间保护；采集拥堵/失败记录缺口并上报，交易线程不等磁盘。

`etf_arb/monitor.py` 是独立的按需监控查询入口，只读实时日志与JSON检查点，输出中文摘要并原子写入 `monitor/status.json` 和 `monitor/status.md`。不接入策略生命周期、不启动交易客户端；每次查询刷新，文件不是持续推送。固定使用说明为 `MONITORING.md`，供手机端已具备服务器访问能力的助手读取。报告区分30秒采样峰值、数据年龄、报价新鲜度、运行覆盖、检查点持仓和交易所真实仓位；无盈亏来源时保留空值。

`etf_arb` 开仓、补齐、平仓全部使用市价单，每批每腿最多1000 USDT，每腿使用可见深度最多80%，预计价差损耗上限0.1个百分点，超限共同缩量。正常平仓要求预计均价偏离≤当前退出阈值，每批重新判断。部分平仓后偏离再次达到2%允许再加仓，保持阶段原基线和原计时，额度按实际成交、费用、盈亏与在途预留计算。`adapters/binance_account.py`直接读取原生账户、持仓模式及各腿杠杆与限额，框架余额不参与策略资金判断。常见网络故障、陈旧行情及只读接口失败暂停新增风险并保留匹配对冲，订单超时先查询核单。明确收盘/公司行动窗口、无法修复单腿及关键状态故障锁定退出。平仓单腿成交不足时仅减仓修复，成交后等待新L1并排除成交前的旧L5。记录计划均价、实际成交和滑点。`adapters/binance.py`的`market_order_limits`解释Binance原始MARKET_LOT_SIZE，校验市价数量上下限和步长。

`special_events.yaml`是公司行动事件表，后台每15秒校验文件并读取exchangeInfo，状态超过45秒失效时暂停新开/加仓。按受影响组合禁开和提前清仓，确认交易所调整安排后设置重建边界，丢弃旧单位的基线样本及参考，用新单位下的完整收盘重新建锚。已生效事件与重建边界保存到检查点，阻止重启绕过事件。临时停牌保留匹配对冲并减掉可交易腿的多余敞口，恢复后按新鲜盘口及正常回归条件继续。合约规格变化暂停相关交易，核实并通过受控重启刷新NT instrument后恢复。SNDK 2026-11-20拆股事件已登记，交易所调整时刻与恢复参数需根据专项公告确认。

`research/snxx_tuning_20261101/` 为SNXX参数研究，`run_tuning.ps1`运行批量逐笔回放。当前候选基线为时段前5分钟一次小幅校准、随后固定，校准不确定时正常使用理论零基线；已比较连续确认与上一批双腿成交后立即复查的串行分段开仓。结果是研究模型，未实现订单簿消耗或实盘下单。

`research/microstructure_20261101/` 研究脉冲形态、阶梯开仓、按持仓小时放宽退出及事件日聚类稳健性。`grid_replay.cjs micro`复用逐笔引擎输出到该目录；`analyze_history.py`、`compare_policies.py`生成SNXX形态与核算结果。`all_pairs/`为七组候选连续历史：1394个合约日文件共用D盘完整日缓存，跳过上线前14天交易。`run_study.ps1`复现多组合基线、共同规则、收盘前定时退出及100/200ms延迟敏感性。该批次旧研究采用收盘前2分钟禁开/加仓、前1分钟退出，最终实盘规则见上文。`collect_public.py`、`collect_nt.py`是独立有时限的公开行情采样，分别使用Binance `/public`盘口与`/market`成交/标记路由；`stress_nt.py`仅重放报价至NT行情引擎。这些研究工具没有执行客户端或下单行为。最终研究报告为`strategy_report.md`。

`research/final_rules_20261101/` 为用户最终规则的2/3/4/5%阈值对照：首次触发后100ms快照复查、十批串行加仓、低于阈值暂停且再次达标继续；按小时退出。`run_comparison.py`复用`grid_replay.cjs micro`，加入美股开盘后5分钟、收盘前30分钟及双腿资金费时点前后5分钟禁开/加仓，收盘前5分钟发起清仓。历史成交以tick代理，沿用50ms下单延迟，未实现历史bid/ask消耗；每组独立资金。`summarize.py`核对逐笔记录并输出实际双腿成交均价偏离、持仓时长、收益及成本压力。

## 主要目录

`etf_arb/close_reference.py`在后台维护七组美股ETF与标的的Yahoo日线实际收盘价，使用`quote.close`，缓存`runtime/close_references.json`并记录首次可见时间；按美东日期及XNYS日历排除尚未收盘的日线。基线仅使用实际价格。收盘前30分钟两腿关系形成稳健锚，保留标的合约收盘价；过去5次收盘正常组合基差稳定时，以同日美股ETF/标的收盘校验异常锚。参考要求的调整超过0.3%且不超过1%时采用价格参考锚，超过1%标记`close_conflict`并采用稳健合约锚。盘前中后等分段前5分钟校准后冻结，后到收盘参考等下一段；持仓FairValue持续冻结。参考缺失/不稳定使用合约理论锚并暴露状态。后台每10分钟更新，盘口路径无HTTP。health与monitor的每组`baseline`提供锚、调整、分段修正、参考锚及参考差值。旧NAV研究保存在`research/nav_reference_20261103/`。公司行动按special_events.yaml与OPERATIONS事件流程管理。

```text
backtest.py                 回测入口
live.py                     live/testnet 入口
run.py                      统一交互和命令行入口
requirements.txt            Python 依赖

adapters/                   交易所和数据源适配器
data/                       跨策略可复用数据
models/                     项目模型
strategies/                 策略、策略配置和策略私有资源
tools/                      独立工具和运维脚本
utils/                      通用框架工具
.codex/                     Codex 项目配置和项目 skill
```

目录层级保持浅而清晰，不为了分类创建过深的嵌套目录。

## utils 当前职责

```text
utils/config.py             YAML 加载、合并和规范化
utils/constants.py          全局常量
utils/runtime_setup.py      运行目录、artifact 和组件规格
utils/live_control.py       Node 停止协议
utils/backtest_setup.py     回测数据、instrument 和批量配置
utils/reports.py            订单和持仓原始报告
utils/summary.py            汇总统计、JSON、文本和终端表格
```

新增或删除通用模块时，应同步更新本文件的职责说明，并检查代码风格和项目限制是否仍然准确。

## 入口方式

通用命令：

```text
python run.py <config_name> <mode>
```

支持的模式：

```text
backtest
live
testnet
```

直接入口：

```text
python backtest.py
python live.py
```

Linux 后台运行使用 tmux。运行目录、日志和报告由 runtime 根据配置生成。

## 配置结构

共享配置：

```text
strategies/global.yaml
```

策略配置：

```text
strategies/{strategy_name}/live_config.yaml
strategies/{strategy_name}/backtest_config.yaml
```

加载顺序：

1. 加载 `strategies/global.yaml`。
2. 加载指定策略配置。
3. 递归合并，策略配置覆盖共享配置。
4. 根据运行模式进行严格校验。
5. 创建策略、actor、client 和回测组件。

配置中的策略参数由对应策略的 NautilusTrader config class 接收。

## 报告和 artifact

每次运行使用独立目录：

```text
strategies/{strategy_name}/report/{live|backtest}-开始时间/
```

通常可能包含：

```text
node.log
orders.csv
positions.csv
summary.json
summary.txt
```

artifact 是 runtime 根据配置传给策略或 actor 的当前运行文件路径，不是固定的全局文件路径。

报告生成分为：

- 通用订单和持仓报告。
- 统计摘要和终端表格。
- 策略或 actor 自己负责的私有 artifact。

## 本地环境

本地工作区：

```text
D:\project\nt_quant
```

本地 Python：

```text
D:\app\miniconda\envs\nt\python.exe
```

本地运行 Python 时使用上述 `nt` 环境，不使用 base Python。

项目依赖定义在 `requirements.txt`。本地密钥和环境变量放在根目录 `.env`，不进入 Git。

本地默认使用 PowerShell 7，并显式使用 UTF-8 读取包含中文的文件。

## AWS 环境

当前主要服务器通过 SSH alias 连接：

```text
ssh aws
```

服务器项目目录：

```text
/home/ubuntu/pycharm_nt
```

服务器 Python：

```text
/home/ubuntu/miniconda/envs/nt/bin/python
```

服务器使用 Linux 和 tmux 运行 live 策略、collector 和 Telegram bot。

服务器环境变量通常位于：

```text
/home/ubuntu/pycharm_nt/.env
```

服务器上的 live 报告、日志和 tmux session 属于运行态数据，读取时不得暴露密钥。

## 工具

`tools/` 下的脚本默认独立调用，不自动接入策略、Node 或策略生命周期。

例如：

- `tools/telegram_codex.py`：Telegram Codex bot。
- `tools/change_leverage.py`：账户杠杆工具。
- `tools/codex_agent.py`：统一的 Codex CLI 执行接口，供 `agent_trading` 和邮件工具复用。
- `tools/send_email.py`：读取文件或目录并调用 Codex agent 发送总结邮件。

只有用户明确要求时，才把工具接入策略或框架运行链路。
