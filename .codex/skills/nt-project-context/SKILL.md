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
- `framework_test`
- `pre_ipo`
- `sk_adr_arb`

新增或删除策略时，必须同步更新本文件中的策略列表、目录说明和相关运行背景。

策略自己的配置、运行数据、报告和策略私有工具放在对应策略目录内。
与策略运行无关的研究代码、研究数据、分析图和研究笔记放在对应策略的 `research/` 目录。

## 主要目录

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
- `tools/rtpr_websocket.py`：独立 websocket 工具。

只有用户明确要求时，才把工具接入策略或框架运行链路。
