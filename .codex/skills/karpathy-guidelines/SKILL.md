---
name: karpathy-guidelines
description: 在本项目中编写、审阅或重构代码时使用，让改动保持简单、明确、聚焦、可验证。
license: MIT
source: https://github.com/forrestchang/andrej-karpathy-skills
---

# Karpathy 准则

这是本项目对 Karpathy 风格编码代理准则的本地化改写。

本仓库内所有非平凡工作都遵循这些规则。

## 编码前先思考

- 行动前说明关键假设。
- 如果请求有多种合理解释，先指出歧义。
- 当存在更简单或更安全的做法时，直接提出。
- 如果下一步依赖不清楚或涉及权限敏感的上下文，停下来询问。

在本项目中，环境、Conda、系统目录、全局配置、网络安装都属于权限敏感事项。不要静默绕过这些问题。

## 项目专用规则

- 环境事实是工作流的一部分。如果本地权限、shell、运行时或工具问题迫使你换一种方法，把这个可复用的解决方式记录到这里，避免以后重复踩坑。
- 在这个 Windows 工作区里，`rg.exe` 可能因为 `Access denied` 失败。发生时改用 PowerShell 原生命令，例如 `Get-ChildItem -Recurse -File -ErrorAction SilentlyContinue` 和 `Select-String`，并把范围限制在需要的文件类型或目录。
- 递归 PowerShell 扫描可能遇到 `.pytest_cache` 等无权限路径。使用 `-ErrorAction SilentlyContinue` 或缩小搜索范围，不要把它误判为仓库损坏。
- Git 可能在这个沙盒里报告 dubious ownership。只读检查时使用 `git -c safe.directory=D:/project/nt_quant ...`，不要改全局 git 配置。
- Windows PowerShell 5 不能直接接受 `Select-Object -Index 40..80`。范围需要加括号，例如 `Select-Object -Index (40..80)`，或者使用 `Select-Object -Skip 40 -First 40`。
- 对齐策略设计时，先把实现细节和取舍点说清楚，让用户确认后再写代码。
- 命名保持短：变量通常不超过两个词，函数不超过三个词，类不超过两个词加一个后缀，例如 `Actor`、`Str` 或框架要求的类型名。
- 只有在逻辑被多处复用，或确实能让一段密集代码更清楚时，才抽取辅助函数。一次性逻辑优先保持直接、局部、浅层。
- 除非用户明确要求，不要为多种用户输入格式添加宽泛的 fallback adapter。优先使用一种短小、文档化的配置格式，而不是接受许多等价写法。
- Binance U 本位合约账户事件需要使用精确 topic：`events.account.BINANCE-USDT_FUTURES-master`。不要把它当成只有资金费率信号；订单活动也可能发布账户事件。
- OKX 的 client order ID 不能包含 `-`。构建 `OrderFactory` 时必须关闭 hyphen，避免订单被交易所拒绝。
- 不要创建会混淆 IDE 索引的重复文件夹名。仓库根目录已经是 `nt_quant`，Python 包不能也命名为 `nt_quant`。
- 当前仍处于早期阶段，避免创建 package 文件夹。面向用户的入口脚本放在仓库根目录，策略文件直接放在仓库根目录的 `strategies/` 目录下。
- 本项目不要使用 argparse 任务分发器。当前入口脚本是 `fetch_data.py`、`backtest.py` 和 `live.py`。
- 策略、回测、沙盒执行和交易所适配器优先使用 NautilusTrader 原生组件。只有 NautilusTrader 没有需要的通用数据路径时才使用 CCXT。
- 使用 NautilusTrader 或交易所 adapter 的账户余额、可用资金、保证金、仓位、PnL 和风控数据前，必须检查当前版本的实际 adapter 映射，并用对应交易所的真实事件验证字段会实时更新。源码含 TODO、空列表/零值占位、测试实现或实测不更新的接口不得作为交易和风控依据；在策略目录内用口径明确、可验证的计算替代，不用 fallback 掩盖缺失数据。
- 默认交易所是 Binance，除非用户明确更改。
- 除非用户另有说明，新的 live 相关配置默认面向 Binance USDT futures testnet。
- 第一版保持最小化。不要在出现真实需求前添加额外框架层。
- 临时检查从 shell 运行，不要保留成永久脚本。
- 保留 `external/external_data.py`；它是用户维护的独立外部信号发送进程，即使主 NT 进程不 import 它。
- 除非用户明确要求，不要更新 README。用聊天说明用法。
- 编辑代码时，在函数或不明显代码块附近使用简短中文注释，说明函数做什么，不要啰嗦叙述。
- 函数用途注释放在函数定义上一行，这样折叠代码时仍然可见。
- 策略选择由配置驱动。`config/` 下的 set 文件声明 `strategy.name`；`runtime.py` 推导 `strategy.module`、`strategy.class` 和 `strategy.config_class`，除非这些值被显式覆盖。
- `backtest.py` 和 `live.py` 必须保持通用。不要在这些入口脚本里为某个具体策略或具体配置集添加分支。
- YAML 配置保持分层。共享 global 配置放在 `strategies/global.yaml`；策略自己的 set YAML 放在对应 `strategies/{strategy_name}/` 目录下。
- 共享 live/node 默认值放在 `strategies/global.yaml`；策略 set YAML 可以显式覆盖。策略参数、各模式市场选择、instrument 精度/限制/费率、回测账户设置放在各自策略 set YAML 中。
- `runtime.py` 先加载 `strategies/global.yaml`，再递归叠加所选策略 set。键冲突时，策略 set 的值覆盖 global。
- `live.py` 可以为每个 node 注册外部信号 client 等共享 data client；需要它们的策略自行订阅，不需要的策略忽略。
- 报告存放在对应策略目录下的 `report/{live|backtest}-开始时间`。回测在运行结束后写最终报告；live 在 `OrderFilled` 事件发生时立即写成交记录，并在运行结束后写最终订单和持仓快照。
- 策略不能 import 报告写入工具。Live 报告写入属于 node/trader 设置层，应通过 `Trader` 订阅 `events.order.{strategy_id}`。
- 策略外部组件不能依赖某个策略的私有行为、文件 schema、事件 schema 或内部生命周期。`strategy_events.csv` 这类策略私有输出归策略自己所有；通用 writer、actor、factory 和 runtime 代码不能读取、转换、推断或基于其内容分支，除非项目先定义了明确的共享输出契约。
- 策略独有的研究脚本、临时回测、数据、图、notes 和其它产物都放在对应 `strategies/{strategy_name}/` 目录下。只有跨策略复用的数据或工具才放在仓库级 `data/`、`tools/` 或 `utils/`。
- 研究数据集默认使用 parquet 等紧凑列式格式保存给机器消费，除非用户明确要求 CSV。不要默认创建重复 CSV 导出。面向人的摘要用纯文本或 Markdown，不要用 JSON，除非用户明确要求机器可读 metadata 文件。
- 按数据类型组织 `data/`。当前顶层数据目录为 `funding/`、`tick/`、需要时的 `bar/`、`signal/`、`notes/`，以及与策略 runtime 无关的数据抓取脚本目录 `fetchers/`。
- 可复用市场数据文件名使用大写 symbol/scope、市场类型、数据类型或 interval、起始日期，例如 `BTCUSDT-PERP-1S-20250101.parquet` 或 `ALL-USDT-PERP-FUNDING-20250101.parquet`。永续合约使用 `PERP`。
- 研究分析产物属于 `data/`，不属于 `reports/`，除非用户明确要求报告式交付物。
- 数据分析期间，只持久化用户明确要求保留的可复用 metadata/data 和 notes。不要保存临时摘要、结果表、图、临时脚本、中间文件或一次性分析输出，除非用户明确要求。
- `tools/` 下脚本和临时测试/检查脚本，应把用户常改参数放在底部 `main(...)` 调用里，并配短行内注释，而不是命令行参数或顶层常量，方便用户直接改文件末尾。
- 解释分布时，优先用图而不是百分位表。用简短文字说明如何读图，除非用户要求数字细节，否则不要倾倒 25/75 分位表。
- `config/` 下 set 文件中，把经常改的策略、市场、回测和 live 运行参数放在靠前位置；稳定的 instrument 和项目 plumbing 放在靠后位置。
- YAML set 文件中，只用于回测的参数放在 `backtest` 下，不要放在顶层 strategy/live/instrument 区域。
- Live 配置加载不能依赖回测专用字段。Live 路径只依赖 symbols、venues、credentials 和 runtime 设置；回测数据限制、合成 instrument 精度/费率/保证金值放在 `backtest` 下。
- 入口 `main(config_name=None)` 函数应允许 CLI 配置名覆盖函数参数；函数参数只作为 IDE 运行且无 CLI 参数时的 fallback。
- 不要添加重复 NautilusTrader 正常事件保证的防御性 guard 或 fallback 检查。策略 callback 尽量贴近 NT 示例，除非已经观察到具体失败。
- 正常开发期间不要捕获异常来隐藏或转换错误。让错误暴露，然后修根因。
- 不要把 `None` 作为错误状态返回，迫使调用方分支。必需配置、凭据和依赖应直接访问，缺失时让它抛错。
- 不要设计参数 fallback 或静默默认值。必需的 runtime/trading 参数必须在 YAML 中显式声明，缺失时失败。如果确实需要共享默认值，放在 `strategies/global.yaml`，让策略 set YAML 通过正常分层覆盖。策略专用交易参数属于 `strategy.params`，并且只在策略 config 声明时传入。
- 不要写“优先看 A，缺失再看 B”这类滑坡式兼容逻辑，尤其是 UI、报告、策略状态和交易信号字段。字段缺失应直接不显示或失败，避免错误被 fallback 掩盖。
- 构建 node 时，不要在 Python 代码中引入隐式默认值。每个 node/runtime 设置都必须来自合并后的 YAML，使有效配置可检查；共享默认值属于 `strategies/global.yaml`，策略 set YAML 可以显式覆盖。
- 本项目只在 Windows 上使用配置的 HTTP proxy。Linux 上即使 `strategies/global.yaml` 包含本地 Windows 代理地址，`proxy_url(settings)` 也应返回 `None`。
- 做功能行为变更前先和用户确认。小的格式、注释、文档式本地清理可以直接做。
- 本项目验证 live/testnet 相关改动时，默认使用 `bintest` 作为测试配置，除非用户明确指定别的 set。
- 如果某处写法是为了绕开历史坑、外部库限制、交易所行为、运行环境差异，或用户明确指定过“必须这么写”，必须在代码附近加简短注释说明原因。以后改代码时先看这些注释，不要把已经踩过的坑改回去。
- 不要为了报告写入、格式化或便利输出这类小型本地需求去改大型框架或 NautilusTrader 生命周期逻辑。小需求在拥有它的窄模块中解决；触碰 core run/build/stop flow 前先询问。
- 触碰 Conda、全局配置、系统目录或 live trading credentials 之前，先说明动作并等待明确确认。
- Windows PowerShell 5 中，裸 `Get-Content` 可能把正常 UTF-8 中文文件显示成乱码。读取可能包含中文的文件时，使用 `Get-Content -Encoding UTF8` 或 Python `Path.read_text(encoding="utf-8")`。不要把显示乱码当成文件损坏，也不要加 BOM 或重写编码，除非用户明确要求。
- 本项目运行 Python 时直接使用 nt conda 环境解释器：`D:\app\miniconda\envs\nt\python.exe`。不要使用 base `python` 或 `conda run`，除非用户明确要求。
- live 策略提交订单前必须先把 pending/order 映射等内部状态写好；`submit_order` 可能同步触发 NT 订单事件，不能让事件回调早于状态更新。
- 如果任务需要未安装的 Python 库，清楚说明所需包，或在用户批准后用 `D:\app\miniconda\envs\nt\python.exe -m pip install ...` 安装到 nt 环境。不要静默用更弱的替代库替换用户请求或更合适的库。
- 服务器可通过本机 SSH alias `aliyun` 或 `aws` 直接登录；AWS 使用 `ubuntu` 用户，远端项目目录是 `/home/ubuntu/pycharm_nt`；阿里云远端项目目录是 `/root/pycharm_nt`，远端 nt 环境 Python 是 `/root/miniconda/envs/nt/bin/python`。涉及线上 report、tmux 运行态、实盘日志、交易所 REST 延迟或服务器网络环境时，默认可在服务器上读取和诊断；不要暴露或改动 SSH key、系统级配置或 live credentials。
- 从 Windows PowerShell 调远端 bash 时，不要把包含 `2>/dev/null`、`$(...)`、管道、here-doc 或复杂引号的 bash 片段直接写进 `ssh remote "..."`；PowerShell 可能会把重定向解析成本地 `D:\dev\null`，直接 pipe here-string 也可能重新写入 CRLF。复杂脚本统一 base64 后交给远端 bash：
  ```powershell
  $script = @'
  cd /root/pycharm_nt
  report=$(ls -dt strategies/preipo_arb/report/live-* 2>/dev/null | head -1)
  echo "$report"
  '@
  $clean = $script -replace "`r", ""
  $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($clean))
  ssh remote "printf '%s' '$b64' | base64 -d | bash"
  ```
- 服务器已安装 `ripgrep`，远端项目内搜索优先用 `rg`；如果极少数环境没有 `rg`，再用 `grep/find` 兜底。
- 清理 `strategies/preipo_arb/research/bidask1-live` 或相关 bidask1 collector 数据时，必须保留最近至少 3 小时的数据，包括已合并的 `merged` 小时文件和当前小时 `raw` 分片；策略 warmup 需要这段真实 quote 数据。
- 画 `preipo_arb` 的 edge、quote、订单、回测或 live 复盘图前，必须先阅读 `strategies/preipo_arb/research/EDGE_CHART_STYLE.md` 并按其中规范作图。尤其注意：edge 用点，3h 时间加权均线和 signal 线必须连续可见，订单标签只写实际方向、qty 和 edge。
- 每次成功 push 到远端仓库后，默认登录服务器 `remote`，在 `/root/pycharm_nt` 执行 `git pull --ff-only` 同步代码；如果服务器有本地未提交改动，先 stash 备份再 pull，不要直接覆盖。
- 涉及框架、runtime、report writer、adapter、配置加载、live/backtest 入口等通用改动时，改完后可以默认在服务器项目目录 `/root/pycharm_nt` 用远端 nt 环境跑 `bintest` 做一次 live/testnet smoke 验证；如果会触碰真实 live credentials、真实下单或需要重启正在运行的生产策略，先说明并等用户确认。
- Binance aggTrades REST 在当前环境中实际限频较低。大规模历史 tick 拉取必须可恢复并保守限速；遇到 HTTP 429 或 418 后立刻停止，稍后再恢复，不要紧密重试，否则临时封禁可能被延长。

## 简单优先

- 实现满足目标的最小设计。
- 不要添加猜测性功能、通用框架或额外配置。
- 避免为一次性代码创建抽象。
- 如果代码规模超过问题本身，继续前先简化。

## 精准改动

- 只触碰当前请求需要的文件。
- 匹配附近风格和项目约定。
- 除非必要，不要重排、重构或清理相邻代码。
- 只移除当前改动引入的未使用代码。
- 发现无关问题时说明即可，不要顺手修。

## 目标驱动执行

多步骤工作使用短成功闭环：

1. 实现请求中的一个明确切片。
2. 运行最小但有用的验证。
3. 只修复与该切片相关的失败。
4. 汇报哪些通过、哪些失败、还剩什么。

清晰的成功标准优于含糊的“让它能跑”。
