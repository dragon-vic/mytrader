---
name: karpathy-guidelines
description: Use for this project when writing, reviewing, or refactoring code to keep changes simple, explicit, focused, and verifiable.
license: MIT
source: https://github.com/forrestchang/andrej-karpathy-skills
---

# Karpathy Guidelines

Project-local adaptation of the Karpathy-inspired coding-agent guidelines.

Use these rules for all non-trivial work in this repository.

## Think Before Coding

- State important assumptions before acting.
- If a request has multiple reasonable interpretations, surface the ambiguity.
- Push back when a simpler or safer approach exists.
- Stop and ask when the next action depends on unclear or permission-sensitive context.

For this project, environment, Conda, system directories, global config, and network install steps are permission-sensitive. Do not work around those silently.

## Project-Specific Rules

- Environment facts are part of the workflow. If a local permission, shell, runtime, or tooling issue forces a different method, add the durable workaround here so future turns do not rediscover it.
- In this Windows workspace, `rg.exe` can fail with "Access denied". When that happens, use PowerShell-native discovery instead, for example `Get-ChildItem -Recurse -File -ErrorAction SilentlyContinue` and `Select-String`, scoped to the needed file types or directories.
- Recursive PowerShell scans can hit access-denied paths such as `.pytest_cache`. Use `-ErrorAction SilentlyContinue` or narrow the search scope instead of treating that as repository corruption.
- Git can report dubious ownership in this sandbox. For read-only git inspection, use `git -c safe.directory=D:/project/nt_quant ...` rather than changing global git config.
- Windows PowerShell 5 does not accept `Select-Object -Index 40..80` directly. Wrap ranges in parentheses, for example `Select-Object -Index (40..80)`, or use `Select-Object -Skip 40 -First 40`.
- When aligning a strategy design, surface implementation details and tradeoff points for user confirmation before coding.
- Keep names short: variables should usually be at most two words, functions at most three words, and classes at most two words plus a suffix such as `Actor`, `Str`, or the framework-required type name.
- Extract helper functions only for logic reused multiple times or genuinely clarifying a dense block. Prefer shallow call depth and direct local code for one-off logic.
- Do not add broad fallback adapters for multiple user input formats unless explicitly requested. Prefer one short, documented config format over accepting many equivalent spellings.
- For Binance U futures account events, use the exact topic `events.account.BINANCE-USDT_FUTURES-master` when account data is needed. Do not treat it as a funding-only signal; order activity can also publish account events.
- Do not create duplicate folder names that confuse IDE indexing. The repository root is `nt_quant`; the Python package must not also be named `nt_quant`.
- At the current early stage, avoid a package folder. Keep user-facing entry scripts in the repository root and put strategy files directly under the repository root `strategies/` directory.
- Do not use an argparse task dispatcher for this project. Current entry scripts are `fetch_data.py`, `backtest.py`, and `live.py`.
- Prefer NautilusTrader native components for strategy, backtesting, sandbox execution, and exchange adapters. Use CCXT only where NautilusTrader does not provide the needed generic data path.
- Default exchange is Binance unless the user explicitly changes it.
- Unless the user says otherwise, new live-oriented configs should target Binance USDT futures testnet.
- Keep the first iteration minimal. Do not add extra framework layers until a real need appears.
- Temporary checks should be run from shell, not kept as permanent scripts.
- Keep `external/external_data.py`; it is a separate external signal sender process maintained by the user, even if the main NT process does not import it.
- Do not update README unless the user explicitly asks. Explain usage in chat instead.
- Use short Chinese comments around functions or non-obvious blocks when editing code, enough to state what the function does without verbose narration.
- Function purpose comments should be placed on the line above the function definition so they remain visible when code is folded.
- Strategy selection is config-driven. A set file under `config/` declares `strategy.name`; `runtime.py` derives `strategy.module`, `strategy.class`, and `strategy.config_class` unless they are explicitly overridden.
- `backtest.py` and `live.py` must stay generic. Do not add branches for a specific strategy or a specific config set in these entry scripts.
- Keep YAML config layered. Put environment-level values such as project paths, exchange name/venue, proxy, and external data-client host/port in `config/global.yaml`.
- Put shared live/node defaults in `config/global.yaml`; strategy set YAML may override them explicitly. Keep strategy params, per-mode market selection, instrument precision/limits/fees, and backtest account settings in each strategy set YAML.
- `runtime.py` loads `config/global.yaml` first, then recursively overlays the selected strategy set. Strategy set values override global values when keys overlap.
- `live.py` may register shared data clients such as the external signal client for every node; strategies that need them subscribe, strategies that do not need them ignore them.
- Reports are stored as `reports/{run_type}/{config_name}`. Backtest writes final reports after the run; live writes fills immediately from `OrderFilled` events and writes final order/position snapshots after the run.
- Strategies must not import report-writing utilities. Live report writing belongs beside the node/trader setup and should subscribe to `events.order.{strategy_id}` through `Trader`.
- Components outside a strategy must not depend on that strategy's private behavior, file schema, event schema, or internal lifecycle. Strategy-private outputs such as `strategy_events.csv` are owned by the strategy itself; generic writers, actors, factories, and runtime code must not read, transform, infer, or branch on their contents unless the project first defines an explicit shared contract for that output.
- For research datasets, keep machine-consumed data in compact columnar formats such as parquet unless the user explicitly asks for CSV. Do not create duplicate CSV exports by default. Human-facing summaries should be plain text or Markdown, not JSON, unless the user explicitly requests a machine-readable metadata file.
- Organize `data/` by data type. Current top-level data folders are `funding/`, `tick/`, `bar/` when needed, `signal/`, `notes/`, and `fetchers/` for data-fetching scripts unrelated to strategy runtime.
- Name reusable market data files with uppercase symbol/scope, market type, data type or interval, and start date, for example `BTCUSDT-PERP-1S-20250101.parquet` or `ALL-USDT-PERP-FUNDING-20250101.parquet`. Use `PERP` for perpetual futures.
- Research analysis artifacts belong under `data/`, not `reports/`, unless the user explicitly asks for a report-style deliverable.
- During data analysis, only persist reusable metadata/data and notes the user explicitly asks to keep. Do not save ad hoc summaries, result tables, plots, temporary scripts, intermediate files, or one-off analysis outputs unless the user explicitly asks for them.
- For scripts under `tools/` and temporary test/check scripts, put user-tuned parameters in the bottom `main(...)` call with short inline comments instead of command-line arguments or top-level constants, so the user can edit them directly at the end of the file.
- When explaining distributions, prefer a chart over percentile tables. Use concise text to explain how to read the chart, and avoid dumping 25/75 percentile tables unless the user asks for numeric detail.
- In set files under `config/`, put frequently changed strategy, market, backtest, and live run parameters near the top; keep stable instrument and project plumbing near the bottom.
- In YAML set files, parameters used only for backtesting belong under `backtest`, not top-level strategy/live/instrument sections.
- Live config loading must not require backtest-only fields. Keep live path dependent on symbols, venues, credentials, and runtime settings; put backtest data limits and synthetic instrument precision/fee/margin values under `backtest`.
- Entry `main(config_name=None)` functions should let CLI config names override function arguments; function arguments are only the fallback for IDE runs without CLI args.
- Do not add defensive guards or fallback checks that duplicate NautilusTrader's normal event guarantees. Keep strategy callbacks close to NT examples unless there is a specific observed failure.
- Do not catch exceptions to hide or convert errors during normal development. Let errors surface, then fix the underlying cause.
- Do not return `None` as an error state that forces callers to branch. For required config, credentials, and dependencies, access them directly and let missing values raise.
- Do not design parameter fallbacks or silent defaults. Required runtime/trading parameters must be explicit in YAML and fail if missing. If a shared default is truly needed, put it in `config/global.yaml` and let the strategy set YAML override it through normal YAML layering. Strategy-specific values such as `trade_notional` belong in `strategy.params` and should only be passed when the strategy config declares them.
- During node construction, do not introduce implicit defaults in Python code. Every node/runtime setting must come from the merged YAML settings so the effective configuration is inspectable; shared defaults belong in `config/global.yaml`, and strategy set YAML may override them explicitly.
- This project uses the configured HTTP proxy on Windows only. On Linux, `proxy_url(settings)` should return `None` even if `config/global.yaml` contains a local Windows proxy address.
- Confirm with the user before making functional behavior changes. Small formatting, comments, and documentation-like local cleanup can be done directly.
- Do not change large framework or NautilusTrader lifecycle logic for a small local requirement such as report writing, formatting, or convenience output. Solve small requirements in the narrow module that owns them, and ask before touching core run/build/stop flows.
- Before touching Conda, global config, system directories, or live-trading credentials, explain the action and wait for explicit confirmation.
- On Windows PowerShell 5, bare `Get-Content` can display normal UTF-8 Chinese files as mojibake. When reading files that may contain Chinese text, use `Get-Content -Encoding UTF8` or Python `Path.read_text(encoding="utf-8")`. Do not treat display mojibake as file corruption, and do not add BOM or rewrite file encodings unless the user explicitly asks.
- For this project, run Python with the nt conda environment executable directly: `D:\app\miniconda\envs\nt\python.exe`. Do not use base `python` or `conda run` unless the user explicitly asks.
- If a task needs an uninstalled Python library, say the required package clearly or install it into the nt environment with `D:\app\miniconda\envs\nt\python.exe -m pip install ...` after user approval. Do not silently replace the requested or better-suited library with a weaker substitute.
- Binance aggTrades REST has a low practical rate limit in this environment. Large historical tick pulls must be resumable and paced conservatively; after HTTP 429 or 418, stop immediately and resume later instead of retrying in a tight loop, otherwise the temporary ban can be extended.

## Simplicity First

- Implement the smallest design that satisfies the requested outcome.
- Do not add speculative features, generic frameworks, or extra configuration.
- Avoid abstractions for one-off code.
- If the code grows beyond the problem, simplify before continuing.

## Surgical Changes

- Touch only files needed for the current request.
- Match nearby style and project conventions.
- Do not reformat, refactor, or clean adjacent code unless it is required.
- Remove only unused code introduced by the current change.
- Mention unrelated problems instead of fixing them opportunistically.

## Goal-Driven Execution

For multi-step work, define a short success loop:

1. Implement the requested slice.
2. Run the smallest useful verification.
3. Fix only failures related to the slice.
4. Report what passed, what failed, and what remains.

Strong success criteria are preferred over vague "make it work" instructions.
