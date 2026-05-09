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
- Put strategy/run-level values such as strategy params, market list, instrument precision/limits/fees, backtest account settings, and live account settings in each strategy set YAML.
- `runtime.py` loads `config/global.yaml` first, then recursively overlays the selected strategy set. Strategy set values override global values when keys overlap.
- `live.py` may register shared data clients such as the external signal client for every node; strategies that need them subscribe, strategies that do not need them ignore them.
- Reports are stored as `reports/{run_type}/{config_name}`. Backtest writes final reports after the run; live writes fills immediately from `OrderFilled` events and writes final order/position snapshots after the run.
- Strategies must not import report-writing utilities. Live report writing belongs beside the node/trader setup and should subscribe to `events.order.{strategy_id}` through `Trader`.
- In set files under `config/`, put frequently changed strategy, market, backtest, and live run parameters near the top; keep stable instrument and project plumbing near the bottom.
- Entry `main(config_name=None)` functions should let CLI config names override function arguments; function arguments are only the fallback for IDE runs without CLI args.
- Do not add defensive guards or fallback checks that duplicate NautilusTrader's normal event guarantees. Keep strategy callbacks close to NT examples unless there is a specific observed failure.
- Do not catch exceptions to hide or convert errors during normal development. Let errors surface, then fix the underlying cause.
- Do not return `None` as an error state that forces callers to branch. For required config, credentials, and dependencies, access them directly and let missing values raise.
- Confirm with the user before making functional behavior changes. Small formatting, comments, and documentation-like local cleanup can be done directly.
- Do not change large framework or NautilusTrader lifecycle logic for a small local requirement such as report writing, formatting, or convenience output. Solve small requirements in the narrow module that owns them, and ask before touching core run/build/stop flows.
- Before touching Conda, global config, system directories, or live-trading credentials, explain the action and wait for explicit confirmation.
- On Windows PowerShell 5, bare `Get-Content` can display normal UTF-8 Chinese files as mojibake. When reading files that may contain Chinese text, use `Get-Content -Encoding UTF8` or Python `Path.read_text(encoding="utf-8")`. Do not treat display mojibake as file corruption, and do not add BOM or rewrite file encodings unless the user explicitly asks.

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
