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
- Do not update README unless the user explicitly asks. Explain usage in chat instead.
- Use short Chinese comments around functions or non-obvious blocks when editing code, enough to state what the function does without verbose narration.
- Function purpose comments should be placed on the line above the function definition so they remain visible when code is folded.
- Strategy selection is config-driven. A set file under `config/` declares `strategy.module`, `strategy.class`, and `strategy.config_class`; entry scripts should not need edits for each new strategy.
- In set files under `config/`, put frequently changed strategy, market, backtest, and live run parameters near the top; keep stable instrument and project plumbing near the bottom.
- Entry `main(config_name=None)` functions should let CLI config names override function arguments; function arguments are only the fallback for IDE runs without CLI args.
- Do not add defensive guards or fallback checks that duplicate NautilusTrader's normal event guarantees. Keep strategy callbacks close to NT examples unless there is a specific observed failure.
- Do not catch exceptions to hide or convert errors during normal development. Let errors surface, then fix the underlying cause.
- Confirm with the user before making functional behavior changes. Small formatting, comments, and documentation-like local cleanup can be done directly.
- Before touching Conda, global config, system directories, or live-trading credentials, explain the action and wait for explicit confirmation.

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
