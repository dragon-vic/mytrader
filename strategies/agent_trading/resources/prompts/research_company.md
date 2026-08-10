# Pre-event company research

## Objective

Conduct decision-quality research for the assigned live event. This is not a lookup task and not an exercise in filling a rule table. Build an independent view of what the market expects, what is already priced in, which facts can change that view, and how the eligible perpetual contract could react when the disclosure arrives.

Your final response is a concise Markdown research memo. The complete research context will remain in this Codex session and the disclosure-analysis turn will resume this same session. Do not return JSON, fixed seven-tier conditions, mechanical scoring rules, or a trade decision.

## Local inputs and boundaries

The working directory is one batch root. Read only:

- `batch.json`
- `market_universe.json`
- `events/<assigned event id>/event.json`
- `events/<assigned event id>/watch/plan.json`

Use `market_universe.json` as the executable instrument set. You may study at most three relevant non-index instruments. When the same symbol is available on both venues, use the Binance instrument for the eventual executable candidate; other venues may still provide useful market evidence.

Do not inspect repository code, unrelated events, credentials, running processes, existing research outputs, or disclosure files. Do not modify files, send messages, place orders, or create execution plans.

The repository-root `.env` may be read only to load these Alpaca Market Data settings: `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, and `APCA_API_BASE_URL`. Never display, log, quote, or return their values, and do not scan other environment fields. Alpaca may be used for historical US cash-equity prices, but it is neither required nor exclusive.

## Information time

This is the normal live pre-event workflow. Use all reliable public information available while the research is running, including information published after the scheduled research start. The research start time is an operational timestamp, not an information cutoff. Prefer the newest reliable evidence and record publication dates when timing changes its meaning.

## Research process

Begin with a brief trading-value screen before creating the question ledger or spawning any subagent. Use enough reliable evidence to decide whether a complete event-specific repricing of at least roughly 5% is plausible and whether an eligible instrument has a usable causal path. Consider historical cash and perpetual reactions, the current catalyst set, forward-information sensitivity, positioning or valuation asymmetry, and basic contract transmission.

End the research early only when positive evidence shows that the event has little trading value: the likely issuer-specific reaction is below 5%, no credible catalyst or positioning setup creates a larger tail, or no eligible instrument has a defensible causal path. Do not stop merely because estimates conflict, exact data are difficult to find, direction is uncertain, or a recently listed perpetual lacks its own earnings history. Cash-equity history and available contract transmission evidence are valid for this screen.

If the event has little trading value, do not spawn subagents. Return a short Markdown memo that explains the evidence for early stopping, the expected full-event range, the relevant instrument if one exists, and the few extraordinary disclosure facts that would invalidate the low-impact view. This is a completed pre-research result, not a research failure.

If the event passes the screen, spawn exactly three subagents and give them independent, non-overlapping mandates:

1. Financial expectations: reconstruct current consensus and credible ranges, guidance definitions, revisions, financial and operating trends, and an independent expectation model.
2. Business and industry: research the company, products, customers, competitors, supply chain, policy and macro drivers, and the causal path from new information to value.
3. Skeptical market-impact review: test what is priced in, valuation and positioning, run-up or de-risking, historical reactions, sell-the-news risk, contrary evidence, and the transmission from cash equity to the 24-hour perpetual.

The main agent owns the synthesis. For an event that passes the screen, create an internal ranked question ledger before delegating. After the first pass, send the subagents a conflict packet containing the most important disagreements, missing evidence, stale estimates, definition mismatches, and causal claims that need falsification. Require each relevant lane to defend, revise, or withdraw its claim. Resolve the conflicts yourself and reach a view; do not treat disagreement or missing consensus as a reason to stop.

Research broadly enough to understand the event rather than collecting a few headline estimates. Depending on materiality, examine:

- official filings, releases, guidance and management history;
- several current analyst or sell-side views and broader market expectations;
- whisper expectations, revisions and definition differences where observable;
- company and industry economics, competitive and policy changes, forward demand and execution risks;
- valuation, positioning, options-implied move, recent run-up, short interest, funding, basis, liquidity or crypto beta where useful;
- previous comparable events and the actual price response, including reversals and delayed reactions.

Treat consensus values as evidence, not the answer. When sources conflict, investigate date, metric definition, period, GAAP versus adjusted treatment, contributor quality and whether estimates have moved. Use primary sources for facts and diverse credible market sources for expectations. Then state your own conclusion and why it is better supported.

## Market-reaction calibration

This strategy trades Binance or Hyperliquid 24-hour contracts, not US cash equities during regular hours only. You may use the perpetual contract, the underlying cash equity, or both; there is no forced source priority. A recently listed contract may require older cash-equity event history plus all available contract basis and transmission evidence.

Use consistent timestamps and windows. Separate the initial jump, conference-call or guidance digestion, and the complete event repricing when the evidence supports it. Distinguish market-wide or crypto moves from issuer-specific residual movement. Do not assume cash and perpetual magnitudes are identical, and do not discard useful cash history merely because the contract listed later.

Estimate realistic upside and downside ranges, asymmetry, an ordinary/no-action region, and tail conditions. The important object is the expected complete event move; NT will later measure how much of that move remains after the decision arrives.

## Decision philosophy to prepare

The future analysis turn must make a judgment, not wait for perfect evidence.

- A reported beat is not automatically bullish. Compare the disclosure with current expectations, forward guidance, valuation, positioning and already-consumed good news.
- An in-line report is not automatically low impact. Whisper expectations, forward commentary, crowded positioning, a prior run-up and sell-the-news dynamics can still create a large move.
- Mixed or incomplete secondary evidence should normally reduce confidence or signal strength, not automatically force HOLD.
- For an event whose plausible complete repricing is greater than 5%, an actionable direction should normally remain more likely than HOLD across the researched scenario set. Treat a HOLD share above 50% as a warning that the thesis may be under-researched or overly conservative, unless the evidence genuinely supports indeterminacy.
- HOLD should mainly remain appropriate when careful research indicates the disclosure is likely ordinary and low impact, or when deep research still leaves the principal direction genuinely indeterminate.

Do not manufacture conviction. The objective is a well-calibrated willingness to trade meaningful surprises, not a high trade count.

## Final memo

Write one self-contained Markdown memo for the assigned event. Keep it compact enough for a human review and an email, but preserve the conclusions the resumed analysis will need. Use natural headings and prose rather than a rigid template. Make clear:

- your independent thesis and the market's true bar;
- the most credible expectation range and how you resolved conflicting estimates;
- what appears priced in and what would be genuinely incremental;
- the decisive financial, forward-looking, business and positioning drivers;
- relevant executable instrument IDs and cash/perpetual reaction calibration;
- the likely directional scenarios, full-event magnitude and asymmetry;
- the few disclosure facts that would most strongly confirm or falsify the thesis;
- material contrary evidence and unresolved uncertainty.

Use inline source links near the claims they support. Distinguish fact, market expectation and your inference. Do not dump an exhaustive source register, reproduce the internal debate, prescribe orders, or encode fixed trading rules. For events that pass the trading-value screen, finish only when additional reasonable research is unlikely to change the directional and impact framework materially.
