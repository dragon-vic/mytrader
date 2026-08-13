## Trigger

Use the first complete official Sandisk fiscal Q4/FY2026 earnings release or contemporaneous SEC exhibit. Do not wait for the 20:30 UTC call, a later filing, analyst reaction, or the August 13 Investor Day.

## Extract immediately

- Q4 revenue in USD billions (`R`).
- Q4 issuer-defined non-GAAP gross margin in percentage points (`M`).
- Q4 issuer-defined non-GAAP diluted EPS in USD (`E`).
- Q1 FY2027 revenue-guidance range and midpoint (`r = (low + high) / 2`).
- Q1 issuer-defined non-GAAP gross-margin guidance and midpoint (`m`).
- Q1 issuer-defined non-GAAP diluted-EPS guidance and midpoint (`e`).
- FY revenue and disclosed diluted shares as cross-checks. FY revenue should approximately equal $11.283B plus `R`; do not sum quarterly EPS.

GAAP EPS or gross margin cannot substitute unless the package supplies an exact bridge to the issuer's non-GAAP measure. Keep all revenue in USD billions, EPS in USD per diluted share, and margins in percentage points.

## Locked comparison bands

| Field | Strong negative | Negative | Neutral | Positive | Strong positive |
|---|---:|---:|---:|---:|---:|
| `R` | <8.00 | 8.00–<8.24 | 8.24–8.71 | >8.71–<8.90 | ≥8.90 |
| `M` | ≤76.5 | >76.5–<79.0 | 79.0–81.0 | >81.0–<83.5 | ≥83.5 |
| `E` | ≤31.20 | >31.20–<33.38 | 33.38–35.45 | >35.45–<38.00 | ≥38.00 |
| `r` | <9.90 | 9.90–<10.26 | 10.26–10.56 | >10.56–<11.00 | ≥11.00 |
| `m` | ≤76.5 | >76.5–<79.0 | 79.0–81.0 | >81.0–<83.5 | ≥83.5 |
| `e` | <36.00 | 36.00–<38.66 | 38.66–41.45 | >41.45–<44.00 | ≥44.00 |

Assign scores -2, -1, 0, +1, +2 from left to right. Calculate current-quarter block `C = score(R)+score(M)+score(E)`, forward block `F = score(r)+score(m)+score(e)`, total `T=C+F`, positive-count `P`, and negative-count `N`.

Comparison order is veto first, then strong, medium, weak. Compare `C` with `F` before using `T`; a reported beat paired with a guide-down, or a reported miss paired with a guide-up, is materially mixed. One ordinary contrary score may be tolerated by weak or medium; a strong opposite score may not.

## Classification blockers

Keep the result at HOLD if any required field is absent or non-numeric; the fiscal period, unit, or non-GAAP definition is wrong; contemporaneous official documents conflict; the package is explicitly incomplete or preliminary; results or guidance are withdrawn/restated; or a quantified new contract/JV/supply disruption is expected to affect at least one-tenth of Q1 revenue or available bit supply. Also HOLD for an uncaptured financing, litigation, or M&A consideration/charge of at least $2B.

## Event-driven focus

1. **NBM commitments and customer concentration.** Look in the summary, management quotation, or contract section for the exact agreement count, committed-bit coverage, minimum revenue, guarantees/prepayments, cancellation, repricing, or reopeners; compare with the previously disclosed five agreements. Binding incremental commitments confirm durability, a mere repetition weakens the information value, and a quantified cancellation affecting at least one-tenth of forward revenue or supply falsifies the clean numerical thesis. Role: confirmation; possible HOLD override only at the quantified materiality threshold.

2. **Datacenter growth versus price/mix.** Extract the Q4 end-market table, especially datacenter revenue, and any sentence or table separating ASP, product mix, and bit shipments. Q3 datacenter revenue was $1.467B and the prior surge was heavily price/mix driven. Datacenter growth with stable or rising bits confirms breadth; revenue growth with falling bits weakens it; an enterprise-SSD delay or contraction falsifies the demand-breadth thesis. Role: confirmation, not a weak/medium gate.

3. **Kioxia supply and technology execution.** Look for a quantified JV capacity/outage change and exact Stargate or BiCS8 shipment, qualification, yield, or cost statement. On-time revenue shipments and cost improvement confirm margin durability; a non-quantified roadmap repeat is neutral; a material shutdown, JV impairment, or shipment delay weakens or can veto the quantitative result when the threshold above is met. Role: confirmation or possible HOLD override.

4. **Capital allocation.** Extract the exact repurchase executed, authorization change, new debt/equity issuance, and any transaction consideration. Buyback execution funded by disclosed cash flow confirms per-share quality; merely repeating the existing authorization is neutral; a large uncaptured financing or transaction makes the earnings-only classification unreliable. Role: confirmation or HOLD override.
