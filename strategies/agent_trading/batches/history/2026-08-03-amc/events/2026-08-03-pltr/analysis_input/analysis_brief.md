# PLTR Q2 2026 first-minute calculation sheet

Use only the first complete official disclosure package. Do not browse, inspect prices, or wait for a call or later filing.

## Required fields

Extract the reporting period, units, Q2 revenue, Q2 AOI and adjusted margin, U.S. commercial revenue, U.S. government revenue, Q3 revenue and AOI guide ranges, new FY26 revenue and AOI guide ranges, new FY26 U.S. commercial amount and growth floors, GAAP operating margin, CFO, capex, adjusted FCF and adjusted-FCF margin. Revenue, AOI, both U.S. segments, Q3 revenue/AOI guidance, and FY revenue/AOI guidance are core fields. If any core field is absent or definition-incomparable, classification is HOLD. Missing GAAP/cash details make Q=0 if no reliability problem is disclosed. TCV, RDV, RPO, deal counts, customers, and NDR are optional and do not affect the score.

All monetary values below are USD billions.

## Locked baselines

Q2 revenue guide 1.797-1.801. Q2 AOI guide 1.063-1.067. Old FY revenue midpoint 7.656. Old FY AOI midpoint 4.446. Old FY U.S. commercial floor 3.224 and growth floor 120%. Old FY adjusted-FCF midpoint 4.300. Q1 U.S. commercial 0.595 and U.S. government 0.687. Q2 2025 comparators: revenue 1.003697 and U.S. commercial 0.306.

## Score R: Q2 revenue

R=+2 if revenue >=1.900; +1 if 1.840<=revenue<1.900; 0 if 1.780<=revenue<1.840; -1 if 1.720<=revenue<1.780; -2 if revenue<1.720.

## Score O: AOI and adjusted margin

O=+2 if AOI>=1.170 and adjusted margin>=61.5%. Otherwise O=+1 if AOI>=1.100 and adjusted margin>=59.5%. O=-2 if AOI<0.930 or adjusted margin<54.0%. Otherwise O=-1 if AOI<1.020 and adjusted margin<57.0%. All other internally neutral or mixed combinations give O=0.

## Score S: U.S. segment breadth

First score U.S. commercial C: +2 if >=0.735; +1 if 0.690-0.734999; 0 if 0.650-0.689999; -1 if 0.610-0.649999; -2 if <0.610.

Score U.S. government G: +2 if >=0.790; +1 if 0.745-0.789999; 0 if 0.690-0.744999; -1 if 0.650-0.689999; -2 if <0.650.

Compress them: S=+2 when C and G are both at least +1 and either is +2; S=+1 when both are non-negative and at least one is positive; S=0 when they have opposite signs or are both zero; S=-1 when both are non-positive and at least one is negative; S=-2 when both are at most -1 and either is -2.

## Score H: incremental H2 revision

Compute delta_h2_revenue = midpoint(new FY revenue guide) - 7.656 - (Q2 revenue - 1.799).

Compute delta_h2_aoi = midpoint(new FY AOI guide) - 4.446 - (Q2 AOI - 1.065).

H=+2 when both deltas exceed +0.050 and either revenue delta is at least +0.100 or AOI delta is at least +0.075. Otherwise H=+1 when both are non-negative and at least one exceeds +0.020. H=-2 when both are below -0.050 and either revenue delta is at most -0.100 or AOI delta is at most -0.075. Otherwise H=-1 when both are non-positive and at least one is below -0.020. All other small, pass-through-only, or opposite-sign combinations give H=0.

## Score F: Q3 and FY U.S. commercial

Compute Q3 revenue midpoint and Q3 implied adjusted margin = Q3 AOI midpoint / Q3 revenue midpoint.

F=+2 if Q3 revenue midpoint>=2.120, implied margin>=60.0%, and the new FY U.S. commercial growth floor>=130%. Otherwise F=+1 if Q3 revenue midpoint>=2.050, implied margin>=58.5%, the new FY U.S. commercial amount floor>=3.224, and its growth floor>=120%. F=-2 if Q3 revenue midpoint<1.900 and implied margin<54.0%, or if the new FY U.S. commercial growth floor<110%. Otherwise F=-1 if Q3 revenue midpoint<1.980, implied margin<56.5%, the FY U.S. commercial amount floor<3.224, or its growth floor<120%. All other neutral or internally mixed combinations give F=0.

## Score Q: GAAP and cash quality

Q=+1 only when GAAP operating margin>=45%, CFO-capex>0, and adjusted-FCF margin>=50%. Q=-1 if GAAP operating margin<38%, CFO-capex<=0, or adjusted-FCF margin<35%. Otherwise Q=0. Missing quality fields also give Q=0 unless the package discloses an unusable reconciliation or reliability problem.

## Classification inputs

Calculate T=R+O+S+H+F+Q. The core drivers are R, O, H, and F.

A hard HOLD applies when the issuer, period, units, or official version cannot be confirmed; any core field is missing or definition-incomparable; a material reclassification prevents comparison; the GAAP/non-GAAP reconciliation is unusable; a restatement, revenue-recognition, collectibility, or cancellation issue makes the package unreliable; or a material unforeseen fact invalidates this policy. A quantified negative fact that remains comparable is not a veto: reflect it in O, S, F, or Q.

An extreme core conflict exists when at least one of R/O/H/F equals +2 and another equals -2. Apply the candidate outcome table from strongest to weakest. If a hard HOLD or extreme core conflict applies, select HOLD regardless of T. Otherwise use T and the core-driver sign requirements exactly. Keep optional fields neutral.
