# Batch 2 — 09:22, 2026-08-11

Ranks 11 to 20: Mexico, Japan, Egypt, Philippines, DR Congo, Vietnam, Iran, Turkey, Germany, Tanzania.
Written up as each landed. The batch turned up the two worst errors found so far — one wrong series and
one country we told readers had no figure when it does.

## Mexico

Verdict from the agent: serious problems, and it was right. I verified before changing anything.

- **Accepted, wrong data.** The plotted series ran from 0.08 in 1951 to 0.69 in 1983, then jumped to 4.21
  in 1985. Those values are impossible. The source table's columns are registration year crossed with age
  band, and registration only begins in 1985 — so every earlier occurrence year held nothing but the few
  births registered decades late. The table has 21,332 births for 1950 against a real total over a
  million. Thirty-five of 73 plotted years were nonsense, and Mexico's headline gap on the map (+2.80)
  was almost entirely that artifact. The series now starts at the file's own first registration year, and
  a check raises if any published year's births fall below half the series median. It caught 2024
  immediately — correctly, that year is still being registered — so incomplete recent years are now
  excluded before the check runs.
- **Still open.** The agent shows the remaining gap over 2019-22 is +0.37 and rising, with a clear age
  gradient in the 2022 bands: 37% short at 15-19, 12% at 35-39. That is the late-registration signature
  we currently only warn about for 2023-24. The warning should extend to the plotted years, and has not
  been written yet.

## Iran

Verdict: a figure is obtainable — so our page was wrong.

- **Accepted, and the country is now plotted.** We told readers no national figure could be found. The
  agent searched the web archive's index of everything ever captured from the statistics office's domain,
  by filename, and found a 2022 report: "Trend of fertility in Iran, 1396 to 1400". I fetched it and read
  table 3 myself. It gives the rate on four bases; the whole-population column is 2.07, 1.97, 1.77, 1.71,
  1.74 for 2017-18 through 2021-22. That is the office's own figure, authored and released by it.
- Iran moves from "No official figure" to "Incomplete registration" — the office itself documents the
  registration gaps it patched, including badly under-recorded births to non-Iranian residents, and
  publishes a separate lower series for Iranian nationals only. Both are now described.
- **Worth doing generally.** Searching a blocked domain's archived filenames, rather than guessing page
  URLs, is a cheap technique we should have used earlier. The UAE is the remaining unplotted country and
  deserves the same treatment.

## Turkey

Verdict: minor problems, but one was a self-contradiction.

- **Accepted.** Block 1 said the population series came from the address register "back to 1935"; block 3
  said pre-2007 figures come from censuses. The register only began in 2007, so block 1 was wrong and
  contradicted our own next paragraph. Now says census until 2006, register from 2007.
- **Accepted, false precision.** We claimed 1.4832 "agrees with TurkStat's own arithmetic to four decimal
  places". It agrees to two. Reworded, and it now says the check covered one year only, which is why the
  country counts as copied rather than validated.
- **Accepted.** "answers a form-encoded POST, though a JSON body gets a 404" is developer detail with
  nothing in it for a reader; cut to whether the portal can be queried automatically.

## Germany

Verdict: minor problems. Every value it could check held up, including the 1991-94 collapse.

- **Accepted.** A sentence about our own GENESIS access token authenticating as a guest account had leaked
  into reader-facing copy. It says nothing about the data. Removed.
- **Accepted.** Destatis publishes the rate separately by the mother's citizenship, and the two are far
  apart; we never mentioned it. Now noted, so the reader knows the single figure averages over a wide gap.
- **Accepted.** "Geburtenstatistik" translated in the source line; "two traps we checked and got right",
  "ships one year per report edition" and "biases a recalculation" all rewritten plainly.
- **Rejected.** Moving to the Human Fertility Database to earn the validated label. It lags several years
  and would not cover the provisional years we show, so it would mean maintaining two vintages for one
  label.

## Japan

Verdict: minor problems. Values check out; the population-base description is right.

- **Accepted.** The link went to the portal's homepage rather than the tables we name. Now deep-links.
- **Accepted.** "Births are published only in five-year bands" is not true from 2015, when a single-year
  table starts — it covers births within marriage only. Narrowed rather than dropped.
- **Accepted.** The ministry and the statistics portal are now named in full on first use. "Coarse" is now
  "less precise". The sentence about the API key needing email registration is gone: same category as
  Germany's, production detail rather than information.
- **Accepted.** We implied that picking the Japanese-only population column resolves the mismatch. A
  government methodology report shows a residual remains — a child of a foreign mother and a Japanese
  father is in the births but the mother is never in a Japanese-only population. Now said.

## DR Congo

Verdict: minor problems. Both values verified against the source, and our arithmetic check reproduced.

- **Accepted.** The source line was untranslated French with an unexplained abbreviation, useless to a
  reader who does not read French. Now "National Institute of Statistics — Demographic and Health Survey".
- **Accepted.** "there is nothing to recalculate from" and "returns a block" both rewritten plainly.

## Vietnam

Verdict: minor problems, but one number had to go.

- **Accepted, unverifiable number.** We stated that dividing the 2019 census's own counts gives 1.85
  against the published 2.09, "so the correction that year was about 13%". The agent could not reproduce
  it: the office publishes only the already-corrected rate, never the figure before correction and never
  the coefficient, so there is nothing public to check 1.85 against. Worse, 1.85 appears in the same census
  materials as the rate for university-educated women and as several provinces' rates — so it may be a
  mix-up rather than a coincidence. Same species as Nigeria's 5.14, and removed on the same grounds. The
  substantive point survives without it: the office raises its figure by an amount it never publishes, so
  how far the published rate sits from a count cannot be worked out from anything public.
- **Accepted.** "Trussell P/F technique" was a proper noun with no meaning attached for a reader; now
  described rather than named. "PxWeb" is now "online database". The dense sentence about the 2025 survey
  tables is split and plainer.
- **Verified and left alone.** Every value the agent could check matched the source exactly, 2009-2019
  digit for digit, and the claim that the office corrects upward without publishing the adjustment is
  confirmed almost verbatim from the census volume.
