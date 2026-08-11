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

## Tanzania

Verdict: serious problems — not in the numbers, in which number we called the country's figure.

- **Accepted, and the plotted value has changed.** We said "NBS presents 4.6 as the country's figure" and
  called it "the figure NBS stands behind". The census report says the opposite, two sentences after
  giving 4.6: "it is recommended to use TFR from TDHS as the official rate." The offices designate the
  survey's 4.8, not the census's 4.6. We now plot 4.8 and say why. The census's 3.2-against-4.6 correction
  is still described, because it is the more interesting content — but it is no longer presented as the
  national figure against the office's own wishes.
- **Accepted.** The source link returned a not-found page. Now points at the report itself.
- **Accepted.** "The 2022 household health survey" is the Demographic and Health Survey, run by the same
  offices — not a separate health-sector survey. Renamed. Zanzibar's statistics office is a co-author and
  is now acknowledged. "Monograph", "Arriaga method" and "vital registration" are all gone or glossed.
- **Verified.** Both printed totals and our re-summed 3.195 and 4.63 are right, and the UN figure checks
  out against an independent mirror.

## Philippines

Verdict: serious problems. Two claims about the source were wrong, and in a way that inverted the
explanation we gave readers.

- **Accepted, wrong claim.** "Registration covers roughly 90% of births" — the authority's ~90% figure is
  the share of births *filed within thirty days*, not the share eventually registered. It states plainly
  that it makes no adjustment for missed births and does not publish a completeness rate at all. Rewritten
  to say what it actually publishes.
- **Accepted, backwards.** We said the data is "tabulated by year of registration". The authority counts
  each birth in the year it happened and closes the books a few months after year end. So the reason our
  figure understates is not mislabeled years — it is that the newest year has had least time to accumulate
  late filings. That inverted explanation is now corrected.
- **Accepted, and it matters for the chart.** Our series falls from 1.61 in 2023 to 1.49 in 2024, a drop
  far steeper than the authority's own reported change in total births. Most of that is probably the
  vintage effect above. The 2024 point is now explicitly flagged as one that should rise, rather than
  presented as a trend.
- **Accepted.** The office and its survey are now named in full, and the survey's figure is dated: it
  describes roughly the three years before late 2022, not 2024.
- **Rejected.** Switching to the survey series to move up a rung. That would replace a rate we build from
  counts with one we copy, and the survey figure is already on the page as the comparison.

## Egypt

Verdict: minor problems. The central claim — that the published rate is modeled rather than counted —
was confirmed verbatim from the bulletin's own footnote.

- **Accepted, unverifiable number.** We said CAPMAS "revised its 2015 figure from 3.7 down to 3.3 between
  editions without explanation". The agent read the 2016, 2017 and 2018 bulletins — none carries a
  fertility table at all — and both editions that do show a back series already agree at 3.3. No 3.7 could
  be found anywhere. Removed, on the same grounds as Nigeria's 5.14 and Vietnam's 1.85.
- **Accepted.** The link was a homepage that leads nowhere; it now points at the catalog that actually
  holds the bulletins. "Crude birth rate" is now "the number of births per 1,000 people", "a very
  different shape" is now about spread across ages, and the sentence about the site being a JavaScript
  application is gone — production detail, not information.

## Thailand

Verdict: minor problems. All three plotted years verified cell by cell against three separate yearbook
editions.

- **Rejected — the agent was wrong, and it mattered that I checked.** It reported the 45-49 births cell
  shown to readers (1,125) as a transcription error against the source's 1,086. The source also has a
  separate row for mothers of 50 and over, at 39, and 1,086 + 39 = 1,125. The number is the deliberate fold
  of the open-ended top group into the last band, the same convention used for Cuba and Australia, and it
  is what produces a standard rate. Had I taken the finding on trust I would have replaced a correct figure
  with a wrong one. It is a fair complaint about the page though: the number matches no single line of the
  source and nothing said why. Now it does.
- **Accepted.** "We divided births by the female population in each age group and summed" describes an
  operation that yields about 0.22, not the 1.11 shown — the step of multiplying by five was missing from
  the description. Fixed.
- **Accepted.** The link was a splash page. Now points into the statistics section.
- **Accepted, and worth knowing.** Other Thai sources circulate about 1.16 for 2022 and 2023 against our
  1.07 and 1.11. Since we construct this rate ourselves and the office publishes none, that discrepancy is
  our problem to disclose, not theirs. It is now on the page, with the most likely cause: the yearbook's
  population table is a registration snapshot rather than a mid-year population, and non-citizen residents
  may be counted in it more completely than their births are registered.
- **Accepted.** The dangling "Earlier editions would extend the series back to about 2017" now says plainly
  that those editions have not been read.
- **Rejected.** Renaming the validation label from "validated" to "calculated" for this country alone. The
  wording covers twenty-four countries and changing one would make the scale inconsistent; the prose now
  says outright that there is no official figure to check against, which is the substance of the point.
