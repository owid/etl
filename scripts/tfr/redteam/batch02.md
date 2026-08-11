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

## Democratic Republic of Congo

Verdict: minor problems. Both values verified against the source, and our arithmetic check reproduced.

- **Accepted.** The source line was untranslated French with an unexplained abbreviation, useless to a
  reader who does not read French. Now "National Institute of Statistics — Demographic and Health Survey".
- **Accepted.** "there is nothing to recalculate from" and "returns a block" both rewritten plainly.

Reviewed a second time by accident — this log had it under "DR Congo" while the registry calls it
"Democratic Republic of Congo", so `--next` never counted it as done. The heading is now the registry
name. The second pass earned its keep:

- **Accepted, and it generalizes.** A survey fertility rate describes roughly the three years before the
  fieldwork, not the year of the report. The institute says so outright for the earlier round — it
  attributes the 6.6 to 2011-2013 — and the 5.5 covers about 2021-2023. We plot them at 2014 and 2024, so
  both points sit about two years later than the fertility they measure. Now stated in the caveats.
  **Not fixed in the plotting**, and deliberately: about half the collection is survey-based, so moving
  the points is a change to the whole chart's convention, not a DRC fix. Flagged for a decision.
- **Accepted.** The report's own appendix recomputes the rate allowing for the sample design and gets the
  same 5.47 we did, with a range of 5.23 to 5.71. That is a stronger check than our age-band sum and it
  was already in the linked PDF, so it is now cited alongside.
- **Rejected as a page problem, accepted as a brief problem.** The agent objected to the word "average" on
  the map difference. The published page says "Difference" and shows one year. The word "average" was in
  the brief `redteam.py` generates, which also computed it over every shared year — so every agent so far
  was told the map shows something it does not. Fixed in `redteam.py`: the brief now names the single
  latest national year the map actually colors.

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

## Italy

Verdict: minor problems. Every spot-checked year matched, and the record-low framing is confirmed by
ISTAT's own release.

- **Accepted.** The third block described our own query failing — a dataflow covering every municipality
  and timing out. Fourth country in this batch with production detail in reader copy. Replaced with what
  the reader needs: there is no age-band comparison for Italy.
- **Accepted.** Nothing told the reader which population the figure covers, though ISTAT publishes it three
  ways — all residents, Italian citizens, foreign citizens. Our values are the all-residents series; now
  said.
- **Accepted, with a correction to the agent.** It argued the series is truncated at 2000, hiding that
  Italy already reached 1.19 in 1995, which weakens the record-low framing. The truncation is the chart's,
  not Italy's — every country starts at 2000. So the fix is wording, not data: the page now says 1.18 is
  the lowest recorded but only just, that 1.19 came in 1995, and that the earlier trough is off the left
  edge of this chart.
- **Accepted.** "SDMX service", "ANPR" and "stato civile" all replaced with plain descriptions.

## England and Wales

Verdict: serious problems. All 16 spot-checked years matched to three decimals, including the 1964 peak
— but three statements around them were wrong.

- **Accepted, false claim about a source.** We said Northern Ireland's office "refuses scripted downloads
  outright, so it would not be reproducible" — and used that to justify not building a UK-wide figure. The
  agent downloaded NISRA's own births and fertility tables with one plain request. The claim is both
  unreproducible and a statement about our tooling rather than the data. Removed, and the page now says a
  UK-wide series could be built and is worth doing.
- **Accepted, stale numbers contradicting our own chart.** We quoted England and Wales against the UK as
  "1.65 against 1.63 in 2019, 1.58 against 1.56 in 2020". Those came from a reference table built on
  pre-2021-census populations; our own chart plots 1.599 and 1.532 for those years. A reader checking the
  prose against the chart would have found a 0.05 mismatch. Recomputing from the three offices' current
  tables puts the UK about 0.005 to 0.01 below England and Wales — 1.409 against 1.415 in 2024 — because
  Scotland has fallen much further and Northern Ireland stayed well above, and they nearly cancel. The
  conclusion held; the numbers behind it did not.
- **Accepted, backwards.** "The 15-49 column is empty for most recent years" — it is empty for every year
  from 1938 to 2024 and filled for 2025 alone.
- **Accepted.** The age-band group labelled 40-44 is ONS's "40 and over" row and includes mothers of 45 and
  above. Now stated rather than left to be discovered.
- **Logged, not built.** A genuine UK-wide series from the three offices' tables: the agent puts it at half
  a day to a day, so it misses the cheap bar — but the excuse for not doing it is gone.

## South Africa

Verdict: serious problems. Our figure was two editions out of date.

- **Accepted, stale source.** We were reading the 2024 mid-year estimates. Stats SA revised the whole
  series down in its 2025 and 2026 editions — 2024 went from 2.41 to 2.15. Re-sourced to the 2026 edition;
  the series now runs 2002 to 2026. The age-band comparison still uses the 2024 edition's female
  population, because the birth counts it divides are that year's.
- **Note for later.** The sheet name in the 2026 workbook is "Total Fertilty Rate" — the office's own
  typo. Pinned as-is; a corrected spelling in a future edition will break the loader loudly, which is the
  behavior we want.

## Kenya

Verdict: minor problems.

- **Logged, not built.** Kenya publishes a registration-based series in its vital statistics reports that
  would sit above the survey rounds we plot. It is a real upgrade but not a cheap one.

## Colombia

Verdict: minor problems, and one framing error worth fixing.

- **Accepted.** We said DANE's projection-based rate sits "far above what its own registry shows" and left
  it there, which reads as the office ignoring its own data. DANE revised that assumption down in July 2025,
  saying births had fallen faster than it first projected — so it is moving toward the registry, not away.
  Now said.
- **Accepted.** DANE itself warns that its registry rates are raw and will differ from figures that adjust
  for missed births. That is the producer's own account of why an estimate like the UN's sits above our
  line, and it belongs in the caveats. Added.
- **Accepted.** The gap against the UN is recent — near zero as late as 2015-18, about 0.4 since. Stating
  that stops a reader from reading it as a permanent disagreement.

## France

Verdict: serious problems. Our stated reason for a choice was backwards, and the choice was wrong.

- **Accepted, and the claim was false.** We told readers INSEE's mainland series exists but that "mainland
  France is not the territory the UN figures cover, so switching would trade one mismatch for another."
  The opposite is true. The UN's "France" excludes the overseas departments — it carries Mayotte, Reunion,
  Guadeloupe, Martinique and French Guiana as separate entities, which I confirmed in our own catalog. And
  the numbers say the same thing: over the thirty years the two sources overlap, the mainland series is
  0.004 from the UN's on average against 0.020 for the whole-republic series we were plotting. Switched to
  the mainland tables. Both source files already carried mainland sheets, and the mainland population
  pyramid turned out to be one path segment away, so the whole fix was four lines.
- **Bonus from the switch.** The Mayotte discontinuity we were warning readers about and had not corrected
  does not exist in the mainland series: Mayotte entered the whole-republic figures in 2014 but was never
  in the mainland ones. That caveat is now a statement that there is no break.
- **Accepted.** "The 2025 figure is provisional" understated it. INSEE marks 2023, 2024 and 2025
  provisional — its own footnote reads "(p) résultats provisoires à fin 2025". All three now named.
- **Accepted.** The untranslated quote and "vintage" are gone, and the paragraph about INSEE's undocumented
  single-year-of-age endpoint is gone with them: that was our tooling, not the data.

## South Korea

Verdict: minor problems, but one of them was hiding five years of data.

- **Accepted, and it bought us data.** We told readers "editions before 2013 print only the current and
  previous year, which is why the series starts in 2003." The 2010 edition prints an eleven-year run back
  to 2000. Added it; the series now starts in 2000.
- **Accepted.** The office's own preliminary release, out each February, already gives 2025 at 0.80 — the
  second yearly rise in a row after the 2023 trough. Added as a fill-only source, so a final edition
  always wins over a preliminary one for the same year. The series now ends 2025 instead of 2024.
- **Accepted.** The KOSIS paragraph — no API key without a Korean phone number or identity number, "no
  route for a foreigner" — was entirely about our own access problems. Cut.
- **Accepted.** "Each edition's first table gives an eleven-year run" is wrong for the older editions,
  where the fertility table is the second one. Now just "a table".
- **Accepted.** The producer has been renamed: Statistics Korea became the Ministry of Data and Statistics
  on 1 October 2025, which is why kostat.go.kr now redirects to mods.go.kr. Confirmed on the agency's own
  English site. Source line updated, with the old name kept so the cited editions still make sense.
- **Explained rather than fixed.** The agent flagged 2004 showing three decimals where its neighbors show
  two. That is the source's own precision: the earliest edition prints three. Now that 2000-2004 all come
  from that edition it reads as a block rather than one odd year, and the caveats say so.

## Sudan

Verdict: minor problems. The plotted 5.2 verified against the archived source, and every claim about the
office's dead website checked out.

- **Accepted.** We said "the 2008 census files that survive give population by area only, with no age
  breakdown." The agent found a 35MB archive of census tables from the same domain containing both
  population by five-year age group and a folder of fertility tables by age of mother. True of the
  per-state files, false as written.
- **Worth knowing.** The agent tried building a TFR from those tables and got about 1.6 — far too low, the
  signature of a twelve-month recall question, and probably why the office's own history table skips 2008.
  So the door it opened may not lead anywhere. The sentence is still wrong.

## Myanmar

Verdict: serious problems. The plotted figure and all seven age bands verified exactly, but the two claims
around them were both wrong.

- **Accepted.** "Nothing more recent is published — a 2024 census has been run but its fertility results
  are not out." They are out: the 2024 census Union Report, published October 2025, gives 1.4 by the direct
  method and 1.8 by the indirect one, and its appendix volume carries the same births-and-women table we
  used for 2019. That is a five-year jump and a very large fall — worth doing properly rather than
  patching, so it is queued as an update, not a wording fix.
- **Accepted.** "The 2014 census gave 2.5 by the same kind of table" mixes methods. The like-for-like 2014
  figure, from the same office's own comparison table, is 2.3; the 2.5 comes from a different report that
  applies an indirect correction. Ours overstated the comparability of the 2014-to-2019 fall.

## Uganda

Verdict: minor problems. Every age-band count and both plotted figures verified digit for digit against
table 7.2, and the agent independently confirmed that the office's own headline 4.5 is the rounded
*adjusted* rate, which is what we plot.

- **Accepted.** The broken-certificate sentence was our download tooling. Cut.
- **Accepted.** "Brass P/F adjustment" and "a change of instrument" both replaced with plain descriptions,
  keeping the method's name as a trailing gloss rather than the explanation.
- **Noted, not changed.** The agent thought "TFR copied from source, not validated" contradicts a caveats
  block that describes recomputing the rate. It does not: the recomputation uses the census's own two
  columns, not an independent count of births and women, which is what the other label means. Two other
  agents raised the same reading, so the label wording is the problem rather than Uganda's use of it.

## Algeria

Verdict: minor problems. Both specific numbers in our own arithmetic check verified exactly, including the
implied 1,032,000 births against the bulletin's 1,034,000.

- **Accepted.** The certificate-chain sentence was our tooling. Cut.
- **Accepted.** "labour force surveys" — British spelling. Fixed.
- **Accepted.** A full plain-language pass on the caveats: "age structure of births", "age curve",
  "population denominator", "rolled forward", "natural increase", "under-registration" and "coverage
  factors" all replaced with plain phrasing, and the 2017 typo described concretely as "7102".
- **Open.** The agent argues "Complete registration" sits badly with ONS's own statement that raw
  registration "ne couvre pas les évènements dans leur totalité", corrected by factors unrevised since
  2002. It could not find the size of the correction, and neither could I. Left as-is pending that number.

## Cross-cutting, from this batch

- **The recurring one, now swept.** Sentences describing our own tooling rather than the data — broken
  certificates, blocked scripted requests, missing API keys, JavaScript pages a script cannot read, stale
  paths returning HTML with a 200 status. Cleared out of Mali, Mozambique, Russia, Nepal, China, Turkey,
  Argentina, Afghanistan, Algeria, Iraq, Uganda, Sweden and South Korea in one pass. Kept where the
  producer's site being gone is itself the reason a figure is missing or came from an archive, which is a
  fact about the data.
- **Open, page-wide.** Three agents in a row read "TFR copied from source, not validated" as claiming more
  than it does, and all three flagged the bare acronym on a page that otherwise always spells out "fertility
  rate". Wording change to both labels, not a per-country fix.

## Iraq

Verdict: serious problems. All five plotted points verified against COSIT's own tables, but we told
readers a census result was unpublished when it is out.

- **Accepted, and the series now runs to 2024.** We said "Iraq ran its first full census since 1987 in
  November 2024, but only headline counts are out; the fertility results are not published yet." They are:
  table 8/2 of the 2024 Annual Statistical Abstract gives a fertility rate by governorate, Iraq 3.1,
  Kurdistan Region 2.6. I read the table myself. Added, so the line now ends in 2024 rather than 2011.
- **Accepted.** The 1997 point (5.7) was plotted but never mentioned in the prose, even though it is one of
  the rows in the very table the prose describes. Now named with the others.
- **Accepted, and it reverses a claim.** "The federal figures do not cover [Kurdistan]" is no longer true
  of the 2024 census, which reports the region separately. Narrowed to the older survey rounds, where the
  zero-for-three-governorates table really does show the gap.
- **Accepted.** "organised" → "organized"; "pipeline" and "on the same reasoning as Nigeria" both gone —
  the second assumed the reader had read another country's entry.
- **Logged, not plotted.** A 2018 survey round reportedly measured 3.6. The report is hosted on a
  Kurdistan Regional Statistics Office path that serves HTML rather than the PDF, so I could not read it.
  The page now says a 2018 figure is reported and why it is not on the chart.

## Spain

Verdict: minor problems. The agent reproduced our whole 2024 recalculation from INE's raw tables and
checked our "within 0.01 every year" claim across all sixteen years, not just the two we quote — the worst
year is 0.009. That claim stands.

- **Accepted, dead link.** The link we showed readers 404s on INE's own server. One missing query
  parameter; confirmed the fix returns 200 and lands on the births statistics page.
- **Accepted.** "female population by single year of age every quarter back to 1971" overstates it: twice
  a year from 1971, quarterly only since 2021.
- **Accepted.** "The 2024 figure is the first release and will revise slightly" overstates how unsettled
  it is. Replaced with what is certainly true — INE keeps revising the population these births are divided
  by, so recent years can still move.
- **Accepted.** INE glossed on first use, "Estadística Continua de Población" replaced with a plain
  description, and the sentence about whole-table CSV downloads with no key cut.

## Argentina

Verdict: minor problems. Every plotted value verified against the ministry's own microdata and printed
yearbook, and the steep fall independently corroborated against the national identity registry's series
for all twelve years — it is real, not an artifact of an incomplete final year.

- **Accepted, dead link.** 404. The live dataset is one path segment away.
- **Accepted, and this one was our own invention.** We said the ministry's "own tables show 95% of a
  year's registrations occurred that year and almost all the rest the year before". The ministry actually
  says more than 95% are registered within three months — a different fact — and separately reports that
  comparing registrations against the 2010 census left a 6% shortfall, still 3.8% after four years of late
  records. We had fused a timeliness statement with a completeness one into a number that is in neither.
  Both facts now stated as the source states them.
- **Accepted.** The ministry warns that provinces had delays sending 2023 figures. That bears directly on
  the years driving the fall and was missing. Added.
- **Accepted.** "unlike Mexico there is no need to stop the series early" assumed the reader had read
  Mexico; "vintage", "basis" and "seam" all replaced with plain wording.
- **Accepted.** "the roughly 2.3 usually quoted" for 2014 undersold the match — the identity registry
  publishes 2.36 against our 2.35. Now cited.

## Poland

Verdict: serious problems, all in the caveats — the plotted values check out against GUS's own published
rate for 2023 and 2024.

- **Accepted, overclaim.** We told readers GUS's residence rule "as a matter of law takes in Ukrainian
  nationals given temporary registration after 2022 — so part of the recent fall is the denominator
  growing." The government's own demographic report describes the population GUS uses for its rates as
  excluding people staying temporarily, which points the other way, and flags the whole question as
  unsettled. Rewritten to say plainly that it is unclear, which way it would cut, and that GUS does not
  quantify it.
- **Accepted.** "Nothing is flagged provisional, even 2025." GUS finalizes a year's rate in the November
  of the following year and has not done so for 2025, so ours should be read as provisional whatever the
  database says. Now said.
- **Accepted, and it hides an extension.** "The series starts in 2013 because that is where the mid-year
  population by single age begins" is true of the database we read, not of GUS's data. Population by single
  year of age exists back to at least the 2002 census, and the Human Fertility Database carries Poland from
  1971. Reworded, and the possible extension is now stated rather than implied away.
- **Accepted.** Added the reason Eurostat's Polish figures run 0.03 to 0.04 above GUS's — a different
  definition of resident — since a reader comparing sources will hit it.
- **Rejected.** The agent reported the prose claiming 1.068 for 2025 against a chart showing 1.067. That
  is my brief generator printing four significant figures of 1.0675; the page itself does not disagree.
- **Accepted, cut.** The note about the printed yearbook reporting births in thousands while the database
  returns raw counts is a note to whoever builds the pipeline, not to a reader.

## Afghanistan

Verdict: serious problems, though the plotted 5.3 verified exactly — including against the survey
programme's own API, where the seven age-specific rates sum to our 5.29.

- **Accepted.** The link we showed readers was the successor office's homepage, which never held the 2015
  report and offers no path to it. Now links to the archived report we actually read.
- **Accepted, softened.** "No Afghan source gives female population by age group outside it" is probably
  false — the statistics office publishes population by sex and age group, reproduced in the UN
  Demographic Yearbook. Neither the agent nor I could reach the office's own site to name the release, so
  the claim is now about what we did not find rather than what does not exist.
- **Accepted.** A 2022 household survey is reported to have measured 5.4. Neither of us could reach the
  report, so it is described and not plotted.
- **Accepted.** "vital registration", "age-specific rates", "band width" and "weighted sample" all
  replaced with plain descriptions, and the web-archive sentence rewritten around what it means for the
  reader rather than how we found the file.

## Saudi Arabia

Verdict: serious problems, in the label rather than the numbers. Every plotted year verified against
GASTAT's own Population Estimates release, including the three-way 2024 split of 2.7 / 0.8 / 2.0 and
the 44% non-Saudi share.

- **Accepted, and the label was contradicting our own text.** GASTAT's methodology documents are
  titled for population *projections* and estimates, define the product as assumption-driven, and
  derive the rate by dividing registered births by a population rolled forward from the 2022 census.
  Our caveats already said as much — "registration data inside a model rather than a straight count" —
  while the label above them said Complete registration. Now Projection only, the same call as
  Algeria.
- **Accepted.** "Getting the nationality split wrong here would be worse than any other country in
  this dataset" is a warning to whoever builds the collection, not to a reader, and "GASTAT resolves
  the trap for us" has the same problem. Both rewritten to say what the reader needs.
- **Accepted.** "Cohort-component projection" replaced with a plain description of what the method
  does.
- **Accepted.** The link went to a generic category browser several clicks from the figure. Now the
  Population Estimates release itself.
- **Open.** Whether a 2025 figure exists could not be settled — the agent found no release, but also
  found a GASTAT methodology note updated this month saying data run to 2025. Worth a recheck.

## Ghana

Verdict: serious problems. The three plotted values verified, but one of the things around them was a
quotation we appear to have made up.

- **Accepted, and this is the worst kind of error.** We put in quotation marks that the office says
  comparing the census against the survey before it "clearly indicates miss-reporting of births". The
  agent, reading the source, reports its actual language is "indicating possible under-reporting of
  births" — hedged where ours was categorical. I could not reach the report myself to adjudicate:
  every URL I tried for the 2010 analytical report is dead and the web archive rate-limited me. So I
  have removed the quotation marks entirely and stated the office's position without claiming its
  exact words, which is right whether or not the phrase exists. **A paraphrase must never sit inside
  quotation marks. Worth checking the other quoted phrases in this collection for the same fault.**
- **Accepted, a missing round.** Ghana ran a survey round in 2014 reporting 4.2. It was absent, so
  the line jumped fourteen years from 2008 to 2022. Added.
- **Accepted, false claim.** "The fertility volume has still not appeared five years on" — the agent
  retrieved it and reports a national figure of 3.1. I could not reach it either, but 3.1 is exactly
  what our own independent division of the census counts gives, which corroborates the number even
  though I could not confirm the volume. The claim about it being unpublished is gone; the reason we
  do not plot it — no corrected figure alongside it — is unchanged and still stands.
- **Accepted.** "we do not use it and neither should anyone else" addresses other researchers. Gone.
- **Accepted.** The office is now named in full, "relational Gompertz" is glossed rather than dropped,
  and the sentence about a browser-only publication list and an unreachable database is cut.

## Malaysia

Verdict: serious problems, and one was a wrong number on the page.

- **Accepted, and it was a real error.** The age-band panel showed 967 births to mothers aged 45-49 in
  2024. The registered count is 1,278 — we were 24% low. The cause: we had no birth counts, so we
  multiplied DOSM's published rate by the female population, and for 2024 DOSM printed that rate
  rounded to a whole birth per thousand women. One instead of 1.32, on a population of 967,400, is
  the entire error.
- **Accepted, and it is why the error existed.** "DOSM does not release those counts" is false. Table
  3.7 of its annual Vital Statistics report gives registered births by age of mother, by state. I
  fetched it and read the national row: 8,088 / 47,655 / 123,935 / 133,139 / 79,127 / 20,623 / 1,278,
  summing to the row's own total. The panel now uses those counts, and the reader gets registered
  births rather than a number implied by the rate.
- **Consequence worth stating.** The old method reproduced DOSM's rate by construction, because the
  rate was what built it. Dividing the real counts by the female population gives 1.55 for 2024
  against DOSM's published 1.60. Rounding explains part of it and not all, and DOSM does not say what
  population its own rate divides by. That is now on the page instead of a circular check that could
  never disagree.
- **Accepted.** "DOSM does not flag it" is wrong — the report labels 2024 preliminary. Our
  decimal-counting inference was also unreliable: 2023 carries the same whole-number rounding.
- **Accepted.** "Population for 2011-19" should be 2015-19, which is the range DOSM's own note gives.
  "2024 onward are projections" conflated preliminary registered data with a projection.
- **Accepted.** "The mirror image of the Japan trap" assumed the reader had read Japan; "on both sides
  of the division" is jargon; the sentences about parquet files with no key and the old site being a
  shell are our tooling. All gone, and the office is named in full.

## Madagascar

Verdict: minor problems. All three plotted values verified against the survey reports, both of our
arithmetic checks reproduced from the census's own tables, and the registration figures confirmed to
the percentage point.

- **Verified, not changed.** The agent found the passage behind our strongest claim and quoted it:
  the census volume's P/F ratios are above 1 in every age group, its own table gives 4.3 declared
  against 4.7 corrected, and it states "il n'y aura aucun ajustement des données" — the data will not
  be adjusted. Our account was accurate.
- **Changed anyway, on a general rule.** That was a translated quotation sitting inside quotation
  marks, which implies exact words that exist only in French. It is now reported speech. Same for
  Mali and Niger, whose quoted phrases were also translations. A sweep of the whole collection found
  eight quoted phrases: four are titles or a term of art and stay, one from Stats SA is verbatim in
  the source and stays, and the four translations or unverifiable ones are now paraphrase.
- **Accepted, link mismatch.** The source line names the Demographic and Health Survey but the link
  went to the census landing page, which holds no survey material at all — none of the three plotted
  values or the registration figures could be checked from it. Now links to the 2021 survey report.
- **Accepted.** "Everywhere else that computes a correction applies it" leans on entries the reader
  has not read. Now says plainly that computing a correction and then declining to use it is unusual.
- **Accepted.** The two 4.8s — the 2008-09 survey's figure and the census's own wider-age-range
  figure — sat one sentence apart and read as the same number checked twice. Now distinguished.
- **Accepted, softened.** "INSTAT says why: in a country where the system is not functional..." could
  not be found stated in those terms in any of the three volumes. Now says only that the census is
  the only source for the age structure, without attributing a rationale.
- **Accepted.** INSTAT and the survey's name are spelled out, the Brass check is described rather
  than named, and "registration is better than in much of the region" — an uncited comparison — is
  gone.

## Cote d'Ivoire

Verdict: serious problems. Two of the numbers on the page were wrong and the newest survey round was
missing because we said it did not exist.

- **Accepted, and the series now reaches 2021.** We told readers "a survey was fielded in 2021 but no
  report for it was ever published on the institute's site." The report exists. I fetched it and read
  the trend chart myself: it gives 4.3 for 2021, alongside 5.3 for 1994, 5.2 for 1998-99 and 5.0 for
  2011-12. Added, so the line ends in 2021 rather than 2016.
- **Accepted, wrong number.** We stated 5.7 for 1994. That same chart — and the 2011-12 report's
  chart independently — gives 5.3. Corrected.
- **Accepted, and it changes what the source line claims.** The source line said Demographic and
  Health Survey, but the 2016 figure of 4.6 comes from a different survey program, the Multiple
  Indicator Cluster Survey. So the 2016 point is not strictly comparable with the ones on either side
  of it, which is now said rather than hidden behind one label.
- **Accepted.** The office was reorganized in 2024: INS became ANSTAT. The page still presented INS as
  current and linked to its dead domain. Both fixed, and the link now goes to the 2021 report.
- **Accepted.** The sentence diagnosing the dead site — resolves but serves a placeholder, since
  January 2026 — is our access problem. Gone, along with "the same gap as Mozambique."
- **Accepted, with a caveat added.** The 72% registration figure for 2016 appears to count children
  holding a birth certificate, a narrower thing than the 55% and 65% figures beside it, which count
  registration. Now flagged rather than presented as a clean trend.

## Nepal

Verdict: serious problems. This was the worst error in the collection: we accused a statistics office
of publishing a figure it could not account for, and it accounts for it in its own report.

- **Accepted, and the accusation was false.** Our page said "no method is named anywhere in the report
  that would explain the difference — we searched for every correction technique other offices use, in
  English and Nepali, and found none." The office's own 2025 fertility report names it plainly. I
  fetched the report and read the passage: "Due to underreporting issues around fertility in censuses,
  an indirect approach is used to estimate fertility rate. The ASFRs and the TFR are calculated using
  the Arriaga method (Arriaga, 1983), which relies on more robust children ever born (CEB) data." It
  goes on to explain that a post-enumeration check found children under five undercounted, and that
  the raw 1.56 sits against 2.1 from its household survey and 2.0 from another — which is why it
  corrected. Its historical table shows an indirect method used at every census since 1971.
- **Accepted, and our conclusion was backwards.** We told readers "the 1.94 should not be relied on."
  Two independent surveys put fertility near 2.0. The outlier is the raw 1.556, not 1.94. The entry
  now explains the raw-versus-corrected split, says the correction is the one to trust, and says why
  adding up the age bands below does not give the plotted figure.
- **Accepted.** The sentence about what we searched for, in which languages, is about our own process
  and would not belong even if it had been right. Gone, with the roll call of six other countries.
- **Accepted.** "A quarter lower" is wrong arithmetic for 1.556 against 1.94 — it is about a fifth.
  The unglossed "gross reproduction rate" and "open census API" are gone with the rewrite.
- **Lesson.** Twice now — here and Ghana — the page has stated more confidently than we could support
  that a source fails to document something. An absence of evidence found by us is not evidence of
  absence in the source, and it should never be written as though it were.

## Peru

Verdict: minor problems. The agent traced every plotted number to INEI's own tables and reproduced
both years it checked in full — 1.513 for 2024 and 1.687 for 2023, exact.

- **Accepted, undisclosed scope.** The birth table we use covers only births registered online, 97 to
  99% of them depending on the year. That was not disclosed anywhere. Now stated in the first block.
- **Accepted, and the caveat was too weak.** We said registration lag makes the latest year "the least
  final" and left it there. Reconstructing occurrence years from INEI's own tables, the agent found
  late records lift a year's total by roughly four to five percent once the following year's report
  lands — enough to account for about half the gap between our figure and INEI's own survey. 2024 has
  had no such catch-up at all. The page now says the recent years should be expected to rise, and why.
- **Accepted.** "Peru follows the Colombia and Mexico pattern" leans on two entries the reader has not
  seen. Gone.
- **Accepted.** "Each annual annex sits at its own unrelated file id, with no pattern to follow" is a
  note about the source's file naming, which is our problem. Gone.
- **Accepted.** INEI spelled out, "vintage" replaced, "0.5 below" given its units, "least final"
  replaced with "least complete".
- **Logged, not built.** The series looks extendable back to 2021, and the health ministry's own live
  birth certificate system would avoid the registration-lag problem entirely — INEI has itself switched
  to it in another publication. Both are real upgrades; neither is cheap.
- **Open.** The link is INEI's whole publications listing rather than the report and annex actually
  used, so a reader cannot check anything from it. INEI's per-report file ids have no pattern, so a
  stable deep link needs picking by hand; left as it is for now and worth fixing.

## Yemen

Verdict: minor problems. The plotted 4.4 verified against both the Arabic preliminary report and the
English final report, our arithmetic check reproduced, and the two competing census figures confirmed
in the office's own yearbook.

- **Accepted, and it changes what the page implies.** We presented the census's 6.1 as coming from
  "adding up rates by age group" and then said a separate study reaches 6.1 by an indirect method —
  which reads as two independent calculations agreeing. The agent found the office's own census
  monograph indicates the rates by age group were scaled to match the indirect estimate, so there is
  really one estimate, written back into a rate table. Now said that way.
- **Accepted.** The survey's own citation names the health ministry as lead and the statistical office
  as collaborator; our source line had it the other way round. Swapped.
- **Accepted.** "So this report came from a web archive" is our retrieval process, and redundant with
  the archive link the reader can already see. Gone.
- **Accepted.** "One figure inside our window" and "a judgment about method rather than a lookup" are
  both jargon — the first is about the chart's own construction, the second is from computing. Both
  rewritten.
- **Noted.** The reader is walked through an age-band calculation but shown no age-band table for
  Yemen, because the source publishes rates rather than counts. Not fixable from the published report;
  only the survey microdata would give the counts.

## Canada

Verdict: serious problems. The extraction and arithmetic are sound — the agent rebuilt our whole
series from Statistics Canada's own bulk files and reproduced it — but our central validation claim
was false and one source fact was backwards.

- **Accepted, and it was a bug in our own check.** We told readers our figures "match Statistics
  Canada's own published rate to the second decimal in every year we checked" and gave three examples.
  I recomputed all 34 years, rounding half-up rather than trusting the binary float: **12 of them
  differ by 0.01**. One of the three examples we quoted is itself a false match — 2024 is 1.2553, which
  rounds to 1.26, not the 1.25 we claimed to match. Python's `round(1.255, 2)` returns 1.25 because
  1.255 is stored as 1.25499999999998934, so the disagreement vanished by accident. What is true, and
  what the page now says, is that we land within 0.01 in every year, with a largest gap of 0.009, and
  fall on the other side of a rounding boundary in about a third of them.
- **Accepted, factually wrong.** We said "births to mothers of 50 and over are folded into the 45-49
  row for confidentiality." Statistics Canada's own footnote 4, which I read in the table's metadata
  file, says they go into "Age of mother, not stated." That matters for what we then do with them:
  because we scale the bands up by the not-stated total, those births end up spread thinly across all
  seven bands rather than sitting at the top. Now described correctly.
- **Accepted.** "So anything automated has to treat the latest year as provisional by hand" is about
  our pipeline. "There is no citizens-only denominator to pick wrongly here" only makes sense to
  someone who has read another country's entry. Both gone, replaced by the Canada-specific fact.
- **Accepted.** "No key", "denominator" and "so recent years do not understate" all rewritten plainly.
- **Rejected as a page problem, accepted as a brief problem — for the second time.** The agent
  objected that the age-band births are shown at absurd precision, like 4492.670822915356, and that
  presenting a redistributed estimate as a count is misleading. The page shows no numbers at all
  there; it draws two dot charts. The raw floats were `redteam.py` printing the cache verbatim. Now
  rounded, and the brief says what the reader actually sees. That generator has now misled agents
  twice, having also claimed the map showed an average gap. **Anything the brief asserts about the
  page needs checking against the page, not against the cache.**

## Cameroon

Verdict: minor problems. Both plotted values verified against the survey reports, the 2005 census's
raw-and-corrected pair confirmed exactly, and our own arithmetic check reproduced decimal for decimal
against the census annex — 4.1607 urban and 6.2130 rural.

- **Accepted, and the claim had gone stale.** We said Cameroon has had "no fertility fieldwork since
  2018, so the newest national figure is eight years old." The office's own site describes a household
  survey in the field through 2026. The headline figure is still the 2018 one, so the page now says
  that, and says a newer figure should follow, rather than asserting nothing has happened.
- **Accepted, backwards.** We said the census's "published long-run trend uses corrected values
  throughout while showing the raw ones only in a technical annex." The raw 2005 figure sits in a
  main-body table; what is in the annex is the corrected-value working. Rewritten to say both appear
  and that the results chapter's trend table uses the corrected ones.
- **Accepted, and it conflated two institutions.** The sentence about a census bureau website "parked
  on an expired hosting account" is our own sourcing problem, is now out of date — the census bureau
  relaunched and serves the identical volume directly — and named no institution, so a reader would
  have taken it for the very-much-alive office linked at the top of the entry. Cut.
- **Accepted.** INS spelled out, the survey's name translated in the source line, the Brass method
  described rather than named, and the four-clause sentence about the fourth census split up.

## Morocco

Verdict: minor problems. Every plotted value verified against HCP's own publications, and two of the
methodology claims matched HCP's wording almost verbatim — the 30% long form and the distinction
between scaling a sample up and correcting for missed births.

- **Accepted, and we were plotting the worse number.** We showed 2.00 for 2024 and told readers in the
  next breath that the census figure is 1.97, justifying it as keeping the line on one source. That
  does not hold: 1.97 is HCP's too, one publication later. I fetched the 2024 census volume and read it
  — ICF-National 1,97, urban 1,77, rural 2,37. Now plotting 1.97.
- **Accepted, dropped.** "Birth registration is essentially complete at 99.5%" could not be traced to
  any Moroccan source, and the one independent figure the agent found was 96.9%. It was not
  load-bearing — the sentence itself said registration is not what the rate is built on — so it is gone
  rather than restated at a number nobody can source.
- **Accepted.** "Watch out for a separate figure of 2.38 sometimes attached to 2014" is written for a
  researcher who might meet 2.38 in another table, not for a reader who has never seen it. Reframed as
  a fact about the data. The agent could only corroborate 2.38 being attached to 2018, not 2014, so the
  year claim is gone too.
- **Accepted.** HCP spelled out; "mixes instruments", "a sampling extrapolation, not a demographic
  correction", "raw-versus-corrected pair" and "rebuilt from components" all replaced with plain
  wording.
- **Accepted, and now disclosed.** 1962 and 1975 fall in no Moroccan census year — the censuses are
  1960, 1971, 1982, 1994, 2004, 2014, 2024 — so they must come from surveys we have not identified.
  The page said nothing; it now says that.
- **Open.** Whether Morocco's civil registration publishes births by the mother's age is unresolved:
  the agent could not reach the relevant government sites. This is the claim shape that has been wrong
  three times in this collection, so it should not be closed by assumption either way.

## Ukraine

Verdict: minor problems. The plotted series matched the office's own workbook year for year, our
arithmetic check reproduced exactly, and the strongest claim on the page — the territorial one — was
confirmed word for word in the workbook's own footnotes: 2014 excludes part of the occupied areas of
Donetsk and Luhansk, and from 2015 the footnote drops "part" and excludes the whole of both.

- **Accepted, and the dates could not be stood behind.** We gave a precise chronology of the office's
  orders — a mobile-network population method approved July 2023 and effective January 2024, an
  interagency group formed October 2025, its membership changed December 2025. The agent could not
  verify any of it, and the one relevant document it could read shows a different order and date and
  never mentions mobile-network data. The substance — the office cannot compute a population to divide
  by and is still working on a replacement — is kept; the order-by-order dates are gone.
- **Accepted.** "Its public data bank returns no observations at all for the births dataflow — the
  structure is defined, including a mother's age dimension, but nothing is loaded" is our query in
  database jargon. Cut.
- **Accepted, scoped.** "Nothing on births, fertility or population by age has appeared for any year
  after 2021" reads as absolute. Birth counts for later years do circulate from the justice ministry's
  registration records — not by mother's age, and with no population to divide by, so they cannot give
  a rate. Now said, which is both more accurate and more useful.
- **Accepted.** Держстат given its English name on first use, "held out to 2021" replaced, and the
  dangling modifier in the arithmetic sentence fixed.

## Angola

Verdict: minor problems. Both plotted values verified against the survey reports, our arithmetic
reproduced exactly, and the strongest claim on the page — that the office re-derived the census figure
downward because women had under-reported lifetime children and over-reported recent births —
confirmed almost word for word in its own projections report.

- **Accepted, and it said the opposite of the source.** We wrote "only 38% of children under five
  registered, and only 36% of those holding a certificate", which reads as a nested figure and implies
  most registered children have no paperwork. The survey's own numbers are two independent shares of
  all children: 38% registered, 36% holding a certificate. So nearly every registered child does have
  one, and registration itself is the bottleneck. Corrected.
- **Accepted.** "5.7 in 2014 against 6.2 measured two years later" oversells how much of that gap is
  elapsed time: the survey figure is a three-year rate whose window reaches back to about 2013 and
  overlaps the census date. Reworded to say the two disagree by more than the interval explains, which
  is the actual finding.
- **Accepted.** Three sentences were written for whoever maintains the page rather than for a reader —
  the transcription check, "so there is nothing to recalculate from", and "a later thematic volume is
  worth watching for". All turned outward.
- **Accepted, softened.** "No Angolan source publishes the number of births by the mother's age" is now
  "we found no Angolan source that publishes" it. The agent checked the four most likely documents and
  found none, but could not rule out a yearbook or a justice-ministry registry bulletin — and this is
  the claim shape that has already been wrong three times here.
- **Accepted.** INE spelled out, the survey's name translated, the Gompertz model described rather than
  named, and "the census figures are not one number" — which the agent could not make sense of even
  after reading the census — removed.

## Uzbekistan

Verdict: minor problems. All sixteen plotted values match the agency's own file digit for digit, and
the rise is real: it shows in the urban and rural files separately and in the UN's own birth counts.
The methodology claims were confirmed verbatim against the agency's metadata sheet.

- **Accepted, and the fourth instance of the same false claim.** We said "the agency publishes no
  births by age of mother... so there is nothing to recalculate from" — and then contradicted ourselves
  two clauses later with "those tables do exist in the printed demographic yearbook". The yearbook's
  own table of contents lists births by mother's age at page 144. The honest claim is about the free
  online data, not about the agency, and that is what the page now says.
- **Accepted, and it was wrong in the years that matter most.** "Rural fertility runs 0.3 to 0.5 above
  urban throughout" is false exactly where the story is. I pulled the agency's urban and rural files
  and computed every year: the gap sits at 0.38-0.54 through 2021, collapses to **0.02 in 2022** and
  0.12 in 2023 — because urban fertility rose faster — then reopens to 0.41 by 2025. Now stated, and it
  is more interesting than the flat claim it replaces.
- **Accepted.** "Through an open endpoint that needs no key" and "the server hosting it times out" are
  both about our own fetching, and the second was being used to excuse the claim above. Gone.
- **Accepted.** "Uzbekistan is the one country here whose fertility rose rather than fell" leans on the
  rest of the collection; now stands alone. "Metadata", "denominator" and "row" all replaced.

## Cross-cutting: claims that something does not exist

Five of these have now been proven false — Malaysia's births by age of mother, Sudan's census age
tables, Nepal's unnamed method, Ghana's unpublished census volume, Uzbekistan's yearbook table. Every
time, the thing we said did not exist was in a printed report, a census annex or a yearbook, while the
claim was really about what we could find online.

Two things done about it rather than noted:

- A sweep of the whole collection found **17** absence claims in reader-facing text. The three most
  categorical have been softened to what we actually established: Bolivia's "no Bolivian source
  publishes births by age of mother, in any year, from any instrument", Switzerland's "publishes no
  births by age of mother at all, in any form", and Benin's "no method is named anywhere". All three
  now say what we found rather than what exists. The rest sit with countries still to be reviewed.
- The standard brief in `PROMPT.md` now tells every remaining agent to attack absence claims
  specifically, and names where the missing thing has actually turned up each time: the statistical
  yearbook, census volumes and their annexes, vital-statistics bulletins, thematic monographs, and the
  relevant ministry rather than the statistics office.

## Australia

Verdict: minor problems, and the strongest metadata on any page so far — the agent checked eight
specific claims against the bureau's own release and methodology pages and found every one correct,
including the Victoria backlog figures, the bureau's own warning against comparing years, and the
occurrence-basis counts. All 25 plotted years match, and it looked for the Canada double-rounding
artifact and found none.

- **Accepted.** The closing sentence — which data cubes a table sits in, that it exists only through a
  data service, that the population file's address moves every edition — is our own collection process
  end to end. Cut.
- **Accepted, and it was a narrower claim than the truth.** We explained the 2023 and 2024 gaps against
  the bureau's own rate as population revisions, as if it were a recent-years phenomenon. The agent
  compared all 25 years: ours sits 0.001 to 0.004 below the bureau's for nearly every year from 2001,
  for the same reason. Now stated as the general fact it is.
- **Accepted.** Calling 1.482 against 1.481 a "match" overstates it; now "within a few thousandths".
  The bureau is named in full and the acronym dropped from the prose.

## Venezuela

Verdict: minor problems, but one was a contradiction between two of our own paragraphs.

- **Accepted, and the label was right while the prose was wrong.** Block 2 said "we take the observed
  points up to 2015"; Block 3 said every point comes out of a projection calculated in 2013. Both
  describe the same four numbers. The agent found INE's own footnote — "Estimaciones y Proyecciones de
  Población con base al Censo 2011" — applies across the whole 2000-2025 range, not just the dashed
  part of its chart. So there are no observed points at all, the label Projection only was already
  right, and the word "observed" was the error. The cutoff stays, because it is where INE stops drawing
  the series as elapsed time, but it is no longer described as a line between measurement and model.
- **Accepted.** The passage about ine.gov.ve no longer resolving, a record removed from a registry, and
  a domain that "still answers authoritatively for other names" is written to a future colleague
  retracing our sourcing, in DNS jargon. Cut — the working address is already the link.
- **Accepted, softened.** "No national fertility figure exists for any year after that" is an overclaim:
  a university-run household survey has reported later figures. We have not used them because we could
  not establish they are measured comparably, and that is now what the page says.
- **Accepted.** The link went to INE's homepage, not the document behind the numbers; now the
  compendium itself. "Digitising" fixed, INE spelled out, and "INE's population" made specific — women
  aged 15 to 49.

## Mozambique

Verdict: serious problems. The four plotted values verified against the survey reports and against
INE's own trend chart, but the entire premise of the caveats paragraph was false.

- **Accepted, and this is the fifth time.** We told readers "INE names no correction anywhere in the
  results volume — and the one folder of its catalogue that might hold an adjusted figure lists
  nothing." INE has published a fertility study from the 2017 census, reachable from its own census
  landing page. I confirmed the document's page exists on ine.gov.mz myself; its site is
  JavaScript-rendered so I could not open the PDF, but the agent read it: it computes an uncorrected
  figure of about 4.18 — the same number we derived — and a corrected 4.9 by two named methods, and
  states INE's own official 2017 figure is 5.2. The claim is gone, and the page now says the study
  exists and that we have not yet incorporated it.
- **Not plotted, deliberately.** The agent's 5.2 would add a fifth point and sit neatly between the
  2003 and 2011 surveys. I could not open the document to verify it, and this campaign has already
  caught one agent misreading a source, so it is described rather than charted until someone reads it.
- **Accepted, an inconsistency of our own.** The last round was plotted at 2023 though its fieldwork ran
  July 2022 to February 2023 — while the 2003 round, whose fieldwork also straddled a new year, is
  plotted at 2003, the majority year. Now 2022, which is consistent, and which roughly halves the gap
  the map colors for Mozambique.
- **Accepted.** The tooling clause about an empty catalogue folder is gone with the false claim it
  supported, "unlike Tanzania, Uganda or Angola" no longer leans on three other entries, and INE and
  the survey's name are spelled out.

## Niger

Verdict: serious problems. All three plotted values verified, the census cross-check reproduced, and
two of the sharper claims — the Gompertz pair 7.5 against 7.8, and the chart that misprints 1992 and
1998 as 7.0 and 7.2 where the tables say 7.4 and 7.5 — confirmed exactly. But two claims about the
census's own reasoning were inverted or misattributed.

- **Accepted, and it was backwards.** We said the office "attributes the gap to under-declaration in
  the census's twelve-month window", referring to its 7.5 against the survey's 7.6. The report uses
  that very comparison to argue the *opposite*: external checks against the 2012 and 2006 surveys "go
  against this conclusion", and it concludes births in the year before the census were probably all
  declared. The under-declaration question it does entertain comes from a different, internal
  comparison. Rewritten to say what the report concluded.
- **Accepted, misattributed — and my own earlier edit preserved the error.** We had the report
  rejecting the Gompertz correction because it preferred field data to "extreme hypothetical
  estimates". That phrase is the report's, but it is about a different method it never computed. Its
  actual stated reason for rejecting Gompertz is a fit test: one of the model's parameters falls
  outside the range its authors recommend. Worth noting that I had already rewritten this sentence once
  in the quotation sweep — taking the quotation marks off a paraphrase does nothing about the
  paraphrase being wrong.
- **Accepted.** "The second case here — after Madagascar" leans on another entry; now stands alone. INS
  spelled out, and the Gompertz model described rather than named.
- **Accepted, stale.** The fifth census was described as still in its pilot phase; it had reached
  post-pilot enumeration by mid-2026.
- **Accepted.** The Niamey registration rate of 4.8 and the survey's 4.2 for the same city cover
  different periods, which "for the same city" hid. Now flagged.

## Mali

Verdict: minor problems. Every one of the eight specific numeric claims in the caveats was checked
against INSTAT's own census fertility report and confirmed — the 112-to-148 sex ratios, the 70%
shortfall against children under one, the office's own words about poor quality requiring adjustment,
the named method and the alternatives it rejected, and the 494,742 to 930,503 correction at 88%. Our
census cross-check reproduces all seven of its published rates and gives 6.0885 against 6.1.

- **Accepted, and this one held up.** "INSTAT does not print the two totals side by side" is the claim
  shape that has failed five times, so the agent checked three separate INSTAT documents: 494,742
  appears in two of them, 930,503 only in the fertility report's annex, never adjacent. The claim
  stands. Worth recording that the instruction to attack absence claims also confirms the true ones.
- **Accepted, a mismatch between prose and chart.** We say "seven survey rounds since 1987" and plot
  five. The two earliest are 1987 and 1995-96, both before the chart's 2000 start, so nothing is
  missing — but a reader counting points against that sentence would think otherwise. Now said.
- **Accepted, two sources conflated.** "INSTAT says between 40 and 60% of births go unregistered, even
  though 83% of people eventually hold a certificate" pairs a 2018 estimate by the civil-registration
  directorate, which INSTAT was citing, with the census's own 2022 finding, as though one office had
  measured both. Now attributed separately.
- **Accepted.** "The largest correction in this dataset by far" compared the entry to a collection the
  reader has not seen. INSTAT is spelled out and the Trussell variant of the Brass technique is
  described rather than named.

## Taiwan

Verdict: serious problems. The 2023 and 2024 figures check out, but the newest point was built on an
unfinished year and we knew it.

- **Accepted, and the page said so while plotting it anyway.** We plotted 0.7084 for 2025 and, three
  sentences later, told readers that year "is still filling in". I checked our own numbers: for finished
  years our recomputation sits +0.34% above the ministry's published rate, every year, like clockwork.
  For 2025 it sits +1.93% — nearly six times off — because our births total for that year is 105,676
  against the ministry's final 107,812, about 2% short. Births in Taiwan are dated by when they
  happened, so the newest year keeps filling in after first publication.
- **Fixed by dropping the year, not by substituting.** The ministry publishes its own 0.695 for 2025,
  and using it was the other option. But this series is ours throughout, rebuilt from counts, and
  splicing in one copied figure would put two methods on one line — the mistake corrected for Myanmar
  earlier in this batch. So the line ends 2024, with a `LAST_COMPLETE` constant to advance when a year
  closes, and the caveats now tell the reader the ministry has a 2025 figure and why ours stops short.
- **Accepted.** The closing sentence about portal pages being navigation shells and a query service
  having to be addressed directly is our own access experience — and overstated: the ministry runs a
  public, named query tool any reader can open. Cut.
- **Accepted.** "The lowest rate in this collection" now stands alone as one of the lowest in the
  world; the zodiac sentence explains why a zodiac year would move births at all; "occurrence-year" and
  "registration-year" are gone; and "the budget agency" is named as Taiwan's statistics agency.

## North Korea

Verdict: minor problems. Both plotted values verified against the census and survey reports, and our
2.008 recomputation reproduced from the census's own tables. The census foreword and the 1993
discrepancy — 2.1 in one bureau publication, 2.20 in another — both confirmed verbatim.

- **Accepted, an inconsistency in our own rule.** We excluded the 2017 survey's 1.9 because "its title
  page names UNICEF as publisher, so the authorship is shared" — while the sentence two lines down says
  "we judge authorship, not hosting". UNICEF's own description of that survey is that the bureau
  conducted it with UNICEF's technical assistance, which is the same framing we accept for the
  UNFPA-backed census. So our stated test points to including it. I have not added the point, because
  neither the agent nor I could read the report's imprint page, but the page now says plainly that by
  our own test it arguably belongs and why it is not there yet.
- **Accepted, overstated.** "The country has no statistics website at all: its two public domains
  resolve but drop every connection before answering" reads as a claim about North Korea's whole
  internet presence, and that is wrong — more than two domains resolve and at least one serves a
  normal site. Narrowed to what we actually mean: there is no North Korean statistics website.
- **Accepted.** The Iran cross-reference is gone, the connectivity diagnosis is gone, the authorship
  rationale is stated as a fact rather than argued against an imagined objection, and "cancelled" is
  now "canceled" — in fact the cancellation claim itself had no located source, so it now says only
  that nothing newer than 2017 has appeared, which the agent did confirm.

## Syria

Verdict: minor problems — and the rare case where every absence claim held up. The agent opened four
separate abstract editions in their original spreadsheet form and confirmed all sixteen values
identical across them, confirmed the registered-births tables rolling forward to 2019 while the
fertility table sat frozen, and confirmed those birth tables carry no age dimension.

- **Accepted, a missing disclosure.** Neither plotted figure is a single year's rate: the source's own
  table title says each covers the three years before its survey. The UN line beside it is annual. The
  page never said so; now it does.
- **Accepted.** Three sentences diagnosing how the bureau's domain died — a parking service, cloaking
  by browser identity, a 410 against a 200 — are our own detective work. Cut to the one clause a reader
  needs, that the site is gone and this comes from an archive.
- **Accepted.** The three-clause sentence is split, and "the native spreadsheet inside the abstract's
  own chapter archive" is now plain.

## Sri Lanka

Verdict: serious problems. The plotted series itself verified — the agent recomputed 2021 from the
department's own tables and got 1.64139 against our 1.641, and found no double-rounding artifact — but
the two claims that justify the series were both wrong.

- **Accepted, and it was the basis for our own label.** We said "the same procedure on the department's
  own 2000 data reproduces the 1.9 it published then, which is what tells us the method is the one it
  used." The department's own page attributes that 1.9 to a survey, not to registration, and re-running
  our registration procedure on its 2000 births gives about 2.16. So the sentence claimed a method match
  that does not exist. It is gone: the page now says plainly that the series is our arithmetic on the
  department's counts, not a figure it has endorsed, and not comparable with its survey and census
  figures.
- **Accepted, false.** "The Department of Census and Statistics stopped publishing a fertility rate of
  its own after 2000" — it has published four since: 2.3 from a 2006-07 survey, 2.4 from the 2012
  census, 2.2 from a 2016 survey, and 1.3 from the 2024 census. We mentioned only the last, which left
  a false impression of a 24-year silence. All four now named, and the accurate claim is narrower: no
  annual rate from registration.
- **Accepted.** The clause about 2022 appearing as a dead link reusing 2021's files is our own
  navigation. The substantive point — that no age breakdown exists for any year after 2021 — is kept and
  now says where we looked.

## Burkina Faso

Verdict: minor problems. Every one of the six specific claims about the census correction verified
against the census volume itself, including the implausible spike at 45-49, the 5.8 from the method it
rejected, and the 5.5-to-5.4 and 4.5-to-4.1 moves. Our own arithmetic check reproduces its 5.4.

- **Accepted, and it is the most useful thing found here.** Insecurity kept the 2021 survey out of 86 of
  its 600 sampled areas, concentrated in the two highest-fertility regions in the country: two thirds of
  sampled areas in the Sahel and a third in the East were never visited, and five provinces were dropped
  entirely. The report warns against relying on those regions' own figures but says nothing about the
  national one. So part of the 6.0-to-4.4 fall may be coverage rather than fertility, and the page now
  says so — it is the difference between a real collapse and a partly artefactual one.
- **Accepted, and it explains a number a reader could not have reconciled.** The 2003 round's own report
  headlines 6.2, over a five-year window. Our 5.9 is the office's later recalculation of that round over
  three years, to match the other rounds. Defensible, but a reader checking the 2003 report would have
  found a different number and no explanation. Now stated.
- **Accepted, and one was wrong as well as out of place.** "INSD's own civil-registration statistics
  page is a chart template filled with randomly generated placeholder data" — that boilerplate sits on
  every page of the site, has no element to render into, and no visitor ever sees a fake chart. It was
  not a fact about the civil-registration data at all. Cut, along with the embedded-font sentence, whose
  page count was understated anyway.
- **Accepted.** "Nothing in either report reconciles them" now notes that the census volumes predate the
  survey's report and could not have. INSD spelled out, the two correction methods described rather than
  named, "the two instruments" and the cross-collection comparison gone.

## Kazakhstan

Verdict: minor problems. Seven plotted years checked against the bureau's own published rate, all
within 0.004 — no rounding-boundary case anywhere, unlike Canada. The registration-basis claim was
confirmed word for word in the bureau's own methodology order.

- **Accepted, and the sixth failure of the same claim.** "The bureau's own metadata claims history from
  1999, but those years sit under superseded identifiers we could not find." The bureau's demographic
  yearbooks carry births by mother's age and population by age and sex back to at least 2009, on its own
  site, found by ordinary search. Our sentence turned a limitation of our search into a limitation of
  the data. It now says the earlier years exist and that the line could be extended.
- **Worth noting on the newest year.** After Taiwan, I expected 2025 to be provisional here too. It is
  not: because Kazakhstan dates a birth to the year it was registered, the annual figure does not keep
  filling in, and the bureau published 2.57 as a full-year statistic. The same property that makes the
  series less comparable with occurrence-based countries makes its latest year safe to plot.
- **Accepted.** "All to a plain request with no key" is our own access route; the Uzbekistan
  cross-reference leans on another entry; "artefact" was British; and "bound", "occurrence basis",
  "classifier" and "codes" are all replaced with plain wording.

## Zambia

Verdict: minor problems — and our sharpest accusation against a statistics office turned out to be
right, with a diagnosis attached.

- **Confirmed, and now explained on the page.** We said the census's own worked example of its
  under-reporting check "does not reproduce from the inputs it quotes — we get 5.66 where the report says
  5.921." The agent found the passage, recomputed the inputs from the census's own tables, and got 5.66
  as well. It also worked out why: the printed formula raises a ratio to the fourth power, but 5.921 is
  what you get by multiplying by four instead. That diagnosis is now on the page, which makes the claim
  concrete and fair rather than a bare assertion that the office got it wrong.
- **Accepted, and the seventh failure of the absence pattern.** "It publishes neither the age-specific
  multipliers nor the adjusted rates, so the 4.6 cannot be rebuilt from anything public." The corrected
  rates are published to four decimals in the report's own annex, and a rounded version appears as a
  figure in the main chapter; adding them up returns 4.6. Removed, and the page now says the corrected
  rates are published and do reproduce the figure.
- **Accepted.** "Which is worth knowing before citing either" is addressed to someone about to cite the
  number in their own work. Gone. "P/F ratio technique", "Brass formula", "indirect estimation" and
  "age-specific multipliers" are all replaced with descriptions of what they do.

## Somalia

Verdict: serious problems. The plotted 6.9 verified twice in the source and our 6.885 recomputation
reproduced exactly. Two of the three exclusions held up under a full read — including the 2013 survey,
where the agent text-searched all 121 pages and confirmed it really does contain no fertility rate.
But the headline claim was contradicted by our own source.

- **Accepted, and the eighth failure of this pattern — this time our own citation refutes us.** We
  wrote "The 2020 survey is the only national fertility figure Somalia has." The 2020 report itself
  compares its 6.9 against a 2006 survey's national 6.7. So the document we cite for the claim disproves
  it. There is a defensible reason not to plot the 2006 figure — it was published by a UN agency with
  three planning ministries rather than by the bureau, and the 2020 report notes its fertility table
  leaves out nomadic households — and the page now gives that reason instead of denying the survey
  exists.
- **Accepted, a dating problem.** The survey is titled 2020, but its own report puts the fieldwork in
  2018 and 2019, and the rate covers the three years before each interview. We plot it at 2020 without
  saying so; now said.
- **Accepted.** "Two documents look like they should count and do not" is written for whoever audited
  the sourcing, and was off by one anyway once the 2006 survey is counted. Rewritten for a reader, and
  the count is now three.
- **Accepted.** "The last census was in 1975" glossed over one attempted in the mid-1980s whose results
  were never released. Now precise.

## Cross-cutting: eight failures, and what they have in common

Malaysia, Sudan, Nepal, Ghana, Uzbekistan, Kazakhstan, Zambia, Somalia. Every one was a sentence
saying a source publishes nothing, names no method, or holds nothing to check against. Every one was
false. And in every case the thing said not to exist sat in a printed report, a census annex or a
yearbook, while the claim was written as though it were about the producer rather than about the online
data we had looked at.

Three of them were worse than wrong:

- Uzbekistan's contradicted itself two clauses later, in the same sentence we published.
- Zambia's said the corrected rates could not be rebuilt from anything public; they are in the report's
  own annex, and a rounded version is a figure in its main chapter.
- Somalia's was refuted by the very document we cite for it.

The instruction added to `PROMPT.md` after the fifth is doing its job — it caught Kazakhstan, Zambia
and Somalia, and it also confirmed Mali's and Syria's absence claims as true, so it is not merely
manufacturing doubt. The remaining lesson is for the writing rather than the checking: say what we
looked at and did not find, never what a producer does not publish.

## Chad

Verdict: serious problems. All three plotted values verified against primary sources, and the 2009
census's fertility volume really does appear to be undigitized. But two absence claims were false and
the page contradicted its own age-band note.

- **Accepted, the ninth and tenth failures of the pattern, on one page.** "Neither of those survey
  reports is on INSEED's site" — both are, in its own microdata catalog, downloadable with a plain
  request. And "no document INSEED hosts publishes births by age of mother, as raw numbers or as rates"
  — both reports carry a full table of rates by age group. Our own prose then said "no age-specific
  rates at all" while the age-band note under the chart tells the reader this office "publishes fertility
  rates only". The page disagreed with itself.
- **Accepted, and it adds a point.** The 2019 survey reports 6.4 nationally, four years after the round
  we had as the newest. Both are genuine separate measurements: the 2015 schedule sums to 6.45 and the
  2019 schedule to 6.43, each printed as 6.4, which I checked arithmetically. The series now runs to
  2019. A 2019 attribution of the *earlier* figure also circulates, but a health ministry document was
  quoting 6.4 in March 2016, so the earlier round is where it comes from.
- **Accepted, softened.** "How the 7.1 was computed cannot be checked" overstates it: the census's own
  results report publishes the figure alongside its other headline rates, even though the dedicated
  fertility volume is not online.
- **Accepted.** The closing sentence about the website serving nothing to a plain request is our own
  tooling, and false besides. "Digitised" fixed, INSEED spelled out, and the survey reference periods —
  three to five years before fieldwork — now disclosed.

## Malawi

Verdict: serious problems. Six of the seven claims about the census's choice of correction verified
exactly, including the office's own stated reason for its choice. But our arithmetic sentence was
broken, our correction figure was double the truth, and another absence claim failed.

- **Accepted, and it could not have been right.** We wrote that dividing "576,525 births over 4,267,788
  women 15-49" gives 4.166. That division gives 0.135. No arrangement of those two numbers produces
  4.166, so the sentence described an operation that cannot yield the figure it claims — checkable with
  a calculator and no source at all. The real check is adding up the census's own rates by age group,
  which gives 4.17 against its printed 4.167. Replaced. The input number was also wrong: the census's
  own total is 576,606.
- **Accepted.** "The correction it applied was about 3%" — 4.167 to 4.234 is 1.6%, and under 1% on the
  rounded headline figures. Overstated by roughly double.
- **Accepted, the eleventh failure.** "The office's website serves no data at all to a plain request;
  everything is behind an undocumented interface" — a plain request returns full pages, and its document
  store serves the very PDFs we cite with no authentication. Cut.
- **Accepted, and now disclosed.** Our 2008 point of 6.0 is not in the 2008 census report, which gives
  5.2. It comes from the 2018 report's own trend series — which is the version comparable with the
  corrected 4.2 we plot for 2018, so it is the right choice, but a reader checking the obvious source
  would have found a contradiction.
- **Accepted.** Three separate comparisons to the rest of the collection removed, the three correction
  methods described rather than named, the office spelled out, and the P/F range corrected to 4.7-5.1,
  which is what its own table says.

## Chile

Verdict: minor problems. The 2024 age-band births sum exactly to INE's own national total, and both
recomputed years round to its published figures.

- **Rejected, and it was my brief's fault again.** The agent flagged the plotted 1.255 for 2022 as
  sitting on a rounding boundary against INE's final 1.25. The true value is 1.25486, which rounds to
  1.25 correctly. It read 1.255 because `redteam.py` printed four significant figures. That is the third
  false finding the generator has caused; it now prints six and warns that trailing digits are not a
  precision claim.
- **Accepted, wrong date and misleading conclusion.** INE rebased its population estimates in January
  2026, not February. And "so the gap has closed" was wrong in substance: INE says it is deliberately
  still calculating the published rates on the older 2017-census base until regional estimates from the
  new census exist. The projections converged; the rate itself has not been rebuilt.
- **Accepted.** 2023 is no longer provisional — INE has finalized it, unchanged at 1.16. The "flat 1.58
  to 2030" claim could not be sourced and INE's own description is of a declining path, so it is gone.
  "Santiago" is now the Metropolitan region, which is what the source says.
- **Accepted.** The bot-challenge sentence, the Latin America framing and the Peru comparison are gone,
  along with "vintage" and "rebased".

## Netherlands

Verdict: minor problems — and the strongest values check in the collection. The agent pulled CBS's own
published rate for all 31 years and every one matches, with no rounding artifact.

- **Accepted, the claim outran its citation.** The link we showed readers holds only seven broad age
  bands and the summary rate, not the single-year births and population the calculation actually uses.
  Those live in tables we never named.
- **Accepted, and it hid an extension.** "CBS only fills in the mean-population column from 1995, so the
  series starts there rather than in 1950" is literally true about that column but overstates the limit:
  the January population needed to work the average out ourselves exists back to 1988 in the same table.
  Now said.
- **Accepted.** "Final rather than provisional" needed a caveat: CBS does fold late notifications back
  into the year of the birth, so even a final year can edge up. Added.
- **Accepted.** "This is the cleanest case in the collection" leans on the rest of the collection; the
  two access sentences are ours, not the data's; and the rounding sentence read as asserting two
  different numbers were equal.

## Senegal

Verdict: minor problems, but one sentence was the same broken arithmetic as Malawi's.

- **Accepted, and it is a repeat.** "We divided the 2023 census's own counts — 487,108 births over
  4,499,636 women — and got 3.69." That division gives 0.108. The real check sums seven age-group rates.
  This is the second page in one batch describing a division that cannot produce the number it claims,
  after Malawi's. Both are now written as what was actually done.
- **Accepted, and we knew less than we could have.** "ANSD's projections document states 4.2 for 2023
  rather than 4.4, and we cannot tell which is the slip." It is resolvable: 4.2 is an earlier provisional
  census estimate that ANSD's projections and annual reports still carry, and 4.4 is the revised figure
  from the final fertility chapter that ran the three methods. Now stated.
- **Accepted.** "Runs its health and demographic survey every year" is false across the plotted range —
  no round ran between 2019 and 2023. And "two thirds of people hold a birth certificate" is a decade
  out of date; the 2023 survey puts it at 80% of children under five.
- **Accepted.** Both researcher-facing sentences gone, and the three method names replaced with
  descriptions of what each does.

## Guatemala

Verdict: serious problems. Our series is now one year shorter.

- **Accepted, and our own numbers settle it.** The 2024 point of 1.90 rests on a birth count 12.7% below
  2023, after four years that moved by about a percent either way — and reporting on 2025 shows part of
  that reversing. I printed our own counts: 343,226, 343,776, 340,625, then 297,408. That is a break, not
  a trend. 2024 is out, with a `LAST_COMPLETE` constant, as Taiwan's 2025 was. Four countries now.
- **Accepted, the twelfth false absence claim.** "There is no published total to check against... which
  is as far as verification can go here." Two of INE's own figures exist and both corroborate us: its
  2018 census estimate of 2.7 for 2018-19 against our 2.6, and about 2.2 for 2022 from its maternal and
  child health survey against our 2.23. The page now cites them, which makes the series better attested,
  not worse.
- **Accepted.** "A stable share" overstates the late-registration pattern, which rose during the pandemic
  and has risen since 2018 for indigenous families. The bot-wall sentence, the Latin America framing and
  "the population base is not naive" are gone.

## Romania

Verdict: serious problems, and the one entry in this batch whose numbers I would not defend.

- **Accepted, and it goes beyond wording.** Our series diverges from both Eurostat and the UN by up to
  0.15 at the ends of the line and up to 0.09 the other way in the middle — I checked ours against the
  UN directly: 2012 is 0.149 low, 2019 to 2021 are 0.13 to 0.15 high, 2023 is 0.185 low, while 2014-2017
  and 2022 agree within 0.02. Two independent sources agreeing with each other and disagreeing with us
  in a shape that flips sign looks like the population we divide by shifting under the series, not a
  pattern in births. The page now says so plainly instead of presenting the rise and fall as a finding.
- **Accepted, and the claim was unverifiable from our own code.** "Our figures match what INS's own
  published age-specific rates imply, to four decimals, in all thirteen years." The loader never fetches
  INS's published rates — it builds the rate from two count tables — so nothing in the pipeline checks
  that. Removed rather than restated.
- **Accepted, the thirteenth false absence claim.** "Its own age-specific rates — but no total... leaves
  the sum to the reader." INS has published a total fertility rate, in a report covering 1960 to 2010.
  Now said, and that report is also a route to extending the series back decades.
- **Accepted.** The export-endpoint sentence is gone, and 2024 is flagged as still moving under our own
  stated three-year revision rule.

## Zimbabwe

Verdict: serious problems. Every plotted value verified against ZIMSTAT's own reports, the 2022
age-band counts match its main census report band for band, and we plot the right side of its
correction — the direct figure, which is what ZIMSTAT itself uses. But three claims around them were
wrong.

- **Accepted, and it is the third instance of one mistake.** "We divided the census's own counts —
  438,776 births over 3,814,701 women 15-49 — and got 3.72." That division gives 0.115, which is the
  general fertility rate ZIMSTAT separately reports as 115 per thousand. The total fertility rate needs
  each age group divided separately, summed, and multiplied by five. Malawi and Senegal carried the same
  sentence in this batch.
- **Acted on beyond the country.** Three instances is a template I propagated, so I swept the whole
  collection: extract every pair of counts sitting near a rate in reader-facing prose and test whether
  the division can produce it. Zimbabwe was the only remaining hit; Hungary showed up as a false
  positive, comparing two birth totals rather than dividing. `redteam.py --audit` now runs that scan on
  every pass, skipping pairs of similar magnitude, since births are always a small fraction of women.
  It reproduces the broken sentence as a hit and passes on the fixed ones.
- **Accepted, the fourteenth false absence claim.** "The 2012 census report is no longer on ZIMSTAT's
  site and is not in any web archive, so its 3.8 survives only as a citation inside the 2022 report."
  It is in the Wayback Machine, readable, and states the figure directly.
- **Accepted, and the link was pointing at the wrong document.** We said the dedicated fertility report
  publishes the counts behind its figure. It publishes rates only; the counts are in the main census
  report. A reader following our link to check them would not have found them.
- **Accepted.** Two sentences grouped Zimbabwe with Madagascar, Niger, Senegal, Mali and Ghana as though
  the reader had read all five. Both gone, and the two adjustment techniques are described rather than
  named.

## Ecuador

Verdict: serious problems. The page predicted its own last point was wrong and plotted it anyway.

- **Accepted, and we had already said it.** The caveats told readers "2024's 1.44 will rise as late
  registrations arrive" — and 2024 was plotted on the same footing as every finished year, and used to
  color the map. Registered births fell 9.7% that year against about 4% the year before. The line now
  stops at 2023, with a `LAST_COMPLETE` constant. Five countries have now had an unfinished final year
  dropped, and this is the one where our own text made the case.
- **Accepted, and the label contradicted our own quotation.** We had Ecuador under complete
  registration while quoting INEC's stated reason for adopting a higher projection: that it should sit
  above the registered births "because those always carry some under-registration". That is the producer
  describing its own registration as incomplete. Moved to incomplete registration, and the page now says
  that is why.
- **Accepted.** "Ecuador is the sixth Latin American country where the registry sits well below the
  headline, and the only one where the office says why in writing" leans on five entries the reader has
  not seen, and on a count I cannot expect a reader to verify. Rewritten to stand alone.
- **Accepted.** "Crude birth rate", "adolescent rates", "P/F ratio", "reverse survival" and "Arriaga"
  all replaced — the four method estimates are now given as a range, which is what the sentence was
  actually for.
- **Open.** Whether INEC publishes a total anywhere could not be settled: its document server refused
  every connection the agent tried. Given this claim shape has now failed fifteen times, the page says
  we did not find one rather than that none exists.

## Benin

Verdict: serious problems. All six values verified against primary sources, and the odd finding — census
and surveys moving in opposite directions — is real, not a mix-up.

- **Accepted, the fifteenth false absence claim.** "The volumes advertised as six thematic census reports
  are the same twenty-page scanned brochure under two paths, with no statistical annex." There are six
  distinct volumes, different lengths and contents, in Benin's own microdata catalog. What is true is
  narrower: each is a short synthesis without a births-by-age annex. Gone.
- **Accepted, an instrument mislabeled.** "The surveys are flat or rising since 2012 — 4.9 in the 2011-12
  round against 5.7 in 2014 and 5.7 again in 2017-18." The 2014 figure is right but comes from a
  different survey program, not a round of the one the source line names. It is now identified as such.
  It does not affect the chart, since 2014 is not plotted.
- **Accepted, overstated.** "The 2013 census publishes a single national figure of 4.8 and nothing behind
  it" — it breaks 4.8 down by region and education. What is missing is only the birth and women counts.
- **Accepted.** "Benin is the case where an office admits..." presumed a set of cases the reader has
  seen; the office is named in full; and indirect estimation is now described rather than named.

## Cambodia

Verdict: minor problems. Every plotted value verified, and all four of the census raw-and-adjusted
pairs confirmed against the primary reports — 1.6 against 3.1 for 2008, 2.05 against 2.8 for 2013,
1.67 against 2.51 for 2019, 1.4 against about 2.3 for 2024. Our re-sum of the 2019 rates reproduced
too. The judgment not to splice the census series onto the survey series was confirmed as the right
call.

- **Accepted, false in half.** "The 2008 census and the 2024 survey are scanned images, so their
  figures had to be read by optical character recognition." True of 2024; the 2008 report is
  born-digital and extracts cleanly, checked across three separate copies. It was also a sentence about
  our own extraction process, so it is gone either way.
- **Accepted.** "Cambodia is the most systematic case of census adjustment we have found anywhere"
  compares the entry to a collection the reader has not seen. Now states the fact instead: the office
  has corrected its census figure on every count since 2008.
- **Accepted.** Four method names given with no gloss are now introduced as what they are, statistical
  techniques for correcting under-reported births. "Brass-Arriaga suits Cambodia best" is narrowed to
  the two rounds that actually say it — the earlier two averaged across a range.
- **Accepted.** "Between half and two thirds" was slightly wrong for 2013, where the ratio is 73%; now
  "about half and three quarters". "Confirms the table but not the figure" is plainer.

## Cross-cutting: the session's agent budget ran out

The campaign reached its limit of 200 subagents with 21 countries still to review. Four were in flight
at that point — Guinea, Rwanda, Burundi and Bolivia — and they will still report, but no replacements
can be launched, so `--audit` will correctly complain that four agents are out rather than five until
the budget resets.

Where the campaign got to: 75 of 100 written up. The remaining 21 are listed under To do in AGENTS.md,
in population-rank order, and every one of them can be reviewed the same way — the brief generator, the
standard prompt with its absence-claim instruction, and the audit all work unchanged.

## Guinea

Verdict: minor problems — and our sharpest claim here was confirmed exactly. The agent found the census
annex, recomputed the rates itself, and reproduced the whole finding: the raw rate for 15-19 is 105.3
per thousand against a published 130.5, a 24% increase, while every band from 20 to 49 differs by
between 0.08% and 1.02%. Our 5.19 against the published 5.3 reproduced too, and the office really does
state only its overall conclusion without pointing out where the correction lands.

- **Accepted, wrong year.** "The 2018 survey's own table carries the rounds back to 1992." It goes back
  to 1999; 1992 appears in that report only in its bibliography and a list of past rounds, with no
  figure attached.
- **Accepted, and doubly wrong.** "The claim that the 2024 census has published preliminary results,
  which we could not confirm." There was no 2024 census — Guinea's fourth was fielded in 2025, and its
  preliminary population results were published in early 2026 through the prime minister's office,
  confirmable without going near the compromised domain. So the year was wrong and the thing we called
  unconfirmable was confirmable. It carries no fertility figure, so nothing plotted changes.
- **Accepted.** Three sentences of forensics about gambling spam served under the real reports' paths cut
  to the one clause a reader needs. INS spelled out, the Arriaga and P/F machinery described rather than
  named, and both sentences addressed to a fellow analyst rewritten.

## Bolivia

Verdict: serious problems in one claim; everything else unusually well grounded. All five plotted
values match INE's own official table exactly, and the whole methodology narrative — the 1.69
assumption, the survey treated as the most robust source, the smoothed curve checked against birth
registrations, school enrollment and health-ministry records, and the refusal to apply the standard
correction to the 2024 census — was confirmed against INE's own 76-page methodology document, with
quotations. The electoral-court claim checked out to the digit: its bulletin's births table totals
2,742,478 certificate printouts for 2021, about thirteen times actual births.

- **Accepted, the sixteenth false absence claim — and the data was in a document we cite two sentences
  earlier.** "We could not recompute a rate: we found no Bolivian source publishing births by age of
  mother, in any year, from any instrument." The survey reports publish the rate for every age group,
  for all five rounds, in the same 2023 fertility report the first block names. Adding those up returns
  each round's published total. The page now says that, and says what is genuinely missing: the counts
  behind the rates, which is why the rate cannot be rebuilt from births and women and why there is no
  age-band chart.
- **Rejected, on the collection's own convention.** The agent argued this should move to "fully
  validated from births & women". It should not: summing an office's published rates checks that its
  table adds up, not that the number is right, and the counts are what the label is about. Uganda,
  Madagascar, Chad, Niger and Cambodia were all held to the same line.
- **Rejected, a misreading of the brief.** The agent flagged the map gap as using a rounded 2.1 where
  the series holds 2.115. Our series holds 2.1; the 2.115 is INE's own unrounded figure, which we do not
  plot. No inconsistency.
- **Accepted.** The opening named six other countries' entries; "one trap to record" and "it says why"
  were written for a compiler rather than a reader; INE and the Brass correction are now explained.

## Rwanda

Verdict: minor problems. Every plotted value verified against NISR's own trend chart and the statistical
yearbook, the UN comparison matched to three decimals, and the whole decade-long story of the office
changing its mind was confirmed step by step — including both stated grounds for rejecting two methods
in 2012, and the 2022 report's explicit finding of no under-reporting. Our 3.635 against its 3.63
reproduced exactly.

- **Accepted, wrong year.** "The 2025 report charts every round since 1992." Its fertility chart starts
  at 2000; 1992 appears once in prose, uncharted.
- **Accepted, a year label doing two jobs.** "The census and survey figures, 14% apart in 2020" compares
  a census estimate for the year before August 2022 with a survey's three-year estimate — neither is
  "in 2020". Reworded to name the readings rather than a shared year.
- **Considered and kept.** The agent argued the first label should be incomplete registration rather
  than survey, since registration is about 93% complete and NISR publishes an annual registration-based
  rate. But the label describes what the plotted figure is built from, and we plot the survey series. The
  real finding is the upgrade: that annual series would give a denser line than five-yearly surveys, and
  the page now says so.
- **Accepted.** "The clearest case of an office changing its mind" and "unusually" both leaned on a
  collection the reader has not seen. The survey reference period is now disclosed, "rising" is qualified
  by the early-2020s dip, and the registration material is split into its own paragraph.

## Burundi

Verdict: serious problems. Both plotted values verified against the survey reports, and our census
recomputation of 5.954 was reproduced exactly from the census's own spreadsheet — including
confirmation that the right operation is per-age-group division, summed and multiplied by five, not
total births over total women.

- **Accepted, the seventeenth false absence claim, and this one was about a reason.** "No correction was
  applied and none is mentioned anywhere... That is our inference, not the office's stated reason." The
  census's own thematic report gives its reason plainly: it checked its birth reporting, found the births
  well declared, and concluded the fertility figures could be used without adjustment. It is in the same
  document whose tables we used. So we credited ourselves with an inference the office had already
  published, which is the mirror image of the usual mistake.
- **Rejected, after checking it myself.** The agent said our claim that the ratio check "gives values
  close to one at the ages usually trusted" is false, reporting ratios of 1.15 to 1.49. I opened the
  census spreadsheet it had left behind and recomputed: across the middle age groups the ratios come out
  1.00, 1.04 and 1.06, and the same computation reproduces our 5.954 exactly. So the claim stands on the
  numbers I can see. I could not reconcile the two results — the workbook has separate urban, rural and
  combined blocks, and mixing parity from one against cumulated fertility from another would inflate the
  ratios exactly this way. Left as it was, with the reasoning now attributed to the census rather than to
  us.
- **Accepted.** "5.96 it publishes" is not the census's figure; it reports 6.0. The more precise 5.95
  that exists is a UN recomputation of the same tables, not something the office published. Now says
  what each source actually gives.
- **Accepted, overstated.** "There is no fertility page on the new site at all" — the office's current
  site hosts both survey reports we rely on. Replaced with the accurate point: the 2024 census has
  published only preliminary population totals so far.
- **Accepted.** The renamed office, the squatted domain and the web-archive detour are our sourcing
  workflow; only the rename survives. The Mali and Senegal comparison is gone, and "the raw one", "the
  standard ratio check" and "ages usually trusted" are now plain.

## Dominican Republic

Verdict: minor problems. The plotted 2019 figure corroborated twice in Dominican reporting, and both of
our side calculations use the right operation — per age group, summed, times five — with the 1.86 coming
out below independent estimates exactly as an incomplete-registration figure should.

- **Accepted, the eighteenth false absence claim.** "The 2022 census has published a fertility volume,
  but every archived copy of it is truncated and the live site is behind a bot wall, so its figure is
  genuinely unknown to us." A demographer at the planning ministry cites 2.3 from that census's own data
  in a public Dominican outlet. The figure is knowable; we had only looked in the volume itself. And the
  sentence was about our own access anyway.
- **Accepted, and there is a newer round.** The same household survey ran again in 2025 and is reported at
  about 1.97 — six years past the point we plot. **Not plotted:** the office's site returned 403 to
  everything I tried, as it did for the agent, so both 2.3 and 1.97 rest on secondary reporting. The page
  now says both exist and that we have not read them at source, which is the honest version of what used
  to be a claim that nothing was knowable.
- **Accepted, overstated.** "A mother needs an identity card to register a birth at all" — Dominican law
  provides other routes, with witnesses or a birth certificate carrying an identity number. A serious
  practical barrier, not an absolute bar. Reworded.
- **Accepted.** "But see the caveat" and "should not be read as one" were written for a colleague;
  "matrix" is now "table"; "a lower bound that will keep rising" is now plain; and the projection's
  assumption is said to be about fertility rather than left vague.
- **Could not verify, left as is.** The 18% late-registration share and the 141,548-to-159,466 cohort
  figures are blocked behind the same 403. They are consistent with the office's documented lag and with
  independent reporting of the same pattern, so they stay, unchanged.

## Cross-cutting: the validation label is now "Recalculated"

"Fully validated from births & women" has been renamed **"Recalculated from births & women"**, and the
short form in the table and map tooltip from "Fully validated" to "Recalculated". Three separate agents
read "fully validated" as a claim that the number is right, when it only ever meant that we rebuilt it
from counts rather than copying it. Uganda's, Guatemala's and Bolivia's reviewers each argued the label
was unearned on that misreading, and Myanmar's noted a reader could take it as vouching for the
underlying survey. The new wording says what we did rather than how much it proves. Turkey's prose, which
said a year "counts as copied rather than validated", follows the same change.

## Tunisia

Verdict: minor problems, and the tightest replication in the collection. The agent found the underlying
yearbook row and confirmed our 2023 reconstruction to the fourth decimal — 1.48903 from the known-age
counts, 1.5791 after spreading the 7,709 age-unknown births in proportion, against INS's published 1.58 —
and its rates by age group match INS's printed ones to within rounding on every band. All nine plotted
values check out across four yearbook editions, including that we correctly use the revised 2019 figure
rather than the stale one.

- **Checked and cleared, unusually.** After five countries whose final year turned out unfinished, I had
  the agent look. INS marks its own provisional figures with an asterisk — 2023 infant mortality and life
  expectancy both carry one — and neither the 2023 births total nor the 2023 rate does. The office is
  saying those are final.
- **Accepted.** "So a full recalculated series would mean opening one edition per year" is about our
  reproduction workflow; kept only the half that matters to a reader, that the other years cannot be
  checked the same way. "Two things we could not settle" was addressed to someone auditing us. INS spelled
  out, "the bands" now "the age groups".
- **Accepted, and reframed rather than dropped.** Our claim that INS "never states" whether births are
  dated to occurrence or registration survived a real attack — the agent checked four editions, the
  office's methods page and two other publications and found nothing either way — but per the standing
  lesson it now says what we could not establish rather than what the office never does. The one concrete
  asymmetry it did find is now on the page: the yearbook labels its deaths total corrected and never
  labels the births total that way.
- **Logged, not built.** Four or five more years could be recalculated from other yearbook editions, each
  carrying one year's age breakdown. The method is proven; it is per-year PDF work.

## Belgium

Verdict: minor problems, but one of them inverted a finding. Every plotted value matches Statbel's own
workbook, and our reconstruction is better than we claimed: it reproduces Statbel's rate for every single
year of the mother's age with zero deviation across all 35 ages, not merely to four decimals.

- **Accepted, and it read backwards.** "1.33 for Belgian mothers against 1.89 for foreign ones in 2024,
  and 2.13 against 1.35 within Flanders." Having just established the order as Belgian then foreign, that
  second pair tells the reader Belgian mothers in Flanders are at 2.13 and foreign mothers at 1.35.
  Statbel's table says the opposite — 1.35 Belgian, 2.13 foreign — and the agent confirmed the same
  ordering in six separate years. The numbers were right and their attribution was reversed, which is
  worse than a wrong number: it inverts what the reader concludes about who is having the children.
- **Accepted, and its premise did not hold.** "The series is the published one rather than rebuilt because
  the population files are hundred-megabyte register extracts, one per year." Those files are that big,
  but the small workbook we already use for every other figure carries the fully computed rates by age,
  nationality and region for every year since 2011 — so the stated obstacle does not block what the
  sentence implies. Cut as our own tooling either way.
- **Accepted.** "The geopolitical situation" is a euphemism where Statbel names the war in Ukraine, and it
  bundled two separate year-specific explanations into one. Now given as the office gives them. "A
  legal-residence concept on both sides of the rate" is plain now.
- **Prompted the label change below.** The agent argued "Rate copied from source, not validated" flatly
  contradicts a "what we did" block describing a successful reconstruction. It does.

## Cross-cutting: both validation labels now describe the operation, not the proof

Following the rename of "Fully validated from births & women" to **"Recalculated from births & women"**,
the other label goes from "Rate copied from source, not validated" to **"Rate copied from source"**.
Belgium is why: its page describes reproducing Statbel's rate for all 35 single ages exactly, under a
label announcing the figure was "not validated". Both halves now say who computed the number and stop
there. That removes the reading that made four separate reviewers argue about whether a label was earned.
