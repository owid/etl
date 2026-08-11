# Batch 1 — 08:16, 2026-08-11

Ten countries in population-rank order: India, China, United States, Indonesia, Pakistan, Nigeria,
Brazil, Bangladesh, Russia, Ethiopia. Reports below in the order they landed.

One thing the batch exposed about the exercise itself, not about any country: the brief handed to the
agents left out the sentence the page actually shows where there is no age-band table. Ethiopia's agent
reasonably concluded the reader was being pointed at a table that was not there. The brief now includes
that fallback sentence, so the remaining ninety agents will not repeat it.

The batch also produced one finding that applies to about forty countries rather than one, and it has
been fixed once at the top of the page rather than forty times in the caveats: a survey's fertility rate
describes the few years before the fieldwork, so a point plotted at the survey year sits later than the
fertility it measures. The overview caption now says so.

## Ethiopia

Verdict from the agent: minor problems. All five plotted values match the source's own trend figure
exactly, and it reproduced our stated arithmetic check (4.05 against the published 4.0) from the source
PDF.

- **Accepted.** "so the years are approximate" understated the problem. A survey point is not fuzzily
  near its label, it is systematically earlier. Rewritten to say the point plotted at 2024 is really
  about 2021 to 2024.
- **Accepted.** The source line read "Statistical Service", which is not the office's name and would be
  unidentifiable outside the page. Now "Ethiopian Statistical Service".
- **Accepted.** "multiplying by the band width" replaced with "multiplying by five, the width of each
  group"; "the age-specific rates behind the latest one" with "breaks the latest one down by the
  mother's age"; "female population by age" with "count of women by age".
- **Rejected.** That the empty age-band section contradicts the prose. The prose refers to table 3 of
  the source, not to anything on our page, and the page does explain why there is no breakdown. This was
  the brief's fault, described above.

## Bangladesh

Verdict: minor problems. It checked all 42 plotted years against the source table and found every one
correct, and independently reproduced our age-rate check.

- **Accepted.** "about 0.2 higher" for the health survey's gap was too round a number: it is about 0.1
  in 2022 and larger in earlier rounds. Now says so.
- **Accepted.** "with a confidence interval" and "which is what the rounding of those rates allows"
  were both replaced with plainer wording; "cloud bucket" with "cloud storage".
- **Rejected.** Spelling out SVRS inside the third block. The source line directly above already reads
  "Sample Vital Registration System".
- **Rejected, but recorded.** Linking to the report rather than the office's homepage. The agent found
  the report at an object-storage address; those rotate, and a dead link is worse than a homepage.

## India

Verdict: minor problems. Every plotted value verified against table 15 of the source PDF, and
corroborated in press reporting.

- **Accepted, factual error.** We called it "Annexure table 15" twice. The report reserves "Annexure"
  for a map and a district list; this is table 15 under "Tables on Trend". Fixed in both places.
- **Accepted, factual error.** "roughly 8 million people are enumerated" — the 2024 edition's own
  preface says about 8.9 million. Now "almost 9 million", and "enumerated" is now "tracked".
- **Rejected.** That the map's average gap is unexplained in the country's own prose. It is defined in
  the caption of the map section where it appears.

## Brazil

Verdict: minor problems. It pulled the 2024 births from the source's interface and reproduced our
plotted 1.44 exactly, and independently confirmed the coverage break we flag — registered births jumped
8.9% between 2002 and 2003 with no real fertility jump behind it.

- **Accepted, accuracy gap.** "IBGE's own published fertility rate comes from projections, not the
  registry" left out the office's most publicized recent figure: 1.55 for 2022 from the census, which
  IBGE called the lowest ever recorded. Our registry figure for that year is 1.52. Both are now named,
  with the reason they differ.
- **Accepted.** The link we give readers no longer lists table 197, the source of the 2000-02 points, so
  those three years cannot be checked from it. The prose now says so outright.
- **Accepted.** "SIDRA database" and "public API" replaced with plainer wording.
- **Rejected.** Changing the label from "Complete registration" to something weaker because coverage was
  incomplete in 2000-02. The label describes what the figure is built from, and Brazilian registration is
  complete now; downgrading it would misdescribe 2003-2024 to fix a caveat that is already stated
  prominently. The alternative — dropping the first three years — would throw away real data to make the
  label tidier.
- **Rejected.** That "validated" might be read as "cross-checked against another source". The wording of
  that scale was chosen deliberately and applies to twenty-four countries; changing it here only would
  make the collection inconsistent.

## Pakistan

Verdict: minor problems. All six plotted values verified against the individual survey report PDFs, and
our 2007 recalculation reproduced independently (3.69 against the published 3.7).

- **Accepted, factual error about the source.** We said the Bureau revived the survey because the
  national database authority told it vital records were not good enough. The report says nothing of the
  kind: it attributes the gap to the repeatedly postponed 2017 census, and to that census then not
  covering fertility. The database authority appears once, having endorsed the relaunch. Rewritten to say
  what the Bureau actually says. This is the worst kind of error to make — a plausible story attributed
  to a source that does not tell it.
- **Accepted.** "ran the Demographic Survey annually until 2007" overstated it. The editions are 1999,
  2000, 2001, 2003, 2005, 2006 and 2007, and the survey is designed to run every other year. Now says so.
- **Logged, not built.** Using the health survey rounds as a second series would let those years be
  rebuilt from birth histories. It would not change the source label, and the agent put it at a day or
  two, so it does not meet the bar for building today.

## Russia

Verdict: minor problems, but nothing wrong. It verified all 29 plotted values, the 2022 age-band births
and women, our 1.39 recalculation, the 1.8% gap, and both territory statements — against Rosstat's own
files, having had to bypass the same broken certificate we warn about.

- **Accepted, all clarity.** Four sentences leaned on words a general reader has no reason to know:
  "the gap is the denominator", "rebasing trap", "broken certificate chain", "certificate checking
  relaxed". All rewritten. The run-on in the second block is now two sentences.
- **Accepted.** It spotted that the gap would close on its own if Rosstat published its average-population
  file for 2022, which would make this a figure we had checked ourselves. That prospect is now in the text.

## China

Verdict: minor problems. It downloaded all three census tables and reproduced the three plotted values to
three decimals, and the 2020 age-band breakdown row for row.

- **Accepted, unverifiable attribution.** We said "the bureau's own companion table computes 1.301 for
  2020 from five-year bands". The agent could find no such separate table — the figure comes out of the
  same table we already cite, re-grouped into five-year groups. Reworded to say that.
- **Accepted.** "census long form's sample" replaced with a description of what it is; the fragment "Only
  census years." folded into a sentence; "at each single age" now "at each age from 15 to 49".
- **Accepted.** We presented the dispute over the 2010 census figure as settled. Some demographers
  concluded under-reporting was not the explanation, and the text now says both.
- **Logged, not built.** Whether the decennial 1% survey publishes a free age-specific fertility table
  that would fill mid-decade years. The agent could not confirm one exists.

## Indonesia

Verdict: minor problems. All four plotted values verified against the source release and corroborated
independently.

- **Accepted, misattribution.** "BPS says plainly that Indonesian civil registration coverage is still
  incomplete and cannot be used for this" — the agent read the linked release end to end and it says
  nothing about registration at all. The claim is true of Indonesia but not of that document. Rewritten so
  it no longer puts words in the source's mouth.
- **Accepted.** BPS was never expanded; the third block was the densest on the page. Both fixed, and
  "inter-censal survey" is now "surveys taken between them" everywhere it appeared.

## Nigeria

Verdict: minor problems. Plotted series, our recalculation, the survey figure and the UN comparison all
verified against primary documents.

- **Accepted, and the most serious finding of the batch.** We stated that the Population Commission's
  interpolated row "had Nigeria at 5.14 in 2022". The agent searched the matching report end to end: 5.14
  appears once, as Borno State's 2012 value, and the report has no national row at all. A specific number
  I could not re-source has been removed. The point it was illustrating — that the row is drawn between
  surveys rather than measured — stands without it.
- **Accepted.** "The Bureau of Statistics" is the National Bureau of Statistics; "Calculated TFR" now
  carries a plain gloss; "reconcile with the survey's own denominators" is now about matching the number of
  women the survey counted.

## United States

Verdict: sound. Every spot-checked year matched, and a second pass corroborated seven years against the
original reports.

- **Accepted.** It ran down an apparent discrepancy and resolved it in our favor: the 2000 figure was
  published at the time as 2.130 and later revised to the 2.056 we plot, after the 2000 census showed the
  population estimates behind it had been too low. Not an error, but a reader who finds the original report
  would think it was one, so the page now says which figure is superseded and why.
