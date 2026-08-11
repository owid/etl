# The standard red-team brief

One agent per country, Sonnet, launched in parallel. Ten per batch. Substitute the country name
everywhere `{COUNTRY}` appears. Add one tailoring sentence to question 1 where the country has a known
hazard — a contested census figure, several competing survey rounds, a territory change, a series we
recalculated ourselves — but keep the rest identical so the reports stay comparable.

The agent must not read anything under `scripts/tfr/` except by running `redteam.py`. That script
prints exactly what the published page shows a reader and nothing else. Letting an agent read the code
would have it judge what we meant rather than what we said, which is the whole point of the exercise.

---

You are red-teaming one country in a published comparison of national statistical offices' total
fertility rates against UN WPP estimates. Country: **{COUNTRY}**.

First, get exactly what the reader sees:

```
cd /Users/edouard/dev/owid/etl && .venv/bin/python scripts/tfr/redteam.py {COUNTRY}
```

That prints the whole of what we publish about this country: the source line, both quality labels, the
three prose blocks, the plotted series, the UN figures it is compared against, and the age-band
breakdown if there is one.

**Do not read any other file under scripts/tfr/.** Your job is to judge what the reader is shown, not
how it was produced — our code would tell you what we meant rather than what we said.

Then answer five questions, in order.

1. **Are the values and years right?** Open the source we point the reader to and check every plotted
   number and year against it. Then look for independent corroboration: the office's other releases,
   its press notices, national media reporting the figure, an academic paper quoting it. Report
   anything that does not check out and what it should be instead.
2. **Is the metadata accurate?** Everything we assert about the source, about the source's method, and
   about our own method. Look hardest for claims that sound right but are wrong, and for anything
   stated more confidently than the source supports.
3. **Is it written plainly?** Flag jargon, acronyms, assumed knowledge, and sentences a non-specialist
   reader would stumble on. Suggest the plainer wording. American spelling.
4. **Is the quality label right?** Both labels. The available values are printed in the brief.
5. **Could we have used a better source that would put this country higher on the quality scale?** If
   so, name it, give the URL, say what it would change, and estimate how much work it would take.

Rules:
- Judge only what is published. If you cannot verify something, write "could not verify" — never guess.
- Be specific. Quote the sentence you are challenging.
- Don't pad the report with praise, but do say plainly when a section is sound.
- Separate real problems from nitpicks; the first list gets acted on, the second gets skimmed.

Report in this format:

```
COUNTRY: {COUNTRY}
VERDICT: sound / minor problems / serious problems
VALUES AND YEARS: <what you checked, against what, and what if anything is wrong>
METADATA ACCURACY: <same>
CLARITY: <specific sentences, with plainer rewrites>
QUALITY LABEL: correct / should be <label>, because ...
BETTER SOURCE AVAILABLE: none found / <name, URL, what it would change, effort>
CONFIDENCE: high/medium/low
```

Then two ranked lists: "Findings worth acting on" and "Nitpicks".
