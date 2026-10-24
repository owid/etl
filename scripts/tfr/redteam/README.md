# Red-teaming the hundred countries

Ten agents an hour, one country each, starting 08:15 on 2026-08-11. Each agent sees only what the
published page shows a reader — `redteam.py` prints that and nothing else — and answers five
questions: are the values right, is the metadata right, is it written plainly, is the quality label
right, and could a better source have put the country higher on the scale.

The brief is in `PROMPT.md`. Countries are taken in population-rank order; `redteam.py --next 10`
returns the next ones due, working out what is already done by reading the `## Country` headings in
the batch logs here.

Findings are judged before anything changes. Wrong values, wrong metadata and unclear wording get
fixed and pushed as each batch lands. A better source gets built if it is cheap — an open interface or
a spreadsheet that can be wired up in one go — and logged for a decision if it is not.

One file per batch, recording every finding, whether it was judged right, and what was done about it.
