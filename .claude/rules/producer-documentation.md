---
paths:
  - "etl/steps/**/*.meta.yml"
  - "etl/steps/viz/**/*.yml"
  - "snapshots/**/*.dvc"
---

# Producer documentation behind metadata

This rule applies whenever you create, update or edit a dataset, and whenever you write, edit or verify its metadata: the snapshot `.dvc` origin, the garden `.meta.yml` (titles, units, `description_short`, `description_key`, `description_processing`, `description_from_producer`), and chart, MDim and explorer text. Every metadata skill follows it.

The documentation has two jobs: it fills the metadata you write, and it is the yardstick for the metadata that already exists.

## Find the latest documentation first

Before writing or judging a sentence, look for the producer's documentation of the release you are working with:

| Source | Often called | What it settles |
|--------|--------------|-----------------|
| Dataset page | the landing page (`url_main`): "about", indicator descriptions, notes | what the dataset is, coverage, cadence, often the definitions too |
| Methodology | methodology, technical notes, sources and methods, definitions | what each indicator measures, units, how it is estimated, coverage |
| Indicator definitions | indicator pages, metadata or "definition and measurement" boxes, glossary, series notes, notes under a table or figure | what exactly each indicator measures, its unit and caveats: the passage to cite for its `description_short` and `description_key` |
| Codebook | codebook, data dictionary, variable list, readme, a notes sheet inside the file | what each column and code means, how missing values are marked |
| Main report | flagship report, publication, paper | context, the producer's own reading of the data, the citation they ask for |
| What's new | release notes, changelog, revisions, "what's new in this edition" | what changed since the previous release: revisions, new or dropped indicators, method changes |

Where to look: the dataset page and the pages it links to; the data files themselves (readme or notes sheets, PDF metadata); the repository page (Zenodo, OSF, Dataverse, GitHub); the producer's publications page; a web search for the producer, dataset and year. Fetch with curl, then WebFetch, then the Wayback Machine before treating a link as dead, and read PDFs with the Read tool. When a page blocks automated fetching (a 403 or a bot check) or renders its content with JavaScript, offer to open it in the user's Chrome through the Claude in Chrome extension (or a headed Playwright Chrome, `channel="chrome"`) before giving up: a real browser gets past the block, can follow the page's links, and can read charts embedded in iframes (a Flourish chart's data is in its `flo.uri.sh/visualisation/<id>/embed` page).

**Make sure it is the latest.** The document's edition or year must match the data you are ingesting. A methodology labeled for an earlier edition, or dated before a revision of the method, is stale: look for the newer one. On an update, the links carried over in the old `.dvc` and `.meta.yml` point at the previous edition, so search again instead of reusing them.

**Record what you found**, one line per source: link and edition, or "not found", or "not accessible" with the reason. For the indicator definitions, say where each indicator, or group of indicators, is explained (page, section, box or sheet). Show the list to the user as links at the workflow's first briefing or checkpoint, so they know where to read up, and put it where the workflow keeps its notes: the PR description for dataset work, Part 0 of a `/fact-check-dataset` report.

**Say what the metadata rests on.** When you hand over metadata you wrote or changed, say for each indicator (or field, where they differ) which source it comes from and where in it: the document and its section, table or page, e.g. "`description_key`: report, 'Definition and measurement' box, p. 164". Mark the rest as inferred from the data, or as unverified, so the reader knows which text to check. Before citing a passage, read your new text against it as you would existing text (next section): cite it only when it supports the claim.

**Snapshots need less.** A `.dvc` origin usually needs only what the dataset page says. When the page covers the fields you are writing, it is enough: look for the other sources only for what it leaves open, and note which ones exist for the later steps. The full search applies from the garden metadata onward.

## Contrast what's already written

Once the documentation is in hand, read the existing text against it, not only the fields you are filling: the `.dvc` origin, the `.meta.yml` fields, `description_from_producer`, and chart, MDim and explorer text. Look for text that is:

- **wrong**: the documentation contradicts a definition, unit, scope or coverage claim;
- **stale**: it describes an earlier edition's method, or a "what's new" item has made it outdated;
- **missing**: the documentation gives a caveat or definition the reader needs, and the text leaves it out.

For each finding, propose a concrete rewrite and cite the passage it rests on. Make the fix through the skill's usual edit path (`/edit-faust-metadata` for shared or user-facing text, with its blast-radius check). Text you couldn't check against any source stays as it is, listed as unverified, not rewritten.

## When to ask the user

Ask the user before writing metadata that depends on a source, rather than filling the gap by inference, when:

- **you can't find it**: for example, no methodology that defines the indicator, or no codebook that says what a column means;
- **what you found is not enough**: too thin to fill the fields, an older edition than the data, silent on the indicators in scope, or contradicting the data;
- **you found it but can't open it**: a paywall, a login or registration form, a 4xx after the full fetch escalation, a PDF that won't parse, a file behind a request form.

For a snapshot, ask only when a `.dvc` field depends on the missing source.

Ask once, in one message: for each source, what you found (link and edition), what is missing or inaccessible (with the URL and the error), and which fields it blocks. Ask the user to share the file or link, or to confirm that it doesn't exist. Where the skill already has a checkpoint with the user, put the question there instead of adding a stop, and keep working on whatever doesn't depend on the answer. A background agent that can't ask lists these items in its report for the caller to raise.

If the user confirms a source doesn't exist, write only what the data and the sources you have support, and flag the fields you had to infer in the review or PR description.
