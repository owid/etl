# OWID Topic Vocabulary Extraction

Simple LLM-based CLI to extract characteristic keywords from OWID chart titles and subtitles, grouped by topic, and to suggest the countries most worth looking at on each topic.

## Overview

For each OWID topic, this builds the short line of suggested search terms shown
under the all-charts block's search box.

Choosing the line is a judgment, so the model makes it, with measurements in hand: it proposes candidate terms, code measures what clicking each one would show (which charts, and their share of the topic's views), and the model then chooses the line from those measured candidates. Code only rules out what can be checked: places, terms that show nothing, and terms matching nearly every chart, which would narrow nothing (unless every candidate does, as on a topic whose charts are all named after it).

Four phases:

1. Load, once: topic names, chart view counts (`analytics_chart_views`), and the
   multi-dim view → config id map that lets a single view's popularity be found.
2. Per topic, fetch the record set the all-charts block actually lists — plain
   charts, multi-dim views and explorer views alike — and weight each record by
   its views.
3. Ask the model for candidate terms (and countries), having shown it that record set.
4. Measure every candidate against it, and ask the model to choose the line, showing it each candidate's chart count, view share and most-viewed charts. The answer is constrained to the candidates offered.

## Quick Start

### Build a new vocabulary
```bash
.venv/bin/python scripts/vocabulary/vocabulary.py
```

This writes the full result, stats included, to `etl/steps/export/r2/owid/latest/topic_vocabulary.json`. Commit it in a PR: once merged, the production ETL run publishes it (see [Where the vocabulary goes](#where-the-vocabulary-goes-and-how-to-try-one-out)). The diff of that file is how one run is compared with the last.

### Extract for specific topics

The committed file *replaces* the vocabulary rather than merging into it, so a `--topic` run can't be written there: every topic it skipped would lose its suggestions once merged. Such a run has to go to another `--output`:

```bash
# Single topic, kept local
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --output vocab.json

# Multiple topics (run in parallel), also uploaded to a scratch key to try on a staging server
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --topic climate-change \
    --output vocab.json --upload-path topic_vocabulary/my-branch.json
```

### Choose model
```bash
# Use a cheaper model (default: claude-opus-5-5)
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --output vocab.json --model claude-sonnet-5-5
```

## CLI Options

- `--topic SLUG` - Topic slug(s) to extract (can be specified multiple times). If not provided, extracts for all topics.
- `--output PATH` - Write the full results, stats included, to this JSON file (default: the committed `etl/steps/export/r2/owid/latest/topic_vocabulary.json`)
- `--model MODEL` - Claude model to use (default: `claude-opus-5-5`; a full run costs about $5, and `--keep-countries` saves about a third). Any model id the API accepts works; models genai-prices has no price for just report their cost as unknown.
- `--upload-path KEY` - Also upload the published part to this scratch key in the `owid-public` bucket, to try it on a staging server (refuses `topic_vocabulary.json`, the key the site reads)
- `--report PATH` - Write an HTML coverage report: per term, what it reveals and what it adds, plus the most-viewed charts nothing covers
- `--weighting {views,uniform}` - Weight charts by views (default) or count them equally
- `--views-column {views_7d,views_14d,views_365d}` - Which window to weight by (default `views_365d`; a vocabulary is read for weeks, so the year is steadier than the week)
- `--max-terms N` - Most terms to publish per topic (default 8; the site shows five, skipping the one that matches what the reader has already typed, so a couple of spares keep the line full)
- `--keep-countries` - Reuse each topic's countries from the committed vocabulary instead of asking for them again, to iterate on the keywords for less (topics missing from it are still asked)

## Chart view data

Weighting reads `analytics_chart_views` from grapher MySQL — the only table with
per-view granularity, which matters because multi-dim and explorer pages
contribute one record per view and dominate some topics. It ships only in the
private data dump, so a local database has it **empty**: either run
`make refresh.private` in owid-grapher, or point `OWID_ENV` at a staging or
production database (with `STAGING=1` in `.env`, it already is). A run with no
view data **aborts** rather than silently weighting everything zero; pass
`--weighting uniform` to generate without popularity.

Plain charts and multi-dim views are weighted by their own view counts. Explorer
views can't be: their index records carry no config id, so each is weighted by
the average views per view of its explorer. The report says how many records fell
back that way.

## Where the vocabulary goes, and how to try one out

The site reads the vocabulary directly from the browser at

    https://files.ourworldindata.org/topic_vocabulary.json

an object in the `owid-public` R2 bucket, served through a worker that adds CORS and a 5-minute edge cache.

**Only the `export://r2/owid/latest/topic_vocabulary` step writes it**, and only with `--export`, which the production ETL run passes after every merge to master. The committed JSON counts toward that step's checksum, so merging a new vocabulary is what publishes it, and the live file is always the one on master. Local and staging builds of the step build it without uploading.

To try a new vocabulary before merging, upload it under a scratch key (the worker serves the whole bucket, so any key works):

```bash
.venv/bin/python scripts/vocabulary/vocabulary.py --upload-path topic_vocabulary/my-branch.json
```

then point a staging server at it (see `TOPIC_VOCABULARY_URL` in owid-grapher's `settings/clientSettings.ts`):

```
TOPIC_VOCABULARY_URL=https://files.ourworldindata.org/topic_vocabulary/my-branch.json
```

Suggestions fall back to the production vocabulary if that key isn't there, so a half-finished experiment leaves the page looking normal.

Keywords are consumed by the all-charts block's "Suggested:" line on topic
pages. It shows the leading few terms per topic, so ordering within a topic
matters: the terms it lists first are the ones most likely to be shown.

## How It Works

1. **Extract chart text**: takes the titles and subtitles of every record the
   topic's all-charts block lists, via the search index — not from the `charts`
   table, which omits multi-dim and explorer views entirely
2. **Deduplication**: Removes duplicate titles/subtitles (many charts share text)
3. **Sampling**: the 200 most-viewed distinct texts (not a random sample — the
   terms proposed should describe what people actually look at, and two runs
   should see the same input)
4. **Candidates**: feeds all text to Claude, asking for candidate search terms. Each of a topic's three requests (this one, countries, choosing the line) whose prompt and model are unchanged reuses its earlier answer from `.cache/vocabulary_llm_answers.json`, so a rerun reproduces the last result; delete that file to sample afresh
5. **Countries**: a second request per topic, shown the same chart texts, asks for the countries whose story on the topic is most worth seeing, best first. Each answer is matched against OWID's country names (names, short names and aliases in the regions definitions), since the site filters charts by those exact names; anything that isn't a current country is dropped and listed in the topic's `unrecognized_countries` stat
6. **Choosing the line**: a third request per topic shows the measured candidates and asks for the line, in order; `keyword_stats` then records what each chosen term shows and adds to the ones before it
7. **Parallel processing**: When multiple topics requested, processes all in parallel using asyncio

## Output Format

### Published file

The uploaded file holds only what the site reads (`PUBLISHED_FIELDS` in the export step), keyed by topic slug, as compact JSON served with `Content-Type: application/json` (which is what lets Cloudflare compress it):

```json
{
  "energy": {"topic_slug": "energy", "topic_name": "Energy", "keywords": ["solar panels", "fossil fuels", ...], "countries": ["Norway", "China", ...]},
  "climate-change": {"topic_slug": "climate-change", "topic_name": "Climate Change", "keywords": [...], "countries": [...]}
}
```

### Committed (`--output`) file

The committed file has the same keys but keeps the full record per topic, stats included (cost, coverage, and per-term shares in `keyword_stats`):

```json
{
  "energy": {
    "topic_slug": "energy",
    "topic_name": "Energy",
    "keywords": [...],
    "countries": [...],
    "stats": {"num_keywords": 8, "num_countries": 5, "unrecognized_countries": [], "weighted_coverage": 0.62, "input_tokens": 4236, ...},
    "keyword_stats": [{"term": "solar panels", "own_share": 0.21, "marginal_share": 0.21, ...}, ...]
  }
}
```

## Example Results

### Energy Topic (30 keywords)
- solar panels, photovoltaics, electric cars, battery-electric, plug-in hybrids
- coal, crude oil, natural gas, fossil fuels
- hydropower, bioenergy, geothermal, wave energy, tidal energy, nuclear
- low-carbon energy, renewables, wind capacity, onshore wind, offshore wind

### Climate Change Topic (30 keywords)
- global warming, greenhouse gas, CO2, methane, nitrous oxide
- sea ice, glaciers, ice sheets, ocean heat, ocean acidification
- Paris Agreement, RCP4.5, Green Climate Fund
- renewable energy, carbon taxes, electric cars, public transport

### Artificial Intelligence Topic (30 keywords)
- artificial intelligence, machine learning, neural networks, generative AI
- natural language processing, computer vision, robotics
- GPT-4, AlphaFold, NVIDIA, TSMC, GPUs, data center
- self-driving car, deepfake, MMLU, parameters, automation

## Filtering Rules

The LLM is instructed to:

**Extract ONLY:**
- Modern, currently-relevant terms
- Specific technologies, concepts people search for TODAY
- Domain-specific terminology

**Skip:**
- Generic terms: consumption, production, prices, investment, trade, access, growth, change, demand, supply, emissions, generation, reserves, intensity, transition, costs, spending
- Measurements: percentage, per capita, share, rate, level, annual, total, average
- Historical/obsolete terms (unless still relevant today)
- Phrases with generic words removed: "oil prices" → "oil"

## Cost & Performance

Each run ends with a cost estimate, priced with [genai-prices](https://github.com/pydantic/genai-prices). Models it has no price for report their cost as unknown.

## Requirements

- Anthropic API key in `.env` file:
  ```
  ANTHROPIC_API_KEY=your_key_here
  ```
- Dependencies: `anthropic`, `python-dotenv`, `click`
- Database access via `etl.config.OWID_ENV`

## Use Cases

- **Search suggestions**: Generate topic-specific search keywords
- **Topic understanding**: Quickly identify key terminology per topic
- **Content discovery**: Understand topic coverage and vocabulary
- **Quality validation**: Verify keyword extraction produces relevant results
