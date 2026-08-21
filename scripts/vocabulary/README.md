# OWID Topic Vocabulary Extraction

Simple LLM-based CLI to extract characteristic keywords from OWID chart titles and subtitles, grouped by topic.

## Overview

For each OWID topic, this builds the short line of suggested search terms shown
under the all-charts block's search box.

Selection is an optimisation, not a judgement call: **cover as much of the
topic's chart list as a few terms can, weighting charts by how much they are
viewed**, first term revealing the most, each next one adding the most that is
still uncovered. The LLM only proposes candidates; arithmetic picks them.

Four phases:

1. Load, once: topic names, chart view counts (`analytics_chart_views`), and the
   multi-dim view → config id map that lets a single view's popularity be found.
2. Per topic, fetch the record set the all-charts block actually lists — plain
   charts, multi-dim views and explorer views alike — and weight each record by
   its views.
3. Ask the model for candidate terms, having shown it that record set.
4. Choose the terms covering the most of it, by greedy weighted maximum coverage.

Two things follow from (4) rather than needing a second LLM pass: a term whose
rows are already covered adds nothing and is not chosen, and a term the block
would show nothing for covers nothing and cannot be chosen at all.

## Quick Start

### Extract for all topics
```bash
.venv/bin/python scripts/vocabulary/vocabulary.py
```

### Extract for specific topics
```bash
# Single topic
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy

# Multiple topics (run in parallel)
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --topic climate-change
```

### Save to file
```bash
# Single topic - saves as single object
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --output vocab.json

# Multiple topics - saves as dict keyed by slug
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --topic climate-change --output vocab.json
```

### Choose model
```bash
# Use faster/cheaper model (default: gemini-3-flash-preview)
.venv/bin/python scripts/vocabulary/vocabulary.py --topic energy --model gemini-2.5-flash-lite
```

## CLI Options

- `--topic SLUG` - Topic slug(s) to extract (can be specified multiple times). If not provided, extracts for all topics.
- `--output PATH` - Output JSON file path (optional, prints to console if not provided)
- `--model MODEL` - Gemini model to use (default: `gemini-3.7-flash`). Any model id the API accepts works; models missing from `PRICING` just report their cost as unknown.
- `--upload-path KEY` - Key to write inside the `owid-public` bucket (default: `topic_vocabulary.json`)
- `--no-upload` - Generate without writing to R2
- `--report PATH` - Write an HTML coverage report: per term, what it reveals and what it adds, plus the most-viewed charts nothing covers
- `--weighting {views,uniform}` - Weight charts by views (default) or count them equally
- `--views-column {views_7d,views_14d,views_365d}` - Which window to weight by (default `views_365d`; a vocabulary is read for weeks, so the year is steadier than the week)
- `--max-terms N` - Most terms to publish per topic (default 8; the site shows five and drops some, so a couple of spares keep the line full)
- `--min-marginal-share F` - Stop once the next term would reveal less than this share of a topic's views (default 0.01)

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

The result is uploaded to the `owid-public` R2 bucket and served through
`files.ourworldindata.org`, whose worker adds CORS and a 5-minute edge cache.
The site reads the default key directly from the browser:

    https://files.ourworldindata.org/topic_vocabulary.json

**A plain run overwrites the vocabulary the live site uses.** To try a new one
without doing that, upload it under a different key — the worker serves the
whole bucket, so any key works:

```bash
python scripts/vocabulary/vocabulary.py \
    --upload-path topic_vocabulary/my-branch.json
```

then point a staging server at it (see `TOPIC_VOCABULARY_URL` in owid-grapher's
`settings/clientSettings.ts`):

```
TOPIC_VOCABULARY_URL=https://files.ourworldindata.org/topic_vocabulary/my-branch.json
```

Suggestions fall back to the production vocabulary if that key isn't there, so
a half-finished experiment leaves the page looking normal.

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
4. **LLM extraction**: Feeds all text to Gemini with instructions to extract modern, searchable keywords
5. **Parallel processing**: When multiple topics requested, processes all in parallel using asyncio

## Output Format

### Single topic
```json
{
  "topic_slug": "energy",
  "topic_name": "Energy",
  "keywords": [
    "solar panels",
    "fossil fuels",
    "coal",
    "wind turbines",
    "nuclear power",
    ...
  ],
  "stats": {
    "num_charts_texts": 200,
    "num_keywords": 30,
    "input_tokens": 4236,
    "output_tokens": 199,
    "total_cost_usd": 0.000189
  }
}
```

### Multiple topics
```json
{
  "energy": {
    "topic_slug": "energy",
    "topic_name": "Energy",
    "keywords": [...],
    "stats": {...}
  },
  "climate-change": {
    "topic_slug": "climate-change",
    "topic_name": "Climate Change",
    "keywords": [...],
    "stats": {...}
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

**Gemini 3 Flash Preview pricing (as of Feb 2025):**
- Input: $0.0375 per 1M tokens
- Output: $0.15 per 1M tokens

**Gemini 2.5 Flash Lite pricing:**
- Input: $0.015 per 1M tokens
- Output: $0.06 per 1M tokens

**Example performance:**
- Single topic: ~2-4 seconds, $0.0001-0.0002
- Multiple topics: Runs in parallel, ~4-6 seconds total, $0.0003-0.0006
- All topics (~125): ~10-15 seconds, $0.02-0.05

## Requirements

- Google Gemini API key in `.env` file:
  ```
  GOOGLE_API_KEY=your_key_here
  ```
- Dependencies: `google-genai`, `python-dotenv`, `click`
- Database access via `etl.config.OWID_ENV`

## Use Cases

- **Search suggestions**: Generate topic-specific search keywords
- **Topic understanding**: Quickly identify key terminology per topic
- **Content discovery**: Understand topic coverage and vocabulary
- **Quality validation**: Verify keyword extraction produces relevant results
