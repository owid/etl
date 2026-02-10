# OWID Topic Vocabulary Extraction

Simple LLM-based CLI to extract characteristic keywords from OWID chart titles and subtitles, grouped by topic.

## Overview

This tool uses a direct LLM approach to extract searchable, domain-specific keywords from chart text. It bypasses traditional NLP processing and asks an LLM to identify terms users would actually search for.

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
- `--model MODEL` - Gemini model to use: `gemini-3-flash-preview` (default) or `gemini-2.5-flash-lite`

## How It Works

1. **Extract chart text**: Queries database for all published charts tagged with requested topic(s)
2. **Deduplication**: Removes duplicate titles/subtitles (many charts share text)
3. **Sampling**: If >200 unique texts, randomly samples 200 for token efficiency
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
