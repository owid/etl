---
theme: default
title: 'R&D: LLMs for Search'
author: OWID
date: 2026-02-09
layout: cover
---

# LLMs for Search

**Improving search with AI**

<div class="absolute bottom-10 left-10">
  <div class="text-sm opacity-75">
    OWID Research & Development<br/>
    February 2026
  </div>
</div>

---

# Why Now?

**The opportunity and the challenge**

- **LLMs are changing UX expectations**
  - Google AI mode shows what's possible
  - Simple search bars may not be enough in the AI age

- **Current system is failing users**
  - 21% of queries return zero results
  - Straightforward queries like "public school funding" fail

- **Reputation considerations**
  - Test AI on structured data (our strength)
  - Avoid ChatGPT/MCP pitfalls (hallucinations, unreliability)
  - Users trust us for accurate data, not generative text

---

# Our North Star: Google AI Mode

**What's possible with AI-powered search**

<div class="grid grid-cols-2 gap-8 mt-4">

<div>

![Google AI Mode on OWID](./google-ai-mode.png)

</div>

<div>

Google's AI mode demonstrates:
- Natural language understanding of queries
- Contextual summaries with source citations
- **<1s latency** even with LLM reasoning
- Better results than traditional keyword search

**This sets our aspiration**

</div>

</div>

---
layout: section
---

# Problems

---

# Problem 1: Mapping Natural Language Queries

Users express intent in natural language, but keyword search doesn't match anything

- **User query**: "How has life expectancy changed over time?"
- **What they mean**: Time-series data on life expectancy
- **What keyword search sees**: No exact phrase match → **zero results**

**Challenge**: Understanding user intent vs. matching text strings

---

# Problem 2: Zero Search Results

**21% of queries return no results** even when relevant content exists

**Real examples that fail:**
- "public school funding"
- "road traffic deaths age"
- "average hours of work per week"

**Why this happens:**
- **Vocabulary mismatch**: "CO2 emissions" vs "carbon dioxide"
- **Spelling variations**: "labour" vs "labor", "urbanisation" vs "urbanization"
- **Phrasing differences**: "GDP per capita" vs "GDP per person"

**Impact**: Users think content doesn't exist → **immediate frustration** → abandon search

---

# Problem 3: Irrelevant Results

Search returns results that technically match but aren't what the user wants

**Real examples of mismatched results:**
- "population" → returns Mpox disease statistics
- "populism" → returns population data
- "money" → returns military expenditures
- "physical exercise" → returns child labor topics

**The problem**: Keyword matching ignores semantic relationships

**The result**: Low-quality results ranked highly, relevant content buried

---
layout: section
---

# Solutions

---

# Evolution of Search Technologies

| Technology | Adoption Era | Key Innovation |
|-----------|--------------|----------------|
| **Keyword Search** | ~1990s | Traditional information retrieval |
| **Semantic Search** | ~2018 | Transformers & embeddings (BERT) |
| **Hybrid Search** | ~2020 | Combining keyword + semantic |
| **Agentic RAG** | ~2023 | LLM agents with reasoning |
| **Future?** | ~2025+ | Multimodal, reinforcement learning |

---

# Keyword Search (~1990s)

**Traditional information retrieval**: Term frequency, inverse document frequency (TF-IDF)

**Pros:**
- Fast and predictable
- Well understood, easy to debug
- Works well for exact matches

**Cons:**
- No understanding of semantics
- Vocabulary mismatch problems
- Can't handle natural language queries

**Cost/Latency:**
- Free with Algolia (our current provider)
- ~10-50ms latency

---
zoom: 0.95
---

# Semantic Search (~2018)

**Neural embeddings**: Convert text to vectors in semantic space

**Pros:**
- Understands meaning, not just words
- Handles synonyms and related concepts
- Better for natural language queries
- **Multilingual**: 33% of users have non-English language

**Cons:**
- Can miss exact keyword matches
- Requires model deployment
- More computationally expensive

**Cost/Latency:**
- ~$10-15/month
- ~50-200ms latency

---

# Hybrid Search (~2020)

**Best of both worlds**: Combine keyword + semantic search with reranking

**Pros:**
- Captures both exact matches and semantic meaning
- More robust than either alone
- Can tune keyword vs semantic weight

**Cons:**
- More complex to implement
- Requires careful balancing of signals
- Higher computational cost

**Cost/Latency:**
- ~$20/month (requires switch from Algolia to Typesense)
- ~100-300ms latency

---
zoom: 0.95
---

# Agentic RAG (~2023)

**LLM-powered retrieval**: Agent reasons about query, retrieves, and generates answers

**Pros:**
- Extremely flexible and powerful
- Can handle complex, multi-step queries
- Natural language interaction

**Cons:**
- Expensive per query
- Slower response times
- Non-deterministic results
- Can fail to return expected JSON format

**Cost/Latency:**
- ~$200/month
- ~1-5s latency

---


# Vocabulary: The Foundation

**Bridging user language to our taxonomy**

<div class="grid grid-cols-2 gap-8">

<div>

**Keyword Search**
- Inverted index (A-B-C)
- Exact match required
- Limited synonym support

**Limitations:**
- "public school funding" → no results
- "road traffic deaths age" → no results
- Phrase matching is brittle

</div>

<div>

**Semantic Search**
- Embeddings/vectors
- Maps "solar power" → "renewable energy"
- Understands related concepts

**Benefits:**
- Handles synonyms naturally
- Works across languages (33% of users non-English)
- Bridges vocabulary gaps

</div>

</div>

---

---
layout: section
---

# Pitches

---
zoom: 0.8
---

# Pitch 1: Hybrid Search with Typesense

**Problem**: 21% of queries return zero results, Algolia only does keyword matching

**Solution**: Migrate to Typesense with built-in hybrid search
- Native keyword + semantic search in one engine
- Reranking combines both signals automatically
- Open-source, self-hosted — full control

**Why Typesense**:
- Hybrid search out of the box (no glue code)
- Matthieu had a prototype in the pipeline
- ~$20/month hosting vs Algolia's growing costs

**Trade-off**:
- Requires Algolia migration (might lose analytics history)
- Need to rebuild integrations

**Cost/Latency**: ~$20/month, ~100-300ms

---
zoom: 0.8
---

# Pitch 2: Suggested Keywords

**Problem**: Users don't know what terms to search for

**Solution**: Show related search keywords as user types
- Real-time keyword suggestions
- Based on vocabulary from our charts/articles
- Help users refine their queries

**Implementation**:
- Build vocabulary index from our content
- Semantic search over the vocabulary
- Display relevant keywords dynamically

**Benefits**:
- Guide users to successful queries
- Expose available content
- Reduce failed searches

**Cost/Latency**:
- Included in semantic search costs (~$10-15/month)
- ~50-100ms latency

---
zoom: 0.75
---

# Pitch 3: Suggested Topics

**Problem**: Users may not know what topics are available

**Solution**: Context-aware topic recommendations
- Show related topics based on current query
- Display when search returns zero results
- Showcase our topic searches (which are really good!)
- Show featured metrics

**Implementation**:
- Trivial: Show LLM our 100+ topics and ask for recommendations
- LLM selects most relevant topics for the query

**Benefits**:
- Increase content discovery
- Improve exploration experience
- Connect related datasets
- Turn failed searches into successful explorations

**Cost/Latency**:
- Minimal (mostly existing infrastructure)
- ~50ms latency

---
zoom: 0.85
---

# Pitch 4: Semantic Search on Cloudflare

**Approach**: Semantic search + query rewriting + reranking via Cloudflare AI Search

**Components**:
1. Query rewriting (expand/normalize)
2. Semantic search (embeddings)
3. Reranking (learned model)

**Pros**:
- Much cheaper than agentic approach
- Predictable latency
- Good quality results

**Cons**:
- Hard to customize (not particularly customizable or good)
- Less flexible than agents
- Cloudflare-specific implementation

**Cost/Latency**: Included in Cloudflare pricing, ~100-200ms

---
zoom: 0.8
---

# Pitch 5: Algolia + Cloudflare Semantic Hybrid

**If Algolia migration is too costly** — a lighter alternative to Pitch 1

**Approach**: Keep Algolia as primary, hydrate results with Cloudflare semantic search
- Keyword results from Algolia (fast, existing infrastructure)
- Semantic results from Cloudflare (fills zero-result gaps)
- Merge and deduplicate results

**Pros**:
- No Algolia migration needed — keep analytics and integrations
- Solves the zero-result problem (80% of the benefit)
- Incremental improvement, low risk
- We can monitor click-through rates of keyword vs semantic results

**Cons**:
- Latency mismatch: Algolia ~10-50ms vs semantic ~100-200ms
- Need to merge results sensibly
- UX challenge: results arriving at different speeds
- Two systems to maintain

**Cost/Latency**: ~$10-15/mo for Cloudflare embeddings, mixed latency

---
zoom: 0.8
---

# Pitch 6: Google Vertex AI Search

**Approach**: Fully managed enterprise search with semantic understanding

**What it does**:
- Hybrid retrieval (lexical + semantic) with Google's ranking
- Structured data support with facets and filtering
- Optional generative answers with citations
- Learns from click-through data over time

**Pros**:
- Best-in-class retrieval quality (Google's search expertise)
- Handles structured metadata well (charts, topics, countries)
- Minimal ML expertise needed

**Cons**:
- Very expensive: **~$1,800/mo** (Standard) to **$4,800/mo** (Enterprise)
- Vendor lock-in to Google Cloud
- Overkill for our corpus size (~thousands of charts, not millions)

---
zoom: 0.78
---

# Pitch 7: Agentic Search

**Approach**: LLM agent orchestrates the entire search process

**Note**: Using generic LLMs for query rewriting and reranking is very inefficient

**Capabilities**:
- Understand complex queries
- Add chart parameters dynamically
- Follow custom instructions
- Multi-step reasoning

**Pros**:
- Extremely flexible
- Natural language interaction
- Can handle edge cases

**Cons**:
- Expensive (~$200/month with Gemini Flash 2.5 Lite)
- Unreliable (JSON response issues)
- Non-deterministic (inconsistent results)
- High latency (~1-5s)

---
layout: section
---

# Outputs

---
zoom: 0.85
---

# Outputs

**From this R&D cycle**

**1. Search Comparison Tool**
- Side-by-side qualitative comparison in Wizard
- [/etl/wizard/search-comparison](http://staging-site-ai-search-api/etl/wizard/search-comparison)

**2. API Endpoints (Cloudflare Workers)**

*Suggested topics:*
- `/api/ai-search/topics?q=population`

*Suggested searches:*
- `/api/ai-search/searches?q=population`

**3. Semantic Search with Reranking**
- `/api/ai-search/charts?q=population`

**4. Agentic Search**
- `/api/ai-search/recommend?q=population`

---

# Risks & Guardrails

<div class="grid grid-cols-2 gap-8">

<div>

## ⚠️ Reputational Risks

- **Chatbot hallucinations**
  - Users trust us for accurate data
  - Not generative text

- **Jailbreak vulnerability**
  - LLMs can be manipulated
  - Need robust safeguards

- **User expectations**
  - We're known for data quality
  - Can't afford AI mistakes

</div>

<div>

## 🛡️ The "Narrow Solution" Strategy

- **Respond with Charts, not Chat**
  - Show actual data visualizations
  - Not generated summaries

- **LLM for Retrieval, not Generation**
  - Use AI to find content
  - Don't create new content

- **Results Trace to Database**
  - Every result links to source
  - Fully auditable

</div>

</div>

---
layout: section
---

# Challenges

---
zoom: 0.83
---

# Problematic Queries

**Real examples from testing that highlight system limitations**

**Zero Results (should find content):**
- "public school funding"
- "road traffic deaths age"
- "average hours of work per week"

**Vocabulary Mismatch:**
- "poland modern economic miracle" → needs "GDP growth" understanding
- "is Italy richer then France?" → needs "GDP per capita" concept

**Natural Language Understanding:**
- "How has life expectancy changed over time?" → needs time-series intent
- "physical exercise" → returns child labor topics

**Ambiguous/Multi-intent:**
- "population" → shows Mpox statistics instead of demographics
- "populism" → returns population data
- "money" → shows military expenditures

---
layout: center
---

# Conclusions & Path Forward

---
zoom: 0.82
---

# Next Steps

<div class="grid grid-cols-2 gap-12">

<div>

## ✅ Immediate Wins

**Can implement now, low risk**

- **Hybrid Search (Typesense)**
  - Fixes the 21% zero-result problem
  - Built-in keyword + semantic search
  - ~$20/month, prototype exists

- **Suggested Keywords & Topics**
  - Guide users to better searches
  - Show related topics on zero results
  - Works on top of any search backend

</div>

<div>

## 🔬 Further Exploration

**Worth investigating**

- **Agentic Prototype**
  - Test capabilities and limitations
  - Understand cost/benefit tradeoffs
  - Learn what's possible with LLMs

- **Context Window Search**
  - Load entire dataset metadata into LLM context
  - Prompt caching to reduce costs
  - Direct search within the LLM

</div>

</div>

---
layout: center
---
# Discussion

**Questions? Feedback? Priorities?**

<div class="text-sm opacity-75 mt-8">
The 'zero result' problem is solvable today.<br/>
True 'answer' capabilities define the future.
</div>
