---
theme: default
title: 'R&D: LLMs for Search'
author: OWID
date: 2026-02-09
---

# R&D: LLMs for Search

Exploring AI-powered approaches to improve search functionality

<div class="text-sm opacity-75 mt-8">
OWID Research & Development
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

Queries return no results even when relevant content exists

**Example**: "public school funding" → zero results

- **Vocabulary mismatch**: User says "CO2 emissions", dataset uses "carbon dioxide"
- **Spelling variations**: "labour" vs "labor", "urbanisation" vs "urbanization"
- **Phrasing differences**: "GDP per capita" vs "GDP per person"
- **Partial matches**: User query too specific or uses wrong terminology

**Impact**: Users think content doesn't exist, abandon search

---

# Problem 3: Irrelevant Results

Search returns results that technically match but aren't what the user wants

- **Example**: Query "population"
  - Returns: "Mpox: Cumulative confirmed cases per million people" as first result
  - User wants: General population data, not disease statistics

- **Problem**: Keyword matching ignores semantic relationships
- **Result**: Low-quality results ranked highly, relevant content buried

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

# Semantic Search (~2018)

**Neural embeddings**: Convert text to vectors in semantic space

**Pros:**
- Understands meaning, not just words
- Handles synonyms and related concepts
- Better for natural language queries

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

# Future Directions

**Context Window Search**: Fit all our data into LLM context window

- Load entire dataset metadata into context
- Use prompt caching to reduce costs
- Search directly within the LLM
- Potentially better understanding of relationships and context
- Challenge: Keeping data fresh and handling updates

---
layout: section
---

# Pitches

---

# Pitch 1: Fixing Zero Searches with Hybrid Search

**Problem**: Users get no results even when relevant content exists

**Solution**: Deploy hybrid search (keyword + semantic)
- Keyword search catches exact matches
- Semantic search catches related concepts
- Reranking combines signals

**Implementation**:
- Switch from Algolia to Typesense
- Matthieu had a prototype in the pipeline

**Benefits**:
- Dramatically reduce zero-result queries
- Better handling of vocabulary mismatches
- Improved user satisfaction

**Cost/Latency**:
- ~$20/month
- ~100-300ms latency

---

# Pitch 2: Suggested Searches (Using Vocabulary)

**Problem**: Users don't know what terms to search for

**Solution**: Auto-suggest based on controlled vocabulary
- Real-time suggestions as user types
- Powered by vocabulary mappings
- Show popular/related searches

**Implementation**:
- Build vocabulary index from our charts/articles
- Semantically search the vocabulary index

**Benefits**:
- Guide users to successful queries
- Expose available content
- Reduce failed searches

**Cost/Latency**:
- Included in semantic search costs (~$10-15/month)
- ~50-100ms latency

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

# Pitch 4: Semantic Search + Query Rewriting + Reranking

**Approach**: Cloudflare AI Search with specialized models

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

# Pitch 5: Agentic Search

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
layout: section
---

# Challenges

---

# Problematic Queries

Examples of queries that are particularly challenging

*Add specific problematic queries from your testing*

Example categories:
- Ambiguous queries
- Multi-intent queries
- Highly specific technical terms
- Queries requiring multiple datasets
- Queries with implicit context

---

# Need for a Benchmark?

Should we create a standardized benchmark for evaluating search quality?

**Why benchmark?**
- Qualitative analysis has limits
- Hyperparameter changes can vastly affect results
- Need objective comparison of approaches
- Track improvements over time
- Catch regressions

**Proposed approach:**
1. Take real search results
2. Generate representative queries for each topic (LLM)
   - Both keyword and natural language queries
3. Judge relevance automatically (LLM)
4. Human review of judgments

**What to measure?**
- Precision/recall on curated query set
- Zero-result rate
- User satisfaction scores
- Latency and cost metrics
