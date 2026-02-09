---
theme: default
title: 'R&D: LLMs for Search'
author: OWID
date: 2026-02-09
---

# R&D: LLMs for Search

TODO:
- don't use v-clicks
- in problem 1, keyword search doesn't matches anything, so no results are returned
- in problem 2, use "public school funding" as an example query that returns zero results
- keyword search by Algolia costs zero for us
- costs do per month, in case of semantic search it's $10-$15/month
- to do hybrid search, we would have to switch from Algolia to Typesense, but monthly costs should still be less than $20
- agentic rag would cost around $200/month


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

Users express intent in natural language, but traditional search expects keywords

<v-clicks>

- **User query**: "How has life expectancy changed over time?"
- **What they mean**: Time-series data on life expectancy
- **What keyword search sees**: Scattered matches for "life", "expectancy", "changed", "time"

</v-clicks>

<v-click>

**Challenge**: Understanding user intent vs. matching text strings

</v-click>

---

# Problem 2: Zero Search Results

Queries return no results even when relevant content exists

<v-clicks>

- **Vocabulary mismatch**: User says "CO2 emissions", dataset uses "carbon dioxide"
- **Spelling variations**: "labour" vs "labor", "urbanisation" vs "urbanization"
- **Phrasing differences**: "GDP per capita" vs "GDP per person"
- **Partial matches**: User query too specific or uses wrong terminology

</v-clicks>

<v-click>

**Impact**: Users think content doesn't exist, abandon search

</v-click>

---

# Problem 3: Irrelevant Results

Search returns results that technically match but aren't what the user wants

<v-clicks>

- **Example**: Query "Monkeypox from population"
  - Returns: General population statistics
  - User wants: Monkeypox prevalence in populations

- **Problem**: Keyword matching ignores semantic relationships
- **Result**: Low-quality results ranked highly, relevant content buried

</v-clicks>

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

<v-clicks>

**Pros:**
- Fast and predictable
- Well understood, easy to debug
- Works well for exact matches

**Cons:**
- No understanding of semantics
- Vocabulary mismatch problems
- Can't handle natural language queries

**Cost/Latency:**
- ~$0.0001/query
- ~10-50ms latency

</v-clicks>

---

# Semantic Search (~2018)

**Neural embeddings**: Convert text to vectors in semantic space

<v-clicks>

**Pros:**
- Understands meaning, not just words
- Handles synonyms and related concepts
- Better for natural language queries

**Cons:**
- Can miss exact keyword matches
- Requires model deployment
- More computationally expensive

**Cost/Latency:**
- ~$0.001-0.01/query
- ~50-200ms latency

</v-clicks>

---

# Hybrid Search (~2020)

**Best of both worlds**: Combine keyword + semantic search with reranking

<v-clicks>

**Pros:**
- Captures both exact matches and semantic meaning
- More robust than either alone
- Can tune keyword vs semantic weight

**Cons:**
- More complex to implement
- Requires careful balancing of signals
- Higher computational cost

**Cost/Latency:**
- ~$0.01-0.05/query
- ~100-300ms latency

</v-clicks>

---

# Agentic RAG (~2023)

**LLM-powered retrieval**: Agent reasons about query, retrieves, and generates answers

<v-clicks>

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
- ~$0.10-0.50/query
- ~1-5s latency

</v-clicks>

---

# Future Directions

<v-clicks>

- **Multimodal search**: Search across text, charts, images
- **Reinforcement learning**: Learn from user interactions
- **Personalized search**: Adapt to user preferences and history
- **Real-time learning**: Update models with new content automatically
- **Explainable AI**: Show why results were returned

</v-clicks>

---
layout: section
---

# Pitches

---

# Pitch 1: Fixing Zero Searches with Hybrid Search

**Problem**: Users get no results even when relevant content exists

TODO:
- mention we'd have to switch from Algolia to Typesense, Matthieu had a prototype in the pipeline

<v-clicks>

**Solution**: Deploy hybrid search (keyword + semantic)
- Keyword search catches exact matches
- Semantic search catches related concepts
- Reranking combines signals

**Benefits**:
- Dramatically reduce zero-result queries
- Better handling of vocabulary mismatches
- Improved user satisfaction

**Cost/Latency**:
- ~$TBD/query
- ~TBD ms latency

</v-clicks>

---

# Pitch 2: Suggested Searches (Using Vocabulary)

**Problem**: Users don't know what terms to search for

TODO: we have to build a vocabulary index from our charts / articles and then semantically search it

<v-clicks>

**Solution**: Auto-suggest based on controlled vocabulary
- Real-time suggestions as user types
- Powered by vocabulary mappings
- Show popular/related searches

**Benefits**:
- Guide users to successful queries
- Expose available content
- Reduce failed searches

**Cost/Latency**:
- ~$TBD/query
- ~TBD ms latency

</v-clicks>

---

# Pitch 3: Suggested Topics

**Problem**: Users may not know what topics are available

TODO: useful as a discovery when search returns zero results and to showcase our topics searches, which are really good and show featured metrics

<v-clicks>

**Solution**: Context-aware topic recommendations
- Show related topics based on current query
- Only display when relevant
- Help users discover related content

**Benefits**:
- Increase content discovery
- Improve exploration experience
- Connect related datasets

**Cost/Latency**:
- ~$TBD/query
- ~TBD ms latency

</v-clicks>

---

# Pitch 4: Semantic Search + Query Rewriting + Reranking

TODO: "Pure ML pipeline without heavy LLM usage" is wrong, we'd just use Cloudflare's AI Search that has special models for query rewriting and reranking. These are cheap, but not particularly cutosmizable or good

**Approach**: Pure ML pipeline without heavy LLM usage

<v-clicks>

**Components**:
1. Query rewriting (expand/normalize)
2. Semantic search (embeddings)
3. Reranking (learned model)

**Pros**:
- Much cheaper than agentic approach
- Predictable latency
- Good quality results

**Cons**:
- Hard to customize via LLMs
- Requires training/fine-tuning
- Less flexible than agents

**Cost/Latency**: ~$TBD/query, ~TBD ms

</v-clicks>

---

# Pitch 5: Agentic Search

**Approach**: LLM agent orchestrates the entire search process

TODO: add a note that using generic LLM for query rewriting and reranking is very inefficient

<v-clicks>

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
- Likely expensive (~$0.10-0.50/query)
- Unreliable (JSON response issues)
- Non-deterministic (inconsistent results)
- High latency (~1-5s)

</v-clicks>

---
layout: section
---

# Outputs

---

# Outputs

TODO: it will be Outputs from this cycle, I'll then paste some screenshots

**TBD**: Add specific results, demos, or comparisons here

<v-clicks>

Possible content:
- Screenshots of different search approaches
- A/B test results
- User satisfaction metrics
- Example queries and results
- Performance comparisons

</v-clicks>

---
layout: section
---

# Challenges

---

# Problematic Queries

Examples of queries that are particularly challenging

<v-clicks>

**TBD**: Add specific problematic queries from your testing

Example categories:
- Ambiguous queries
- Multi-intent queries
- Highly specific technical terms
- Queries requiring multiple datasets
- Queries with implicit context

</v-clicks>

---

# Need for a Benchmark?

Should we create a standardized benchmark for evaluating search quality?

<v-clicks>

**Why benchmark?**
- Objective comparison of approaches
- Track improvements over time
- Catch regressions
- Guide optimization efforts

**What to measure?**
- Precision/recall on curated query set
- Zero-result rate
- User satisfaction scores
- Latency and cost metrics

**Open questions?**
- What queries to include?
- How to label relevance?
- How often to run?

</v-clicks>
