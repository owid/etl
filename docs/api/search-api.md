---
tags:
  - API
  - Catalog
icon: material/api
---

!!! danger "This is a work in progress"


# Search API

<!-- ## `/api/search` -->

This route provides a search API for both charts and pages (articles, about pages).

### Query Parameters

#### Common Parameters

| Parameter     | Type                    | Default    | Description                                      |
| ------------- | ----------------------- | ---------- | ------------------------------------------------ |
| `q`           | string                  | `""`       | Search query text                                |
| `type`        | `"charts"` \| `"pages"` | `"charts"` | Type of content to search                        |
| `page`        | number                  | `0`        | Page number for pagination (0-indexed, max 1000) |
| `hitsPerPage` | number                  | `20`       | Number of results per page (min 1, max 100)      |

#### Chart Search Parameters

| Parameter             | Type    | Description                                                  |
| --------------------- | ------- | ------------------------------------------------------------ |
| `countries`           | string  | Country names separated by `~` (e.g., `United States~China`) |
| `topics`              | string  | Topic name (e.g., `Health`)                                  |
| `requireAllCountries` | boolean | If `true`, only return charts with ALL specified countries   |

### Response Schemas

#### Chart Search Response (`type=charts`)

```typescript
{
    query: string // The search query
    results: Array<{
        title: string
        slug: string
        subtitle?: string
        variantName?: string
        type: "chart" | "explorerView" | "multiDimView"
        queryParams?: string // For explorer/multi-dim views
        availableEntities: string[]
        originalAvailableEntities?: string[]
        availableTabs: string[]
        url: string // Full URL to the chart
    }>
    nbHits: number // Total number of results
    page: number // Current page (0-indexed)
    nbPages: number // Total number of pages
    hitsPerPage: number // Results per page
}
```

#### Page Search Response (`type=pages`)

```typescript
{
    query: string
    results: Array<{
        title: string
        slug: string
        type: string // "article" | "about-page"
        thumbnailUrl?: string
        date?: string
        content?: string
        authors?: string[]
        url: string // Full URL to the page
    }>
    nbHits: number // Total number of results
    offset: number // Current offset
    length: number // Results per page
}
```

### Examples

**Search for charts about population:**

```
GET /api/search?q=population
```

**Search for charts about GDP in United States and China:**

```
GET /api/search?q=gdp&countries=United%20States~China
```

**Search for articles about climate change:**

```
GET /api/search?q=climate%20change&type=pages
```

**Paginated search (page 2, 50 results per page):**

```
GET /api/search?q=health&page=1&hitsPerPage=50
```
