# Search API

Search API for accessing Our World in Data charts and content pages.

This API allows you to search through:
- **Charts**: Interactive data visualizations with country-specific filtering
- **Pages**: Articles, research papers, and informational pages


!!! info "API Information"

    **Version:** `1.0.0`

    **Base URL:** `https://ourworldindata.org/api` — Site API

    **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

---

## <span style="color: blue; font-weight: bold;">GET</span> `/search`

**Search for charts or pages**

Search through Our World in Data's collection of charts and pages.

- For **chart search**: Filter by countries, topics, and entity availability
- For **page search**: Find articles, research papers, and informational content


### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string |  | Search query text<br/><small>default: `""`</small><br/><small>Example: `population`</small> |
| `type` | string (`charts`, `pages`) |  | Type of content to search<br/><small>default: `charts`</small><br/><small>Example: `charts`</small> |
| `page` | integer |  | Page number for pagination (0-indexed)<br/><small>default: `0` | min: 0 | max: 1000</small><br/><small>Example: `0`</small> |
| `hitsPerPage` | integer |  | Number of results per page<br/><small>default: `20` | min: 1 | max: 100</small><br/><small>Example: `20`</small> |
| `countries` | string |  | Country names separated by `~` (tilde). Only applicable when `type=charts`. <br/><small>Example: `United States~China`</small> |
| `topics` | string |  | Topic name for filtering charts. Only applicable when `type=charts`. <br/><small>Example: `Health`</small> |
| `requireAllCountries` | boolean |  | If `true`, only return charts that include ALL specified countries. Only applicable when `type=charts` and `countries` parameter is provided. <br/><small>default: `False`</small><br/><small>Example: `False`</small> |

### Responses

???+ success "✅ 200 - Successful search response"

    **Content-Type:** `application/json`

    === "Chart search results"

        **Request:** `GET https://ourworldindata.org/api/search?q=gdp`

        **Code samples:**

        === "cURL"

            ```bash
            curl "https://ourworldindata.org/api/search?q=gdp"
            ```

        === "Python"

            ```python
            import requests
            
            params = {
                "q": "gdp"
            }
            response = requests.get("https://ourworldindata.org/api/search", params=params)
            data = response.json()
            ```

        === "JavaScript"

            ```javascript
            const params = new URLSearchParams({ q: "gdp" });
            const response = await fetch(`https://ourworldindata.org/api/search?${params}`);
            const data = await response.json();
            ```

        === "Rust"

            ```rust
            let response = reqwest::get("https://ourworldindata.org/api/search")
                    .query(&[("q", "gdp")])
                .await?
                .json::<serde_json::Value>()
                .await?;
            ```

        **Response:**

        ```json
        {
          "query": "gdp",
          "results": [
            {
              "title": "GDP per capita",
              "slug": "gdp-per-capita",
              "subtitle": "Gross domestic product per capita adjusted for inflation",
              "type": "chart",
              "availableEntities": [
                "United States",
                "China",
                "India"
              ],
              "availableTabs": [
                "chart",
                "map",
                "table"
              ],
              "url": "https://ourworldindata.org/grapher/gdp-per-capita"
            }
          ],
          "nbHits": 125,
          "page": 0,
          "nbPages": 7,
          "hitsPerPage": 20
        }
        ```

    === "Page search results"

        **Request:** `GET https://ourworldindata.org/api/search?q=climate+change`

        **Code samples:**

        === "cURL"

            ```bash
            curl "https://ourworldindata.org/api/search?q=climate+change"
            ```

        === "Python"

            ```python
            import requests
            
            params = {
                "q": "climate change"
            }
            response = requests.get("https://ourworldindata.org/api/search", params=params)
            data = response.json()
            ```

        === "JavaScript"

            ```javascript
            const params = new URLSearchParams({ q: "climate change" });
            const response = await fetch(`https://ourworldindata.org/api/search?${params}`);
            const data = await response.json();
            ```

        === "Rust"

            ```rust
            let response = reqwest::get("https://ourworldindata.org/api/search")
                    .query(&[("q", "climate change")])
                .await?
                .json::<serde_json::Value>()
                .await?;
            ```

        **Response:**

        ```json
        {
          "query": "climate change",
          "results": [
            {
              "title": "Climate Change: Evidence and Causes",
              "slug": "climate-change-evidence",
              "type": "article",
              "thumbnailUrl": "https://ourworldindata.org/images/climate-thumbnail.jpg",
              "date": "2023-05-15",
              "authors": [
                "Hannah Ritchie",
                "Max Roser"
              ],
              "url": "https://ourworldindata.org/climate-change-evidence"
            }
          ],
          "nbHits": 42,
          "offset": 0,
          "length": 20
        }
        ```

??? failure "❌ 400 - Bad request - invalid parameters"

    **Content-Type:** `application/json`

    **Example:**

    ```json
    {
      "error": "Invalid parameter value",
      "message": "hitsPerPage must be between 1 and 100"
    }
    ```

??? failure "❌ 500 - Internal server error"

    **Content-Type:** `application/json`

    **Example:**

    ```json
    {
      "error": "Internal server error",
      "message": "An unexpected error occurred"
    }
    ```

---

## Schemas

??? info "ChartSearchResponse"

    | Property | Type | Required | Description |
    |----------|------|----------|-------------|
    | `query` | string | ✓ | The search query that was executed<br/><small>Example: `population`</small> |
    | `results` | array[any] | ✓ | Array of chart search results |
    | `nbHits` | integer | ✓ | Total number of results found<br/><small>Example: `125`</small> |
    | `page` | integer | ✓ | Current page number (0-indexed)<br/><small>Example: `0`</small> |
    | `nbPages` | integer | ✓ | Total number of pages available<br/><small>Example: `7`</small> |
    | `hitsPerPage` | integer | ✓ | Number of results per page<br/><small>Example: `20`</small> |

??? info "ChartResult"

    | Property | Type | Required | Description |
    |----------|------|----------|-------------|
    | `title` | string | ✓ | Chart title<br/><small>Example: `Life expectancy at birth`</small> |
    | `slug` | string | ✓ | URL-friendly identifier for the chart<br/><small>Example: `life-expectancy`</small> |
    | `subtitle` | string |  | Chart subtitle providing additional context<br/><small>Example: `Period life expectancy at birth, measured in years`</small> |
    | `variantName` | string |  | Name of the chart variant (if applicable)<br/><small>Example: `Default`</small> |
    | `type` | string (`chart`, `explorerView`, `multiDimView`) | ✓ | Type of visualization<br/><small>Example: `chart`</small> |
    | `queryParams` | string |  | URL query parameters for explorer or multi-dimensional views<br/><small>Example: `tab=chart&time=2020`</small> |
    | `availableEntities` | array[string] | ✓ | List of countries/entities available in this chart<br/><small>Example: `United States`, `China`, `India`</small> |
    | `originalAvailableEntities` | array[string] |  | Original list of entities before filtering<br/><small>Example: `United States`, `United Kingdom`</small> |
    | `availableTabs` | array[string] | ✓ | Available visualization tabs<br/><small>Example: `chart`, `map`, `table`</small> |
    | `url` | string (uri) | ✓ | Full URL to access the chart<br/><small>Example: `https://ourworldindata.org/grapher/life-expectancy`</small> |

??? info "PageSearchResponse"

    | Property | Type | Required | Description |
    |----------|------|----------|-------------|
    | `query` | string | ✓ | The search query that was executed<br/><small>Example: `climate change`</small> |
    | `results` | array[any] | ✓ | Array of page search results |
    | `nbHits` | integer | ✓ | Total number of results found<br/><small>Example: `42`</small> |
    | `offset` | integer | ✓ | Current offset in the result set<br/><small>Example: `0`</small> |
    | `length` | integer | ✓ | Number of results returned<br/><small>Example: `20`</small> |

??? info "PageResult"

    | Property | Type | Required | Description |
    |----------|------|----------|-------------|
    | `title` | string | ✓ | Page title<br/><small>Example: `CO₂ emissions`</small> |
    | `slug` | string | ✓ | URL-friendly identifier for the page<br/><small>Example: `co2-emissions`</small> |
    | `type` | string | ✓ | Type of page content<br/><small>Example: `article`</small> |
    | `thumbnailUrl` | string (uri) |  | URL to page thumbnail image<br/><small>Example: `https://ourworldindata.org/images/co2-thumbnail.jpg`</small> |
    | `date` | string (date) |  | Publication or last update date (ISO 8601 format)<br/><small>Example: `2023-05-15`</small> |
    | `content` | string |  | Excerpt or snippet of page content<br/><small>Example: `Carbon dioxide emissions are the primary driver of global climate change...`</small> |
    | `authors` | array[string] |  | List of page authors<br/><small>Example: `Hannah Ritchie`, `Max Roser`</small> |
    | `url` | string (uri) | ✓ | Full URL to access the page<br/><small>Example: `https://ourworldindata.org/co2-emissions`</small> |

??? info "Error"

    | Property | Type | Required | Description |
    |----------|------|----------|-------------|
    | `error` | string | ✓ | Error type or category<br/><small>Example: `Invalid parameter`</small> |
    | `message` | string |  | Detailed error message<br/><small>Example: `The 'page' parameter must be between 0 and 1000`</small> |
