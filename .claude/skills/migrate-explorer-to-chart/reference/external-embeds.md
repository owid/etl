# External sites that embed the explorer

Other websites embed explorers in an iframe. The redirect keeps those iframes working (the browser
follows the 302 to the multidim), but the sites should point at the multidim directly, so they
don't depend on the redirect. List them so the user can tell them.

## Find them

The internal analytics Datasette (`http://analytics/`, Tailscale only, always send a User-Agent)
records page views with their referrer and whether the page was loaded in an iframe
(`views_detailed`, from August 2024). The referrer of an iframed view is the embedding page,
usually reduced to its origin. The instance runs DuckDB, not SQLite: SQLite functions such as
`date('now')` return no rows without an error, so use DuckDB syntax (`current_date - interval 365 day`).

```bash
SLUG=<explorer slug>
curl -s -G -H "User-Agent: Claude" "http://analytics/analytics.json" --data-urlencode "_shape=arrays" \
  --data-urlencode "sql=select referrer_name, referrer, sum(views) as views, min(day) as first_day, max(day) as last_day
    from views_detailed
    where (url = 'https://ourworldindata.org/explorers/$SLUG' or url like 'https://ourworldindata.org/explorers/$SLUG?%')
      and page_in_iframe = 1
      and coalesce(referrer_name, '') not in ('ourworldindata.org', 'admin.owid.io', 'owid.cloud')
      and day >= current_date - interval 365 day
    group by 1, 2 order by views desc limit 30"
```

- A null referrer means the embedding page sent none; it can't be attributed.
- Our own pages show up as `ourworldindata.org` or `admin.owid.io` (gdoc embeds and previews, covered by the reference scan) and
  are excluded above.
- `last_day` tells a site that still embeds it from one that stopped.

Report the sites with their views over the last year, largest first.

## What to tell them

Give the user, per site, the embed that replaces the explorer's: the multidim URL of the view
they show (the redirect target of their explorer URL, from the review page's mapping), as an
iframe. The multidim's Share → Embed menu gives the same code:

```html
<iframe src="https://ourworldindata.org/grapher/<multidim slug>?<view params>" loading="lazy"
  style="width: 100%; height: 600px; border: 0px none;" allow="web-share; clipboard-write"></iframe>
```

The country list shows beside the chart only when the chart is wider than 940px (grapher's
`shouldShowEntitySelectorAs`); narrower, readers open it from the "Edit countries" button. So
suggest an iframe at least 960px wide where their layout allows. There is no URL parameter for
this: `size: widest` belongs to the chart block in OWID's own Google Docs, not to iframes.

Contacting these sites is outward-facing: draft the message for the user, who sends it.
