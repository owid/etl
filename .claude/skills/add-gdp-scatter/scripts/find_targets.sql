/* Find published "X vs. GDP per capita" scatter charts and, for each, the chart that
   should GAIN the scatter view (the /add-gdp-scatter target).

   A valid target is a published, NON-scatter, single-y-indicator chart that plots the
   source scatter's non-GDP indicator on its Y axis.

   Fixes vs. the first version, all in candidate_top_per_scatter:

     1. cxi.property = 'y'
        The target must plot the indicator on its Y AXIS. Without this, a chart that
        uses the indicator as its X qualifies -- which is how the scatter twins
        cereal-yield-vs-extreme-poverty-scatter, energy-use-per-capita-vs-co2 and
        mean-income-2011-vs-2017 were picked over the line charts we wanted.

     2. exclude ScatterPlot candidates
        A chart config holds exactly ONE x dimension, so a chart that is already a
        scatter against something else can never also plot GDP. The applier script
        guards on `if "x" not in props`, so such a target silently gains no GDP
        dimension at all.

     3. exclude the stacked family
        ScatterPlot cannot coexist with StackedArea / StackedBar /
        StackedDiscreteBar; the applier script returns SKIPPED for them.

     4. coalesce(type, 'LineChart')
        `type` is NULL for 229 charts, and a bare `type != 'ScatterPlot'` evaluates
        to NULL for those, dropping them silently.

     5. is_single_indicator = TRUE stays a HARD filter
        A scatter plots ONE y series, so a multi-indicator target is ambiguous.
        Verified against prod_semantic: the flag is exactly
        COUNT(DISTINCT indicator_id WHERE property='y') = 1 -- 3274 TRUE / 1171
        FALSE, zero disagreements. Combined with fixes 1-3 it no longer falls back
        to a scatter twin; a source with no single-y target now correctly returns
        NULL instead.

   Note: prod_semantic.charts holds published charts only, so no published filter is
   needed. Block comments, not `--`: read_analytics flattens the SQL onto one line,
   where a line comment would swallow the rest of the query. */

WITH gdp_ids AS (
  SELECT indicator_id
  FROM `prod_semantic.indicators`
  WHERE lower(short_name) IN ('ny_gdp_pcap_pp_kd', 'gdp_per_capita', 'rgdpo_pc')
),

chart_axes AS (
  SELECT
    charts.chart_id,
    charts.chart_slug,
    charts.admin_url  AS chart_admin_url,
    charts.url        AS grapher_url,
    charts.views_365d AS scatter_views_365d,
    xmap.indicator_id AS x_indicator_id,
    ymap.indicator_id AS y_indicator_id,
    ix.catalog_path   AS x_catalog_path,
    iy.catalog_path   AS y_catalog_path,
    /* the indicator that is NOT GDP per capita: what the target must plot on its y */
    CASE
      WHEN xmap.indicator_id IN (SELECT indicator_id FROM gdp_ids)
       AND (ymap.indicator_id IS NULL OR ymap.indicator_id NOT IN (SELECT indicator_id FROM gdp_ids))
        THEN ymap.indicator_id
      WHEN ymap.indicator_id IN (SELECT indicator_id FROM gdp_ids)
       AND (xmap.indicator_id IS NULL OR xmap.indicator_id NOT IN (SELECT indicator_id FROM gdp_ids))
        THEN xmap.indicator_id
      ELSE NULL
    END AS non_gdp_indicator_id
  FROM `prod_semantic.charts` AS charts
  LEFT JOIN `prod_semantic.charts_x_indicators` xmap
         ON charts.chart_slug = xmap.chart_slug AND xmap.property = 'x'
  LEFT JOIN `prod_semantic.charts_x_indicators` ymap
         ON charts.chart_slug = ymap.chart_slug AND ymap.property = 'y'
  LEFT JOIN `prod_semantic.indicators` ix ON xmap.indicator_id = ix.indicator_id
  LEFT JOIN `prod_semantic.indicators` iy ON ymap.indicator_id = iy.indicator_id
  WHERE charts.type = 'ScatterPlot'
    AND (   xmap.indicator_id IN (SELECT indicator_id FROM gdp_ids)
         OR ymap.indicator_id IN (SELECT indicator_id FROM gdp_ids))
),

reference_counts AS (
  SELECT ca.chart_slug, COUNT(DISTINCT cxr.reference_url) AS reference_count
  FROM chart_axes ca
  LEFT JOIN `prod_semantic.charts_x_references` cxr
         ON cxr.slug = ca.chart_slug AND cxr.type = 'chart' AND cxr.reference_published = TRUE
  GROUP BY ca.chart_slug
),

candidate_top_per_scatter AS (
  SELECT
    ca.chart_slug  AS scatter_chart_slug,
    ca.non_gdp_indicator_id,
    ch.chart_slug  AS candidate_chart_slug,
    ch.url         AS candidate_chart_url,
    ch.admin_url   AS candidate_chart_admin_url,
    ch.views_365d  AS candidate_chart_views_365d,
    coalesce(ch.type, 'LineChart') AS candidate_type,
    ROW_NUMBER() OVER (
      PARTITION BY ca.chart_slug
      ORDER BY ch.views_365d DESC, ch.chart_slug
    ) AS rn
  FROM chart_axes ca
  JOIN `prod_semantic.charts_x_indicators` cxi
    ON cxi.indicator_id = ca.non_gdp_indicator_id
   AND cxi.property = 'y'                                  /* (1) must be the target's Y */
  JOIN `prod_semantic.charts` ch
    ON ch.chart_slug = cxi.chart_slug
  WHERE ca.non_gdp_indicator_id IS NOT NULL
    AND ch.chart_slug != ca.chart_slug
    AND ch.is_single_indicator = TRUE                       /* (5) exactly one y indicator */
    AND coalesce(ch.type, 'LineChart') != 'ScatterPlot'     /* (2) one x dimension only */
    AND coalesce(ch.type, 'LineChart') NOT IN               /* (3) cannot coexist with scatter */
        ('StackedArea', 'StackedBar', 'StackedDiscreteBar')
)

SELECT
  ca.chart_id,
  ca.chart_slug,
  ca.chart_admin_url,
  ca.grapher_url,
  ca.scatter_views_365d,
  ca.x_catalog_path,
  ca.y_catalog_path,
  rc.reference_count,
  CASE
    WHEN STRPOS(lower(coalesce(ca.x_catalog_path, '')), 'ny_gdp_pcap_pp_kd') > 0 THEN 'World Bank'
    WHEN STRPOS(lower(coalesce(ca.x_catalog_path, '')), 'rgdpo_pc')          > 0 THEN 'Penn World Table'
    WHEN STRPOS(lower(coalesce(ca.x_catalog_path, '')), 'gdp_per_capita')    > 0 THEN 'Maddison Project Database'
    WHEN STRPOS(lower(coalesce(ca.y_catalog_path, '')), 'ny_gdp_pcap_pp_kd') > 0 THEN 'World Bank'
    WHEN STRPOS(lower(coalesce(ca.y_catalog_path, '')), 'rgdpo_pc')          > 0 THEN 'Penn World Table'
    WHEN STRPOS(lower(coalesce(ca.y_catalog_path, '')), 'gdp_per_capita')    > 0 THEN 'Maddison Project Database'
    ELSE NULL
  END AS gdp_source,
  ca.non_gdp_indicator_id,
  ctp.candidate_chart_url       AS target_chart_url,
  ctp.candidate_chart_admin_url AS target_chart_admin_url,
  ctp.candidate_type            AS target_type,
  ctp.candidate_chart_views_365d AS target_views_365d
FROM chart_axes ca
LEFT JOIN reference_counts rc ON ca.chart_slug = rc.chart_slug
LEFT JOIN (
  SELECT scatter_chart_slug, candidate_chart_url, candidate_chart_admin_url,
         candidate_chart_views_365d, candidate_type
  FROM candidate_top_per_scatter
  WHERE rn = 1
) ctp ON ca.chart_slug = ctp.scatter_chart_slug
ORDER BY ctp.candidate_chart_views_365d ASC
