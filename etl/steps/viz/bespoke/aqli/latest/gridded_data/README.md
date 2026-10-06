# SatPM gridded PM2.5 — access notes

Scratch area for exploring the **SatPM V6.GL.03** satellite-derived PM2.5 surface
(<https://www.satpm.org/v6-gl-03>), the ~1 km grid that AQLI's GADM0/1/2 aggregates are averaged
from. Nothing here is wired into the DAG — it exists to decide whether the grid is worth ingesting,
and in what form.

## How to get the data

Use SatPM's **public S3 bucket**, not the WashU Box links the site advertises. The bucket is
anonymously readable over plain HTTPS — no credentials, no AWS CLI, and it honours range requests,
so it is the only access route a snapshot step could ever automate.

```
https://satpmdata.s3.amazonaws.com/V6GL03/<FineResolution|CoarseResolution>/<REGION>/<Annual|Monthly>/<file>.nc
```

`REGION` is `GL` (global) or a continental cut: `AF`, `AS`, `EU`, `NA`, `SA`. File naming is
`V6GL03.CNNPM25.GL.202401-202412.nc` (fine) and `V6GL03.CNNPM25.0p10.GL.202401-202412.nc` (coarse).

List the bucket with any HTTP client:

```bash
curl -s "http://satpmdata.s3.amazonaws.com/?list-type=2&delimiter=/&prefix=V6GL03/"
```

| Product | Resolution | Per year | 1998–2024 |
|---|---|---|---|
| Fine, global | 0.01° (~1 km) | ~450 MB | ~12 GB |
| Fine, continental (e.g. `AS`) | 0.01° | ~93 MB | ~2.5 GB |
| Coarse, global | 0.1° (~11 km) | 5.2 MB | ~140 MB |

Country/subnational summary CSVs live under `V6GL03/RegionSummaries/` — notably
`GlobalPM25-V6GL03-Annual-1998-2024-wThresFrac.csv` (594 KB, country × year, population-weighted
and geographic means plus the share of population above each WHO threshold).

> That CSV is the direct-download replacement for `snapshots/washu/2026-04-22/pm25_air_pollution.csv.dvc`,
> which is on v5-gl-06 and carries the comment *"download sheet from here - step can't directly
> download the file"*. On v6 that manual Box step is no longer necessary.

## Gotchas

- **There is no `_FillValue` attribute, and the fill value differs between resolutions.** The
  coarse 0.1° files use `-999.0`; the fine 0.01° files use `-999.9`. Both read back as real
  numbers, so any mean taken without masking is silently wrong — 63% of the global coarse grid is
  fill. Testing against either literal silently fails to mask the other, which is a nasty way to
  lose an hour. PM2.5 cannot be negative and the only negatives in either product *are* the fill
  (verified over a full coarse and a full fine file: the smallest real values are 0.99 and 2.37),
  so **mask on sign** — `explore_country_grid.is_valid()` does this.
- **Latitude only spans -59.95 to 69.95** — no Antarctica, no high Arctic.
- Some prefixes carry `._`-prefixed AppleDouble junk files next to the real ones. Filter them out
  when globbing a listing.
- `rioxarray`'s `.rio` accessor is rebuilt on every derived object, so `set_spatial_dims` has to be
  re-applied after any `.sel()` — declaring it once at open time is not enough.
- Plain lon/lat axes need a `1/cos(lat)` aspect, otherwise the map is stretched east-west.

## The explorer webapp

`prepare_webapp_data.py` + `pm25_explorer.html` — a zoomable world map of the SatPM grid, one
raster per year for **1998–2024**, with a timeline and hover readout.

```bash
.venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/prepare_webapp_data.py
.venv/bin/python -m http.server 8000 --directory ai/aqli_gridded/webapp
# then open http://localhost:8000/
```

SatPM only. An earlier version of this prototype also carried AQLI's GADM2 district polygons as a
second layer with a "Both" mode; that was dropped to keep it focused on the gridded product.
`../prepare_pm_map_data.py` still owns the district layer if it is wanted back.

### Masked to land

SatPM's grid is **not** land-only. It emits values over coastal and enclosed water too — the North
Sea, the Great Lakes, shelf seas — and those are **16% of every cell carrying a value**. Left in,
they haze every coastline and are silently averaged into any statistic described as "over land".

`land_mask()` rasterises the GADM1 polygons at the grid's own resolution and masks the grid before
anything is drawn *or counted*. Measured on 2024:

| | share of all cells |
|---|---|
| Land mask | 31.2% |
| Cells with data (unmasked) | 37.1% |
| Data **and** land | 31.2% |
| Data but **not** land — removed | 6.0% (= 16% of all data cells) |
| Land but no data — wrongly cut | **0.0%** |

`all_touched=True` is what makes the mask a superset of the land that actually has data; with it
off, small islands and thin coastal strips fall through the 0.1° grid and get cut.

This moved the headline figures, because the water cells were cleaner air pulling the "over land"
means down:

| | before masking | after |
|---|---|---|
| Global mean over land, 2024 | 15.3 µg/m³ | **16.6** |
| Global mean over land, 1998 | 12.1 | **12.8** |
| Land area above WHO guideline, 2024 | 78.1% | **82.8%** |

The mask is cached to `ai/aqli_gridded/cache/landmask_<h>x<w>.npy`, so the 2 GB shapefile extraction
happens once.

> **This is total PM2.5 — it includes mineral dust and sea salt.** It is *not* the human-caused
> measure OWID's published "Exposure to air pollution from human sources" chart shows, which strips
> those out. Globally the difference is dominated by the Sahara, the Arabian peninsula, the
> Taklamakan and the Gobi, which are the brightest things on the map and nearly uninhabited. The app
> says so on screen.

### Resolution, and the browser ceiling

The app draws each year as a single `L.imageOverlay` PNG. That sets a hard ceiling, and it is the
canvas **area**, not the download or the canvas dimension. Measured in this Chromium:

| Canvas | Pixels | Result |
|---|---|---|
| World raster at 0.1° — 3443 × 1673 | 6M | OK |
| 13772 × 6692 | 92M | OK |
| 16384 × 16384 | 268M | OK |
| **Fine world at 0.01° — 36000 × 13000** | **468M** | **fails** |

So the full 0.01° world cannot be shown this way at all — it is 1.87 GB of RGBA. True ~1 km needs
tiling (COG or pre-rendered XYZ to zoom 8, which at 611 m/px fully resolves the 1.1 km grid); that
is a different app, and hover would need a different mechanism since you cannot sample a tile layer
the way one image is sampled here.

What is done instead:

| Years | Source | Rendered | Ground |
|---|---|---|---|
| 1998–2023 | coarse 0.1° global | 3443 × 1673 | ~11 km |
| **2024** | **fine 0.01° global, block-averaged 4×** | **8000 × 3887** | **~5 km** |

`open_downsampled_fine()` reads the fine file in row blocks and averages each block down, so the
1.87 GB array is never resident — peak memory stays in the low hundreds of MB. Averaging rather
than subsampling is what preserves the local maxima that make the fine source worth using.
Decoded, the 2024 raster is ~124 MB of RGBA in the hover canvas, against ~24 MB for a coarse year;
`RASTER_CACHE_MAX` is 3 to keep that bounded.

The app shows a `~5 km` / `~11 km` badge beside the timeline, and says in a callout that the map
sharpening on 2024 is a change in resolution, not in the data. Add years to `FINE_YEARS` to extend
it — each costs a 450 MB download.

### City lookup

A searchable box over the map flies to any of the **1,000 largest urban centres** (down to ~520,000
people). Coordinates come
from the `urbanization/2025-12-10/ghsl_urban_centers.xlsx` snapshot already tracked in this repo —
nothing is hand-entered.

Two traps in that source, both avoided in `load_cities()`:

- **GHSL runs to 2100.** Ranking on `Year.max()` ranks cities by their *projected* 2100 population:
  that list is headed by Dhaka at 55M and puts Luanda (30M) above Tokyo. The repo's own garden step
  sets `START_OF_PROJECTIONS = 2025`, so **2020** is the last observed epoch, and that is what this
  ranks on. The 2020 list reads correctly — Jakarta, Tokyo, Dhaka, Delhi, Shanghai.
- **Low-plausibility figures.** The source rates each population estimate 0–3; the garden step
  treats 0 as unreliable for observed years. Same rule here (331 centres dropped), rather than
  inventing a different one.

A third thing to know, which is not a bug: **a GHSL "urban centre" is the contiguous dense
built-up core under the Degree of Urbanisation methodology, not the metropolitan area.** For
sprawling cities the two nearly coincide (London 10.0M, Los Angeles 12.5M), but for compact cities
with detached suburbs the centre is much smaller than the figure people expect — Zurich reads
0.7M, not the ~1.4M of its agglomeration. The populations shown in the dropdown are the source's,
unmodified; they are ranking and disambiguation aids, not a claim about metro size.

An assert checks every selected city falls inside the grid's 60°S–70°N extent, so the lookup can
never fly to a blank spot. Search folds accents and case, in both directions — the source spells it
`Zurich`, and both `zurich` and `zürich` find it; likewise `tokyo` → *Tōkyō (Tokyo)*, `cairo` →
*Al-Qahirah (Cairo)*. Exact prefix matches rank above interior ones, then by population, so `lo`
offers Los Angeles and London before Lomé. Seven names are duplicated across countries (Hyderabad,
Suzhou, Saint Petersburg, Barcelona, Valencia, Cape Town, Yingkou); the dropdown shows the country
on every row, so they stay distinguishable. Anything below ~520,000 (Reykjavík at 198,000, for
instance) returns "No match among the largest cities" — raise `N_CITIES` if that bites.

`CITY_ZOOM = 8`: at 0.1° a grid cell is ~11 km, which is ~18 px at z8 — enough to read a city
against its surroundings without the map becoming four enormous squares.

### The colour scale

19 classes in 5 µg/m³ steps to `90+`. The eight anchor colours were sampled pixel-by-pixel from
OWID's own published legend, plus one darker anchor, then **resampled across all 19 classes**
rather than pinned to the first 8. Pinning them and only darkening the tail left eight
near-identical browns above 35 (adjacent Lab ΔE ~2–5, below the ~5 needed to read a classed
choropleth); respreading lifts the worst adjacent pair to ΔE 4.9 and averages 10.8. The cost is
that the middle classes are no longer byte-identical to the published chart.

### Implementation notes

- Rasters are reprojected to **EPSG:3857** before being written. Leaflet's `ImageOverlay` stretches
  an image into a lat/lon box but renders in Web Mercator, so an unprojected lon/lat raster is
  squashed north-south by a growing amount away from the equator — for a whole-world image that is
  a gross distortion, not a subtle one.
- They are written as **palettised PNGs**, one palette index per bracket, with the no-data index
  transparent (65% of the world raster once masked: everything that is not land, plus everything
  outside 60°S–70°N). That is what
  keeps 27 world-years to 4.1 MB, and it is also how the app reads values back: hover samples the
  decoded pixel on an offscreen canvas and maps the colour to its bracket. Hover therefore reports a
  range (`15–20 µg/m³`), not an exact value.
  **If you change the colour ramp, hover breaks unless the PNGs are rebuilt** — the lookup matches
  on exact RGB.
- Decoded rasters are cached in a **bounded** `Map` (4 entries): each world raster is ~23 MB of
  RGBA once decoded, so holding all 27 would be well over half a gigabyte.
- The map sets `maxBounds` to the raster extent. A single `ImageOverlay` does not wrap the way a
  tile layer does, so without it you can pan off into empty repeated basemap.
- The basemap is Esri's light-gray canvas, which serves without an API key (CARTO's now demands
  one) and uses `{z}/{y}/{x}` tile order, not the usual `{z}/{x}/{y}`.

### Validation

There is no global row in SatPM's published country summary, so the world rasters cannot be
cross-checked against a published figure the way the per-country ones were. Instead
`check_landmarks()` reads the **written PNG** back and confirms known places land in the expected
class — this covers the whole reproject → classify → palettise → encode chain, and catches a
flipped axis, an off-by-one in the bounds, or a broken palette, none of which show up in the
summary statistics:

| Landmark | 2024 |
|---|---|
| Delhi | `90+` |
| Beijing | `45–50` |
| London | `5–10` |
| Sahara (Chad) | `15–20` |
| mid-Pacific / mid-Atlantic | no data |
| North Sea | no data *(carried a value before masking — the regression guard)* |

Global area-weighted mean over land: **12.8 µg/m³ (1998) → 16.7 (2024)**.

2024 is built from the fine source and every other year from the coarse one, which makes the two a
useful cross-check on each other: rendered coarse, 2024's mean is 16.6; rendered fine, 16.7. The
peak rises more (136.1 → 145.7), which is what you would expect — a ~5 km cell preserves more of a
local maximum than an 11 km one.

One number worth knowing about: the highest single grid cell sits at 120–148 µg/m³ in most years,
always over Riyadh (desert dust) — **except 2023, at 285.5 µg/m³ at 58.05°N, −120.05°W**, northern
British Columbia, with 66 cells above 150. That is the record 2023 Canadian wildfire season, and it
is real signal, not an artefact.

## Scripts

`explore_country_grid.py` — download one annual grid, clip it to a country using AQLI's GADM1
boundaries, cross-check the result against SatPM's own published country mean, and plot it.

```bash
.venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/explore_country_grid.py China
```

Writes `ai/aqli_gridded/<country>_pm25_<year>.png`; grids and the summary CSV are cached under
`ai/aqli_gridded/cache/` (gitignored). Countries outside `COUNTRY_REGION` fall back to the ~450 MB
global grid, so add the mapping rather than eating that download.

### Validation

For China 2024 the clipped grid gives an area-weighted geographic mean of **23.02 µg/m³** against
SatPM's published **23.10 µg/m³** — 0.3% apart, which is the expected residual from clipping with
AQLI's GADM1 boundaries rather than SatPM's own country mask. A gap much larger than that means the
fill-value mask, the clip, or the regional cut is wrong.

## Country explorer (main prototype view)

The default page now pairs a population-weighted country choropleth with a selected-country
fine-grid map. The original world raster view is preserved at `global.html`, with navigation
between both views. This is still a local prototype, not a DAG step or a published site.

```bash
# Prepare country outlines and published averages. Fine source blocks load on demand.
MPLCONFIGDIR=ai/aqli_gridded/mplconfig .venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/prepare_country_explorer.py

# Serve both pages and generate country-clipped tiles on demand.
MPLCONFIGDIR=ai/aqli_gridded/mplconfig .venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/serve_country_explorer.py --port 8000
```

Open <http://localhost:8000/>. Add `--download-all` to the preparation command to download and classify all
27 annual grids for offline use; this is optional. The original `prepare_webapp_data.py` still builds the global view's `manifest.json`
and `rasters/`; run it once if those outputs do not exist. It now copies that page to
`global.html` and keeps the country explorer at `index.html`.

The country view requires the custom local server, not `python -m http.server`. It uses the
**0.01° native grid for every year, 1998–2024**, with no fourfold averaging of 2024 and no coarse
fallback for earlier years. The server reads native cells using verified HTTP byte ranges and caches source blocks on disk.
Where a local bracket GeoTIFF exists, it uses that instead. Each prepared cell stores a uint8
bracket ID (0–14), with 255 reserved for missing data. Reprojection uses nearest-neighbor sampling
to preserve the classes. Source NetCDF files retain the original concentrations. Zooming out necessarily displays fewer samples, while zooming in
reveals the native cells. The source is roughly 1 km north–south, with east–west cell width varying
by latitude. Display tiles are 256 × 256 and are masked to the selected country's original GADM
geometry. Hover reads the bracket directly from the loaded PNG tile in the browser, with no separate
server request. It reports intervals such as 10–<15 µg/m³ and the open-ended 90+ class. Only outlines on the overview are simplified.

Annual averages come directly from the same SatPM edition's published population-weighted
country summary. They are not calculated from display pixels. Unmatched country names are
reported during preparation and included in `countries.json`; they appear gray rather than
receiving a guessed average. The source covers only 60°S–70°N. Areas outside that range remain
blank. The initial country extent follows its largest landmass; “Show all territories” fits
all components, including islands and territories across the date line.

All downloads, boundary caches, tiled rasters, and generated PNG tiles stay under
`ai/aqli_gridded/`. On-demand mode downloads only the source blocks touched by selected countries and years.
A first uncached view can take longer to load, especially for large countries. Optional full
offline preparation needs roughly 12 GB of source downloads plus the compact classified
tiled rasters and generated tiles. Downloads and raster conversions use temporary files and
only rename after completion. If rendering or boundaries change, clear the generated
`webapp/tiles/` cache before testing so it cannot serve old images. The server binds to localhost;
public hosting would require a tile service or a static tile packaging step.

The Paracel Islands have negative population-weighted averages in the source CSV. This known
invalid series is shown as missing and labeled explicitly; it is never colored as clean air.

### Country detail overlays

City labels and GADM level-1 (state/province) boundaries are on by default, with independent
toggles above the detail map. Labels are drawn above the raster, prioritized by 2020 GHSL
urban-centre population, and filtered for screen collisions whenever the view changes. They
are geographic context and do not change with the pollution year. Markers and boundary lines
allow the grid's hover readout to continue working underneath.

`prepare_country_overlays.py` builds static per-country files under `webapp/overlays/` from the
existing AQLI shapefile and GHSL snapshots. The main preparation script builds missing overlays
automatically. To regenerate all overlays explicitly:

```bash
MPLCONFIGDIR=ai/aqli_gridded/mplconfig .venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/prepare_country_overlays.py
```

City assignment uses original administrative geometries; only display boundaries are simplified.
The current source supplies 11,088 usable urban centres; 11,053 fall within the supplied boundaries.
The 35 unmatched centres are recorded in `overlays/unmatched_cities.json` for inspection rather
than being assigned to a guessed country. Boundary coverage includes 252 countries and territories.

Bracket storage: the 2024 prepared raster is 6.9 MB versus 457.0 MB for the previous float32
raster (98.5% smaller). The original source files and any previously prepared float rasters are
retained; new builds write `cache/<scale-id>/brackets_YYYY.tif`. Country averages remain unchanged.

## Compare scales page

`comparison.html` is a separate, three-panel view: AQLI GADM1, AQLI GADM2, and the SatPM grid.
It has a shared country/year selection (1998–2024), common brackets, and synchronized map views.
Open <http://localhost:8765/comparison.html> when running the local server on port 8765.

Build using:

```bash
MPLCONFIGDIR=ai/aqli_gridded/mplconfig .venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/prepare_comparison.py
```

The builder reuses the simplified boundary geometry and explicit name crosswalks from
`../prepare_pm_map_data.py`, requiring its existing `ai/aqli_pm_map/` export. It reads all years
from the original AQLI snapshots, checks 2024 values against that export, and writes per-country
series under `webapp/comparison/`. Country IDs match the grid server. Missing matches stay gray;
the grid is not substituted for administrative estimates. Three district-name keys have
conflicting source values (Neijiang in China, Saquisili in Ecuador, Bujenje in Uganda): their
81 region-years remain missing and hover labels explicitly identify ambiguous matches.
`comparison/matching_audit.json` records missing and ambiguous matches. This is a comparison of
published products, not a claim that the administrative values are arithmetic means of the shown grid.


### Shared design and city selection

All three pages use `publish_prototype_pages.py` and `site_shell.css` for the shared sticky
navigation and compact yellow title area. Each preparation command republishes these assets;
run `.venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/publish_prototype_pages.py`
to publish HTML/CSS changes alone.

The country page dedicates 36% of its width to the world map and 64% to local detail.
Its searchable city dropdown uses `cities.json`, generated from the existing country overlays
(11,053 GHSL urban centers, 2020 population). Choosing a city selects its spatially matched
country and zooms to its coordinates at zoom 10, keeping the current pollution year. Search
matches city and country names; duplicate names include coordinates. The chooser moves above
the maps after selection so another city can be chosen without reloading.

The shared scale now contains 19 classes: 0–<5 through 85–<90, then 90+ µg/m³.
Fine-resolution bracket rasters and rendered tiles are versioned by a hash of the thresholds
and palette. Global PNGs use the same version. Original sources and previous caches are
preserved. All 27 years (1998–2024) have been regenerated for this scale.

### Compare local exposure over time

The country page also includes an independent comparison section below the original visualization.
`time_comparison.js` renders two synchronized country-clipped grid maps, initially 1998 and 2024.
A shared country/city lookup sets both viewports. Two keyboard-accessible range controls on one
track independently set the left and right years; the handles can cross. Both maps use the same
versioned palette and native-resolution source rasters. The first visualization keeps its own state.

Verification: `node ai/aqli_gridded/verify_time_comparison.cjs` checks selection, year independence,
viewport synchronization, and clearing using map/DOM mocks. Live HTTP checks confirmed both
endpoint years serve PNG tiles. Browser visual verification remains pending because the browser
runtime is unavailable in this session.
