# Step 3 — Export the SVGs

> Read at Step 3.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


> **Local SVG on disk: there is nothing to export.** When the input is a file from an
> `export://static_viz` step, skip this whole step. The step chose its own `figsize` to match a
> template's proportions, so none of the `imType` / `imFontSize` / `imWidth` aspect solving below
> applies, and there is no chart-only "embed" to export — the file already *is* the framed chart,
> carrying its own title, subtitle, `Note:`, `Data source:` and license at that template's own slot
> positions. So the two assets below come from the step's own output rather than from a `curl`: the
> **PNG** it emits beside the SVG is the flat reference copy for the page, and the **SVG** is what
> goes into the template clone. `upload_assets` takes a local path unchanged. Then follow the
> local-SVG route in Steps 5 and 7 — it replaces the measure-solve-export-fit ordering entirely,
> because a frame that already matches the template has nothing left to solve.

`imWidth`/`imHeight` set the **aspect ratio only** — the server renormalizes the SVG to ~510k px², so you cannot request a bigger SVG (irrelevant: it's a vector; you scale it in Figma). Sanity-check what came back:

> **This is true of the default and `uncaptioned` routes only.** `extractOptions` (`functions/_common/imageOptions.ts`) **returns early** for `imType=thumbnail` and `imType=square`, so neither the `MIN/MAX_ASPECT_RATIO` clamp nor the ~510k normalization runs on them. On the thumbnail route `imWidth`/`imHeight` set the size outright — `staticBounds` becomes `imWidth/4 × imHeight/4` — which is what lets a 302-wide small chart arrive at exactly 302px and skip the Figma rescale entirely (SMALL-CHARTS.md → The export).

```bash
head -c 300 $DIR/embed.svg   # expect <svg ... width="..." height="...">, no <html
```

> **[`scripts/solve_export.py`](../scripts/solve_export.py) does this arithmetic — don't do it by hand.**
> Run it from the repo root through the venv — `.venv/bin/python .claude/skills/create-figma-chart/scripts/solve_export.py …`;
> it is committed non-executable like the rest of that directory.
> `--band 508x371 --slug <slug> --params '<the view's query string>'` returns the solved
> `imFontSize`, the `imWidth`/`imHeight` to
> request, the predicted content box, the **height-first** scale into the band, the leftover width
> the x-map has to close, the final label size, and the finished `curl`. **Omit `--params` and that
> `curl` exports the DEFAULT chart** — a valid, plausible SVG of the wrong entities. Two things more:
> it reports the leftover width rather than a predicted gap, because the gap is exact by
> construction once you fit the height (Step 7) — that leftover is the same quantity
> `measure_fit.js` reports as `xMapShortfall`, and it is the aspect miss expressed in px; and every
> number comes from the **rounded** `imFontSize`, since that is what the URL carries, so the label
> size quoted is the one the `curl` will actually produce (it prints the ideal font alongside when
> rounding moves it). It is a TWO-PASS tool: `--band` alone is pass 1, a probe under the symmetric
> `1.4 × imFontSize` inset model; pass 2 re-runs with `--declared`/`--ink`/`--im-font-size` measured
> off the probe's import and is exact, because the real inset is per-axis, not symmetric (see Step 7
> and reference/FITTING.md). It also carries its own `--self-test` (the worked examples, the band
> round-trip, and a real run's two measured-inset passes) and the `--thumbnail` route for a 302-wide
> chart.
>
> **It solves for `band − 2×--gap`, not for the band** — the gap below is a requirement of the fit,
> so a solve that ignores it lands the chart edge to edge and you re-export. `--gap` defaults to 14
> and takes 30 for the Instagram portrait; a 508×371 band makes the target 508×343, 14px at each
> end. The canvas model is confirmed against the real renderer — a `--gap 0` solve predicted 828×616
> and grapher returned **829×616**, landing labels at exactly 13.5px — so it is the target fed into
> it that the gap changes. After you have measured a real import, run the `nextPass` command that
> `measure_fit.js` prints — with its `CONFIG.declared` and `CONFIG.imFontSize` set from the probe,
> it is the exact measured-inset second pass rather than another guess — see Step 7.

**The aspect you request is the *canvas*, not the chart — so solve for the padding, or you re-export every page.** Grapher insets the drawing inside the SVG it returns, and it is the imported *group* that has to fill the band, so requesting the aspect you want yields a chart that misses it (measured: a 336.9px chart where 343 was needed — a 17px gap against a 14px target). `solve_export.py` does this arithmetic; its `--help` carries the closed-form solve, the canvas model and the measured per-axis insets, so read it there rather than re-deriving it here.

**The inset is per-axis, and it grows once the furniture you are replacing is out of the measurement — so pass 1 is a probe even with measured numbers.** Dropping the end label from the measurement on a single-series chart took this chart's `insetX` from the 64.1 its own docstring records to **122.08**, because the reserved right margin left the ink: the requested aspect came back as a 1.31 group where 1.4033 was solved for, and the x-map correctly REFUSED to close a 7.2% miss by squeezing. Exclude everything you are replacing from the measurement — `measure_fit.js`'s `hideNames`/`hideIds` compute the aspect *as if* those nodes were hidden **without** hiding them, so the probe needs no mutation at all — then run the pass-2 command.

**Then be ready for the target itself to move: taking the replaced furniture out changes the group's aspect.** `connectors` extend to the right of the plot, so dropping them (Step 8) narrows the group and makes it relatively *taller* — the same export that was solved for a 1.6026 content aspect measured 1.5558 once the elbows were gone, turning a 14px gap into 9.5px. Account for the connectors and the year markers **before** you measure and scale, not after, and re-read the aspect from the group you are actually going to fit.

**And the two cases take different actions — this is where hiding bites.** For the probe, *exclude* the furniture from the measurement as above; nothing is mutated. Before the **final** measurement and fit, *remove* the replaced nodes from the working clone. A node switched off with `visible = false` still contributes to its group's `absoluteBoundingBox`, which is exactly what `box-alignment` reads — the 2.28px failure worked through in reference/FITTING.md, on a chart whose visible ink measured 508.001 exactly.

> **Square charts, second route:** grapher's `imType=square` render re-lays out the chart for a square
> canvas (legend placement, font sizing tuned by the web team). Import the full square SVG and delete its
> `header` and `footer` groups after import. Its attraction is that it can land ladder-exact with no
> rescale at all — measured once at a ≈505×328 chart area with every label at exactly **15px**, where
> reaching 15px through `imType=uncaptioned` took `imFontSize≈36` — which also spent more of the frame
> on furniture, coming back with a **279px** plot against the square route's **294.6px**.
>
> **But it is laid out under grapher's OWN header and footer, not the template's, so its chart area is
> sized for a band you are not filling — and that usually loses.** Across five DI pages it came back
> **314.9px** tall for a **371px** band: a 28px gap at each end, twice the target, closable only by a
> rescale that then breaks the width. So **measure the band first (Step 7), then pick**, comparing the two
> exports on three numbers — final font size at the template width, plot height, plot width. The square
> route wins only when its chart area happens to fill your band (a short template header, or a map or
> big-legend chart whose square re-layout is genuinely better). The band is knowable only once the
> template texts are in, which is why Step 6 comes before the embed export.

**Size the text at export time with `imFontSize` — scaling in Figma cannot fix it.** Grapher picks a base font for the canvas it renders (`max(10, height/25)`, so ~24 for the default uncaptioned export), and every label is derived from it — the segment values and country names land at about **0.75 × the base**. Placing that export at 508px wide shrinks all of it by the same factor, so a default export ends up with ~12px labels: legal, but on the floor of the 12px minimum. Ask for a bigger base instead — `imFontSize=28` gives ~13.5px labels and ~14px legend text in a 540 frame, which matches the template's own 14px source line. Check the export before importing:

```bash
grep -oE 'font-size="[0-9.]+"' chart.svg | sort | uniq -c | sort -rn | head -3
# multiply the most common value by (508 / the export's content width) to get the final size
```

Bigger text needs more room, so this trades against how much fits — see the axis rule in Step 8 and, failing that, the entity count.

**`tab=` FALLS BACK SILENTLY when the chart does not declare that type — check the mark group, not the HTTP status.** `co-emissions-per-capita?tab=marimekko` returns 200 and a plausible 9 KB SVG containing `lines`: a line chart. `tab=stacked-discrete-bar` on the same slug returns `stacked-areas`. Nothing errors and the file looks right, so a whole "chart-type sweep" can be built on two charts that are not the types you asked for — which is what happened here. Two defences: find charts that actually declare the type (`json_extract_string(cc.config, '$.chartTypes')` on the public Datasette — `/query-grapher-db`), and assert the mark group in the returned SVG:

| type | mark group `id` |
|---|---|
| line | `lines` |
| discrete bar | `bars` + `entity-labels` + `value-labels` |
| stacked discrete bar | `bars` |
| stacked area | `stacked-areas` |
| slope | `slopes` |
| scatter | `points` |
| marimekko | `marimekko-chart` |
| map | `map` |

Caveats: `?tab=table` is silently ignored (renders the default tab); `imSquareSize` affects PNG only; add `nocache` when re-exporting after a config change.
