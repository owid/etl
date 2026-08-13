# Bespoke visualizations → a chart-only SVG

How to get an OWID **bespoke component** — a client-rendered React viz embedded in an article, with no
`/grapher/<slug>.svg` endpoint — into a static chart template. Everything after the SVG exists is
ordinary SKILL.md: it behaves like grapher's `uncaptioned` embed, so Steps 5 through 9 apply unchanged.

**Last verified: 2026-08-13.** Facts below marked *(unverified)* have not been run end to end.

## Is this actually a bespoke component?

OWID articles carry two kinds of chart, and only one of them needs any of this.

```bash
curl -sL "<article-url>" -o page.html
grep -oE 'grapher/[a-z0-9-]+' page.html          # a Grapher embed -> use the .svg endpoint, stop here
grep -oiE '"type":"bespoke-component"[^}]*' page.html
```

A Grapher embed has a static export already; fetch `https://ourworldindata.org/grapher/<slug>.svg`
and go back to SKILL.md Step 1. Only a bespoke block needs this file. It looks like:

```json
{"type":"bespoke-component","bundle":"migration","config":{"country":"Malaysia","urlSync":"true"},"variant":"sankey"}
```

You need all three of **`bundle`**, **`variant`** and **`config`**. The bundles that exist are listed
in `site/bespokeComponentRegistry.ts` — as of 2026-08-13: `example`, `causes-of-death`, `demography`,
`food-trade`, `migration`. Each registry entry carries only a `scriptUrl`; **none has a `cssUrl`**,
which matters below.

## Two routes, local preferred

### Local — the project is checked out (preferred)

```bash
cd owid-grapher && yarn startBespokeDevServer     # NOT `yarn dev` in bespoke/ — there is no such script
# then http://localhost:8089/<project>/demo
```

The demo page mounts every variant of the project inside a Shadow DOM, matching production
(`bespoke/readme.md:281`). Prefer it because:

- **It works before publication**, which is the common case when a viz is being built.
- **You choose the render size**, which is how you hit the template band's aspect ratio without
  distorting anything.

Each variant is wrapped in `div.variant#variant--<name>` and carries a **"Download SVGs"** button
(`bespoke/server/component-demo.html`) that calls `downloadSvgs(shadowRoot, "<project>-<variant>")`.

**But the demo page shows each variant's `demoConfig`, which is almost never the view you want.**
`food-trade`'s sankey ships `demoConfig: {}` — that renders *Maize*, not the product you were asked
for. So the button and the bare `--demo` route only help when the defaults happen to be right.

For a specific view, pass `--config` **with** `--demo`: the script then loads the demo page (for its
dev-only global CSS and same-origin module resolution) and mounts its *own* instance of the bundle,
in its own Shadow DOM, with your config:

```bash
node scripts/bespoke_svg.mjs --demo food-trade --variant sankey \
  --config '{"product":"Wine","country":"All countries","flow":"both","hideControls":"true"}' \
  --viz-width 923 --base wine-world-trade --out .
```

**Read the config keys out of the project, not out of the gdoc.** `src/config.ts` has the parser —
`food-trade` takes `product`, `country`, `flow` (`both|import|export`), `title`, `subtitle`,
`hideControls`, `hideFlowSwitcher`, `urlSync`. Sentinel values live in `src/helpers.ts`: the global
view is `country: "All countries"` (`ALL_COUNTRIES`), and there is **no `World` entity** in the data,
so "worldwide" is that sentinel rather than an entity name. Set `hideControls: "true"` — the controls
are HTML and would not serialize anyway, but they change the layout.

### Published article — the project isn't checked out *(unverified)*

The bundles are served in production at:

```
https://ourworldindata.org/assets/bespoke/<bundle>/index.js     # verified 200
```

Each exports `{ VARIANTS, mount }`, with `mount(element, { variant, config })` returning an
`unmount()` function.

**Load the article first and check whether it renders on its own.** The site's `owid.mjs` hydration
may or may not fire under automation; if the component's container ends up with a populated shadow
root, you are done and there is no trick to perform. Only if it stays empty do you mount it yourself.

**When you do mount it yourself, mount into a Shadow DOM.** This is the part that is easy to get
wrong, and the failure is silent: production mounts through `mountBespokeComponentInShadow`
(`site/gdocs/components/BespokeComponent.tsx`), and because the registry defines no `cssUrl`, each
bundle injects **its own CSS scoped to `:host`** (7 `:host` rules in the migration bundle). Mount into
a plain element and every one of those rules fails to match — the component renders **unstyled**, and
since the serializer reads `getComputedStyle`, you get a structurally plausible SVG with the wrong
paint, weights and geometry. Nothing errors.

```js
const el = document.querySelector('[class*=bespoke-component]')
const root = el.shadowRoot ?? el.attachShadow({ mode: 'open' })
const host = document.createElement('div')
root.appendChild(host)
const mod = await import('/assets/bespoke/migration/index.js')
window.__unmount = mod.mount(host, {
    variant: 'sankey',
    config: { country: 'Malaysia', urlSync: false },   // urlSync off: don't fight the page URL
})
```

Give it 1–3s to paint.

## Serialize with the repo's own logic

`bespoke/shared/exportSvg.ts` already solves the hard part, and `scripts/bespoke_svg.mjs` mirrors it
inside `page.evaluate`:

- `findVizSvgs(root)` — collects every `<svg>` whose larger side is ≥ `MIN_VIZ_SIZE` (60px), descending
  into shadow roots. The size floor is what skips icons and legend swatches.
- `serializeStandaloneSvg(svg)` — clones it, walks source and clone in lockstep back-to-front,
  **drops `display:none` nodes** (hover tooltips, inactive states) and inlines ~28 computed paint and
  text properties so the markup stands alone. Adds `xmlns` and a `viewBox` fallback from the bounding
  rect.

> **Keep the script's serializer in sync.** It is a copy, not an import — the production page serves
> built bundles, not `bespoke/shared/*.ts`, so there is nothing to import on the article route. If
> `exportSvg.ts` changes (particularly its `STYLE_PROPS` list), re-copy it.

**One thing the script adds on top of `exportSvg.ts`: it widens the viewBox to the content bbox.**
These components draw their side labels *outside* their own `<svg>` bounds and rely on the page not
clipping. A standalone SVG clips at its viewBox, so the outermost labels would be cut. The script
unions `source.getBBox()` into the declared box and rewrites `viewBox`/`width`/`height`. On the wine
sankey that widened 784 → 787px; on a component with longer labels it will matter more.

### Sizing the export: the container decides, and height may not follow

`--viz-width` sets the width of the mounted container, and **that** is what these components lay out
from (`useContainerWidth`) — not the viewport. Measured on the food-trade sankey:

| container | SVG |
|---|---|
| 700 | 666 × 450 |
| 818 | 784 × 450 |
| 874 | 840 × 450 |
| 950 | 916 × 450 |

So the SVG is `container − 34`, and **the height is fixed** — 450px, set by the node count, not by the
width. That is the opposite of a grapher export, where you request an aspect. Here you get one degree
of freedom, so solve the container width against the band you have to fill:

```
containerWidth = 34 + (bandWidth / bandHeight) × naturalHeight
```

For the Horizontal template's band minus 12px gaps (818 × 414) that gave 923 → an 889 × 450 SVG →
scale 0.92 → 818 × 414. Check the natural height first: a component that *does* scale height with
width needs a different sum.

### Two things the downloaded draft got wrong for this purpose

The prose this file replaces described a different deliverable — a standalone SVG you could hand to
anyone — and two of its steps are actively harmful when the destination is a Figma template:

- **Do not reconstruct the HTML text word by word.** The draft measured each word with a `Range` and
  emitted one `<text>` per word to reproduce line wrapping. In Figma that yields a title you cannot
  retype, and it is wasted effort besides: **the template supplies the title, subtitle, note and
  source**, so the HTML scaffolding around the chart is exactly what you want to leave behind. Take
  the chart `<svg>`s and nothing else.
- **Do not embed the fonts as base64 `@font-face`.** That exists so the file renders on a machine
  without Playfair Display and Lato installed. Figma resolves both by name, and the embed adds
  ~100 KB per weight to a file you are about to discard the text from anyway.

## Caveats to state to the user

- **Only SVG content comes through.** Anything the component draws in HTML — legends, controls,
  captions, the logo — is absent. A legend usually has to be rebuilt in Figma (GUIDELINES.md → Direct
  labeling is often the better answer anyway) or dropped.
- **A split viz returns several SVGs.** The migration sankey is two (immigrants left, emigrants
  right), named `-1` and `-2`. The script writes a JSON sidecar with each one's frame-relative
  bounding box so Step 5 can place them without re-measuring; grabbing "the biggest SVG" gives you
  half the chart.
- **This captures one state.** Whatever `config` selected — one country, one year, one direction. Say
  so, and re-run for another.

## How it joins the existing steps

| Step | What changes |
|---|---|
| 1 | Nothing to resolve — the file *is* the export. Read the block's `bundle`/`variant`/`config` from the article's gdoc JSON, and the body text from it too if you want annotation content. |
| 2 | Formats as usual. A wide viz (a sankey) wants Static Horizontal; a tall one, Vertical. |
| 3 | No `imWidth`/`imHeight`/`imFontSize`. **The puppeteer viewport is the aspect control**, so measure the template band first (Step 7), then render at that aspect. Same ordering as the grapher route, different lever. |
| 5 | Ordinary `upload_assets` + the `unwrap` helper. Two SVGs means two imports, positioned from the sidecar. |
| 7 | **Fit the WIDTH, not the height** — the reverse of the grapher route. There, you fit height first and close the width with a scripted x-map; a sankey's bands cannot be x-mapped without distorting them, so the width is the constraint and the vertical gap falls out of it (17.3px each side on the wine chart). Expect **one correction after import**: Figma's unwrapped group measured **913** wide against the SVG's declared **889**, because a group's bbox includes stroke extents — so the aspect you solved against the SVG is a few percent off once it lands. Measure the group, then scale. |
| 8 | Node names are the component's own, derived from its DOM. There is **no `connectors` group** to hide and no `horizontal-grid-lines`, so every named lookup in Steps 7–8 has to be re-derived: `grep -oE 'id="[^"]*"' <file>.svg \| sort -u`. |
| 8c | Unchanged. Note the fills come from the component's own palette, not necessarily the Chart colors library, so expect the off-palette sweep to have findings — report them to the component's author rather than repainting in Figma. |

## Tooling

`scripts/bespoke_svg.mjs` drives the system Chrome through `puppeteer-core` — not `puppeteer`, which
downloads its own ~150 MB Chromium. Nothing is assumed pre-installed; bootstrap into the session
scratchpad rather than the user's project:

```bash
cd <scratchpad>
npm init -y >/dev/null 2>&1
npm install puppeteer-core@23 >/dev/null 2>&1     # ~5s, no browser download
node <skill>/scripts/bespoke_svg.mjs --help
```

Check the prerequisites first — `which node`, then Chrome:

```bash
ls "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  || which google-chrome chromium chromium-browser
```

The script tries the usual macOS and Linux paths and accepts `--chrome <path>` to override.

**Run it from the directory where you installed `puppeteer-core`.** The script lives in the skill
folder, which has no `node_modules`, so a bare `import "puppeteer-core"` resolves from *there* and
fails with `Cannot find package`. It therefore resolves the dependency from `process.cwd()` — which
only works if that is the scratchpad you installed into.

**Rasterizing the result to eyeball it has two traps of its own.** Chrome loads a bare `.svg` as an
SVG document, so `document.head` is `null` and you cannot inject CSS; and an exact-size viewport plus
the browser's default body margin crops the right and bottom edges in a way that looks *exactly* like
the SVG clipping itself — which sent me chasing a viewBox bug that did not exist. Wrap the markup in
an HTML page with `margin:0` and screenshot the **element**, not the page.

## Which template — and the shape problem

**There is no bespoke-specific template.** A bespoke chart goes into the ordinary static templates,
same as a grapher export: `Static Chart Template_Horizontal` (`5332:75`, 850×638) or
`Static Chart Template_Vertical` (`5332:93`, 850×1095). Verified against the Templates page — don't go
looking for one.

**Drive both dimensions of the container, and the component will fill whatever band you give it.**
This is the part worth getting right, because the wrong conclusion is very easy to reach: render the
wine sankey at its default height and it comes out 818 × 403, which fills 92% of the Horizontal band
and only **45%** of the Vertical one — from which it looks as though a sankey is inherently landscape
and cannot be made portrait. It isn't. Two knobs get you there:

1. **`--viz-height`.** These components size themselves from their container — the bilateral sankey via
   `useParentSize()`, which reads height as well as width. Set the container's height and the layout
   follows.
2. **`--viz-css`, when a component pins its height in SCSS.** food-trade does:
   `FoodTradeChart.scss` sets `.food-trade-captioned-chart__chart-area { height: 450px }`, and
   everything below it is `height: 100%`. That single rule is why `--viz-height` alone changes nothing.
   Override it in the Shadow DOM:

```bash
--viz-width 852 --viz-height 900 \
--viz-css '.food-trade-captioned-chart__chart-area{height:869px!important}'
```

That returned an **818 × 869** SVG — the Vertical band's exact content width, and 97% of its height with
12px gaps, imported at **scale 1.0005**. Nothing was scaled, so every label stayed at its natural
12/16px.

**And a taller render is a better chart, not merely a bigger one.** The component labels what it has
room to label: the 450px render emitted 33 text nodes, the 869px render **42** — Canada, Russia,
Portugal and China gained their values (380,000 / 340,000 / 280,000 / 250,000 tonnes) because the
bands were finally thick enough to caption. So don't scale a short render up to fill a tall band;
re-render at the band's height and gain the labels.

The workflow that follows: **measure the band first, then render to it.** Solve the container width for
the content width (852 → 818 here; the SVG is `container − 34`) and set the container height to the
band minus the gaps you want. Injected CSS re-lays out asynchronously through the component's
ResizeObserver, so the script waits before serializing — without that pause you capture the old
geometry.

For the record on food-trade's other layouts: `FoodTradeBilateralSankey` (the `All countries` view)
only shortens its numbers and drops from 10 nodes to 8 below `MOBILE_BREAKPOINT` (500px) — it never
stacks. The **single-country** view is different: `SplitFlowSankey` stacks its imports and exports
halves vertically below 500px, which is a genuinely different composition rather than a resize.

## What to check, and what to hand back

The Step 8c pass applies, with one route-specific split: **the palette is the component's, not
yours.** The wine sankey's ten series colors are OWID palette values (`#b13507` Rusty Orange,
`#00295b` Midnight Blue, `#4c6a9c` Blue, …) and `color_audit.py --separated` reports a deuteranopia
floor of **ΔE 7.6** (Chile/New Zealand), with Spain/Germany at 8.2 and Australia/Portugal at 9.4 —
below the ΔE 20 bar. Do **not** repaint it in Figma: that would make the image disagree with the
interactive component. Report it to the component's author, and note the mitigation when it applies —
in a sankey every node carries a direct text label, so color is a secondary cue for identity and
matters mainly for following a band across the crossing.
