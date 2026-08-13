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
cd owid-grapher/bespoke && yarn install && yarn dev
# then http://localhost:8089/<project>/demo
```

The demo page mounts every variant of the project inside a Shadow DOM, matching production
(`bespoke/readme.md:281`). Prefer it because:

- **It mounts them for you** — no manual `mount()` call, so none of the failure modes below apply.
- **It works before publication**, which is the common case when a viz is being built.
- **You choose the render size**, which is how you hit the template band's aspect ratio without
  distorting anything.

Each variant is wrapped in `div.variant#variant--<name>` and carries a **"Download SVGs"** button
(`bespoke/server/component-demo.html`) that calls `downloadSvgs(shadowRoot, "<project>-<variant>")`.
For a one-off, clicking that button *is* the export — no scripting needed. Use
`scripts/bespoke_svg.mjs` when you want a specific viewport, or several variants, or repeatability.

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
| 7 | The ordinary band fit. Unlike the `static_viz` local-SVG route there is no template-aspect guarantee and no `rescale(100/96)` — this is a chart-only SVG at whatever size you rendered it. |
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
