// verify_page.js — the MECHANICAL rows of Step 8c, in one read-only `use_figma` call.
//
// reference/CHECKS.md is the gate before anyone sees a frame, and most of its rows are property
// reads of a single page: font sizes, stroke weights, dash patterns, gaps, box alignment, margins,
// bound-vs-raw fills, annotation knockouts. Run one at a time that is a dozen MCP round trips at
// ~8-10s each; run as one traversal it is one. This is the same move `verify_templates.js` makes for
// the template geometry, applied to a finished page.
//
// Read-only. It sets no property and creates no node.
//
// USAGE
//   Fill in CONFIG and paste the whole file as one `use_figma` call.
//     frameId       — the finished frame (the template clone, after Step 8).
//     frameIds      — optional array, for a chart-rows SET: every frame is checked and the result is
//                     one entry per frame. All of them must be on the SAME page, since a script may
//                     switch pages only once.
//     chartName     — the chart group's name. Resolved by NAME, never by a captured id: a re-import
//                     replaces the group and a stale id returns null (reference/GOTCHAS.md). If a
//                     designer has ungrouped it, pass null and the plot subgroups are used instead.
//     gapTarget     — [min, max] px per end. 12-16 on the 540-wide frames, 30 on the IG portrait.
//                     Left null it is derived from the frame width, which is right for the nine
//                     in-scope templates and wrong for anything else — so pass it when you know it.
//     tightlyMeasured — set true for an axis-less chart whose furniture was trimmed and label boxes
//                     hugged (Step 8). CHECKS.md's gap row does not apply as written there; the row
//                     reports SKIPPED with the reason rather than failing a correct chart.
//     highlightTreatment — true when the chart uses the muting-gray highlight treatment, which
//                     changes the mark-weight bar (context 1px, protagonist 3px, halo 2x), makes
//                     the muting grays a standing palette exception, and makes a muted CONTEXT line
//                     legal to cross with an annotation (CHECKS.md allows exactly that).
//     xlAnnotations — true when a lead annotation deliberately carries the message and takes
//                     Annotation XL 16, level with the subtitle. That is the documented ceiling, spent
//                     only when the annotation IS the message — so it has to be declared rather than
//                     inferred, and left false a 16px annotation is a defect.
//     textFloor     — px. Left null it is derived from the frame width: the 302-wide small and pull
//                     templates run an 11px floor, because their own subtitle, source and year text
//                     is 11px by design (SMALL-CHARTS.md overrides it) — a pull chart ALWAYS carries
//                     an 11px source line, so the 12px bar reports every one of them as broken.
//
// WHAT IT DOES NOT COVER, and never silently passes: every row it cannot judge is returned as
// SKIPPED with the reason and the tool that owns it. Colour-vision and grayscale seams are
// `scripts/color_audit.py`; spelling is `codespell`; "the text is true of the indicator" is
// `/adversarial-data-review`; the entity-completeness row needs the EFFECTIVE selection from
// outside Figma (Step 1's table); the arrow row needs rendered pixels (CHECKS.md's four-render
// protocol); leader-on-map is a VECTOR ray-cast against the country's rings, with the pixel mask only
// as its fallback; and the page census is a page-level count, where this script is handed frames. A
// check that cannot fail is worse than no check, so those are reported as gaps in coverage, not as
// passes.

const CONFIG = {
  frameId: "26000:6",
  frameIds: null,
  chartName: "chart",
  gapTarget: null,
  tightlyMeasured: false,
  highlightTreatment: false,
  textFloor: null,
  xlAnnotations: false,
};

const LADDER_FULL = [12, 13, 14, 15, 16]; // Annotation XS..XL; the ceiling is L 15, XL only when
const LADDER_CEILING = 15;                // the annotation IS the message (GUIDELINES.md)
const SMALL_FRAME_W = 302;                // the small/pull templates, whose floor is 11 not 12
const HOUSE_LINE = 3, HOUSE_HALO = 4;
const FURNITURE_W = 1, FURNITURE_DASH = [4, 4];
const BLOCK_CLEARANCE = 27;          // annotation block vs header/footer on the 540x540 pages
const GRAPHER_RESIDUAL = "#585c64";  // emitted for residual categories; in no library group

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);
// WCAG relative luminance and contrast, so the "direct labels readable as text" row is computed
// rather than declared unchecked. Pure function of two hexes; no geometry needed.
const lin = (c) => (c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4));
const lum = (hex) => { const n = parseInt(hex.slice(1), 16); const R = ((n >> 16) & 255) / 255, G = ((n >> 8) & 255) / 255, B = (n & 255) / 255;
  return 0.2126 * lin(R) + 0.7152 * lin(G) + 0.0722 * lin(B); };
const contrast = (a, b) => { const la = lum(a), lb = lum(b); return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05); };
const hexOf = (f) => (f && f.type === "SOLID"
  ? "#" + [f.color.r, f.color.g, f.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("")
  : null);
// A paint that PAINTS NO PIXELS is not a colour this file may report. Figma switches a paint off two
// independent ways — `visible: false` and `opacity: 0` — and only the first of those was tested at the
// sites that pick a FILL, so a fully transparent decoy could still supply a mark's colour. That mark
// then entered the colour-audit palette as a category nobody can see (and could be recommended a
// replacement for), and stood in as the backdrop a label's contrast was measured against. The stroke
// collector already applied both tests; this is that same predicate, named once so the sites cannot
// drift apart again. It is the same reasoning that already excludes zero-area nodes from the fill
// inventory: a paint nobody can see is a phantom colour, and reporting one sends a reviewer to go and
// fix a mark that is not on the canvas.
// The test is ZERO, not a floor. A 0.01 cutoff was a threshold nobody chose: a paint at 0.005 does
// reach the canvas, and dropping it took its node out of EVERY row — text-floor, overlap, margins —
// with nothing anywhere saying so. Silently auditing a subset is the failure this file exists to
// remove, and it does not become acceptable because the subset is faint. Anything positive is
// TRANSLUCENT: held out of the palette, named, and still judged by every row that is about geometry
// rather than colour.
const renders = (p) => !!p && p.type === "SOLID" && p.visible !== false
                       && (p.opacity === undefined || p.opacity > 0);
// PARTIAL opacity is a different problem from zero, and it is not solved by dropping the node: the mark
// IS on the canvas, so it still has a box, still blocks an annotation, still needs its stroke weight
// judged. What it does NOT have is a reportable COLOUR. `hexOf` returns the paint's raw RGB, and what
// the reader sees is that RGB composited onto whatever is behind it — LABELING.md works the same sum
// the other way round and measures the gap as `#ecebe8` against `#fbf9f3`, "close enough to look
// deliberate and wrong enough to see". Auditing the raw value asks color_audit.py about a colour that
// is not on the page: its deltaE and grayscale verdicts, and any `--suggest` replacement, are then all
// answers to the wrong question. This is not hypothetical — grapher exports non-focused series at
// `stroke-opacity="0.5"` (GUIDELINES.md) and this skill's own period band is a fill at 50%.
// Compositing it here is the tempting fix and is refused: it needs what is DIRECTLY behind the mark,
// which for a mark inside a plot is usually another mark rather than the frame, plus every ancestor's
// opacity. Guessing the frame would just relocate the confident wrong answer. So a translucent paint
// is declared UNMEASURABLE and named, the same way a sequential ramp and an unsafe `--names` are.
const translucent = (p) => !!p && typeof p.opacity === "number" && p.opacity < 0.999;
let rows = [];
const add = (name, status, detail, extra) => rows.push({ check: name, status, detail, ...(extra || {}) });
const skip = (name, why, owner) => add(name, "SKIPPED", why, owner ? { ownedBy: owner } : null);

// One frame's worth of checking. Split out so a CHART-ROWS SET — 3-5 frames that ship as one
// deliverable (SMALL-CHARTS.md) — sweeps in a single call instead of one per frame. All frames
// must live on the same page: a script may switch pages only once (the figma-use page rule).
const checkFrame = async (frameId) => {
  rows = [];
  const frame = await figma.getNodeByIdAsync(frameId);
  if (!frame) throw new Error(`frameId ${frameId} not found`);
  // The FRAME is the one ancestor EVERY mark shares, and it sits ABOVE the walk below — which was
  // seeded with a literal 1, so the frame was the only node whose opacity was never examined. A frame
  // left at reduced opacity (or the section holding it) dims every mark on the canvas while the colour
  // rows go on reporting raw paints: the same wrong-verdict-about-what-is-not-there this file guards
  // against everywhere else, reached by the one path that skipped the guard. Accumulated on the climb
  // that already runs to find the PAGE, so it picks up section and group ancestors too.
  // VISIBILITY is the OTHER switch, and it is inherited the same way. `collect` tests `visible` on every
  // node it walks, but it starts at the frame's CHILDREN — so a `visible: false` on the frame itself, or
  // on a group or section holding it, was never read, and a hidden node's descendants keep their own
  // `visible: true`. The frame then certified a sheet of verdicts about a deliverable that is switched
  // off. Same climb, same reason as the opacity above; collected as a LIST because unhiding the nearest
  // ancestor is not enough when two of them are off.
  let frameOpacity = 1;
  const hiddenAncestors = [];
  let page = frame;
  while (page && page.type !== "PAGE") {
    if ("opacity" in page && typeof page.opacity === "number") frameOpacity *= page.opacity;
    if ("visible" in page && page.visible === false) hiddenAncestors.push(`${page.name || "(unnamed)"} (${page.type})`);
    page = page.parent;
  }
  if (page && figma.currentPage !== page) await figma.setCurrentPageAsync(page);

  const fb = frame.absoluteBoundingBox;
  // A frame that PAINTS NO PIXELS is not a frame with a lot of passing rows on it. Figma switches a node
  // off two independent ways — `visible: false` and opacity — and this file already treats those as ONE
  // state everywhere else (see `renders` for a paint and the zero-opacity return in `collect` for a node);
  // the frame is the last place they were handled as two. Whichever way it is off, every row below would
  // be a verdict about something no reader can see, so the frame is REPORTED as not rendered instead and
  // no other row is emitted. That is the honest shape: not "nothing failed", but "nothing was checked".
  if (hiddenAncestors.length || frameOpacity <= 0) {
    const why = hiddenAncestors.length
      ? `switched off with visible=false on ${hiddenAncestors.join(", then ")}`
      : "at effective opacity ZERO, counting every group and section above it";
    add("frame-not-rendered", "FAIL",
        `NOT CHECKED — this frame paints no pixels: ${why}. Both switches are INHERITED, and the walk`
        + " below starts at the frame's children, so a descendant's own visible=true and opacity=1 say"
        + " nothing about whether it renders. Every other row would be a verdict about something the"
        + " reader cannot see, so none was emitted: this is not an empty or a passing frame. Unhide it,"
        + " or reset its opacity, along with any group or section above it, and re-run.");
    return {
      frame: { id: frame.id, name: frame.name, w: fb ? r(fb.width) : null, h: fb ? r(fb.height) : null },
      resolved: { chartBy: "not resolved — nothing under a frame that paints no pixels was walked",
                  contentBox: [null, null], band: [null, null],
                  counts: { texts: 0, stroked: 0, plotLeaves: 0, annotations: 0 } },
      verdict: `NOT CHECKED — the frame paints no pixels (${hiddenAncestors.length ? "hidden" : "effective opacity " + r(frameOpacity)})`,
      rows,
    };
  }
  const isSmall = fb ? Math.round(fb.width) <= SMALL_FRAME_W : false;
  const TEXT_FLOOR = CONFIG.textFloor !== null && CONFIG.textFloor !== undefined ? CONFIG.textFloor : (isSmall ? 11 : 12);
  const LADDER = isSmall ? [11, ...LADDER_FULL] : LADDER_FULL;
  const frameFill = (() => { const f = frame.fills && frame.fills[0];
    if (!renders(f)) return null;
    return "#" + [f.color.r, f.color.g, f.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join(""); })();
  const rel = (n) => {
    const b = n.absoluteBoundingBox;
    return b ? { l: b.x - fb.x, t: b.y - fb.y, rr: b.x - fb.x + b.width, bb: b.y - fb.y + b.height, w: b.width, h: b.height } : null;
  };
  // `fontSize` is `figma.mixed` — a SYMBOL — on a node with per-range sizes, so a `typeof === "number"`
  // gate drops the WHOLE node rather than the odd range: a partially restyled annotation or label could
  // carry an 8px or off-ladder run while text-floor, ladder-sizes, named-styles and text-hierarchy all
  // reported on everything else and never mentioned it. Read the RANGES; each one is judged on its own.
  const sizeRanges = (n) => {
    if (typeof n.fontSize === "number") return [{ size: n.fontSize, range: null }];
    try {
      return n.getStyledTextSegments(["fontSize"])
        .map((s) => ({ size: s.fontSize, range: `${s.start}-${s.end}` }))
        .filter((s) => typeof s.size === "number");
    } catch (e) { return []; }
  };
  const unreadableText = [];

  // --- structural resolution, the same rule verify_templates.js and measure_fit.js use. Names are not
  // stable across design edits and the logo is a SIBLING of the header, not a child.
  const logo = frame.children.find((c) => /^logo/i.test(c.name) || /^Logos\//.test(c.name)) || null;
  const autos = frame.children.filter((c) => "layoutMode" in c && c.layoutMode !== "NONE" && c !== logo).sort((a, b) => a.y - b.y);
  const header = autos[0] || null;
  const footer = autos.length > 1 ? autos[autos.length - 1] : null;
  const footerTop = footer && footer.children.length ? footer.y + Math.min(0, Math.min(...footer.children.map((c) => c.y))) : footer ? footer.y : null;
  const bandTop = header ? header.y + header.height : null;
  const contentL = header ? header.x : null;
  const contentR = header ? header.x + header.width : null;

  // The chart, by NAME. A designer's rework ungroups it, leaving the plot subgroups as direct frame
  // children — so fall back to those rather than reporting nothing.
  //
  // That fallback has to be STRUCTURAL, not a list of group names. A whitelist of the axis/grid/lines
  // containers was line-chart-shaped: it missed a map's `map`, a bar's `bars`, a scatter's point
  // container and a slope's `slopes` outright. Those children were then walked with `insidePlot` false,
  // which does not fail a row — it EMPTIES one, and an empty row skips with a reason that is not true:
  // `off-palette` reported "no solid fills found in the plot" on a chart full of them, the mark
  // inventory both annotation rows test against came back empty, and `isMap` never set means a map was
  // judged by the rules for a chart whose aspect we control. A skip with a false reason is exactly the
  // confident silence this script exists to remove.
  //
  // So: everything that is not the header, the footer, the logo or one of OUR annotations is plot
  // content. Annotations are excluded because they are ours rather than the chart's — they have their
  // own four rows, and counting their fills among the plot's invents off-palette entries.
  // Exact name first, then `<chartName>__<slug>`. Measured on the real file: a `static_viz` import
  // lands as `chart__agriculture-share`, so an exact-only match resolved NOTHING on those pages and
  // fell through to the ungrouped branch — which still walked the right node, but reported "the chart
  // group looks ungrouped" about a frame whose chart group is right there and correctly named.
  // A wrong explanation for a working result is how the next reader is sent to fix the wrong thing.
  // Three variants counted on the live file, so match all three rather than the one this default
  // was written for: `chart` (grapher import), `chart__<slug>` (a static_viz import), and
  // `chart-desktop` / `chart-mobile` (a two-format page). Exact first, then a `__` or `-` suffix.
  const chartSuffixed = (c) => c.name.startsWith(CONFIG.chartName + "__") || c.name.startsWith(CONFIG.chartName + "-");
  let chart = CONFIG.chartName
    ? frame.children.find((c) => c.name === CONFIG.chartName) || frame.children.find(chartSuffixed)
    : null;
  let plotRoots = chart
    ? [chart]
    : frame.children.filter((c) => c !== header && c !== footer && c !== logo && !/^annotation__/.test(c.name));
  // NOT a nested template literal: `inline_script.py`'s stripper is not nesting-aware, so a backtick
  // inside a ${} closes the outer context, desynchronizes, and silently stops stripping comments for
  // the rest of the file. Caught by --check jumping 75% -> 96% of cap on a 1.8KB edit.
  const chartSuffix = chart && chart.name !== CONFIG.chartName ? ' (matched "' + CONFIG.chartName + '__|-<suffix>")' : "";
  const chartResolvedBy = chart
    ? `name "${chart.name}"` + chartSuffix
    : plotRoots.length
      ? `${plotRoots.length} ungrouped frame child(ren) — the chart group looks ungrouped: ${plotRoots.map((c) => c.name).join(", ")}`
      : "NOT RESOLVED — no frame child left after the header, footer, logo and annotations";

  // --- one traversal collects everything the rows below read.
  // ONE walk over the whole frame, with `insidePlot` set by which top-level child we descended from.
  // Walking only [chart, header, footer] is the trap: Step 8's annotations are appended to the FRAME,
  // so they are none of those three — `annotations` then comes back empty on every real page and its
  // four rows report "no annotation__* nodes" forever, which reads as "nothing to check" rather than
  // "I never looked". Caught by planting an annotation and watching the rows stay silent.
  const texts = [], stroked = [], fills = [], leaves = [], annotations = [], vectors = [], markBoxes = [];
  // Grapher groups its axis furniture under stable container names. Identifying furniture as
  // "stroked and not a series line" instead is wrong the moment the chart is not a line chart: on a
  // map EVERY country vector is a stroked non-series plot node, so the prescribed 0.22px borders and
  // 0.3-0.35px highlight outlines (per-chart-type/maps.md) all report as rescale defects.
  // Zero lines are furniture and are NOT inside an axis or grid container: on a discrete bar the plot's
  // only furniture is a top-level `vertical-zero-line`, and on a slope a `horizontal-zero-line`. Left
  // out, those charts report "no furniture" and the 1px rule never runs on the one node it applies to.
  const FURNITURE_GROUPS = /^(horizontal|vertical)-(axis|grid-lines|zero-line)$|^grid|^axis|^ticks?$|zero-line$/i;
  // The series identity can sit on a GROUP rather than on the stroked node. On a SLOPE chart
  // `slope__<Entity>` and `outline__<Entity>` are groups of {start-point, end-point, line}, and the
  // only stroked node is called plain `line` — so matching names on the stroked node alone finds no
  // series at all and the weight row skips in silence. Same shape as `datapoints__<Entity>`: name on
  // the group, properties on the child. So carry the nearest naming ancestor down the walk.
  const SERIES_ANY = /^(line|slope|outline)__(.+)$/;
  // The CATEGORY a node belongs to, by grapher's `<kind>__<Entity>` naming. Broader than SERIES_ANY on
  // purpose: `datapoints__<Entity>`, `bar__<Entity>` and `country__<ISO>` all name a category and none
  // of them is a series line. Like `seriesOf` it has to travel DOWN the walk, because the name sits on
  // the group while the paint sits on its leaves — a `datapoints__<Entity>` marker is a filled leaf
  // called `Ellipse 12`, so reading the category off the painted node's own name loses it every time.
  const CATEGORY_ANY = /^[a-z][a-z-]*__(.+)$/i;
  // A map is detected structurally, by grapher's own container. It matters because a map is the one
  // chart type whose INK ASPECT IS FIXED: measured live, a map requested at a 1.4033 target came back
  // at 1.5428 (and at 1.7446 unsolved), because the projection sets the aspect and any extra canvas
  // height becomes letterbox. So a map cannot be aspect-solved into the band, and per-chart-type/
  // maps.md fits it WIDTH-first instead ("scale so it spans the content width") — which leaves gaps
  // far larger than the 12-16px band rule by construction.
  const MAP_GROUPS = /^(map|countries|countries-without-data)$/i;
  // A LEGEND is furniture that happens to be filled, and grapher draws it INSIDE the chart group and
  // OUTSIDE the map: measured on the file, a map page reads `chart > numeric-color-legend > {lines,
  // swatches, labels, swatch-hit-areas}` as a SIBLING of `map`. So every swatch is a filled non-text
  // plot leaf whose ancestors MAP_GROUPS does not match, and an ordinary map came back holding "both a
  // map and chart marks" — its own legend audited as a second, chart-side palette, against the wrong
  // set, with a recommendation to go and restyle the legend rather than the categories. The names are
  // read off grapher's own `makeFigmaId` calls, not guessed: `numeric-color-legend`,
  // `categorical-color-legend` and `vertical-color-legend`, each holding a `swatches` group.
  const LEGEND_GROUPS = /^(numeric|categorical|vertical)-color-legend$|^legend$|^legend[-_]/i;
  let isMap = false;
  const collect = (n, insidePlot, inFurniture, seriesOf, furnitureGroup, insideMap, catAncestor, nodeOpacity, insideLegend) => {
    if ("visible" in n && !n.visible) return;
    // NODE opacity dims every paint under it, and it ACCUMULATES down the tree — a group at 0.5 holding
    // a leaf at 0.5 renders the leaf at 0.25. Carried as the PRODUCT, not a boolean, because ZERO and
    // PARTIAL are two different answers and a boolean cannot tell them apart. Read as a boolean, a node
    // the reader cannot see at all came back as a held-back TRANSLUCENT mark: the audit named an
    // invisible category and told the operator to reset its opacity or judge it by eye — a verdict
    // about something that is not on the canvas. The product is also what sees a group at 0 holding an
    // opaque leaf, which no single-node test can.
    if ("opacity" in n && typeof n.opacity === "number") nodeOpacity *= n.opacity;
    // Effective opacity ZERO paints NO PIXELS. That is the same non-rendering state as `visible: false`
    // directly above and as a zero-opacity paint under `renders`, so it takes the same treatment both of
    // those take — out of every row entirely — rather than being grouped with genuinely visible partial
    // opacity. The test is exactly zero, and the multiplication is what reaches it: a factor of 0
    // anywhere on the climb zeroes the product exactly. It is NOT a floor. A near-zero node — 0.05
    // inside 0.05 — is faint, not absent, and dropping it here took it out of every row in the file,
    // including the ones that never look at colour: an 8px label at 0.005 left `text-floor` reporting
    // that all of its ranges cleared the floor. A subset audited as a whole is the defect this script
    // exists to catch, so faintness is reported through the translucent path instead, which names it.
    if (nodeOpacity <= 0) return;
    const dimmed = nodeOpacity < 0.999;
    // The furniture CONTAINER name is carried, not just the boolean: the dash target is decided by what
    // a node IS (a gridline vs a zero line vs a tick), and deciding it from the node's own current dash
    // instead makes a cleared gridline dash self-justifying. See the furniture-dash row.
    if (FURNITURE_GROUPS.test(n.name)) { inFurniture = true; furnitureGroup = n.name; }
    if (insidePlot && MAP_GROUPS.test(n.name)) { isMap = true; insideMap = true; }
    if (insidePlot && LEGEND_GROUPS.test(n.name)) insideLegend = true;
    const sm = SERIES_ANY.exec(n.name);
    if (sm) seriesOf = { kind: sm[1], series: sm[2] };
    const cm = CATEGORY_ANY.exec(n.name);
    if (cm) catAncestor = cm[1];
    if (n.type === "TEXT") {
      const tf = Array.isArray(n.fills) && renders(n.fills[0]) ? hexOf(n.fills[0]) : null;
      const tfDim = dimmed || translucent(Array.isArray(n.fills) ? n.fills[0] : null);
      // The WEIGHTS, not just whether they differ: a uniformly non-Regular node is style-unbindable
      // for the same API reason a mixed-weight one is, and named-styles has to know the difference
      // between "bold throughout" (prescribed) and "Regular and unbound" (a defect).
      let mixedWeight = false, weights = [];
      try {
        const fsegs = n.getStyledTextSegments(["fontName"]);
        mixedWeight = fsegs.length > 1;
        weights = [...new Set(fsegs.map((s) => s.fontName && s.fontName.style).filter(Boolean))];
      } catch (e) { mixedWeight = false; weights = []; }
      const ranges = sizeRanges(n);
      // A node whose size cannot be read at all is DECLARED, never dropped on the floor.
      if (!ranges.length) unreadableText.push(n.name || n.type);
      for (const sr of ranges) {
        texts.push({ node: n, name: n.name, chars: (n.characters || "").slice(0, 30), size: sr.size,
                     sizeRange: sr.range, mixedSize: ranges.length > 1,
                     styleId: n.textStyleId || "", box: rel(n), insidePlot, fill: tf, translucent: tfDim, mixedWeight, weights });
      }
    }
    if (/^annotation__/.test(n.name)) annotations.push({ node: n, name: n.name, box: rel(n), type: n.type, opacity: nodeOpacity });
    if ("strokeWeight" in n && typeof n.strokeWeight === "number" && n.strokes && n.strokes.length) {
      // The stroke's COLOUR travels with it, picked the same way the knockout row picks one: the first
      // paint that actually RENDERS, not `strokes[0]`, which can be a switched-off or transparent decoy.
      // A line chart's series colour lives here and nowhere else — it is not a fill — so without this
      // the colour rows can only ever see a chart's fills.
      const strokePaint = n.strokes.find(renders);
      stroked.push({ node: n, name: n.name, type: n.type, w: n.strokeWeight,
                     dash: "dashPattern" in n && n.dashPattern ? [...n.dashPattern] : [],
                     align: n.strokeAlign, insidePlot, inFurniture, furnitureGroup: furnitureGroup || null, insideMap: !!insideMap,
                     hex: hexOf(strokePaint), translucent: dimmed || translucent(strokePaint),
                     seriesKind: seriesOf ? seriesOf.kind : null, seriesName: seriesOf ? seriesOf.series : null });
    }
    // Zero-area nodes are EXCLUDED from the fill inventory and KEPT for the stroke rows (CHECKS.md).
    // A grapher import's tick vectors are zero-width and carry a default black fill that paints no
    // pixels, so counting them reports phantom #000000 and sends a reviewer to bind a mark that is not
    // there. Their strokes are real, which is why the exclusion is per-row rather than per-node.
    const areaBox = rel(n);
    const hasArea = areaBox && areaBox.w > 0 && areaBox.h > 0;
    if (hasArea && "fills" in n && Array.isArray(n.fills)) {
      for (const f of n.fills) {
        if (renders(f)) fills.push({ name: n.name, type: n.type, hex: hexOf(f), styleId: n.fillStyleId || "", insidePlot });
      }
    }
    // Marker groups and value labels. The NAME is on the group and the GEOMETRY is on its children, and
    // both halves matter. `collect` returns after descending, so a GROUP is never a leaf — and grapher's
    // stable marker name sits on the `datapoints__<Entity>` GROUP while its descendants are unnamed, so
    // filtering leaves by name matches nothing and the dot rule never fires. But the group's own bbox is
    // the whole series (measured: 210x114 and 401x290 on a two-line chart), so testing against it would
    // flag every annotation placed anywhere over the plot. So take the name from the group and a box per
    // marker from its leaf descendants.
    // Filled DATA MARKS — a bar segment, a stacked band, a map shape. CHECKS.md forbids an annotation
    // covering "a bar segment carrying a number", and a strokeless filled rect appears in neither the
    // stroke inventory nor the dot/value one, so on a segmented bar an annotation could cover a coloured
    // segment while missing its `value__*` text and the row still returned ok. Anything inside the plot
    // that carries a visible solid fill, is not furniture, and is not text is a mark the reader sees.
    if (insidePlot && !inFurniture && n.type !== "TEXT" && !(("children" in n) && n.children.length)) {
      const mb0 = rel(n);
      // The mark's own COLOUR travels with its box. Without it, a label sitting on a mark can only ever be
      // measured against the frame, which is not what is behind it (see label-contrast-on-background).
      const markPaint = Array.isArray(n.fills) ? n.fills.find(renders) : null;
      const filled = !!markPaint;
      // `fromMap` is carried because a MAP SHAPE's bbox is not its ink: per-chart-type/maps.md, a
      // country split across the antimeridian has a box spanning almost the whole map, so an annotation
      // over open ocean falls inside it. Counting those as covered marks reports a FAIL that is not
      // there, so the annotation row drops them and says so rather than judging them by bbox.
      // A legend swatch KEEPS its box — an annotation dropped over the legend covers something the
      // reader needs — but it is named for what it is, because "covers a filled data mark" sends the
      // reader looking for a bar that is not there. The palette below drops it on the same grounds.
      if (filled && mb0 && mb0.w > 0 && mb0.h > 0) markBoxes.push({ name: n.name, box: mb0,
                                      why: insideLegend ? "a legend swatch" : "a filled data mark",
                                      insidePlot, fromMap: !!insideMap, fromLegend: !!insideLegend, hex: hexOf(markPaint),
                                      translucent: dimmed || translucent(markPaint), cat: catAncestor || null });
    }
    if (/^datapoints__|^dot__|^value__/.test(n.name)) {
      const why = /^value__/.test(n.name) ? "a value label" : "a dot";
      const pushLeafBoxes = (m) => {
        if ("visible" in m && !m.visible) return;
        if ("children" in m && m.children.length) { m.children.forEach(pushLeafBoxes); return; }
        const b = rel(m);
        if (b && b.w > 0 && b.h > 0) markBoxes.push({ name: n.name, box: b, why, insidePlot, fromMap: false, fromLegend: !!insideLegend });
      };
      pushLeafBoxes(n);
    }
    // The series identity must travel WITH the vector. On a slope chart the stroked vector is called
    // plain `line` and the identity is on its `slope__<Entity>` / `outline__<Entity>` group (the shape
    // the fixture models), so a bare node here loses it and the name-only filter below matches nothing —
    // every slope segment silently absent from `polylines`, and annotation-overlap unable to fail.
    if (n.type === "VECTOR" && insidePlot) vectors.push({ node: n, seriesOf });
    if ("children" in n && n.children.length) { n.children.forEach((c) => collect(c, insidePlot, inFurniture, seriesOf, furnitureGroup, insideMap, catAncestor, nodeOpacity, insideLegend)); return; }
    const b = rel(n);
    if (b && b.w > 0 && b.h > 0) leaves.push({ name: n.name, type: n.type, box: b, insidePlot, fromMap: !!insideMap });
  };
  for (const child of frame.children) {
    if (child === logo) continue;
    collect(child, plotRoots.indexOf(child) !== -1, false, null, null, false, null, frameOpacity, false);
  }

  // ---------------------------------------------------------------- rows
  // Text floor (CHECKS.md: nothing below 12px)
// #region type
  {
    const under = texts.filter((t) => t.size < TEXT_FLOOR - 0.01);
    // A node whose sizes could not be READ is not a pass. `sizeRanges` swallows a throwing or
    // non-numeric segment read and returns [], so the node contributes no ranges at all — and the
    // status was decided by `under.length` alone, which is 0 when the only suspect node is the one
    // nobody could measure. The frame then certified with an uninspected range on it. It is recorded
    // in the detail, so REVIEW rather than FAIL: some text was judged, some could not be.
    add("text-floor", under.length ? "FAIL" : unreadableText.length ? "REVIEW" : "ok",
        (under.length ? `${under.length} text range(s) below ${TEXT_FLOOR}px: ` + under.map((t) => `"${t.chars}"${t.sizeRange ? ` chars ${t.sizeRange}` : ""} ${r(t.size)}px`).join(", ")
                      : `all ${texts.length} text range(s) at or above ${TEXT_FLOOR}px`) +
        ` (floor ${TEXT_FLOOR}px, ${CONFIG.textFloor != null ? "from CONFIG" : isSmall ? "302-wide format — SMALL-CHARTS.md overrides 12 to 11" : "540/850-wide format"})` +
        (texts.some((t) => t.mixedSize) ? `. ${texts.filter((t) => t.mixedSize).length} range(s) come from MIXED-size node(s), read per range rather than per node` : "") +
        (unreadableText.length ? `. ${unreadableText.length} text node(s) NOT judged — no readable fontSize on the node or its ranges: ${unreadableText.slice(0, 4).join(", ")}` : ""));
  }

  // Annotation ladder + ceiling. Only annotation__* nodes are ours; an imported chart's label sizes
  // come from the export and are governed by the floor row above, not by the ladder.
  {
    const ann = texts.filter((t) => /^annotation__/.test(t.name));
    if (!ann.length) skip("annotation-ladder", "no annotation__* text nodes on this frame");
    else {
      // The ceiling is L 15, and XL 16 is legal only when the annotation IS the message — a deliberate
      // choice, so it is declared via CONFIG rather than inferred from the number being present.
      const ceiling = CONFIG.xlAnnotations ? 16 : LADDER_CEILING;
      const off = ann.filter((t) => !LADDER.some((L) => Math.abs(t.size - L) < 0.01));
      const over = ann.filter((t) => t.size > ceiling + 0.01);
      const bad = [...off.map((t) => `"${t.chars}" ${r(t.size)}px off-ladder`),
                   ...over.map((t) => `"${t.chars}" ${r(t.size)}px above the ${CONFIG.xlAnnotations ? "XL 16" : "L " + LADDER_CEILING} ceiling` + (CONFIG.xlAnnotations ? "" : " — set CONFIG.xlAnnotations if this annotation IS the message"))];
      add("annotation-ladder", bad.length ? "FAIL" : "ok",
          bad.length ? bad.join(", ") : `all ${ann.length} annotation(s) on the ladder and at or below ${ceiling}` + (CONFIG.xlAnnotations ? " (XL declared)" : ""));
    }
  }

  // Sizes are named styles — CHECKS.md's bar is "no arbitrary sizes left over from scaling the export
  // (13.7, 16.8)", which is about the NUMBER, not about carrying a Figma style id. An SVG import can
  // never carry an id, so judging only bound nodes lets exactly the defect this row exists for through:
  // a fitted chart's labels land wherever the rescale puts them (13.36 measured on a live run) and the
  // row still reported ok. So check the numeric size of every plot and annotation text against the
  // ladder, and keep the style-id question as its own row below.
  {
    const subject = texts.filter((t) => t.insidePlot || /^annotation__/.test(t.name));
    if (!subject.length) skip("ladder-sizes", "no plot or annotation text to size");
    else {
      const off = subject.filter((t) => !LADDER.some((L) => Math.abs(t.size - L) < 0.01));
      const distinct = [...new Set(off.map((t) => r(t.size)))].sort((a, b) => a - b);
      // Split the verdict by WHO set the size, because only one half is actionable.
      //
      // An annotation is a node we author, so it must sit exactly on a rung: FAIL.
      //
      // An imported chart's labels are sized by grapher and then by the fit factor, and they cannot
      // land on a rung except by chance. Snapping them is not free either: text metrics are part of
      // what sets the group's width, so snapping moves the box off the content edge, and re-fitting to
      // the edge moves the sizes back off the ladder. Exact ladder, exact 508 box and a fitted import
      // are mutually exclusive unless the width is closed by stretching GEOMETRY while MOVING text
      // (reference/FITTING.md) rather than by scaling the group. Judged strictly, this row therefore
      // failed on 8 of 8 real frames — a row that fails on every chart carries no information and
      // trains a reader to skip it. So imported text is REVIEW with the distance to the nearest rung,
      // and only a distance too large to be fit drift is a FAIL: at that point the text is not a
      // rescaled rung but the wrong size (a scatter's bubble legend measured 5.99px against a 12px
      // floor, 6.01 from its nearest rung).
      const isAnn = (x) => /^annotation__/.test(x.name);
      const annOff = off.filter(isAnn);
      const impOff = off.filter((x) => !isAnn(x));
      const nearest = (v) => LADDER.reduce((a, b) => (Math.abs(b - v) < Math.abs(a - v) ? b : a));
      const drift = (x) => Math.abs(x.size - nearest(x.size));
      const FIT_DRIFT = 0.75;
      const wayOff = impOff.filter((x) => drift(x) > FIT_DRIFT);
      const maxDrift = impOff.length ? Math.max(...impOff.map(drift)) : 0;
      const status = annOff.length || wayOff.length ? "FAIL" : impOff.length ? "REVIEW" : "ok";
      add("ladder-sizes", status,
          !off.length
            ? `all ${subject.length} plot/annotation text node(s) on the ${LADDER.join("/")} ladder`
            : [annOff.length ? `${annOff.length} ANNOTATION(s) off the ${LADDER.join("/")} ladder: ` + [...new Set(annOff.map((x) => r(x.size)))].join(", ") + "px — an annotation is authored here, so set it to a rung" : "",
               wayOff.length ? `${wayOff.length} imported text node(s) further than ${FIT_DRIFT}px from any rung (max ${r(maxDrift)}px): ` + [...new Set(wayOff.map((x) => `"${x.chars}" ${r(x.size)}px`))].slice(0, 5).join(", ") + " — too far to be fit drift, so these are the wrong size rather than a rescaled rung" : "",
               impOff.length && !wayOff.length ? `${impOff.length} of ${subject.length} imported text node(s) off the ladder but all within ${FIT_DRIFT}px of a rung (max ${r(maxDrift)}px): ${distinct.join(", ")}px. Expected for a fitted export — snapping them to rungs moves the group off the content edge, so it is a designer's call, not a defect` : ""].filter(Boolean).join(". "),
          { offLadderSizes: distinct, maxDriftFromRung: r(maxDrift), annotationsOffLadder: annOff.length });
    }
  }

  // Style BINDING, separately from the numbers above. An SVG import cannot carry a style id, so this
  // row judges OUR nodes only and reports the imported ones as context.
  {
    const ann = texts.filter((t) => /^annotation__/.test(t.name));
    // TWO exemptions, both the same API limitation, and both prescribed rather than tolerated.
    //
    // 1. An annotation that bolds its key phrase — the house convention — is MIXED-WEIGHT, and Figma
    //    drops the node-level textStyleId when it is (reference/GOTCHAS.md).
    // 2. An annotation that is bold THROUGHOUT is equally unbindable, because the whole ladder is Lato
    //    Regular: applying the style would strip the bold, so GUIDELINES.md → Named styles tells you
    //    outright to "set fontSize to the ladder value and leave the weight alone rather than binding
    //    the style and losing the bold". Confirmed against the design team's own finished highlight map
    //    (Chart Library `273:320`), whose nine country labels ship 12px Lato Bold with an EMPTY
    //    textStyleId. Before this exemption existed the row fired on nine correctly-built labels.
    //
    // Both are judged on their ladder size instead, which ladder-sizes above already enforces. What is
    // still a real defect: Regular text that is unbound — there the binding was simply not applied.
    //
    // The exemption is keyed on the weight being BOLD, not merely on it being "not Regular". Written the
    // loose way it swallowed every other single-weight face too — an annotation left in Lato Light,
    // Medium or Italic is not the prescribed exception, nothing in GUIDELINES.md licenses it, and it was
    // being waved through AND reported back as "wholly-bold", which is a false statement about the page.
    const BOLD_WEIGHT = /bold|black/i;   // Bold, Semibold, Extrabold, Black, and their italics
    const boldThroughout = (t) => t.weights && t.weights.length === 1 && BOLD_WEIGHT.test(t.weights[0]);
    const unbound = ann.filter((t) => !t.styleId && !t.mixedWeight && !boldThroughout(t));
    const mixed = ann.filter((t) => t.mixedWeight);
    const bold = ann.filter((t) => !t.mixedWeight && boldThroughout(t));
    const importedRaw = texts.filter((t) => t.insidePlot && !/^annotation__/.test(t.name) && !t.styleId).length;
    // An unbound node is reported in the weight it actually carries. Calling a Lato Light node "REGULAR"
    // sends the reader looking for a missing binding on text whose weight is the real problem.
    const weightOf = (t) => (t.weights && t.weights.length === 1 ? t.weights[0] : "mixed");
    const named = (t) => `"${t.chars}"`;
    const unboundRegular = unbound.filter((t) => !t.weights || !t.weights.length || weightOf(t) === "Regular");
    const unboundOther = unbound.filter((t) => unboundRegular.indexOf(t) === -1);
    if (!ann.length) skip("named-styles", "no annotation__* text nodes; an imported chart's text cannot carry a style id");
    else add("named-styles", unbound.length ? "FAIL" : "ok",
             (unbound.length
                ? [unboundRegular.length ? `${unboundRegular.length} REGULAR annotation(s) with no textStyleId — setting fontSize looks like the ladder and is not it: ` + unboundRegular.map(named).join(", ") : "",
                   unboundOther.length ? `${unboundOther.length} annotation(s) unbound in a weight the ladder does not prescribe (${[...new Set(unboundOther.map(weightOf))].join(", ")}) — the exemption covers BOLD only, so either bind the style or make the emphasis bold on purpose: ` + unboundOther.map(named).join(", ") : ""].filter(Boolean).join(". ")
                : `all ${ann.length - mixed.length - bold.length} regular-weight annotation(s) bound to a text style`) +
             (mixed.length ? ` ${mixed.length} mixed-weight annotation(s) exempted — Figma drops the node-level style id when a phrase is bolded, which is the prescribed recipe (GOTCHAS.md).` : "") +
             (bold.length ? ` ${bold.length} wholly-bold annotation(s) exempted — the ladder is all Lato Regular, so binding a style would strip the bold; GUIDELINES.md prescribes size-without-binding here and the finished pages ship it (weights seen: ${[...new Set(bold.flatMap((t) => t.weights))].join(", ")}).` : "") +
             ((mixed.length || bold.length) ? " Their sizes are covered by ladder-sizes." : "") +
             ` ${importedRaw} imported chart text node(s) are raw, which is expected.`);
  }

  // Text hierarchy: nothing may exceed the subtitle (CHECKS.md row 26).
  {
    // Structurally: the header's second TEXT child. Picking index 1 out of a list of collected texts
    // sorted by y is fragile — anything that lands two nodes at the same top, or collects a node
    // twice, silently promotes the TITLE into the subtitle's place, and a 25px bar passes everything.
    // Same mixed-size trap as the collector: filtering the header's children on a NUMERIC fontSize
    // skipped a subtitle that carries a per-range size, which then shifted the wrong node into index 1.
    // The ceiling takes the LARGEST range of the subtitle, since nothing in the plot may exceed it.
    const headerTexts = header ? header.children.filter((c) => c.type === "TEXT") : [];
    const subtitleSizes = headerTexts.length > 1 ? sizeRanges(headerTexts[1]) : [];
    const subtitle = subtitleSizes.length
      ? { size: Math.max(...subtitleSizes.map((s) => s.size)), chars: (headerTexts[1].characters || "").slice(0, 24) }
      : null;
    if (!subtitle) skip("text-hierarchy", "could not resolve the subtitle (header has fewer than two TEXT children)");
    else {
      const over = texts.filter((t) => (t.insidePlot || /^annotation__/.test(t.name)) && t.size > subtitle.size + 0.01);
      add("text-hierarchy", over.length ? "FAIL" : "ok",
          (over.length ? `${over.length} in-plot text node(s) exceed the subtitle's ${r(subtitle.size)}px: ` + over.map((t) => `"${t.chars}" ${r(t.size)}px`).join(", ")
                      : `nothing in the plot exceeds the subtitle's ${r(subtitle.size)}px`) +
          ". CEILING ONLY — the rest of CHECKS.md's hierarchy (annotations outranking supporting text and labels, same-rank items sharing a size) needs a rank per node, which nothing here supplies, so an inverted lower order is NOT covered. See text-hierarchy-ranks.",
          { distinctPlotSizes: [...new Set(texts.filter((t) => t.insidePlot).map((t) => r(t.size)))].sort((a, b) => a - b) });
    }
  }

  // The footer's source line: bold on the `Data source:` prefix ONLY.
  //
  // Added after a run shipped `Data source: V-Dem (2026)` bold THROUGHOUT and every other row passed
  // it. The cause is generic enough to be worth stating: assigning `characters` collapses a text node
  // to its FIRST run's style, and the template's first run is the bold prefix — so filling the slot
  // silently bolds the producer name. It renders as a slightly heavy line that reads as fine.
  // Nothing else here inspects weight outside `annotation__*`, which is why it went unnoticed.
  {
    // findAll, not findOne: the static templates nest the source inside a row frame, and findOne is
    // surface the test harness's figma stub does not implement.
    const src = footer ? footer.findAll((c) => c.type === "TEXT" && /^\s*Data source:/.test(c.characters || ""))[0] : null;
    if (!footer) skip("source-line-weight", "no footer resolved on this frame");
    else if (!src) skip("source-line-weight", "no footer TEXT starting `Data source:` — a template whose footer names its source differently is not judged here");
    else {
      const PREFIX = "Data source:";
      let segs = [];
      try { segs = src.getStyledTextSegments(["fontName"]); } catch (e) { segs = []; }
      if (!segs.length) skip("source-line-weight", "footer source line has no readable font segments");
      else {
        // Two different questions, so two different regexes. BOLDISH classifies the TAIL: the
        // collapse copies whatever the prefix is, so anything bold-family there is the symptom.
        // The PREFIX itself is judged exactly, like the tail's Regular — TEXTS.md prescribes
        // `LATO("Bold")` unconditionally, so Semibold, ExtraBold, Black or Bold Italic there is
        // off-contract, and a substring test certifies all four as ok.
        const BOLDISH = /bold|black/i;
        const BOLD = /^bold$/i;
        // The tail is judged against Regular, not merely against "not bold". TEXTS.md states the target
        // twice and unconditionally — `"Data source:" Bold unbound` + `" <citation>" Regular BOUND` — so
        // a Medium, Light or Italic producer name is off-contract too, and a not-bold test certifies it.
        const REGULAR = /^regular$/i;
        const weightAt = (i) => { const s = segs.find((x) => i >= x.start && i < x.end); return s && s.fontName ? s.fontName.style : null; };
        const prefixWeights = [...new Set([...Array(Math.min(PREFIX.length, src.characters.length)).keys()].map(weightAt).filter(Boolean))];
        const restWeights = [...new Set([...Array(Math.max(0, src.characters.length - PREFIX.length)).keys()]
          .map((k) => weightAt(k + PREFIX.length)).filter(Boolean))];
        const offTail = restWeights.filter((w) => !REGULAR.test(w));
        const bad = [];
        if (!prefixWeights.length || !prefixWeights.every((w) => BOLD.test(w))) bad.push(`"${PREFIX}" is ${prefixWeights.join("/") || "unreadable"} (want Bold)`);
        if (offTail.some((w) => BOLDISH.test(w))) bad.push(`the producer name is ${restWeights.join("/")} — setting \`characters\` collapses the node to its FIRST run's style, which here is the bold prefix (GOTCHAS.md). Re-assert Regular across the whole string, then bold the prefix`);
        else if (offTail.length) bad.push(`the producer name is ${restWeights.join("/")}, and TEXTS.md prescribes Regular — re-assert it across the tail. If a template genuinely ships ${offTail.join("/")} there, measure it and record it in TEXTS.md rather than loosening this row`);
        add("source-line-weight", bad.length ? "FAIL" : "ok",
            bad.length ? bad.join("; ") : `"${PREFIX}" bold, producer name ${restWeights.join("/") || "empty"}`,
            { prefixWeights, restWeights });
      }
    }
  }

  // Mark weight — the series lines and their halos.
  //
  // Two things vary by chart type and both were wrong when this only knew line charts. The series line
  // is `line__X` on a line/stacked chart and `slope__X` on a slope chart, so a slope's series were
  // never checked at all — the row reported "no line__*" and skipped in silence. And `outline__X` is a
  // halo ONLY when a series line of the same name exists: on a SCATTER, `outline__<Entity>` is the ring
  // around a point, which measured 3.5-4.1px here and was being judged against a line's halo bar.
  // So a halo is paired or it is not a halo.
// #endregion
// #region series
  {
    const lineNames = new Set();
    for (const s of stroked) if (s.seriesKind && s.seriesKind !== "outline") lineNames.add(s.seriesName);
    const unpairedOutlines = stroked.filter((s) => s.seriesKind === "outline" && !lineNames.has(s.seriesName));
    const series = stroked.filter((s) => s.seriesKind && (s.seriesKind !== "outline" || lineNames.has(s.seriesName)));
    if (!series.length) skip("series-weight", `no series stroke found — looked for line__*/slope__* names on the stroked node OR on an ancestor group, plus their paired outline__*` +
        (unpairedOutlines.length ? `. ${unpairedOutlines.length} outline__* node(s) have no paired series line, so they are point rings or shape outlines rather than halos and are not judged here (a scatter's point rings run 3.5-4.1px by design)` : ""));
    else if (CONFIG.highlightTreatment) {
      // A shared set of allowed numbers is the wrong shape: it rejects a valid 1px context line whose
      // halo is the required 2px, and accepts nonsense like a 4px `line__` beside a 1px `outline__`.
      // The bar is a RELATIONSHIP — context 1, protagonist 3, halo 2x (or line+1 where nothing crosses).
      const lineW = {};
      for (const s of series) if (s.seriesKind !== "outline") lineW[s.seriesName] = s.w;
      const bad = [];
      for (const s of series) {
        if (!s.seriesKind) continue;
        if (s.seriesKind !== "outline") {
          if (!(Math.abs(s.w - 1) < 0.05 || Math.abs(s.w - HOUSE_LINE) < 0.05)) bad.push(`${s.name} ${r(s.w)} — a series line is 1 (muted context) or ${HOUSE_LINE} (protagonist)`);
        } else {
          const lw = lineW[s.seriesName];
          if (lw === undefined) { bad.push(`${s.name} has no paired series line for ${s.seriesName} to size its halo against`); continue; }
          const ok = Math.abs(s.w - lw * 2) < 0.05 || Math.abs(s.w - (lw + 1)) < 0.05;
          if (!ok) bad.push(`${s.name} ${r(s.w)} against a ${r(lw)}px line — a halo is 2x (${r(lw * 2)}) or line+1 (${r(lw + 1)})`);
        }
      }
      add("series-weight", bad.length ? "FAIL" : "ok",
          bad.length ? bad.join("; ") : `all ${series.length} series stroke(s) hold the highlight relationship (line 1 or ${HOUSE_LINE}; halo 2x or line+1)`);
    } else {
      const bad = series.filter((s) => Math.abs(s.w - (s.seriesKind === "outline" ? HOUSE_HALO : HOUSE_LINE)) >= 0.05);
      add("series-weight", bad.length ? "FAIL" : "ok",
          (bad.length ? `off the house ${HOUSE_LINE}/${HOUSE_HALO}: ` + bad.map((s) => `${s.seriesKind}__${s.seriesName} ${r(s.w)} -> ${s.seriesKind === "outline" ? HOUSE_HALO : HOUSE_LINE}`).join(", ") + ". rescale() multiplies stroke weight — set these AFTER the last scale."
                      : `all ${series.length} series stroke(s) at the house ${HOUSE_LINE}/${HOUSE_HALO}`) +
          (unpairedOutlines.length ? ` (${unpairedOutlines.length} unpaired outline__* node(s) excluded — point rings, not halos)` : ""));
    }
  }

  // Furniture weight and dash — the two rows that are easiest to miss because you never set them.
  {
    const furn = stroked.filter((s) => s.insidePlot && s.inFurniture && !s.seriesKind);
    if (!furn.length) skip("furniture-weight", "no stroked node sits under an axis/gridline group (" + FURNITURE_GROUPS + ") — the 1px rule covers gridlines, zero lines and tick marks only, so it is not applied to whatever else this chart draws (a map's country borders run 0.22px by design)");
    else {
      const bad = furn.filter((s) => Math.abs(s.w - FURNITURE_W) >= 0.05);
      add("furniture-weight", bad.length ? "FAIL" : "ok",
          bad.length ? `${bad.length} of ${furn.length} furniture stroke(s) off ${FURNITURE_W}px (rescale() thinned them): ` + [...new Set(bad.map((s) => r(s.w)))].join(", ") + `px seen`
                     : `all ${furn.length} furniture stroke(s) at ${FURNITURE_W}px`);
      // Dash target is PER NODE TYPE (CHECKS.md): the gridlines are [4,4], while the zero line and the
      // tick marks are solid. Which target applies must be derived from what the node IS — its name or
      // its furniture container — and NOT from the dash it currently carries. Classifying by the current
      // pattern is circular: a gridline whose dashPattern was cleared falls into the "solid" bucket, is
      // never compared to anything, and the row returns ok on the exact defect it exists to catch.
      // Zero lines and ticks are reported, not forced: a slope chart's native zero line ships [3,2] and
      // must NOT be pulled to the gridline target (per-chart-type/slope-charts.md).
      const SOLID_BY_DESIGN = /zero-line|zero|tick|axis/i;
      const isSolidByDesign = (s) => SOLID_BY_DESIGN.test(s.name) || SOLID_BY_DESIGN.test(s.furnitureGroup || "");
      const isGridByName = (s) => !isSolidByDesign(s) && (/grid/i.test(s.name) || /grid/i.test(s.furnitureGroup || ""));
      // grapher names each gridline after its TICK VALUE, so the zero line arrives as "0", "0%" or
      // "0-years" and matches none of the words above. It is solid by design, and judging it against the
      // [4,4] target reported a cleared dash on five of eight real frames — a false positive on every
      // chart that has a zero line. Reclassify by IDENTITY (does the name denote zero?), never by the
      // dash the node currently carries, which is the circularity the comment above warns about. And
      // reclassify rather than exempt: these then go through the solid-by-design validation below, so a
      // zero line that really was restyled to [4,4] still fails.
      const ZERO_TICK = (name) => {
        const m = /^(-?\d+(?:[.,]\d+)?)\s*[a-z%\-_ ]*$/i.exec(String(name).trim());
        return !!m && Math.abs(parseFloat(m[1].replace(",", "."))) < 1e-9;
      };
      // A furniture group with fewer than three members is not necessarily a grid — it can be the pair
      // of axis lines a slope chart draws at its two ends (named "1980"/"2023"), which are solid and
      // which, judged as gridlines, reported 2 of 2 "cleared".
      // But the COUNT is not that shape, and count alone cut both ways on a legitimate two-line
      // HORIZONTAL grid: correctly dashed it FAILED ("2 zero-line/tick/axis node(s) should be solid but
      // are dashed"), and with its dash CLEARED it passed — the exact defect this row exists to catch,
      // hidden by the exemption. So require the shape as well as the count: a slope's end axes are
      // VERTICAL, a y-grid's lines are horizontal, and only a small group of verticals is reclassified.
      // Residual, declared rather than hidden: a chart whose x-grid holds exactly two vertical lines has
      // the slope's shape and travels with it. `axisOnlyGroups` names everything that was moved.
      // Group by the actual PARENT, not by `furnitureGroup`. A gridline is itself named "grid-N", which
      // matches FURNITURE_GROUPS, so collect() overwrites furnitureGroup with the node's own name and
      // every gridline reads as a group of one — which made this rule reclassify a whole grid as axis
      // lines. The parent is the container the siblings actually share.
      const AXIS_GROUP_MIN = 3;
      const isVertical = (s) => { const b = rel(s.node); return !!b && b.h > b.w; };
      const byGroup = new Map();
      for (const s of furn.filter(isGridByName)) {
        const parent = s.node.parent;
        const k = parent ? parent.id : "(ungrouped)";
        if (!byGroup.has(k)) byGroup.set(k, { name: parent ? parent.name : "(ungrouped)", members: [] });
        byGroup.get(k).members.push(s);
      }
      const axisOnly = [], gridCandidates = [], axisOnlySet = new Set();
      for (const g of byGroup.values()) {
        if (g.members.length < AXIS_GROUP_MIN && g.members.every(isVertical)) {
          axisOnly.push({ group: g.name, names: g.members.map((s) => s.name) });
          g.members.forEach((s) => axisOnlySet.add(s));
        } else gridCandidates.push(...g.members);
      }
      const zeroTicks = gridCandidates.filter((s) => ZERO_TICK(s.name));
      const grids = gridCandidates.filter((s) => !ZERO_TICK(s.name));
      const native = furn.filter((s) => !isGridByName(s) || axisOnlySet.has(s) || ZERO_TICK(s.name));
      const matches = (d, t) => d.length === t.length && d.every((v, i) => Math.abs(v - t[i]) < 0.05);
      const badDash = grids.filter((s) => !matches(s.dash, FURNITURE_DASH));
      const cleared = badDash.filter((s) => !s.dash.length);
      // The zero-line/tick/axis bucket is VALIDATED too, not merely reported. Reporting it was the
      // mirror image of the cleared-gridline defect: a tick restyled to the gridline's [4,4] counted as
      // "native" and passed. Allowed is empty (CHECKS.md) — and the [3,2] exception is a slope chart's
      // native ZERO LINE only, so it is gated on that identity rather than granted to the whole bucket:
      // as a blanket allowance it also accepted an ordinary tick or axis line dashed [3,2], which
      // CHECKS.md does not permit anywhere. Anything else on furniture meant to be solid is the restyle
      // this row exists to catch.
      // And it is gated on the CHART TYPE as well as the node, because "a slope chart's native zero
      // line" is two conditions and identity alone is one of them: a zero line on a line or bar chart
      // dashed [3,2] is a restyle, and it passed. The slope is detected the way the series-weight row
      // detects one — a `slope__<Entity>` naming ancestor carried onto a stroked node — never from the
      // dash, which would be the same circularity again.
      const isZeroLine = (s) => /zero/i.test(s.name) || /zero/i.test(s.furnitureGroup || "") || ZERO_TICK(s.name);
      const isSlopeChart = stroked.some((s) => s.seriesKind === "slope");
      const badNative = native.filter((s) => !matches(s.dash, []) && !(isSlopeChart && isZeroLine(s) && matches(s.dash, [3, 2])));
      const badTotal = badDash.length + badNative.length;
      add("furniture-dash", badTotal ? "FAIL" : "ok",
          badTotal
            ? [badDash.length
                 ? `${badDash.length} of ${grids.length} gridline(s) off [${FURNITURE_DASH}]: ` +
                   [...new Set(badDash.map((s) => JSON.stringify(s.dash.map(r))))].join(", ") +
                   (cleared.length ? ` — ${cleared.length} carry NO dash at all (${cleared.map((s) => s.name).join(", ")}), and a cleared pattern is a restyled grid, not a solid node by design` : "")
                 : "",
               badNative.length
                 ? `${badNative.length} zero-line/tick/axis node(s) should be solid but are dashed: ` +
                   badNative.map((s) => `${s.name} ${JSON.stringify(s.dash.map(r))}`).join(", ") +
                   `. CHECKS.md keeps these at an EMPTY pattern; the [3,2] exception is a SLOPE chart's native ZERO line only` +
                   (isSlopeChart ? ` (a slope IS detected here, so a zero line at [3,2] would be exempt — a tick or axis line still is not)`
                                 : ` and no slope__* series was found on this frame, so nothing here is exempt`)
                 : ""].filter(Boolean).join(". ")
            : `all ${grids.length} gridline(s) at [${FURNITURE_DASH}]; all ${native.length} zero-line/tick/axis node(s) solid or at the slope's native [3,2] (` +
              (native.length ? [...new Set(native.map((s) => (s.dash.length ? JSON.stringify(s.dash.map(r)) : "solid")))].join(", ") : "none") + ")",
          { reclassifiedAsSolidByDesign: {
              zeroTicksByName: zeroTicks.map((s) => s.name),
              axisOnlyGroups: axisOnly } });
    }
  }

  // Box alignment — the chart's edges against the header box, to the pixel.
// #endregion
// #region geometry
  {
    const boxes = plotRoots.map(rel).filter(Boolean);
    if (isSmall) skip("box-alignment", "302-wide format: the header HUGS its own text (206-278 against a 278 content box), so the chart's width is not meant to match it — SMALL-CHARTS.md");
    else if (!boxes.length || contentL === null) skip("box-alignment", "chart or header box not resolved");
    else {
      const l = Math.min(...boxes.map((b) => b.l)), rr = Math.max(...boxes.map((b) => b.rr));
      const dl = l - contentL, dr = rr - contentR;
      const bad = Math.abs(dl) > 1 || Math.abs(dr) > 1;
      add("box-alignment", bad ? "FAIL" : "ok",
          `chart ${r(l)}..${r(rr)} against the header's ${r(contentL)}..${r(contentR)} (left ${r(dl) >= 0 ? "+" : ""}${r(dl)}, right ${r(dr) >= 0 ? "+" : ""}${r(dr)})` +
          (isMap ? " — on a map this is the BINDING axis, not a cross-check: the width is what the fit sets and the gap row is skipped" : ""));
    }
  }

  // Gap — top and bottom, against the band of the template actually filled.
  {
    const boxes = plotRoots.map(rel).filter(Boolean);
    const target = CONFIG.gapTarget || (fb && Math.round(fb.width) === 560 ? [30, 30] : [12, 16]);
    if (isMap) skip("gap", "map: its ink aspect is fixed by the projection (measured 1.74 unsolved, 1.54 even when solved for 1.40), so it cannot be aspect-solved into the band and maps.md fits it WIDTH-first — which leaves gaps well outside 12-16 by construction. Centre it in the band and check the width instead, which box-alignment does");
    else if (isSmall) skip("gap", "302-wide format: free frame height and no fit step, so the 12-16px band rule does not apply as written — SMALL-CHARTS.md");
    else if (CONFIG.tightlyMeasured) skip("gap", "tightlyMeasured: CHECKS.md's band figure does not apply to a trimmed, hugged group — match the reference page's own measurement (typically 20-30px) and record it");
    else if (!boxes.length || bandTop === null || footerTop === null) skip("gap", "band or chart not resolved");
    else {
      const t = Math.min(...boxes.map((b) => b.t)) - bandTop;
      const b2 = footerTop - Math.max(...boxes.map((b) => b.bb));
      const within = (v) => v >= target[0] - 0.5 && v <= target[1] + 0.5;
      // Symmetry to 0.5px, not 1.5. The gap is set BY CONSTRUCTION by the height-first fit plus a
      // centring step, so equal-to-0.01px is achievable on every frame — measured across eight, the
      // asymmetry ran 0.34-2.24px and a 1.5px allowance passed seven of them while a reviewer called
      // all eight asymmetric. Slack that only ever hides a defect. Measured BOX-to-box on purpose: box
      // and ink symmetry are mutually exclusive (the source line carries constant leading, the subtitle
      // varies with descenders), so equalising one throws the other out by their sum — FITTING.md.
      const SYMMETRY = 0.5;
      const bad = !within(t) || !within(b2) || Math.abs(t - b2) > SYMMETRY;
      add("gap", bad ? "FAIL" : "ok",
          `top ${r(t)}, bottom ${r(b2)} against a ${target[0]}-${target[1]}px target${Math.abs(t - b2) > SYMMETRY ? ` — and the two ends differ by more than ${SYMMETRY}px` : ""}`);
    }
  }

  // Nothing in the margins.
  {
    // On the 302-wide format the header hugs its text, so a header-derived right edge would reject ink
    // that is legitimately inside the format's own 12..290 content box. Take the bounds from the FORMAT
    // there and from the header everywhere else (SMALL-CHARTS.md -> Checks).
    const marginL = isSmall ? 12 : contentL;
    const marginR = isSmall && fb ? fb.width - 12 : contentR;
    // An ANTIMERIDIAN STRADDLER's bbox is not its ink. Fiji is split across the map's two edges, so its
    // box spans nearly the whole plot at ~4px tall (measured 515x3.75, aspect 137) while its actual ink
    // is two small clusters. Testing that box against the margins is a guaranteed false breach on every
    // map, however it is fitted — the same bbox-is-not-the-shape problem maps.md flags for the FIT.
    // Excluded here and reported, never silently dropped.
    const plotBox = (() => { const bs = plotRoots.map(rel).filter(Boolean);
      return bs.length ? { w: Math.max(...bs.map((b) => b.rr)) - Math.min(...bs.map((b) => b.l)),
                           h: Math.max(...bs.map((b) => b.bb)) - Math.min(...bs.map((b) => b.t)) } : null; })();
    // Gated on MAP GEOMETRY, not on the shape alone. Wide-and-thin describes plenty of legitimate marks
    // on other chart types — a nearly flat line, a thin horizontal bar — and exempting those hands back
    // an `ok` on a real overflow. The documented exception is specific to multipart map shapes, so the
    // node must actually sit under grapher's own map container (`fromMap`), which is stricter than
    // "this chart is a map": a legend swatch on a map is not a straddler either.
    const isStraddler = (x) => x.fromMap && plotBox && x.box.w > 0.3 * plotBox.w && x.box.h < 0.05 * plotBox.h;
    const straddlersSeen = leaves.filter((x) => x.insidePlot && isStraddler(x));
    if (marginL === null || marginR === null) skip("margins", "content box not resolved");
    else {
      const out = leaves.filter((x) => (x.insidePlot || /^annotation__/.test(x.name)) && !isStraddler(x) && (x.box.l < marginL - 0.5 || x.box.rr > marginR + 0.5));
      add("margins", out.length ? "FAIL" : "ok",
          (out.length ? `${out.length} mark(s) outside ${r(marginL)}..${r(marginR)}: ` + out.slice(0, 6).map((x) => `${x.name} at ${r(x.box.l)}..${r(x.box.rr)}`).join(", ")
                      : `no ink outside ${r(marginL)}..${r(marginR)} across ${leaves.filter((x) => x.insidePlot).length} plot leaves`) +
          ` (bounds from ${isSmall ? "the 302-wide FORMAT, not its hugging header" : "the header box"})` +
          (straddlersSeen.length ? `. ${straddlersSeen.length} antimeridian straddler(s) excluded — their bbox spans the plot while their ink does not: ${straddlersSeen.slice(0, 3).map((x) => `${x.name} ${r(x.box.w)}x${r(x.box.h)}`).join(", ")}` : ""));
    }
  }

  // Off-palette fills. Two standing exceptions are listed rather than flagged.
  {
    const plotFills = fills.filter((f) => f.insidePlot);
    if (!plotFills.length) skip("off-palette", "no solid fills found in the plot");
    else {
      const unbound = plotFills.filter((f) => !f.styleId);
      const residual = plotFills.filter((f) => f.hex.toLowerCase() === GRAPHER_RESIDUAL);
      const distinct = [...new Set(unbound.map((f) => f.hex))];
      // An imported chart arrives with raw fills by construction, so this row REPORTS rather than
      // fails unless grapher's residual gray is present, which is never a library colour.
      add("off-palette", residual.length ? "FAIL" : "REVIEW",
          (residual.length ? `grapher's residual-category ${GRAPHER_RESIDUAL} is present (${residual.length} fill(s)) — it is in no library group. ` : "") +
          `${unbound.length} of ${plotFills.length} plot fills carry no style id, across ${distinct.length} distinct colour(s): ${distinct.slice(0, 10).join(", ")}. ` +
          "An SVG import cannot bind a style, so bind the ones you keep and confirm each is a library colour. Standing exceptions, not defects: a highlight treatment's muting grays" +
          (CONFIG.highlightTreatment ? " (declared for this frame)" : "") + ", and a grapher-managed sequential map ramp.",
          { distinctUnboundFills: distinct });
    }
  }

  // ---- Crossings, computed BEFORE the two rows that read them. Vertices are LOCAL to their node, so
  // map them through absoluteTransform — a bbox is not a substitute for a line (CHECKS.md), and an
  // untransformed read puts the geometry somewhere else entirely (reference/GOTCHAS.md).
  //
  // What an annotation may sit on is a CLASSIFICATION, not a yes/no: gridlines, empty space and a
  // muted context line are legal; a protagonist line, a dot and a value label are not. Failing every
  // intersection alike reports a correct highlighted chart as broken, because crossing the muted
  // context is exactly what the treatment is for.
// #endregion
// #region annotations
  let crossings = null;
  {
    const map = (n, pt) => { const m = n.absoluteTransform; return { x: m[0][0] * pt.x + m[0][1] * pt.y + m[0][2] - fb.x, y: m[1][0] * pt.x + m[1][1] * pt.y + m[1][2] - fb.y }; };
    const segHitsRect = (p, q, b) => {
      const inside = (pt) => pt[0] >= b.l && pt[0] <= b.rr && pt[1] >= b.t && pt[1] <= b.bb;
      if (inside(p) || inside(q)) return true;
      const cr = (ax, ay, bx, by) => ax * by - ay * bx;
      const edges = [[[b.l, b.t], [b.rr, b.t]], [[b.rr, b.t], [b.rr, b.bb]], [[b.rr, b.bb], [b.l, b.bb]], [[b.l, b.bb], [b.l, b.t]]];
      for (const [e0, e1] of edges) {
        const d1 = cr(q[0] - p[0], q[1] - p[1], e0[0] - p[0], e0[1] - p[1]);
        const d2 = cr(q[0] - p[0], q[1] - p[1], e1[0] - p[0], e1[1] - p[1]);
        const d3 = cr(e1[0] - e0[0], e1[1] - e0[1], p[0] - e0[0], p[1] - e0[1]);
        const d4 = cr(e1[0] - e0[0], e1[1] - e0[1], q[0] - e0[0], q[1] - e0[1]);
        if (((d1 > 0) !== (d2 > 0)) && ((d3 > 0) !== (d4 > 0))) return true;
      }
      return false;
    };
    const overlaps = (a, b) => a.l < b.rr && a.rr > b.l && a.t < b.bb && a.bb > b.t;

    // Muted-or-not is a property of the SERIES, not of the node. Under the highlight treatment a 1px
    // `line__X` is the muted context — and so is its halo, `outline__X`, which sits at 2x (CHECKS.md:
    // "halo 2x, or line+1 where nothing crosses"). Classifying each node by its own weight therefore
    // reports the halo of a legally-crossed context line as a protagonist. So read the weight of each
    // series' `line__` node and let its `outline__` inherit the verdict.
    // Identity comes from the node's OWN name when it has one, and otherwise from the naming ancestor
    // `collect` carried down — a slope's stroked vector is called plain `line` under a `slope__<Entity>`
    // group, so a name-only read here finds no slope segment at all.
    // The fallback is deliberately narrow — the ancestor's name is only inherited by the child grapher
    // actually calls `line`. A `slope__<Entity>` group also holds `start-point` and `end-point` VECTORs,
    // and letting those inherit it would sample a 6px marker as if it were the series stroke and report
    // a phantom crossing against it.
    const identify = (v) => {
      const m = /^(line|slope|outline)__(.+)$/.exec(v.node.name);
      if (m) return { kind: m[1], series: m[2], label: v.node.name };
      if (v.seriesOf && /^line$/i.test(v.node.name)) return { kind: v.seriesOf.kind, series: v.seriesOf.series, label: `${v.seriesOf.kind}__${v.seriesOf.series}` };
      return null;
    };
    const lineWeightBySeries = {};
    for (const v of vectors) {
      const id = identify(v);
      if (id && id.kind !== "outline" && typeof v.node.strokeWeight === "number") lineWeightBySeries[id.series] = v.node.strokeWeight;
    }
    const polylines = [];
    for (const v of vectors) {
      const id = identify(v);
      if (!id) continue;
      let net = null;
      try { net = v.node.vectorNetwork; } catch (e) { net = null; }
      if (!net || !net.vertices || !net.vertices.length) continue;
      const w = typeof v.node.strokeWeight === "number" ? v.node.strokeWeight : null;
      const seriesW = lineWeightBySeries[id.series];
      const muted = CONFIG.highlightTreatment && seriesW !== undefined && Math.abs(seriesW - 1) < 0.05;
      const pts = net.vertices.map((pt) => { const q = map(v.node, pt); return [r(q.x), r(q.y)]; });
      // Connectivity lives in `segments`, NOT in vertex order. A series with a gap (a missing interval
      // in a time range) has disconnected subpaths, and joining consecutive vertices invents a stroke
      // across the gap — which then reports an annotation sitting in that empty space as crossing the
      // line, and demands a knockout for it. Fall back to vertex order only when segments are absent.
      const segs = Array.isArray(net.segments) && net.segments.length
        ? net.segments.filter((s) => pts[s.start] && pts[s.end]).map((s) => [pts[s.start], pts[s.end]])
        : pts.slice(1).map((q, i) => [pts[i], q]);
      polylines.push({ name: id.label, muted, w, seriesLineW: seriesW === undefined ? null : r(seriesW),
                       points: pts, segments: segs, fromSegments: !!(Array.isArray(net.segments) && net.segments.length) });
    }
    if (!polylines.length) skip("polylines", "no line__*/outline__* VECTOR carried a readable vectorNetwork");
    else add("polylines", "ok", `${polylines.length} series polyline(s) sampled, ${polylines.reduce((s, p) => s + p.points.length, 0)} vertices and ${polylines.reduce((s, p) => s + p.segments.length, 0)} segments (connectivity ${polylines.every((p) => p.fromSegments) ? "from vectorNetwork.segments" : "PARTLY from vertex order — a gapped series may report a phantom crossing"}), in frame coordinates` +
          (CONFIG.highlightTreatment ? `; ${polylines.filter((p) => p.muted).length} classified as muted context (legal to cross)` : ""),
          { polylines: polylines.map((p) => ({ name: p.name, n: p.points.length, muted: p.muted, first: p.points[0], last: p.points[p.points.length - 1] })) });

    // Furniture and forbidden marks, by bbox. A gridline, axis or tick is axis-aligned, so its bbox IS
    // its geometry; dots and value labels are small and compact, so a bbox is right for them too.
    // The predicate is the WEIGHT row's, not "stroked and not named like a series": the loose version
    // counted every stroked plot node as furniture, so on a map each country entered `crossings` by its
    // bounding box and the knockout row then demanded a 3px stroke on an annotation sitting over open
    // ocean — while the filled-mark path above deliberately excludes that same geometry.
    const furnitureBoxes = stroked.filter((s) => s.insidePlot && s.inFurniture && !s.seriesKind)
      .map((s) => ({ name: s.name, box: rel(s.node) })).filter((x) => x.box);
    // A MAP SHAPE is inventoried by nobody here: per-chart-type/maps.md, a country's bounding box is not
    // its painted geometry — an antimeridian straddler's box spans almost the whole map — so an
    // annotation over open ocean inside that box would be reported as covering a filled mark. Same
    // reason the margins row excludes straddlers and the gap row skips maps. Declared, not silently
    // dropped: the row says how many were left to the pixel probe.
    const mapMarks = markBoxes.filter((x) => x.insidePlot && x.fromMap);
    const forbiddenBoxes = markBoxes.filter((x) => x.insidePlot && !x.fromMap);

    if (!annotations.length) { skip("annotation-overlap", "no annotation__* nodes on this frame"); }
    // `forbiddenBoxes` counts as something to test against, and leaving it out of this guard was a real
    // gap: an axis-less tightly-measured bar has no polylines and no furniture, but its bar segments and
    // value labels ARE geometry. The row skipped with the segments already inventoried, and because the
    // knockout row keys off `crossings`, that skipped too — two rows silent on a chart type the skill
    // ships regularly.
    else if (!polylines.length && !furnitureBoxes.length && !forbiddenBoxes.length) {
      skip("annotation-overlap", "nothing to test against: no readable polylines, no furniture and no filled marks" +
           (mapMarks.length ? `. The ${mapMarks.length} map shape(s) here are deliberately not judged by bbox (maps.md) — on a map with no furniture this row covers NOTHING, and whether an annotation sits on land is the rendered-pixel probe's call` : ""));
    }
    else {
      crossings = {};
      const illegal = [];
      for (const a of annotations) {
        if (!a.box) continue;
        const hits = [];
        for (const pl of polylines) {
          for (const [p, q] of pl.segments) {
            if (segHitsRect(p, q, a.box)) {
              hits.push(pl.name);
              if (!pl.muted) illegal.push(`${a.name} crosses ${pl.name}` + (CONFIG.highlightTreatment ? ` (its series line is ${pl.seriesLineW}px — protagonist, not the 1px muted context)` : ""));
              break;
            }
          }
        }
        for (const f of furnitureBoxes) if (overlaps(a.box, f.box)) hits.push(f.name);
        for (const f of forbiddenBoxes) if (overlaps(a.box, f.box)) { hits.push(f.name); illegal.push(`${a.name} covers ${f.name} — ${f.why}`); }
        crossings[a.name] = [...new Set(hits)];
      }
      const uniq = [...new Set(illegal)];
      add("annotation-overlap", uniq.length ? "FAIL" : "ok",
          (uniq.length ? uniq.join("; ") + ". Gridlines, empty space and a muted context line are legal; a protagonist line, a dot or a value label is not."
                       : `no annotation covers a prohibited mark (${annotations.length} annotation(s) vs ${polylines.length} line(s), ${furnitureBoxes.length} furniture node(s), ${forbiddenBoxes.length} individual dot/value mark(s))`) +
          (mapMarks.length ? ` — ${mapMarks.length} map shape(s) NOT judged here: a country's bbox is not its ink (maps.md), so an annotation over ocean inside it would read as covering it. Whether an annotation sits on land is the rendered-pixel probe's call.` : ""),
          { crossingsPerAnnotation: crossings, approximate: "segment-vs-rect on sampled vertices; a near-miss is settled by CHECKS.md's four-render pixel probe." });
    }
  }

  // Annotation knockout tier. The TIER IS DECIDED BY WHAT IS CROSSED, so this row cannot be a
  // property check alone: an annotation crossing a gridline with NO stroke is a missing knockout, and
  // judging only the nodes that already have one certifies exactly that defect. Crossings are computed
  // below (furniture bboxes + series polylines) and this row reads them.
  {
    if (!annotations.length) skip("annotation-knockout", "no annotation__* nodes on this frame");
    else if (!crossings) skip("annotation-knockout", "crossings not computed (no furniture and no readable polylines) — cannot decide the tier, so not judging the strokes either");
    else {
      const bad = [], unsure = [];
      for (const a of annotations) {
        const n = a.node;
        if (!("strokeWeight" in n) || typeof n.strokeWeight !== "number") continue;
        // Presence is decided by a paint that actually RENDERS. `strokes.length` counts a paint that
        // has been switched off (`visible: false`) or made transparent (`opacity: 0`), so an annotation
        // whose knockout paints nothing passed the weight, alignment and colour checks — the row
        // certified the missing knockout it exists to catch. And the colour check read `strokes[0]`
        // unconditionally, which is the wrong paint the moment an invisible one sits in front of it.
        const paint = (n.strokes || []).find((s) => s && s.visible !== false && (s.opacity === undefined || s.opacity > 0));
        const hasStroke = !!paint && n.strokeWeight > 0;
        const crosses = crossings[a.name] || [];
        if (crosses.length && !hasStroke) {
          bad.push(`${a.name} crosses ${crosses.length} thing(s) (${crosses.slice(0, 3).join(", ")}) but carries NO knockout — CHECKS.md requires a 3px OUTSIDE stroke whenever furniture is crossed`);
          continue;
        }
        if (!crosses.length && hasStroke) {
          bad.push(`${a.name} crosses nothing yet carries a ${r(n.strokeWeight)}px knockout — an annotation over empty space takes no stroke and no frame`);
          continue;
        }
        // A knockout works by PAINTING the frame's colour over what it crosses, so a translucent one
        // does not do the job its 3px and OUTSIDE alignment are for: at 0.005 the crossing shows
        // straight through, and the geometry checks below would still return ok. Zero is already
        // handled above as no knockout at all; anything between is on the canvas but cannot be
        // certified from here — how much it masks depends on what is behind it, which is the same
        // question the palette refuses to guess at. So it is REVIEWED with the number named, rather
        // than passed or failed: at 0.98 it masks fine, at 0.05 it does not, and this script cannot
        // tell the reader which side of that they are on.
        const knockoutAlpha = (a.opacity === undefined ? 1 : a.opacity)
                              * (hasStroke && paint.opacity !== undefined ? paint.opacity : 1);
        if (hasStroke && knockoutAlpha < 0.999) {
          unsure.push(`${a.name} carries a knockout at effective opacity ${r(knockoutAlpha)} (paint x node), so it does not fully mask the ${crosses.length} thing(s) it crosses — a knockout paints the frame's colour OVER them, and a partly transparent one lets them through. Set it to 1, or judge this one by eye`);
        }
        if (hasStroke && Math.abs(n.strokeWeight - 3) >= 0.05) {
          bad.push(`${a.name} knockout ${r(n.strokeWeight)}px (want 3)` + (n.strokeWeight < 1 ? " — sub-pixel means the stroke was set before a rescale()" : ""));
        }
        if (hasStroke && n.strokeAlign !== "OUTSIDE") bad.push(`${a.name} strokeAlign ${n.strokeAlign} (want OUTSIDE)`);
        if (hasStroke && frameFill) {
          const sf = paint;
          if (sf.type === "SOLID") {
            const hex = "#" + [sf.color.r, sf.color.g, sf.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("");
            if (hex.toLowerCase() !== frameFill.toLowerCase()) bad.push(`${a.name} knockout is ${hex}, not the frame's own ${frameFill} — read the colour off the frame, never hardcode white`);
          }
        }
      }
      add("annotation-knockout", bad.length ? "FAIL" : unsure.length ? "REVIEW" : "ok",
          (bad.length ? bad.join("; ") : `all ${annotations.length} annotation(s) carry the tier their crossings require`)
          + (unsure.length ? `. ${unsure.length} NOT certified: ${unsure.join("; ")}` : ""));
    }
  }

  // Annotation block gap — the block's outer edges, not the plot's.
  {
    if (!annotations.length) skip("annotation-block-gap", "no annotation__* nodes on this frame");
    else if (isSmall) skip("annotation-block-gap", "302-wide format: SMALL-CHARTS.md replaces the 27px constant with 'scale to the frame', so the 540x540 figure would reject a valid scaled layout. Measure it against that page's own rule and record the number");
    else if (bandTop === null || footerTop === null) skip("annotation-block-gap", "band not resolved");
    else {
      const plotBoxes = plotRoots.map(rel).filter(Boolean);
      const annBoxes = annotations.map((a) => a.box).filter(Boolean);
      const all = [...annBoxes, ...plotBoxes];
      const top = Math.min(...all.map((b) => b.t)), bot = Math.max(...all.map((b) => b.bb));
      const cTop = top - (header ? header.y + header.height : bandTop), cBot = footerTop - bot;
      // The 27px rule is for annotations that sit OUTSIDE the plot, in bands above and below it —
      // there the reader sees one content block whose outer edges owe the template's own gaps. When
      // every annotation is inside the plot the block IS the plot, so this row would demand 27px of
      // exactly the geometry the `gap` row requires to be 12-16: the two become unsatisfiable
      // together, and a correctly-fitted chart fails one of them whatever you do. Measured on a DI
      // whose annotations sat in the plot: gap ok at 14/14.21, this row FAIL wanting >= 27.
      const plotTop = Math.min(...plotBoxes.map((b) => b.t)), plotBot = Math.max(...plotBoxes.map((b) => b.bb));
      const outside = annBoxes.filter((b) => b.t < plotTop - 0.5 || b.bb > plotBot + 0.5);
      if (!outside.length) {
        skip("annotation-block-gap",
             `all ${annBoxes.length} annotation(s) sit inside the plot's vertical extent, so the block IS the plot and its clearance is the \`gap\` row's business (measured ${r(cTop)}/${r(cBot)} here). This row governs annotations placed in bands ABOVE or BELOW the plot`);
      } else {
        const bad = cTop < BLOCK_CLEARANCE - 0.5 || cBot < BLOCK_CLEARANCE - 0.5;
        add("annotation-block-gap", bad ? "FAIL" : "ok",
            `block clears header by ${r(cTop)} and footer by ${r(cBot)} (want >= ${BLOCK_CLEARANCE}); ${outside.length} annotation(s) extend past the plot, which is what puts this row in scope`);
      }
    }
  }

  // Direct labels readable as text — computed, not declared. CHECKS.md wants 4.5:1 against the
  // background for every category label drawn on it; that is a pure function of two hexes.
  //
  // This row used to require `insidePlot`, which made it DEAD on every correctly-built page: annotations
  // are appended to the FRAME (GOTCHAS.md), so `insidePlot` is false for all of them, and
  // `insidePlot && /^annotation__/` is a contradiction. Same can't-fail bug this file already fixed once
  // for the `annotations` walk, in a second row, unnoticed until a nine-label map reported SKIPPED with
  // "no annotation text with a solid fill" while carrying nine filled annotations. So: annotations are
  // judged against the FRAME's fill, which is what is actually behind them; `label__*` nodes stay gated
  // on insidePlot because what is behind those is a mark, not the frame.
  {
    const candidates = texts.filter((t) => t.fill && (/^annotation__/.test(t.name) || (t.insidePlot && /^label__/.test(t.name))));
    // Two ways a label can be UNJUDGEABLE against the frame, and neither may be dropped: the on-fill row
    // is a DECLARED gap below (it needs label->mark pairing, which nothing here has), so anything routed
    // to it is reported by NOBODY, and a defect that leaves no row is worse than a row a human looks at.
    // Both go to REVIEW.
    //
    // The first is COLOUR. A label in the frame's own colour reads either as the prescribed
    // white-on-dark-mark label (GUIDELINES.md → maps: "values written inside countries take whichever
    // colour reads against the fill"), which measured against the frame reports 1:1 and fails correct
    // work, or as an annotation accidentally given the frame's colour — invisible text. Matching colours
    // are no evidence either way.
    //
    // The second way in is GEOMETRY. An annotation over a MAP SHAPE is the case: in-country labels are
    // prescribed there, and a country's bbox is deliberately not judged by annotation-overlap (a bbox is
    // not a country's ink, maps.md), so nothing else looks at it either — black text on a dark country
    // would be measured against the white FRAME, score 21:1 and pass. Over a NON-map mark the position
    // is already illegal and annotation-overlap FAILs it, so those need no second opinion here and the
    // row keeps its information value on ordinary charts.
    const overlaps = (a, b) => a.l < b.rr && a.rr > b.l && a.t < b.bb && a.bb > b.t;
    const mapShapes = markBoxes.filter((x) => x.insidePlot && x.fromMap && x.hex);
    const marksUnder = (t) => (t.box ? mapShapes.filter((m) => overlaps(t.box, m.box)) : []);
    // A THIRD way in: a translucent label. Its `fill` is the raw paint, but what the reader sees is that
    // colour composited onto the background, which is always LOWER contrast than the raw value — so
    // measuring the raw one passes text that is genuinely too faint, the one direction this row must
    // not fail in. The ratio is not computable here for the same reason the palette will not audit it,
    // so it joins the by-eye bucket rather than being scored or dropped.
    const ambiguous = candidates.filter((t) =>
      (frameFill && t.fill.toLowerCase() === frameFill.toLowerCase()) || marksUnder(t).length || t.translucent);
    const onBg = candidates.filter((t) => ambiguous.indexOf(t) === -1);
    // Named per label, with the arithmetic where it exists: against a map shape's own fill the ratio IS
    // computable, it is just not certain to be the fill behind the text.
    const why = (t) => {
      const over = marksUnder(t);
      const worst = over.length ? over.reduce((a, m) => (contrast(t.fill, m.hex) < contrast(t.fill, a.hex) ? m : a)) : null;
      return `"${t.chars}" ${t.fill}` +
             (t.translucent ? " — partly transparent, so the colour that reaches the reader is this one composited onto what is behind it and ALWAYS lower contrast than the raw value; reset the opacity to 1 to have it measured" : "") +
             (frameFill && t.fill.toLowerCase() === frameFill.toLowerCase() ? " — the frame's own colour, so either inside a darker mark (correct) or invisible text on the frame" : "") +
             (worst ? ` — overlaps ${over.length} map shape(s), worst ${worst.name} ${worst.hex} = ${r(contrast(t.fill, worst.hex))}:1` : "");
    };
    const review = (n) => `${n} label(s) cannot be judged against the frame — a bbox overlap is not proof of what is behind the text, and no geometry here pairs a label with its mark, so check these by eye: ` +
                          ambiguous.slice(0, 8).map(why).join("; ");
    if (!frameFill) skip("label-contrast-on-background", "frame carries no solid fill to measure against");
    else if (!onBg.length && !ambiguous.length) skip("label-contrast-on-background", "no label__*/annotation__* text sits on the frame's own background");
    else if (!onBg.length) add("label-contrast-on-background", "REVIEW", `nothing measurable against the background — all ${review(ambiguous.length)}`);
    else {
      const bad = onBg.map((t) => ({ t, c: contrast(t.fill, frameFill) })).filter((x) => x.c < 4.5);
      add("label-contrast-on-background", bad.length ? "FAIL" : ambiguous.length ? "REVIEW" : "ok",
          (bad.length ? bad.map((x) => `"${x.t.chars}" ${x.t.fill} on ${frameFill} = ${r(x.c)}:1 (want 4.5)`).join(", ")
                     : `all ${onBg.length} label(s) clear 4.5:1 against ${frameFill} (lowest ${r(Math.min(...onBg.map((t) => contrast(t.fill, frameFill))))}:1)`) +
          (ambiguous.length ? `. Plus ${review(ambiguous.length)}` : ""));
    }
  }

  // ---------------------------------------------------------------- declared gaps in coverage
// #endregion
  // These two are owned by color_audit.py, which cannot run in here — this is a Figma plugin
  // sandbox with no shell. But the palette it needs is already on the canvas, so emit the command
  // ready to paste instead of a tool name to go and look up. A declared gap the operator has to
  // reconstruct by hand is the one most likely to be skipped for real.
  {
    // The palette is built from IDENTIFIED DATA MARKS AND SERIES STROKES, never from the `fills`
    // inventory. `fills` holds every solid paint on an area node inside the plot and a TEXT node has
    // both, so axis and legend labels would enter the palette as categories; meanwhile a line chart's
    // series colours live on STROKES and are not in `fills` at all. Measured on this script's own test
    // fixture, that inventory returned exactly one entry — the fill of `label__A`, a text node — with
    // the series colour it should have audited missing entirely. Auditing furniture while omitting the
    // data is the confident wrong answer these rows exist to prevent.
    // `outline__*` strokes are excluded along with it: that is the white halo grapher draws under a
    // line so crossings stay readable. Every series shares it, and it is not a category colour.
    // `cat` comes from the walk's nearest categorical ANCESTOR, not from the painted node's own name.
    // grapher puts the stable name on the group and the paint on its leaves — a `datapoints__<Entity>`
    // marker is a filled leaf called `Ellipse 12` — so reading it off `m.name` loses the category on
    // exactly the shape the collector already goes out of its way to preserve, and the clash note below
    // then cannot fire for those marks.
    // The category is also the LABEL: `--names` exists so the audit's findings name the categories that
    // need attention, and a marker's painted leaf is called `Ellipse 12`. Reporting a failing pair as
    // "Ellipse 12 vs Ellipse 12" identifies nothing. Fall back to the node's own name only when there
    // is no categorical ancestor. Map shapes keep their node names, which already carry the country.
    // LEGEND SWATCHES are not categories, they are a picture OF the categories: a legend repeats the
    // colours the marks already carry and adds none of its own. Audited as marks they turn every
    // ordinary map into a "map plus chart marks" frame (the legend sits inside the chart group and
    // outside `map`), get run against the wrong palette under `--separated`, and their names — grapher
    // ids each swatch after its bin label, and a numeric legend's bins are unnamed rects — either
    // impersonate a category or trip the import-default gate and drop `--names` for the whole run.
    const markSrc = markBoxes.filter((m) => m.insidePlot && m.hex && !m.fromLegend);
    const legendSrc = markBoxes.filter((m) => m.insidePlot && m.hex && m.fromLegend);
    const strokeSrc = stroked.filter((s) => s.insidePlot && !s.inFurniture && s.hex
                                            && (s.seriesKind === "line" || s.seriesKind === "slope"));
    // Translucent paints are held back rather than audited at their raw value (see `translucent`).
    // COUNTED, though, and named below: a silently shorter palette is a subset audit reported as a
    // whole one, which is the failure this row exists to avoid.
    const dimMarks = markSrc.filter((m) => m.translucent);
    const dimStrokes = strokeSrc.filter((s) => s.translucent);
    // `fromMap` and `stroke` travel with the entry because they decide WHICH PALETTE the audit searches,
    // and that is a property of the mark, not of the frame it sits in.
    const marks = markSrc.filter((m) => !m.translucent)
      .map((m) => ({ id: (m.fromMap ? null : m.cat) || m.name, hex: m.hex, cat: m.fromMap ? null : m.cat,
                     fromMap: !!m.fromMap, stroke: false }));
    const seriesStrokes = strokeSrc.filter((s) => !s.translucent)
      .map((s) => ({ id: s.seriesName, hex: s.hex, cat: s.seriesName, fromMap: false, stroke: true }));
    const paletteSrc = [...marks, ...seriesStrokes];
    // The name is the category where there is one, so the note points at something the operator can
    // find on the canvas rather than at a count.
    const dimNames = [...new Set([...dimMarks.map((m) => (m.fromMap ? null : m.cat) || m.name),
                                 ...dimStrokes.map((s) => s.seriesName)].filter(Boolean))];
    const dimNote = dimMarks.length + dimStrokes.length
      ? ` ${dimMarks.length + dimStrokes.length} translucent mark(s)/series are NOT in this palette`
        + (dimNames.length ? ` (${dimNames.slice(0, 6).join(", ")})` : "")
        + " — a partly transparent paint reads as its colour composited onto whatever is behind it, which"
        + " this script cannot determine from inside the plot, so auditing its raw value would answer the"
        + " wrong question. Reset the opacity to 1 (GUIDELINES.md does this for grapher's non-focused"
        + " series) and re-run, or judge those by eye."
      : "";
    // Dropping them SILENTLY would be the same subset-reported-as-a-whole this file refuses elsewhere,
    // and one legend colour genuinely can be missing from the marks: an empty bin nobody falls into is
    // drawn in the legend and nowhere else. So the count is stated and any colour the palette does not
    // already carry is named, rather than being quietly audited or quietly dropped.
    const legendOnly = [...new Set(legendSrc.filter((m) => !m.translucent).map((m) => m.hex.toLowerCase()))];
    const paletteHexes = new Set(paletteSrc.map((x) => x.hex.toLowerCase()));
    const legendUnmatched = legendOnly.filter((h) => !paletteHexes.has(h));
    const legendNote = legendSrc.length
      ? ` ${legendSrc.length} legend swatch(es) are NOT in this palette — a legend repeats the`
        + " categories' own colours rather than adding any, and grapher draws it inside the chart group"
        + " and outside the map, so counting it made an ordinary map report a second, chart-side palette"
        + " and audited its own furniture."
        + (legendUnmatched.length
            ? ` ${legendUnmatched.length} of its colour(s) appear ONLY in the legend and on no mark`
              + ` (${legendUnmatched.slice(0, 4).join(", ")}) — an empty bin, or a mark this script could`
              + " not read; judge those by eye."
            : "")
      : "";
    // `--maps` swaps in the CATEGORICAL Maps palette, and the deltaE 20 all-pairs gate is a categorical
    // test. A SEQUENTIAL choropleth — Viridis, a ColorBrewer ramp — is ordered by construction and set
    // in grapher: its adjacent stops are MEANT to sit close together, so that gate fails a correct ramp,
    // and the script's own "rerun with --suggest" would then answer with an unordered categorical set.
    // Nothing here can tell a ramp from a categorical choropleth off the fills alone, so it is not
    // guessed: the map branch says which chart the command is for and which chart it does not apply to.
    const mapNote = " — this frame is a MAP, and this row covers a CATEGORICAL choropleth ONLY."
      + " If these fills are a SEQUENTIAL ramp, do not run it: the ramp is ordered and grapher sets it,"
      + " adjacent stops are meant to be close, so the deltaE 20 gate fails correct work and --suggest"
      + " would offer an unordered categorical palette in its place. Judge a ramp by lightness order.";
    const lineNote = " — `--line` is `--separated` plus the Line and Slope Chart variants, the darker set"
      + " meant for thin marks and text on white, so a --suggest rerun recommends stroke colours rather"
      + " than fill colours. A line or slope chart has no seams, so nothing here needs reordering.";
    const separatedNote = " — `--separated` assumes nothing shares an edge, which holds for lines, maps"
      + " and plain/grouped bars. On a STACKED or SEGMENTED chart drop it and reorder the"
      + " colours into stack order first, because the seam check reads adjacency off that order.";
    // ONE RUN PER PALETTE FAMILY, not one per FRAME. A frame can hold both at once: combination.md's
    // exemplar is a line chart with an inset locator map whose countries are filled with THE SAME
    // COLOURS AS THEIR SERIES. `isMap` is a frame-level flag, so on that frame the map won and the line
    // strokes were audited under `--maps` — whose "rerun with --suggest" answers out of the LIGHTER
    // Categorical Maps set, recommending FILL colours for thin strokes. That is the exact swap `--line`
    // exists to prevent, arrived at because one verdict was picked for a frame that holds two different
    // things. The frame is one; the palettes are two, so each is deduped, named, flagged and emitted on
    // its own, and neither is judged against the other's palette.
    const mapPal = paletteSrc.filter((p) => p.fromMap);
    const chartPal = paletteSrc.filter((p) => !p.fromMap);
    const mixed = mapPal.length > 0 && chartPal.length > 0;
    const paletteRun = (src) => {
      // Dedupe by COLOUR, because color_audit.py takes a PALETTE: hand it forty bar segments in six
      // colours and every real finding competes with dozens of deltaE 0 self-pairs.
      const seen = new Map();  // hex -> first mark/series name, in walk order
      for (const p of src) if (!seen.has(p.hex.toLowerCase())) seen.set(p.hex.toLowerCase(), p.id);
      const hexes = [...seen.keys()];
      if (!hexes.length) return "";
      // BOTH rows compare PAIRS — the deltaE gate across all of them, the seam gate across adjacent
      // ones — so a palette of one colour has nothing to compare. `color_audit.py` does not say so: it
      // runs, prints an empty pair list and an overall minimum deltaE of `inf`, and exits 0, which
      // reads exactly like a clean audit. GUIDELINES.md already rules on this case in as many words —
      // "one categorical color against neutral grays has no pair to check, and reporting no failures
      // from a two-color audit reads as coverage you don't have" — and names the two checks that ARE
      // live instead. So the command is withheld and those two are handed over in its place. Emitting
      // it would be this file's own failure mode: a runnable-looking gate that cannot fail.
      // Computed BEFORE the single-colour exit below, not after it. Two categories painted the same
      // colour ARE a one-colour palette, and that is the severest collision there is — so the one
      // branch that withholds the command is the branch that most needs to say what it found.
      const catsByHex = new Map();
      for (const p of src) {
        if (!p.cat) continue;
        const h = p.hex.toLowerCase();
        if (!catsByHex.has(h)) catsByHex.set(h, new Set());
        catsByHex.get(h).add(p.cat);
      }
      const shared = [...catsByHex].filter(([, c]) => c.size > 1)
        .map(([h, c]) => `${h} carries ${[...c].slice(0, 4).join(" + ")}`);
      const sharedNote = shared.length
        ? ` One colour, two categories — the audit sees these as ONE entry and cannot report the clash,`
          + ` so judge them by eye: ${shared.slice(0, 4).join("; ")}. Correct for a highlight treatment;`
          + " a defect if they are separate categories."
        : "";
      const soleName = [...seen.values()][0];
      if (hexes.length < 2) {
        return (mixed ? (src.some((x) => x.fromMap) ? " MAP shapes —" : " Chart marks and series —") : "")
          + ` NOTHING TO RUN: this palette holds ONE colour (${hexes[0]}${soleName ? ", " + soleName : ""})`
          + ` drawn from ${src.length} plot mark(s)/series, and both rows compare PAIRS, so there is no`
          + " categorical pair to check. color_audit.py would still run on it and exit 0, printing an"
          + " overall minimum deltaE of `inf` from an empty pair list — a clean-looking verdict from a"
          + " comparison that never happened. GUIDELINES.md says not to run it on a chart like this."
          + " Check the two things that ARE live: this colour's contrast against the frame's background,"
          + " and whether it still separates from the neutral grays in grayscale."
          + sharedNote + dimNote + legendNote;
      }
      // What that collapse HIDES is stated above, not swallowed. Two categories on one colour is the
      // severest collision there is — deltaE 0, indistinguishable to everyone — and the audit cannot
      // report it, because to the audit they are one entry.
      // Not graded, and phrased as a question, because sharing a colour is often correct: a highlight
      // treatment greys every unhighlighted series to the same value on purpose. Map shapes are left out
      // of the note altogether — a choropleth puts every country in a bin into one colour by definition,
      // so flagging that would fire on every map. Only grapher's `<kind>__<Entity>` names count as a
      // category; an SVG import's `Rectangle 12` is unique per node and means nothing.
      // A name carrying a comma would silently split into two --names entries and misalign every label
      // after it; a name carrying an apostrophe would end the single-quoted shell argument mid-name, so
      // "Women's employment" turns a paste-ready command into a shell syntax error. Both drop the flag
      // wholesale rather than emitting it subtly wrong. State the ACTUAL reason: an unnamed mark, a
      // comma'd one and an apostrophe'd one all disqualify the flag, and reporting one as another is the
      // same wrong-verdict habit these checks exist to catch.
      const names = [...seen.values()];
      // A name that is not DISTINCT across the palette is the worst of the lot, because it looks
      // right. Measured on the real file: a `static_viz` import names every bar group `bars__<slug>`
      // — the dataset, not the category — and every paint-bearing leaf `Vector`, so four genuinely
      // different colours came out labelled `agriculture-share,agriculture-share,...`. The audit
      // would then print four rows under one name and the operator would read a per-category verdict
      // off labels that name no category. Distinctness is the property `--names` actually promises.
      const genericName = /^(vector|rectangle|ellipse|group|frame|line|path|polygon|union|subtract)\b/i;
      const nameProblem = names.some((n) => !n) ? "a mark has no name"
        : names.some((n) => n.includes(",")) ? "a name contains a comma, which would misalign the labels"
        : names.some((n) => n.includes("'")) ? "a name contains an apostrophe, which would break the quoted shell argument"
        : new Set(names).size !== names.length
          ? `${names.length} colours share only ${new Set(names).size} distinct name(s) (${[...new Set(names)].slice(0, 3).join(", ")}) — the names identify the series or the import, not the category`
        : names.some((n) => genericName.test(n))
          ? `a name is an import default (${names.find((n) => genericName.test(n))}), which labels nothing`
        : null;
      const namesSafe = !nameProblem;
      // The mode flag also selects which PALETTE color_audit.py searches when the operator follows its
      // "rerun with --suggest" instruction, so `--separated` is not interchangeable with the others.
      // `--separated` only turns the seam gate off; `--line` turns it off AND installs the Line and Slope
      // variants, the darker set meant for thin marks on white. A line or slope palette given plain
      // `--separated` therefore gets fill colours recommended for strokes. All three imply "nothing
      // shares an edge", which is why only the stacked/segmented case ever needs the flag dropped.
      // Read off THIS palette's own marks, so a mixed frame cannot hand one family the other's flag.
      const isMapPal = src.some((p) => p.fromMap);
      const mode = isMapPal ? "--maps" : src.some((p) => p.stroke) ? "--line" : "--separated";
      const cmd = ".venv/bin/python .claude/skills/create-figma-chart/scripts/color_audit.py "
        + `'${hexes.join(",")}'`
        + (namesSafe ? ` --names '${names.join(",")}'` : "")
        + ` ${mode}`;
      // The other case GUIDELINES.md rules on: one highlight against neutral grays. Those grays are
      // FURNITURE, not categories, so a palette that is one real colour plus muting grays is the
      // single-colour case wearing a larger count — and "no failures" out of it is coverage nobody has.
      // Not suppressed, because which entries are muting grays is a judgement this script cannot make
      // (a category legitimately painted gray is a category), and a check silently withheld is worse
      // than one a human is told to weigh. Fires only when the treatment is DECLARED.
      const highlightNote = CONFIG.highlightTreatment
        ? " CONFIG.highlightTreatment is set: if this palette is one highlight plus muting grays, do NOT"
          + " run it — GUIDELINES.md rules that out, because the grays are furniture and leave no"
          + " categorical pair. Check the highlight's contrast against the background and its grayscale"
          + " separation from the grays instead. Run it only for the pairs that are real categories."
        : "";
      return (mixed ? (isMapPal ? " MAP shapes —" : " Chart marks and series —") : "")
        + highlightNote
        + ` Run: ${cmd}`
        + (isMapPal ? mapNote : mode === "--line" ? lineNote : separatedNote)
        + ` Palette: ${hexes.length} distinct colour(s) drawn from ${src.length} plot mark(s)/series.`
        + sharedNote
        + (namesSafe ? "" : ` (--names omitted: ${nameProblem}.)`);
    };
    // The overlap between the two is NOT a finding: combination.md fills the locator map's countries
    // with their own series colours on purpose, so saying so here stops the split from reading as a
    // clash the operator has to go and reconcile.
    const mixedNote = mixed
      ? " This frame holds BOTH a map and chart marks/series. They take DIFFERENT color_audit.py"
        + " palettes — Categorical Maps against the Line and Slope variants — so they are audited as two"
        + " separate runs rather than one, and a colour appearing in both is expected (combination.md"
        + " fills an inset locator map's countries with their series colours)."
      : "";
    const how = paletteSrc.length
      ? mixedNote + paletteRun(chartPal) + paletteRun(mapPal) + dimNote + legendNote
      // An all-translucent plot — or one whose only fills are its legend — must not report "no data
      // marks found": that reads as an empty frame and sends the operator looking for missing nodes
      // instead of at the opacity they set or the legend they are looking straight at. A frame that
      // paints no pixels at all never reaches here: it returns one `frame-not-rendered` row instead.
      : dimNote || legendNote
        ? ` No auditable palette:${dimNote}${legendNote}`
        : " No data marks or series strokes found in the plot, so there is no palette to audit.";
    skip("colour-vision", "all-pairs deltaE 20 for deuteranopia/protanopia on CATEGORICAL fills." + how, "scripts/color_audit.py");
    skip("grayscale-seams", "adjacent pairs above ~1.6:1 — same command, same run as colour-vision." + how, "scripts/color_audit.py");
  }
  skip("spelling-and-prose", "American spelling, typos, style-guide breaches", ".venv/bin/codespell + /check-metadata-style");
  skip("text-true-of-indicator", "every claim in every string checked against the producer's documentation", "/adversarial-data-review");
  skip("entities-all-render", "needs the EFFECTIVE selection (URL country=, or the MDim view's resolved list from the DB) — never the SVG's own labels, which makes the check unable to fail", "Step 1's table + /query-grapher-db");
  skip("year-stated-not-stale", "a single-time image must name its year; a time series must not gain a caption", "/check-hardcoded-years");
  skip("legend-agreement", "swatch->label pairing by geometry; not attempted here because a direct-labelled chart has no legend — run it by hand if this frame has one");
  skip("text-hierarchy-ranks", "annotations must outrank supporting text and labels, and same-rank items must share a size. Needs a rank per text node; the ceiling half is enforced by text-hierarchy above", "CHECKS.md");
  skip("direct-label-pairing", "each category label's fill and x against the segment it names, in the reference row");
  // The OTHER 4.5:1 row. Declared rather than computed because it needs the label->segment pairing the
  // row above owns: the bar's own fill is the background here, and picking it by geometry is that
  // pairing problem. The contrast arithmetic is already in this file (see label-contrast-on-background)
  // and can be reused the moment pairing exists.
  skip("label-contrast-on-fill", "4.5:1 for every label drawn INSIDE a fill, at 13.5px regular — the 3:1 large-text allowance does not apply. Needs label->segment pairing to know which fill is behind each label. Because this row is not computed, label-contrast-on-background does NOT route the ambiguous cases here and drop them: a label carrying the frame's own colour is reported there as REVIEW", "CHECKS.md + the direct-label-pairing row");
  skip("arrow-clearance", "arrow pixels vs target pixels; needs 3N+1 renders (the four-render protocol, pair-specific)", "CHECKS.md");
  // Vector-first, because that is what CHECKS.md now prescribes and it is the CHEAP half: the old
  // wording named the PIXEL mask as the method, which sent a reader following this output straight
  // past a one-call exact test and into a multi-render fallback.
  skip("leader-on-map", "terminal vertex against the country's own GEOMETRY, not its bounding box. Do it in VECTORS first: transform the terminal into the country's local space through the inverse of its `absoluteTransform`, parse `vectorPaths` into rings, ray-cast. Exact, and one call. The PIXEL mask (hide the country vector, diff the renders, require the dot within ~1px of that pixel set) is the FALLBACK, for the cases the ray-cast cannot answer — a country a few pixels across whose ring is smaller than the dot, or a fill rule that makes the cast ambiguous", "CHECKS.md + per-chart-type/maps.md");
  // CHECKS.md -> "How much is on the page" prescribes this, so it gets a row: a prescribed check with
  // no row at all is how a run returns "no mechanical row failed" while the reader is looking at a
  // pile of near-identical maps. Declared rather than computed for two reasons, and only the first is
  // fixable here. This script is handed FRAME ids and never a page, and `page.children` on a page
  // nobody switched to comes back SHORT without erroring (GOTCHAS.md) — so an undercount would read
  // as "clean", which is the exact failure the row exists to catch. `PageNode.loadAsync()` would
  // settle that half (diff_against_template.js does it). The other half does not go away: the target
  // is one object per INTENDED item, and how many reference copies were meant to be left on the page
  // is not a property of the file.
  skip("page-census", "count the plot-bearing objects ANYWHERE on the page — `countries-with-data` groups on a map, the equivalent plot group otherwise — and name what each one is for. One per intended item: the deliverable plus the reference copies you meant to place; a spare is clutter. Do NOT substitute an overlap test between top-level children — three maps at three distinct positions pass it while the reader sees a pile. And do not key the census on a SHORTENED node name, which merges `<slug>` with `<slug> — original SVG (unstyled)` into one bucket", "CHECKS.md -> How much is on the page");

  const fails = rows.filter((x) => x.status === "FAIL");
  const review = rows.filter((x) => x.status === "REVIEW");
  const skipped = rows.filter((x) => x.status === "SKIPPED");
  return {
    frame: { id: frame.id, name: frame.name, w: fb ? r(fb.width) : null, h: fb ? r(fb.height) : null },
    resolved: { chartBy: chartResolvedBy, contentBox: [r(contentL), r(contentR)], band: [r(bandTop), r(footerTop)],
                counts: { texts: texts.length, stroked: stroked.length, plotLeaves: leaves.filter((x) => x.insidePlot).length, annotations: annotations.length } },
    verdict: fails.length ? `FAIL on ${fails.length} row(s): ${fails.map((f) => f.check).join(", ")}`
                          : `no mechanical row failed (${review.length} to review, ${skipped.length} not covered here)`,
    rows,
  };

};

// frameIds wins when present; frameId stays the single-frame spelling.
const targets = Array.isArray(CONFIG.frameIds) && CONFIG.frameIds.length ? CONFIG.frameIds : [CONFIG.frameId];
if (targets.length === 1) return await checkFrame(targets[0]);
const results = [];
for (const id of targets) {
  try { results.push(await checkFrame(id)); }
  catch (e) { results.push({ frame: { id }, verdict: `THREW: ${e.message}`, rows: [] }); }
}
return {
  swept: results.length,
  summary: results.map((x) => `${x.frame && x.frame.name ? x.frame.name : x.frame.id}: ${x.verdict}`),
  frames: results,
};