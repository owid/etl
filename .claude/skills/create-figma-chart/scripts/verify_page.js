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
//                     changes the mark-weight bar (context 1px, protagonist 3px, halo 2x) and makes
//                     the muting grays a standing palette exception.
//
// WHAT IT DOES NOT COVER, and never silently passes: every row it cannot judge is returned as
// SKIPPED with the reason and the tool that owns it. Colour-vision and grayscale seams are
// `scripts/color_audit.py`; spelling is `codespell`; "the text is true of the indicator" is
// `/adversarial-data-review`; the entity-completeness row needs the EFFECTIVE selection from
// outside Figma (Step 1's table); the arrow and leader-on-map rows need rendered pixels (CHECKS.md's
// four-render protocol). A check that cannot fail is worse than no check, so those are reported as
// gaps in coverage, not as passes.

const CONFIG = {
  frameId: "26000:6",
  chartName: "chart",
  gapTarget: null,
  tightlyMeasured: false,
  highlightTreatment: false,
};

const LADDER = [12, 13, 14, 15, 16]; // Annotation XS..XL; the ceiling is L 15, XL only when the
const LADDER_CEILING = 15;           // annotation IS the message (GUIDELINES.md -> Annotations)
const TEXT_FLOOR = 12;
const HOUSE_LINE = 3, HOUSE_HALO = 4;
const FURNITURE_W = 1, FURNITURE_DASH = [4, 4];
const BLOCK_CLEARANCE = 27;          // annotation block vs header/footer on the 540x540 pages
const GRAPHER_RESIDUAL = "#585c64";  // emitted for residual categories; in no library group

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);
const rows = [];
const add = (name, status, detail, extra) => rows.push({ check: name, status, detail, ...(extra || {}) });
const skip = (name, why, owner) => add(name, "SKIPPED", why, owner ? { ownedBy: owner } : null);

const frame = await figma.getNodeByIdAsync(CONFIG.frameId);
if (!frame) throw new Error(`frameId ${CONFIG.frameId} not found`);
let page = frame;
while (page && page.type !== "PAGE") page = page.parent;
if (page && figma.currentPage !== page) await figma.setCurrentPageAsync(page);

const fb = frame.absoluteBoundingBox;
const rel = (n) => {
  const b = n.absoluteBoundingBox;
  return b ? { l: b.x - fb.x, t: b.y - fb.y, rr: b.x - fb.x + b.width, bb: b.y - fb.y + b.height, w: b.width, h: b.height } : null;
};

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
let chart = CONFIG.chartName ? frame.children.find((c) => c.name === CONFIG.chartName) : null;
let plotRoots = chart ? [chart] : frame.children.filter((c) => /^(horizontal-axis|vertical-axis|horizontal-grid-lines|vertical-labels|lines)$/.test(c.name));
const chartResolvedBy = chart ? `name "${CONFIG.chartName}"` : plotRoots.length ? `plot subgroups (${plotRoots.length}) — the group looks ungrouped` : "NOT RESOLVED";

// --- one traversal collects everything the rows below read.
// ONE walk over the whole frame, with `insidePlot` set by which top-level child we descended from.
// Walking only [chart, header, footer] is the trap: Step 8's annotations are appended to the FRAME,
// so they are none of those three — `annotations` then comes back empty on every real page and its
// four rows report "no annotation__* nodes" forever, which reads as "nothing to check" rather than
// "I never looked". Caught by planting an annotation and watching the rows stay silent.
const texts = [], stroked = [], fills = [], leaves = [], annotations = [], vectors = [];
const collect = (n, insidePlot) => {
  if ("visible" in n && !n.visible) return;
  if (n.type === "TEXT" && typeof n.fontSize === "number") {
    texts.push({ node: n, name: n.name, chars: (n.characters || "").slice(0, 30), size: n.fontSize,
                 styleId: n.textStyleId || "", box: rel(n), insidePlot });
  }
  if (/^annotation__/.test(n.name)) annotations.push({ node: n, name: n.name, box: rel(n), type: n.type });
  if ("strokeWeight" in n && typeof n.strokeWeight === "number" && n.strokes && n.strokes.length) {
    stroked.push({ node: n, name: n.name, type: n.type, w: n.strokeWeight,
                   dash: "dashPattern" in n && n.dashPattern ? [...n.dashPattern] : [],
                   align: n.strokeAlign, insidePlot });
  }
  if ("fills" in n && Array.isArray(n.fills)) {
    for (const f of n.fills) {
      if (f.type === "SOLID" && f.visible !== false) {
        const hex = "#" + [f.color.r, f.color.g, f.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("");
        fills.push({ name: n.name, type: n.type, hex, styleId: n.fillStyleId || "", insidePlot });
      }
    }
  }
  if (n.type === "VECTOR" && insidePlot) vectors.push(n);
  if ("children" in n && n.children.length) { n.children.forEach((c) => collect(c, insidePlot)); return; }
  const b = rel(n);
  if (b && b.w > 0 && b.h > 0) leaves.push({ name: n.name, type: n.type, box: b, insidePlot });
};
for (const child of frame.children) {
  if (child === logo) continue;
  collect(child, plotRoots.indexOf(child) !== -1);
}

// ---------------------------------------------------------------- rows
// Text floor (CHECKS.md: nothing below 12px)
{
  const under = texts.filter((t) => t.size < TEXT_FLOOR - 0.01);
  add("text-floor", under.length ? "FAIL" : "ok",
      under.length ? `${under.length} text node(s) below ${TEXT_FLOOR}px: ` + under.map((t) => `"${t.chars}" ${r(t.size)}px`).join(", ")
                   : `all ${texts.length} text nodes at or above ${TEXT_FLOOR}px`);
}

// Annotation ladder + ceiling. Only annotation__* nodes are ours; an imported chart's label sizes
// come from the export and are governed by the floor row above, not by the ladder.
{
  const ann = texts.filter((t) => /^annotation__/.test(t.name));
  if (!ann.length) skip("annotation-ladder", "no annotation__* text nodes on this frame");
  else {
    const off = ann.filter((t) => !LADDER.some((L) => Math.abs(t.size - L) < 0.01));
    const over = ann.filter((t) => t.size > LADDER_CEILING + 0.01);
    const bad = [...off.map((t) => `"${t.chars}" ${r(t.size)}px off-ladder`), ...over.map((t) => `"${t.chars}" ${r(t.size)}px above the L${LADDER_CEILING} ceiling`)];
    add("annotation-ladder", bad.length ? "FAIL" : "ok", bad.length ? bad.join(", ") : `all ${ann.length} annotation(s) on the ladder and at or below ${LADDER_CEILING}`);
  }
}

// Named styles. An SVG import can never carry a style id, so this row judges OUR nodes only and
// reports the imported ones as context rather than failing them.
{
  const ann = texts.filter((t) => /^annotation__/.test(t.name));
  const unbound = ann.filter((t) => !t.styleId);
  const importedRaw = texts.filter((t) => t.insidePlot && !/^annotation__/.test(t.name) && !t.styleId).length;
  if (!ann.length) skip("named-styles", "no annotation__* text nodes; an imported chart's text cannot carry a style id");
  else add("named-styles", unbound.length ? "FAIL" : "ok",
           (unbound.length ? `${unbound.length} annotation(s) with no textStyleId — setting fontSize looks like the ladder and is not it: ` + unbound.map((t) => `"${t.chars}"`).join(", ")
                           : `all ${ann.length} annotation(s) bound to a text style`) + `. ${importedRaw} imported chart text node(s) are raw, which is expected.`);
}

// Text hierarchy: nothing may exceed the subtitle (CHECKS.md row 26).
{
  // Structurally: the header's second TEXT child. Picking index 1 out of a list of collected texts
  // sorted by y is fragile — anything that lands two nodes at the same top, or collects a node
  // twice, silently promotes the TITLE into the subtitle's place, and a 25px bar passes everything.
  const headerTexts = header ? header.children.filter((c) => c.type === "TEXT" && typeof c.fontSize === "number") : [];
  const subtitle = headerTexts.length > 1 ? { size: headerTexts[1].fontSize, chars: headerTexts[1].characters.slice(0, 24) } : null;
  if (!subtitle) skip("text-hierarchy", "could not resolve the subtitle (header has fewer than two TEXT children)");
  else {
    const over = texts.filter((t) => (t.insidePlot || /^annotation__/.test(t.name)) && t.size > subtitle.size + 0.01);
    add("text-hierarchy", over.length ? "FAIL" : "ok",
        over.length ? `${over.length} in-plot text node(s) exceed the subtitle's ${r(subtitle.size)}px: ` + over.map((t) => `"${t.chars}" ${r(t.size)}px`).join(", ")
                    : `nothing in the plot exceeds the subtitle's ${r(subtitle.size)}px`,
        { distinctPlotSizes: [...new Set(texts.filter((t) => t.insidePlot).map((t) => r(t.size)))].sort((a, b) => a - b) });
  }
}

// Mark weight — the series lines and their halos.
{
  const series = stroked.filter((s) => /^(line|outline)__/.test(s.name));
  if (!series.length) skip("series-weight", "no line__*/outline__* nodes found in the plot");
  else if (CONFIG.highlightTreatment) {
    const bad = series.filter((s) => !(Math.abs(s.w - 1) < 0.05 || Math.abs(s.w - HOUSE_LINE) < 0.05 || Math.abs(s.w - HOUSE_HALO) < 0.05));
    add("series-weight", bad.length ? "FAIL" : "ok",
        bad.length ? `highlight treatment allows 1 (context) / ${HOUSE_LINE} (protagonist) / ${HOUSE_HALO} (halo); off-bar: ` + bad.map((s) => `${s.name} ${r(s.w)}`).join(", ")
                   : `all ${series.length} series stroke(s) on the highlight-treatment bar (1 / ${HOUSE_LINE} / ${HOUSE_HALO})`);
  } else {
    const bad = series.filter((s) => Math.abs(s.w - (/^outline__/.test(s.name) ? HOUSE_HALO : HOUSE_LINE)) >= 0.05);
    add("series-weight", bad.length ? "FAIL" : "ok",
        bad.length ? `off the house ${HOUSE_LINE}/${HOUSE_HALO}: ` + bad.map((s) => `${s.name} ${r(s.w)} -> ${/^outline__/.test(s.name) ? HOUSE_HALO : HOUSE_LINE}`).join(", ") + ". rescale() multiplies stroke weight — set these AFTER the last scale."
                   : `all ${series.length} series stroke(s) at the house ${HOUSE_LINE}/${HOUSE_HALO}`);
  }
}

// Furniture weight and dash — the two rows that are easiest to miss because you never set them.
{
  const furn = stroked.filter((s) => s.insidePlot && !/^(line|outline)__/.test(s.name));
  if (!furn.length) skip("furniture-weight", "no stroked non-series nodes in the plot");
  else {
    const bad = furn.filter((s) => Math.abs(s.w - FURNITURE_W) >= 0.05);
    add("furniture-weight", bad.length ? "FAIL" : "ok",
        bad.length ? `${bad.length} of ${furn.length} furniture stroke(s) off ${FURNITURE_W}px (rescale() thinned them): ` + [...new Set(bad.map((s) => r(s.w)))].join(", ") + `px seen`
                   : `all ${furn.length} furniture stroke(s) at ${FURNITURE_W}px`);
    // Dash target is PER NODE TYPE: a node that already had a dash keeps [4,4]; the zero line and
    // tick marks are solid and must keep an EMPTY pattern. One blanket target restyles the grid.
    const dashed = furn.filter((s) => s.dash.length);
    const solid = furn.filter((s) => !s.dash.length);
    const badDash = dashed.filter((s) => s.dash.length !== FURNITURE_DASH.length || s.dash.some((v, i) => Math.abs(v - FURNITURE_DASH[i]) >= 0.05));
    add("furniture-dash", badDash.length ? "FAIL" : "ok",
        badDash.length ? `${badDash.length} dashed node(s) off [${FURNITURE_DASH}]: ` + [...new Set(badDash.map((s) => JSON.stringify(s.dash.map(r))))].join(", ")
                       : `${dashed.length} dashed node(s) at [${FURNITURE_DASH}], ${solid.length} solid node(s) keeping an empty pattern`);
  }
}

// Box alignment — the chart's edges against the header box, to the pixel.
{
  const boxes = plotRoots.map(rel).filter(Boolean);
  if (!boxes.length || contentL === null) skip("box-alignment", "chart or header box not resolved");
  else {
    const l = Math.min(...boxes.map((b) => b.l)), rr = Math.max(...boxes.map((b) => b.rr));
    const dl = l - contentL, dr = rr - contentR;
    const bad = Math.abs(dl) > 1 || Math.abs(dr) > 1;
    add("box-alignment", bad ? "FAIL" : "ok",
        `chart ${r(l)}..${r(rr)} against the header's ${r(contentL)}..${r(contentR)} (left ${r(dl) >= 0 ? "+" : ""}${r(dl)}, right ${r(dr) >= 0 ? "+" : ""}${r(dr)})`);
  }
}

// Gap — top and bottom, against the band of the template actually filled.
{
  const boxes = plotRoots.map(rel).filter(Boolean);
  const target = CONFIG.gapTarget || (fb && Math.round(fb.width) === 560 ? [30, 30] : [12, 16]);
  if (CONFIG.tightlyMeasured) skip("gap", "tightlyMeasured: CHECKS.md's band figure does not apply to a trimmed, hugged group — match the reference page's own measurement (typically 20-30px) and record it");
  else if (!boxes.length || bandTop === null || footerTop === null) skip("gap", "band or chart not resolved");
  else {
    const t = Math.min(...boxes.map((b) => b.t)) - bandTop;
    const b2 = footerTop - Math.max(...boxes.map((b) => b.bb));
    const within = (v) => v >= target[0] - 0.5 && v <= target[1] + 0.5;
    const bad = !within(t) || !within(b2) || Math.abs(t - b2) > 1.5;
    add("gap", bad ? "FAIL" : "ok",
        `top ${r(t)}, bottom ${r(b2)} against a ${target[0]}-${target[1]}px target${Math.abs(t - b2) > 1.5 ? " — and the two ends differ by more than 1.5px" : ""}`);
  }
}

// Nothing in the margins.
{
  if (contentL === null) skip("margins", "content box not resolved");
  else {
    const out = leaves.filter((x) => (x.insidePlot || /^annotation__/.test(x.name)) && (x.box.l < contentL - 0.5 || x.box.rr > contentR + 0.5));
    add("margins", out.length ? "FAIL" : "ok",
        out.length ? `${out.length} mark(s) outside ${r(contentL)}..${r(contentR)}: ` + out.slice(0, 6).map((x) => `${x.name} at ${r(x.box.l)}..${r(x.box.rr)}`).join(", ")
                   : `no ink outside ${r(contentL)}..${r(contentR)} across ${leaves.filter((x) => x.insidePlot).length} plot leaves`);
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

// Annotation knockout tier, and the sub-pixel tell.
{
  if (!annotations.length) skip("annotation-knockout", "no annotation__* nodes on this frame");
  else {
    const bad = [];
    for (const a of annotations) {
      const n = a.node;
      if (!("strokeWeight" in n) || typeof n.strokeWeight !== "number") continue;
      const hasStroke = n.strokes && n.strokes.length && n.strokeWeight > 0;
      if (hasStroke && Math.abs(n.strokeWeight - 3) >= 0.05) {
        bad.push(`${a.name} knockout ${r(n.strokeWeight)}px (want 3)` + (n.strokeWeight < 1 ? " — sub-pixel means the stroke was set before a rescale()" : ""));
      }
      if (hasStroke && n.strokeAlign !== "OUTSIDE") bad.push(`${a.name} strokeAlign ${n.strokeAlign} (want OUTSIDE)`);
    }
    add("annotation-knockout", bad.length ? "FAIL" : "ok", bad.length ? bad.join(", ") : `all ${annotations.length} annotation(s) carry a 3px OUTSIDE knockout or none at all`);
  }
}

// Annotation block gap — the block's outer edges, not the plot's.
{
  if (!annotations.length) skip("annotation-block-gap", "no annotation__* nodes on this frame");
  else if (bandTop === null || footerTop === null) skip("annotation-block-gap", "band not resolved");
  else {
    const all = [...annotations.map((a) => a.box), ...plotRoots.map(rel)].filter(Boolean);
    const top = Math.min(...all.map((b) => b.t)), bot = Math.max(...all.map((b) => b.bb));
    const cTop = top - (header ? header.y + header.height : bandTop), cBot = footerTop - bot;
    const bad = cTop < BLOCK_CLEARANCE - 0.5 || cBot < BLOCK_CLEARANCE - 0.5;
    add("annotation-block-gap", bad ? "FAIL" : "ok", `block clears header by ${r(cTop)} and footer by ${r(cBot)} (want >= ${BLOCK_CLEARANCE})`);
  }
}

// Polylines, for the annotations-cover-only-furniture row. Vertices are LOCAL to their node, so map
// them through absoluteTransform — a bbox is not a substitute (CHECKS.md), and an untransformed read
// puts the geometry in the wrong place entirely (reference/GOTCHAS.md).
{
  const map = (n, p) => { const m = n.absoluteTransform; return { x: m[0][0] * p.x + m[0][1] * p.y + m[0][2] - fb.x, y: m[1][0] * p.x + m[1][1] * p.y + m[1][2] - fb.y }; };
  const polylines = [];
  for (const v of vectors) {
    if (!/^(line|outline)__/.test(v.name)) continue;
    let net = null;
    try { net = v.vectorNetwork; } catch (e) { net = null; }
    if (!net || !net.vertices || !net.vertices.length) continue;
    polylines.push({ name: v.name, points: net.vertices.map((pt) => { const q = map(v, pt); return [r(q.x), r(q.y)]; }) });
  }
  if (!polylines.length) skip("polylines", "no line__*/outline__* VECTOR carried a readable vectorNetwork");
  else add("polylines", "ok", `${polylines.length} series polyline(s) sampled, ${polylines.reduce((s, p) => s + p.points.length, 0)} vertices total, in frame coordinates`,
           { polylines: polylines.map((p) => ({ name: p.name, n: p.points.length, first: p.points[0], last: p.points[p.points.length - 1] })) });

  // Annotations cover only furniture — segment-vs-rect, using those polylines.
  if (!annotations.length) skip("annotation-overlap", "no annotation__* nodes on this frame");
  else if (!polylines.length) skip("annotation-overlap", "no polylines available to test against");
  else {
    const hits = [];
    const segHitsRect = (p, q, b) => {
      const inside = (pt) => pt[0] >= b.l && pt[0] <= b.rr && pt[1] >= b.t && pt[1] <= b.bb;
      if (inside(p) || inside(q)) return true;
      const cross = (ax, ay, bx, by) => ax * by - ay * bx;
      const edges = [[[b.l, b.t], [b.rr, b.t]], [[b.rr, b.t], [b.rr, b.bb]], [[b.rr, b.bb], [b.l, b.bb]], [[b.l, b.bb], [b.l, b.t]]];
      for (const [e0, e1] of edges) {
        const d1 = cross(q[0] - p[0], q[1] - p[1], e0[0] - p[0], e0[1] - p[1]);
        const d2 = cross(q[0] - p[0], q[1] - p[1], e1[0] - p[0], e1[1] - p[1]);
        const d3 = cross(e1[0] - e0[0], e1[1] - e0[1], p[0] - e0[0], p[1] - e0[1]);
        const d4 = cross(e1[0] - e0[0], e1[1] - e0[1], q[0] - e0[0], q[1] - e0[1]);
        if (((d1 > 0) !== (d2 > 0)) && ((d3 > 0) !== (d4 > 0))) return true;
      }
      return false;
    };
    for (const a of annotations) {
      if (!a.box) continue;
      for (const pl of polylines) {
        for (let i = 1; i < pl.points.length; i++) {
          if (segHitsRect(pl.points[i - 1], pl.points[i], a.box)) { hits.push(`${a.name} crosses ${pl.name}`); i = pl.points.length; }
        }
      }
    }
    const uniq = [...new Set(hits)];
    add("annotation-overlap", uniq.length ? "FAIL" : "ok",
        uniq.length ? uniq.join(", ") + ". A highlighted line, a dot or a value label is never acceptable under an annotation; a muted context line or a gridline is."
                    : `no annotation rect crosses any series polyline (${annotations.length} annotation(s) vs ${polylines.length} line(s))`,
        { approximate: "segment-vs-rect on sampled vertices. For a near-miss, CHECKS.md's four-render pixel probe is the arbiter." });
  }
}

// ---------------------------------------------------------------- declared gaps in coverage
skip("colour-vision", "all-pairs deltaE 20 for deuteranopia/protanopia on CATEGORICAL fills", "scripts/color_audit.py");
skip("grayscale-seams", "adjacent pairs above ~1.6:1; needs --separated for non-stacked charts", "scripts/color_audit.py");
skip("spelling-and-prose", "American spelling, typos, style-guide breaches", ".venv/bin/codespell + /check-metadata-style");
skip("text-true-of-indicator", "every claim in every string checked against the producer's documentation", "/adversarial-data-review");
skip("entities-all-render", "needs the EFFECTIVE selection (URL country=, or the MDim view's resolved list from the DB) — never the SVG's own labels, which makes the check unable to fail", "Step 1's table + /query-grapher-db");
skip("year-stated-not-stale", "a single-time image must name its year; a time series must not gain a caption", "/check-hardcoded-years");
skip("legend-agreement", "swatch->label pairing by geometry; not attempted here because a direct-labelled chart has no legend — run it by hand if this frame has one");
skip("direct-label-pairing", "each category label's fill and x against the segment it names, in the reference row");
skip("arrow-clearance", "arrow pixels vs target pixels; needs 3N+1 renders (the four-render protocol, pair-specific)", "CHECKS.md");
skip("leader-on-map", "terminal vertex against the country's PIXELS, not its bounding box", "CHECKS.md + per-chart-type/maps.md");

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
