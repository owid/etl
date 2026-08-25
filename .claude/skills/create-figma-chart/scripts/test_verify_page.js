// Stubbed-figma harness for verify_page.js — the only way to test that script off-canvas.
//
// verify_page.js executes ONLY inside Figma (pasted as a `use_figma` call), so every review of it is
// reading-only. A first review found five real defects in it, four of which were rows that could not
// fail — the exact class this harness exists to catch. It fakes the `figma` global, builds a frame
// per scenario, injects a test CONFIG into the committed file verbatim, and asserts on the returned
// rows.
//
// Run after ANY edit to verify_page.js:
//     node .claude/skills/create-figma-chart/scripts/test_verify_page.js
//
// It is a MOCK: it validates control flow and arithmetic against the Plugin API's documented shapes,
// never Figma's actual behaviour. Keep the mock honest — every node needs an `id`, leaves must have
// NO `children` key, and a VECTOR that stands in for a series line needs both `vectorNetwork` and
// `absoluteTransform`.

const fs = require("fs");
const path = require("path");

const SRC = fs.readFileSync(path.join(__dirname, "verify_page.js"), "utf8");

// `figma.mixed` is a SYMBOL, not a string or null. A node with per-range font sizes reports it as its
// node-level `fontSize`, which is why a `typeof === "number"` gate silently drops the whole node.
const MIXED = Symbol("figma.mixed");

let AUTO = 0;
function node(props) {
  const n = Object.assign({ visible: true, x: 0, y: 0, width: 0, height: 0, type: "FRAME", name: "" }, props);
  if (!n.id) n.id = `auto:${++AUTO}`;
  // `segments: { fontSize: [[v, start, end], ...], fontName: [...] }` serves getStyledTextSegments with
  // the CHARACTER OFFSETS the real API returns. A field the fixture did not declare throws, exactly as
  // an unsupported field would, so the script's try/catch is exercised rather than bypassed.
  if (n.segments) {
    const S = n.segments;
    n.getStyledTextSegments = (fields) => {
      const f = fields[0];
      if (!S[f]) throw new Error(`mock: no ${f} segments declared on "${n.name}"`);
      return S[f].map(([v, start, end]) =>
        f === "fontName" ? { fontName: { family: "Lato", style: v }, start, end } : { [f]: v, start, end });
    };
  }
  if (n.children) for (const c of n.children) c.parent = n;
  if (!("absoluteBoundingBox" in n)) n.absoluteBoundingBox = { x: n.x, y: n.y, width: n.width, height: n.height };
  n.findAll = function (fn) {
    const out = [];
    const rec = (m) => { for (const c of m.children || []) { if (!fn || fn(c)) out.push(c); rec(c); } };
    rec(this);
    return out;
  };
  return n;
}
const solid = (hex) => [{ type: "SOLID", visible: true, color: {
  r: parseInt(hex.slice(1, 3), 16) / 255, g: parseInt(hex.slice(3, 5), 16) / 255, b: parseInt(hex.slice(5, 7), 16) / 255 } }];

const text = (name, chars, size, x, y, w, h, fill, extra) => node(Object.assign({
  type: "TEXT", name, characters: chars, fontSize: size, x, y, width: w, height: h,
  fills: fill ? solid(fill) : [], textStyleId: "", textAutoResize: "HEIGHT",
  layoutSizingVertical: "HUG", layoutGrow: 0, layoutPositioning: "AUTO",
}, extra || {}));

// A series line: VECTOR with a readable vectorNetwork and an identity transform.
const line = (name, pts, weight) => node({
  type: "VECTOR", name, x: Math.min(...pts.map((p) => p[0])), y: Math.min(...pts.map((p) => p[1])),
  width: Math.max(...pts.map((p) => p[0])) - Math.min(...pts.map((p) => p[0])) || 1,
  height: Math.max(...pts.map((p) => p[1])) - Math.min(...pts.map((p) => p[1])) || 1,
  strokeWeight: weight, strokes: solid("#4c6a9c"), dashPattern: [], strokeAlign: "CENTER",
  absoluteTransform: [[1, 0, 0], [0, 1, 0]],
  vectorNetwork: { vertices: pts.map(([x, y]) => ({ x, y })),
                   segments: pts.slice(1).map((_, i) => ({ start: i, end: i + 1 })) },
});

const gridline = (name, y, dashed, dashOverride) => node({
  type: "VECTOR", name, x: 16, y, width: 508, height: 1,
  strokeWeight: 1, strokes: solid("#dddddd"), dashPattern: dashOverride || (dashed ? [4, 4] : []), strokeAlign: "CENTER",
  absoluteTransform: [[1, 0, 16], [0, 1, y]],
  vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 508, y: 0 }], segments: [{ start: 0, end: 1 }] },
});

// A slope chart's end axis lines are VERTICAL — a column 260px tall and 1px wide at the plot's edge —
// which is what separates them from a two-line horizontal grid. Modelling them with the horizontal
// helper above made the fixture contradict its own comment and let a count-only rule look correct.
const vgridline = (name, x, dashOverride) => node({
  type: "VECTOR", name, x, y: 200, width: 1, height: 260,
  strokeWeight: 1, strokes: solid("#dddddd"), dashPattern: dashOverride || [], strokeAlign: "CENTER",
  absoluteTransform: [[1, 0, x], [0, 1, 200]],
  vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 0, y: 260 }], segments: [{ start: 0, end: 1 }] },
});

function buildFrame(opts = {}) {
  const W = opts.frameW || 540;
  const contentW = W - 32;
  const labelSize = opts.labelSize !== undefined ? opts.labelSize : 13;
  const lineWeight = opts.lineWeight !== undefined ? opts.lineWeight : 3;

  const title = text("title", "A title", 25, 16, 16, 428, 29);
  const subtitle = text("subtitle", "A subtitle line", 16, 16, 51, contentW, 19);
  const header = node({ name: "header", layoutMode: "VERTICAL", primaryAxisSizingMode: "AUTO", itemSpacing: 6,
    x: 16, y: 16, width: contentW, height: 92, children: [title, subtitle] });
  // "Data source: X" — the prefix is chars 0-12, the producer name 12-14. `boldSource` models the
  // real defect: assigning `characters` collapses the node to its first run's style, so the bold
  // prefix takes the producer name with it. `sourceTailWeight` sets the tail to any other weight,
  // for the off-contract-but-not-bold cases a not-bold test would wave through. `sourcePrefixWeight`
  // does the same for the PREFIX, where a substring test on /bold/ certifies Semibold and Black.
  const src = text("source", "Data source: X", 13, 16, 488, contentW, 16, undefined, {
    segments: { fontName: opts.boldSource
      ? [["Bold", 0, 14]]
      : [[opts.sourcePrefixWeight || "Bold", 0, 12], [opts.sourceTailWeight || "Regular", 12, 14]] },
  });
  const footer = node({ name: "footer", layoutMode: "VERTICAL", x: 16, y: 488, width: contentW, height: 36, children: [src] });
  const logo = node({ name: "logo", x: W - 80, y: 16, width: 64, height: 35 });

  // `clearedGrid` drops the dash off the gridlines while leaving everything else alone — the shape a
  // rescale-and-repair pass produces when it resets weights and forgets the pattern.
  const gridDashed = !opts.clearedGrid;
  // A real gridline group carries 6-12 lines; three is the minimum that still reads as a GRID rather
  // than a slope chart's pair of end axis lines, which verify_page.js reclassifies as solid by design.
  // `valueNamedZero` models what grapher actually emits: each gridline is named after its TICK VALUE,
  // so the zero line arrives as "0%" and matches none of the zero/tick/axis words. `zeroTickDash` lets a
  // case restyle that node to the grid target, which must still fail.
  // `gridLines` overrides the count, for the two-line grid that a count-only exemption misjudged in
  // both directions; `noZeroLine` keeps that group at exactly that many members.
  const gridKids = opts.gridLines !== undefined
    ? Array.from({ length: opts.gridLines }, (_, i) => gridline(`grid-${i + 1}`, 200 + i * 80, gridDashed))
    : [gridline("grid-1", 200, gridDashed), gridline("grid-2", 300, gridDashed), gridline("grid-3", 360, gridDashed)];
  if (!opts.noZeroLine) {
    if (opts.valueNamedZero) gridKids.push(gridline("0%", 460, false, opts.zeroTickDash));
    else gridKids.push(gridline("zero", 460, false));
  }
  const grid = node({ name: "horizontal-grid-lines", x: 16, y: 200, width: contentW, height: 260, children: gridKids });
  // A slope chart draws exactly two solid VERTICAL lines at its ends, named for the years. A small
  // group of verticals is an axis pair, not a grid, so these must not be judged against [4,4].
  const axisPair = opts.slopeAxisPair
    ? node({ name: "vertical-grid-lines", x: 16, y: 200, width: contentW, height: 260,
             children: [vgridline("1980", 16), vgridline("2023", 523)] })
    : null;
  const kids = [
    grid,
    ...(axisPair ? [axisPair] : []),
    line("line__A", [[40, 440], [200, 300], [440, 260]], lineWeight),
    line("outline__A", [[40, 440], [200, 300], [440, 260]], lineWeight + 1),
    text("label__A", "Country A", labelSize, 450, 250, 60, 16, opts.labelFill || "#4c6a9c"),
    // grapher's stable marker name is on the GROUP; its descendants are unnamed. A leaf-only filter
    // never sees it, which is what let the dot rule sit inert.
    // Deliberately WIDE group with two small dots inside, mirroring grapher: the group's bbox spans
    // the series (measured 210x114 live) while each marker is ~8px. Testing the group's box would
    // flag any annotation over the plot, so this shape is what keeps that regression visible.
    node({ name: "datapoints__A", x: 60, y: 150, width: 250, height: 120,
           children: [node({ type: "ELLIPSE", name: "dp1", x: 300, y: 150, width: 8, height: 8 }),
                      node({ type: "ELLIPSE", name: "dp2", x: 60, y: 262, width: 8, height: 8 })] }),
  ];
  if (opts.extraLine) kids.push(line("line__B", [[40, 420], [200, 340], [440, 300]], opts.extraLine));
  // A second category in a SECOND colour. The palette rows compare pairs, so a one-colour fixture has
  // no pair for them to check and the command is withheld by design — any case asserting on that
  // command therefore needs two colours, or it is asserting on a branch that no longer runs.
  if (opts.secondColour) kids.push(node({ type: "RECTANGLE", name: "bar__B", x: 420, y: 400,
    width: 40, height: 30, fills: solid("#883039") }));
  if (opts.zeroAreaTick) kids.push(node({ name: "horizontal-axis", x: 40, y: 470, width: 400, height: 6, children: [
    node({ type: "VECTOR", name: "tick-0", x: 40, y: 470, width: 0, height: 6,
      strokeWeight: 1, strokes: solid("#dddddd"), dashPattern: opts.tickDash || [], strokeAlign: "CENTER", fills: solid("#000000"),
      absoluteTransform: [[1, 0, 40], [0, 1, 470]],
      vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 0, y: 6 }], segments: [{ start: 0, end: 1 }] } })] }));
  // A map: every country is a stroked non-series plot node at 0.22px by design. Nested under NO
  // axis/grid group, so the furniture rule must not reach it.
  // A segmented bar: a filled rect with NO stroke and NO value label of its own.
  if (opts.barSegment) kids.push(node({ type: "RECTANGLE", name: "bar__A", x: 100, y: 380, width: 160, height: 40,
    fills: solid("#4c6a9c") }));
  // A scatter: `outline__<Entity>` is the RING around a point, with no paired series line — it runs
  // 3.5-4.1px by design and must not be judged against a line halo's bar.
  if (opts.scatterRings) kids.push(node({ name: "points", x: 100, y: 200, width: 200, height: 100, children: [
    node({ type: "ELLIPSE", name: "outline__India", x: 100, y: 200, width: 20, height: 20, strokeWeight: 4.09,
           strokes: solid("#ffffff"), dashPattern: [], fills: solid("#4c6a9c") }),
    node({ type: "ELLIPSE", name: "outline__China", x: 200, y: 250, width: 18, height: 18, strokeWeight: 3.53,
           strokes: solid("#ffffff"), dashPattern: [], fills: solid("#b13507") })] }));
  // A slope chart names its series `slope__<Entity>`, not `line__<Entity>`.
  // Grapher's real slope shape, measured live: `slope__<Entity>` and `outline__<Entity>` are GROUPS of
  // {start-point, end-point, line}, and the only stroked node is called plain `line`. Matching the
  // stroked node's own name finds nothing here.
  if (opts.slopeSeries) {
    const seg = (w) => node({ type: "VECTOR", name: "line", x: 60, y: 200, width: 340, height: 150, strokeWeight: w,
      strokes: solid("#4c6a9c"), dashPattern: [], absoluteTransform: [[1, 0, 0], [0, 1, 0]],
      vectorNetwork: { vertices: [{ x: 60, y: 200 }, { x: 400, y: 350 }], segments: [{ start: 0, end: 1 }] } });
    kids.push(node({ name: "slopes", x: 60, y: 200, width: 340, height: 150, children: [
      node({ name: "outline__USA", x: 60, y: 200, width: 340, height: 150, children: [
        node({ type: "VECTOR", name: "start-point", x: 60, y: 200, width: 6, height: 6 }), seg(opts.slopeHalo || 4)] }),
      node({ name: "slope__USA", x: 60, y: 200, width: 340, height: 150, children: [
        node({ type: "VECTOR", name: "end-point", x: 400, y: 350, width: 6, height: 6 }), seg(opts.slopeWeight || 3)] })] }));
  }
  // A discrete bar's only furniture is a top-level zero line, inside no axis or grid container.
  // `zeroLineDash` stands in for a slope chart's NATIVE [3,2] zero line, which must not be pulled to
  // the gridline target.
  if (opts.zeroLineOnly) kids.push(node({ type: "VECTOR", name: "vertical-zero-line", x: 40, y: 160, width: 1, height: 300,
    strokeWeight: opts.zeroLineOnly, strokes: solid("#333333"), dashPattern: opts.zeroLineDash || [], strokeAlign: "CENTER",
    absoluteTransform: [[1, 0, 40], [0, 1, 160]], vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 0, y: 300 }], segments: [{ start: 0, end: 1 }] } }));
  // Fiji: split across the antimeridian, so its BOX spans the plot at ~4px tall while its ink is two
  // small clusters. It must be excluded from the margins row or it breaches on every map.
  if (opts.straddler) kids.push(node({ name: "map", x: 16, y: 300, width: 400, height: 200, children: [
    node({ type: "VECTOR", name: "Fiji", x: 10, y: 320, width: 505, height: 4, fills: solid("#4c6a9c") }),
    node({ type: "VECTOR", name: "Brazil", x: 150, y: 340, width: 80, height: 60, fills: solid("#b13507") })] }));
  if (opts.mapCountries) kids.push(node({ name: "map", x: 40, y: 160, width: 400, height: 200, children: [
    node({ type: "VECTOR", name: "country__FRA", x: 40, y: 160, width: 60, height: 40, strokeWeight: 0.22,
           strokes: solid("#ffffff"), dashPattern: [], fills: solid("#4c6a9c"),
           absoluteTransform: [[1, 0, 40], [0, 1, 160]], vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 60, y: 40 }], segments: [{ start: 0, end: 1 }] } }),
    node({ type: "VECTOR", name: "country__DEU", x: 120, y: 160, width: 60, height: 40, strokeWeight: 0.33,
           strokes: solid("#ffffff"), dashPattern: [], fills: solid("#b13507"),
           absoluteTransform: [[1, 0, 120], [0, 1, 160]], vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 60, y: 40 }], segments: [{ start: 0, end: 1 }] } })] }));
  if (opts.gappedLine) kids.push(node({ type: "VECTOR", name: "line__G", x: 40, y: 200, width: 400, height: 100,
    strokeWeight: 3, strokes: solid("#b13507"), dashPattern: [], strokeAlign: "CENTER",
    absoluteTransform: [[1, 0, 0], [0, 1, 0]],
    // two disconnected subpaths with a wide gap between vertex 1 and vertex 2
    vectorNetwork: { vertices: [{ x: 40, y: 200 }, { x: 120, y: 220 }, { x: 380, y: 280 }, { x: 440, y: 300 }],
                     segments: [{ start: 0, end: 1 }, { start: 2, end: 3 }] } }));
  // `chartShort` narrows the chart's box by a sub-pixel amount, the shape a rescale-and-repair pass
  // leaves when an ornament stops setting the right edge. It used to slip through a +/-1px gate.
  const chart = node({ name: "chart", type: "GROUP", x: 16, y: 122,
    width: contentW - (opts.chartShort || 0), height: 352, children: kids });

  // `ungrouped` models the documented rework case: the chart GROUP is gone and its subgroups sit as
  // direct frame children, so CONFIG.chartName resolves nothing and the fallback has to find the plot.
  const children = opts.ungrouped ? [header, footer, logo, ...kids] : [header, footer, logo, chart];
  // An OPAQUE layer UNDER the tint — a plot background — pushed first so it sits at the bottom of the
  // stack. With both present the ground behind the annotation is the ORDERED composite of the two, which
  // equals neither shape's own composite over the frame.
  if (opts.tintBase) children.push(node({ type: "RECTANGLE", name: "plot-background", x: 80, y: 180,
    width: 220, height: 80, fills: solid(opts.tintBase) }));
  // A shaded region behind the annotation — the shape whose COMPOSITE the knockout has to match.
  // `tintOpacity` sits on the node, which is where this skill's own wedge carries it; the halo must
  // match `tint over frameFill`, never the raw `tint`.
  if (opts.tint) children.push(node({ type: "RECTANGLE", name: "wedge__below", x: 90, y: 190,
    width: 200, height: 60, fills: solid(opts.tint),
    opacity: opts.tintOpacity === undefined ? 0.45 : opts.tintOpacity }));
  if (opts.annotation) children.push(opts.annotation);

  return node({ id: "F:1", name: "test frame", x: 0, y: 0, width: W, height: opts.frameH || 540,
                fills: solid(opts.frameFill || "#ffffff"), children });
}

// `strokeVisible: false` / `strokeOpacity: 0` model a knockout paint that is PRESENT but renders
// nothing — `strokes.length` counts it either way. `decoyStroke` puts such a paint in FRONT of the real
// one, which is what makes reading `strokes[0]` the wrong paint rather than merely a redundant one.
const annotation = (o) => text("annotation__test", o.chars || "Note", o.size || 14, o.x, o.y, o.w || 100, o.h || 18,
  o.fill || "#2d2e2d", {
    strokes: o.stroke
      ? (o.decoyStroke
          ? [Object.assign(solid(o.decoyStroke)[0], { visible: false }), solid(o.stroke)[0]]
          : [Object.assign(solid(o.stroke)[0],
              Object.assign({}, o.strokeVisible === false ? { visible: false } : {},
                                o.strokeOpacity !== undefined ? { opacity: o.strokeOpacity } : {}))])
      : [],
    strokeWeight: o.strokeWeight !== undefined ? o.strokeWeight : 0,
    strokeAlign: o.strokeAlign || "OUTSIDE",
    textStyleId: o.styleId === undefined ? "S:abc" : o.styleId,
  });

// `wrap` puts the frame under an ancestor — a section or a group — because two of the switches that
// decide whether a frame renders at all live ABOVE it, and a page whose only child is the frame cannot
// model either. It takes the frame and returns whatever should sit on the page in its place.
async function run(frame, config, wrap) {
  const byId = {};
  const index = (n) => { if (n.id) byId[n.id] = n; for (const c of n.children || []) index(c); };
  const page = node({ id: "P:1", type: "PAGE", name: "page", children: [wrap ? wrap(frame) : frame] });
  index(page);
  const figma = { currentPage: page, getNodeByIdAsync: async (id) => byId[id] || null, setCurrentPageAsync: async () => {} };
  const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m, "const CONFIG = __CONFIG__;");
  const fn = new Function("figma", "__CONFIG__", `return (async () => { ${body} })();`);
  return fn(figma, Object.assign({ frameId: "F:1", chartName: "chart", gapTarget: null, tightlyMeasured: false, highlightTreatment: false, textFloor: null }, config));
}

const results = [];
const check = (name, cond, detail) => results.push({ name, ok: !!cond, detail: cond ? "" : String(detail).slice(0, 220) });
const row = (out, name) => out.rows.find((x) => x.check === name);

(async () => {
  // 1 — clean 540 frame: the mechanical rows pass, and every uncovered row is DECLARED.
  {
    const out = await run(buildFrame(), {});
    check("1 text-floor ok", row(out, "text-floor").status === "ok", row(out, "text-floor").detail);
    check("1 floor is 12 on a 540 frame", /floor 12px/.test(row(out, "text-floor").detail), row(out, "text-floor").detail);
    check("1 ladder-sizes ok at 13px", row(out, "ladder-sizes").status === "ok", row(out, "ladder-sizes").detail);
    check("1 series-weight ok at 3/4", row(out, "series-weight").status === "ok", row(out, "series-weight").detail);
    check("1 furniture-weight ok", row(out, "furniture-weight").status === "ok", row(out, "furniture-weight").detail);
    check("1 furniture-dash ok", row(out, "furniture-dash").status === "ok", row(out, "furniture-dash").detail);
    check("1 polylines sampled", row(out, "polylines").status === "ok", row(out, "polylines").detail);
    // the fifth review finding: BOTH contrast rows must exist, one computed and one declared
    check("1 label-contrast-on-background present", !!row(out, "label-contrast-on-background"), "row missing");
    check("1 label-contrast-on-fill DECLARED", row(out, "label-contrast-on-fill") && row(out, "label-contrast-on-fill").status === "SKIPPED", "row missing");
    // Every row CHECKS.md prescribes has to EXIST, even where it is not computed — a prescribed check
    // with no row is how a run reports "no mechanical row failed" and means "nobody looked".
    check("1 page-census DECLARED", row(out, "page-census") && row(out, "page-census").status === "SKIPPED", "row missing");
    check("1 page-census says COUNT, not overlap",
          /count the plot-bearing objects/i.test(row(out, "page-census").detail) && /overlap test/i.test(row(out, "page-census").detail),
          row(out, "page-census").detail);
    // leader-on-map is declared, but it must declare the VECTOR test as the method and pixels as the
    // fallback. Named backwards it sends the reader past a one-call exact check.
    check("1 leader-on-map DECLARED", row(out, "leader-on-map") && row(out, "leader-on-map").status === "SKIPPED", "row missing");
    check("1 leader-on-map prescribes vectors first",
          /VECTORS first/.test(row(out, "leader-on-map").detail) && /FALLBACK/.test(row(out, "leader-on-map").detail),
          row(out, "leader-on-map").detail);
    check("1 leader-on-map does not call the bbox the target", /not its bounding box/.test(row(out, "leader-on-map").detail), row(out, "leader-on-map").detail);
    check("1 no row silently absent", out.rows.length >= 26, `${out.rows.length} rows`);
  }

  // 2 — the 302-wide floor. 11px text is legitimate there (SMALL-CHARTS.md), a failure on a 540.
  {
    const small = await run(buildFrame({ frameW: 302, frameH: 400, labelSize: 11 }), {});
    check("2 302-wide derives an 11px floor", /floor 11px/.test(row(small, "text-floor").detail), row(small, "text-floor").detail);
    check("2 302-wide 11px text passes", row(small, "text-floor").status === "ok", row(small, "text-floor").detail);
    check("2 302-wide ladder includes 11", row(small, "ladder-sizes").status === "ok", row(small, "ladder-sizes").detail);
    const big = await run(buildFrame({ labelSize: 11 }), {});
    check("2 540-wide 11px text FAILS", row(big, "text-floor").status === "FAIL", row(big, "text-floor").detail);
    const override = await run(buildFrame({ labelSize: 11 }), { textFloor: 11 });
    check("2 CONFIG.textFloor overrides", row(override, "text-floor").status === "ok" && /from CONFIG/.test(row(override, "text-floor").detail), row(override, "text-floor").detail);
  }

  // 3 — the ladder verdict is split by WHO set the size. An imported label at 13.36 is 0.36 from a
  // rung, which is ordinary fit drift: reported, not failed, because judged strictly this row failed on
  // 8 of 8 real frames and a row that always fails carries no information. An ANNOTATION at the same
  // size is authored here, so it still fails. And a size too far from any rung to be drift fails too.
  {
    const out = await run(buildFrame({ labelSize: 13.36 }), {});
    const d = row(out, "ladder-sizes");
    check("3 an imported label 0.36 off a rung is REVIEW, not FAIL", d.status === "REVIEW", d.detail);
    check("3 names the offending size", /13\.36/.test(d.detail), d.detail);
    check("3 and reports the distance to the nearest rung", d.maxDriftFromRung === 0.36, String(d.maxDriftFromRung));

    const wayOff = await run(buildFrame({ labelSize: 5.99 }), {});
    const dw = row(wayOff, "ladder-sizes");
    check("3 but 5.99px is too far from any rung and FAILS", dw.status === "FAIL", dw.detail);
    check("3 and says why it is not fit drift", /too far to be fit drift/.test(dw.detail), dw.detail);

    const annOff = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, size: 13.36, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    const da = row(annOff, "ladder-sizes");
    check("3 an ANNOTATION off the ladder still FAILS", da.status === "FAIL" && da.annotationsOffLadder > 0, da.detail);
    check("3 and says an annotation is authored here", /authored here/.test(da.detail), da.detail);
  }

  // 4 — an annotation crossing furniture with NO knockout (review finding 3: the old code passed it).
  {
    const out = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: null, strokeWeight: 0 }) }), {});
    check("4 missing knockout over furniture FAILS", row(out, "annotation-knockout").status === "FAIL", row(out, "annotation-knockout").detail);
    check("4 says NO knockout", /carries NO knockout/.test(row(out, "annotation-knockout").detail), row(out, "annotation-knockout").detail);
  }

  // 5 — an annotation over empty space must NOT carry a stroke.
  {
    const out = await run(buildFrame({ annotation: annotation({ x: 60, y: 140, w: 90, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("5 needless knockout FAILS", row(out, "annotation-knockout").status === "FAIL", row(out, "annotation-knockout").detail);
    check("5 says crosses nothing", /crosses nothing/.test(row(out, "annotation-knockout").detail), row(out, "annotation-knockout").detail);
  }

  // 6b — a knockout paint that is PRESENT but renders nothing is a missing knockout. `strokes.length`
  //      counts a paint switched off or made transparent, so the row passed the weight, alignment and
  //      colour checks on an annotation whose knockout paints no pixels at all.
  {
    const off = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3, strokeVisible: false }) }), {});
    check("6b an invisible knockout paint counts as NO knockout",
          row(off, "annotation-knockout").status === "FAIL" && /carries NO knockout/.test(row(off, "annotation-knockout").detail),
          row(off, "annotation-knockout").detail);
    const clear = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3, strokeOpacity: 0 }) }), {});
    check("6b and so does a fully transparent one",
          row(clear, "annotation-knockout").status === "FAIL" && /carries NO knockout/.test(row(clear, "annotation-knockout").detail),
          row(clear, "annotation-knockout").detail);
    // 6b-2 — between the two. A knockout works by PAINTING the frame's colour over what it crosses,
    //        so a nearly transparent one masks nothing while still passing the weight, alignment and
    //        colour checks — a clean `ok` on a crossing the reader can see straight through. Zero is
    //        the case above; anything positive is on the canvas but cannot be certified from here,
    //        since how much it masks depends on what is behind it. REVIEW, with the number named.
    const faintKO = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3, strokeOpacity: 0.005 }) }), {});
    check("6b-2 a nearly transparent knockout is not certified",
          row(faintKO, "annotation-knockout").status === "REVIEW", row(faintKO, "annotation-knockout").detail);
    check("6b-2 and the effective opacity is named, not just flagged",
          /0\.005|effective opacity/.test(row(faintKO, "annotation-knockout").detail), row(faintKO, "annotation-knockout").detail);
    // The NODE's own opacity dims the knockout just as effectively as the paint's, and it is the half
    // a paint-only read cannot see: this paint is fully opaque.
    const dimAnn = annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 });
    dimAnn.opacity = 0.4;
    const dimKO = await run(buildFrame({ annotation: dimAnn }), {});
    check("6b-2 a dimmed annotation node is not certified either",
          row(dimKO, "annotation-knockout").status === "REVIEW", row(dimKO, "annotation-knockout").detail);
    // The control: a fully opaque knockout still passes cleanly, or this becomes a row that always
    // reviews and therefore says nothing.
    const solidKO = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("6b-2 while an opaque knockout still passes",
          row(solidKO, "annotation-knockout").status === "ok", row(solidKO, "annotation-knockout").detail);
    // ...and the COLOUR check must read the paint that renders, not strokes[0]
    const decoy = await run(buildFrame({ frameFill: "#fffbf5", annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#fffbf5", strokeWeight: 3, decoyStroke: "#ffffff" }) }), {});
    check("6b an invisible paint in front does not become the colour that is judged",
          row(decoy, "annotation-knockout").status === "ok", row(decoy, "annotation-knockout").detail);
    const visible = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("6b while a real visible knockout still passes", row(visible, "annotation-knockout").status === "ok", row(visible, "annotation-knockout").detail);
  }

  // 6c — a text node whose sizes cannot be READ must not leave text-floor at a clean `ok`. It is
  //      recorded in the detail, but the status came from `under.length` alone — which is 0 when the
  //      only suspect node is the one nobody could measure, so the frame certified with an
  //      uninspected range on it.
  {
    const f = buildFrame();
    const chart = f.children.find((c) => c.name === "chart");
    // MIXED fontSize and NO declared segments: the mock has no getStyledTextSegments, so the read
    // throws and sizeRanges() returns [] — exactly the swallow being tested.
    chart.children.push(text("label__unreadable", "?", MIXED, 300, 240, 40, 16, "#4c6a9c", { fontSize: MIXED }));
    chart.children[chart.children.length - 1].parent = chart;
    const out = await run(f, {});
    check("6c an unreadable text node makes text-floor REVIEW, not ok",
          row(out, "text-floor").status === "REVIEW", row(out, "text-floor").detail);
    check("6c and it is named in the detail",
          /NOT judged/.test(row(out, "text-floor").detail) && /label__unreadable/.test(row(out, "text-floor").detail),
          row(out, "text-floor").detail);
    // and it has to reach the FRAME verdict, not just the row: the top line is what gets read.
    check("6c and the frame verdict counts it among the rows to review",
          /no mechanical row failed \(\d+ to review/.test(out.verdict) && out.rows.filter((x) => x.status === "REVIEW").length >= 1,
          out.verdict);
    // a real breach still outranks it, and a clean frame is still ok
    check("6c a clean frame is still ok", row(await run(buildFrame(), {}), "text-floor").status === "ok", "");
    const under = await run(buildFrame({ labelSize: 8 }), {});
    check("6c and a genuine breach still FAILS", row(under, "text-floor").status === "FAIL", row(under, "text-floor").detail);
  }

  // 6 — knockout colour must be the frame's own fill, never hardcoded white.
  {
    const out = await run(buildFrame({ frameFill: "#fffbf5", annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("6 white knockout on cream FAILS",
          row(out, "annotation-knockout").status === "FAIL"
          && /the frame's own #fffbf5/.test(row(out, "annotation-knockout").detail),
          row(out, "annotation-knockout").detail);
  }

  // 6d — the knockout's ground is what is behind THIS annotation, not the frame. Demanding the frame's
  //      fill unconditionally FAILED a correct chart: an annotation inside a tinted region takes a halo
  //      the colour of the tint, and a canvas-coloured one there is a white outline around every letter.
  {
    // #dddddd at 45% over white composites to #f0f0f0 — what the reader sees, and what the halo must be.
    const onTint = (stroke) => buildFrame({ tint: "#dddddd", tintOpacity: 0.45,
      annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke, strokeWeight: 3 }) });

    const good = await run(onTint("#f0f0f0"), {});
    const g = row(good, "annotation-knockout");
    check("6d a halo matching the tint's COMPOSITE does not FAIL", g.status !== "FAIL", g.detail);
    check("6d and it is REVIEWED, since the ground was matched by bounding box",
          g.status === "REVIEW" && /BOUNDING BOX/.test(g.detail), g.detail);
    check("6d and the detail shows the sum it accepted",
          /wedge__below/.test(g.detail) && /#dddddd/.test(g.detail) && /0\.45/.test(g.detail), g.detail);

    // The trap the compositing exists for: matching the tint's RAW fill is still wrong, because that
    // is not the colour on the canvas. A check that skipped the alpha would wave this through.
    const raw = row(await run(onTint("#dddddd"), {}), "annotation-knockout");
    check("6d a halo matching the tint's RAW fill still FAILS", raw.status === "FAIL", raw.detail);
    check("6d and it names the composite it wanted instead", /#f0f0f0/.test(raw.detail), raw.detail);

    // A canvas-coloured halo over shading is the original defect, and the bbox cannot prove the
    // annotation is over the tint's ink rather than beside it — so it is raised, not failed.
    const white = row(await run(onTint("#ffffff"), {}), "annotation-knockout");
    check("6d a canvas halo over a tint is REVIEWED, not passed silently", white.status === "REVIEW", white.detail);
    check("6d and it names the white-outline symptom", /white outline/.test(white.detail), white.detail);

    // ...and with no tint on the frame, the same canvas halo stays clean: the review must be caused by
    // the shading, not fire on every annotation.
    const bare = row(await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {}), "annotation-knockout");
    check("6d and a canvas halo on bare canvas is still ok", bare.status === "ok", bare.detail);
  }

  // 6e — the TIER branch has to know about the ground too. An annotation inside a tint that happens to
  //      cross no gridline hit `!crosses.length && hasStroke` and FAILED as "over empty space" before
  //      the colour test ever ran — which is the opposite of what ANNOTATIONS-AND-ARROWS.md now says:
  //      the halo stays on a tint precisely because a region that is clear today fills at the next
  //      refresh. Test 5 above is the control that keeps the FAIL alive on genuinely bare canvas.
  {
    // y=205..223 clears the gridlines at 200/300/360/460 and both series segments, so nothing is crossed.
    const clear = (extra) => buildFrame(Object.assign({ tint: "#dddddd", tintOpacity: 0.45,
      annotation: annotation({ x: 100, y: 205, w: 120, h: 18, stroke: "#f0f0f0", strokeWeight: 3 }) }, extra || {}));
    const t = row(await run(clear(), {}), "annotation-knockout");
    check("6e a halo on an EMPTY tint is not failed as 'over empty space'", t.status !== "FAIL", t.detail);
    check("6e and it is REVIEWED, naming the tint that earns the halo",
          t.status === "REVIEW" && /keeps its halo even where the tint is empty/.test(t.detail), t.detail);
  }

  // 6f — grounds STACK. A translucent tint over an opaque plot background renders as the ordered
  //      composite of both; compositing each candidate over the FRAME instead matches neither, and a
  //      correct halo came back FAIL.
  {
    // #dddddd at 45% over #eeeeee is #e6e6e6. Over the frame's white it would be #f0f0f0, and the base
    // alone is #eeeeee — so a per-candidate test rejects the one colour the reader actually sees.
    const stack = (stroke) => buildFrame({ tint: "#dddddd", tintOpacity: 0.45, tintBase: "#eeeeee",
      annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke, strokeWeight: 3 }) });

    const ok = row(await run(stack("#e6e6e6"), {}), "annotation-knockout");
    check("6f a halo matching the STACKED ground does not FAIL", ok.status !== "FAIL", ok.detail);
    check("6f and it names the paint order it folded",
          ok.status === "REVIEW" && /composited in paint order/.test(ok.detail)
          && /plot-background/.test(ok.detail) && /wedge__below/.test(ok.detail), ok.detail);

    // Matching nothing is still not a FAIL once the grounds overlap: any SUBSET of the stack is a
    // possible ground and bounding boxes cannot say which, so the call goes to a human.
    const miss = row(await run(stack("#123456"), {}), "annotation-knockout");
    check("6f overlapping grounds downgrade an unmatched halo to REVIEW", miss.status === "REVIEW", miss.detail);
    check("6f and it says why it cannot decide", /cannot be settled from BOUNDING BOXES/.test(miss.detail), miss.detail);

    // ...and with a SINGLE ground there is nothing to be ambiguous about, so the FAIL stands.
    const one = row(await run(buildFrame({ tint: "#dddddd", tintOpacity: 0.45,
      annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#123456", strokeWeight: 3 }) }), {}), "annotation-knockout");
    check("6f but a single ground still FAILS an unmatched halo", one.status === "FAIL", one.detail);
  }

  // 7 — crossing a MUTED context line is legal under the highlight treatment (review finding 4).
  {
    const ann = annotation({ x: 150, y: 330, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 });
    const muted = await run(buildFrame({ lineWeight: 1, annotation: ann }), { highlightTreatment: true });
    check("7 muted 1px crossing is legal", row(muted, "annotation-overlap").status === "ok", row(muted, "annotation-overlap").detail);
    const protag = await run(buildFrame({ lineWeight: 3, annotation: ann }), { highlightTreatment: true });
    check("7 protagonist 3px crossing FAILS", row(protag, "annotation-overlap").status === "FAIL", row(protag, "annotation-overlap").detail);
    const plain = await run(buildFrame({ lineWeight: 3, annotation: ann }), {});
    check("7 without the treatment, any series crossing FAILS", row(plain, "annotation-overlap").status === "FAIL", row(plain, "annotation-overlap").detail);
  }

  // 8 — covering a dot is never legal, WITHOUT also crossing the line. The earlier version of this
  // case put the annotation over both and asserted /a dot/, which matched the row's stock explanatory
  // suffix ("...a dot or a value label is not") rather than a finding — a vacuous assertion in the
  // harness built to catch vacuous checks. Assert on the NODE NAME instead.
  {
    const out = await run(buildFrame({ annotation: annotation({ x: 298, y: 148, w: 16, h: 14, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    const d = row(out, "annotation-overlap").detail;
    check("8 covering a marker group FAILS", row(out, "annotation-overlap").status === "FAIL", d);
    check("8 names datapoints__A specifically", /covers datapoints__A/.test(d), d);
    // and the group's own wide bbox must NOT be the test surface: an annotation inside the group's
    // box but away from every marker is legal.
    const inGroupBoxOnly = await run(buildFrame({ annotation: annotation({ x: 150, y: 190, w: 60, h: 16, stroke: null, strokeWeight: 0 }) }), {});
    const d2 = row(inGroupBoxOnly, "annotation-overlap").detail;
    check("8 group bbox is not the test surface", !/datapoints__A/.test(d2), d2);
  }

  // 12 — a gapped series must not invent a stroke across the gap (segments, not vertex order).
  {
    const inGap = await run(buildFrame({ gappedLine: true, annotation: annotation({ x: 200, y: 240, w: 120, h: 18, stroke: null, strokeWeight: 0 }) }), {});
    const d = row(inGap, "annotation-overlap").detail;
    check("12 annotation inside the gap does NOT cross line__G", !/crosses line__G/.test(d), d);
    check("12 connectivity came from segments", /from vectorNetwork\.segments/.test(row(inGap, "polylines").detail), row(inGap, "polylines").detail);
    // and it must still be caught when it genuinely sits on a drawn subpath
    const onPath = await run(buildFrame({ gappedLine: true, annotation: annotation({ x: 60, y: 200, w: 70, h: 18, stroke: null, strokeWeight: 0 }) }), {});
    check("12 annotation on a drawn subpath DOES cross", /crosses line__G/.test(row(onPath, "annotation-overlap").detail), row(onPath, "annotation-overlap").detail);
  }

  // 13 — a zero-area tick's default black fill must not appear as an off-palette colour.
  {
    const out = await run(buildFrame({ zeroAreaTick: true }), {});
    const d = row(out, "off-palette").detail;
    check("13 phantom #000000 excluded from fills", !/#000000/.test(d), d);
    const without = await run(buildFrame(), {});
    const n = (x) => Number(/all (\d+) furniture/.exec(row(x, "furniture-weight").detail)[1]);
    check("13 the tick's STROKE is still counted", n(out) === n(without) + 1, `${n(without)} -> ${n(out)} furniture strokes`);
  }

  // 13b — box-alignment is EXACT, not "within a pixel". The old gate allowed +/-1px, which passed a
  // chart ending 0.57px short of the content box — invisible in a render, plainly wrong in the
  // properties panel next to a header and footer that do land on it.
  {
    const exact = await run(buildFrame(), {});
    check("13b an exactly-aligned chart passes", row(exact, "box-alignment").status === "ok", row(exact, "box-alignment").detail);

    const short = await run(buildFrame({ chartShort: 0.57 }), {});
    check("13b a 0.57px shortfall now FAILS", row(short, "box-alignment").status === "FAIL", row(short, "box-alignment").detail);
    check("13b and the failure points at the re-pin recipe", /re-pin per FITTING\.md/.test(row(short, "box-alignment").detail), row(short, "box-alignment").detail);

    // float residue from rescale must still pass, or every fitted page fails on arithmetic noise
    const noise = await run(buildFrame({ chartShort: 0.004 }), {});
    check("13b rescale float residue still passes", row(noise, "box-alignment").status === "ok", row(noise, "box-alignment").detail);

    // and the tolerance is stated in the row, so a reader knows what it is being held to
    check("13b the row states the tolerance", /must be exact to 0\.05/.test(row(exact, "box-alignment").detail), row(exact, "box-alignment").detail);
  }

  // 14 — the rest of the 302-wide geometry, not just the text floor.
  {
    const out = await run(buildFrame({ frameW: 302, frameH: 400, labelSize: 11 }), {});
    check("14 box-alignment SKIPPED on 302", row(out, "box-alignment").status === "SKIPPED", row(out, "box-alignment").detail);
    check("14 gap SKIPPED on 302", row(out, "gap").status === "SKIPPED", row(out, "gap").detail);
    check("14 margins use the FORMAT bounds", /from the 302-wide FORMAT/.test(row(out, "margins").detail), row(out, "margins").detail);
    const big = await run(buildFrame(), {});
    check("14 540-wide still checks box-alignment", row(big, "box-alignment").status !== "SKIPPED", row(big, "box-alignment").detail);
    check("14 540-wide still checks gap", row(big, "gap").status !== "SKIPPED", row(big, "gap").detail);
    check("14 540-wide margins use the header box", /from the header box/.test(row(big, "margins").detail), row(big, "margins").detail);
  }

  // 9 — the computed contrast row.
  {
    const good = await run(buildFrame({ labelFill: "#4c6a9c" }), {});
    check("9 blue on white clears 4.5:1", row(good, "label-contrast-on-background").status === "ok", row(good, "label-contrast-on-background").detail);
    const bad = await run(buildFrame({ labelFill: "#c8c8c8" }), {});
    check("9 light gray on white FAILS", row(bad, "label-contrast-on-background").status === "FAIL", row(bad, "label-contrast-on-background").detail);
    check("9 reports the ratio", /:1/.test(row(bad, "label-contrast-on-background").detail), row(bad, "label-contrast-on-background").detail);
  }

  // 10 — annotations are FRAME children; the walk must find them (the bug the Figma run caught).
  {
    const out = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("10 annotation rows are not skipped", row(out, "annotation-ladder").status !== "SKIPPED", row(out, "annotation-ladder").detail);
    check("10 knockout row is not skipped", row(out, "annotation-knockout").status !== "SKIPPED", row(out, "annotation-knockout").detail);
  }

  // 11 — the ladder ceiling, and text hierarchy against the subtitle (structural resolution).
  {
    const out = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, size: 17, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("11 17px annotation off-ladder", row(out, "annotation-ladder").status === "FAIL", row(out, "annotation-ladder").detail);
    check("11 17px exceeds the 16px subtitle", row(out, "text-hierarchy").status === "FAIL", row(out, "text-hierarchy").detail);
    check("11 subtitle resolved as the header's 2nd TEXT", /16px/.test(row(out, "text-hierarchy").detail), row(out, "text-hierarchy").detail);
  }

  // 15 — a map's country strokes are NOT furniture (0.22px by design, per-chart-type/maps.md).
  {
    const out = await run(buildFrame({ mapCountries: true }), {});
    const d = row(out, "furniture-weight").detail;
    check("15 map country strokes not judged as furniture", row(out, "furniture-weight").status === "ok" && !/0\.22|0\.33/.test(d), d);
    // A width-first map centred in the band has ~49px gaps by construction; the band rule must not
    // fail it, and box-alignment must say it is the binding axis.
    check("15 gap SKIPPED on a map", row(out, "gap").status === "SKIPPED" && /projection/.test(row(out, "gap").detail), row(out, "gap").detail);
    check("15 box-alignment flags itself as binding", /BINDING axis/.test(row(out, "box-alignment").detail), row(out, "box-alignment").detail);
    const notMap = await run(buildFrame(), {});
    check("15 a non-map still checks gap", row(notMap, "gap").status !== "SKIPPED", row(notMap, "gap").detail);
  }

  // 16 — the highlight bar is a RELATIONSHIP, not a set of allowed numbers.
  {
    const ok1 = await run(buildFrame({ lineWeight: 1 }), { highlightTreatment: true });   // 1px line, 2px halo
    check("16 1px context with its 2px halo passes", row(ok1, "series-weight").status === "ok", row(ok1, "series-weight").detail);
    const ok3 = await run(buildFrame({ lineWeight: 3 }), { highlightTreatment: true });   // 3px line, 4px halo (line+1)
    check("16 3px protagonist with a 4px halo passes", row(ok3, "series-weight").status === "ok", row(ok3, "series-weight").detail);
    const bad = await run(buildFrame({ lineWeight: 4 }), { highlightTreatment: true });   // 4px line — never valid
    check("16 a 4px series line FAILS", row(bad, "series-weight").status === "FAIL", row(bad, "series-weight").detail);
  }

  // 17 — XL 16 is legal only when declared.
  {
    const undeclared = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, size: 16, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("17 undeclared 16px annotation FAILS", row(undeclared, "annotation-ladder").status === "FAIL", row(undeclared, "annotation-ladder").detail);
    const declared = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, size: 16, stroke: "#ffffff", strokeWeight: 3 }) }), { xlAnnotations: true });
    check("17 declared XL passes", row(declared, "annotation-ladder").status === "ok", row(declared, "annotation-ladder").detail);
  }

  // 18 — a mixed-weight annotation legitimately has no node-level style id.
  {
    const mixedAnn = annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3, styleId: "" });
    mixedAnn.getStyledTextSegments = () => [{ fontName: { family: "Lato", style: "Regular" } }, { fontName: { family: "Lato", style: "Bold" } }];
    const out = await run(buildFrame({ annotation: mixedAnn }), {});
    check("18 mixed-weight annotation exempt from binding", row(out, "named-styles").status === "ok", row(out, "named-styles").detail);
    check("18 and says why", /mixed-weight/.test(row(out, "named-styles").detail), row(out, "named-styles").detail);
    const single = annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3, styleId: "" });
    single.getStyledTextSegments = () => [{ fontName: { family: "Lato", style: "Regular" } }];
    const out2 = await run(buildFrame({ annotation: single }), {});
    check("18 single-weight unbound still FAILS", row(out2, "named-styles").status === "FAIL", row(out2, "named-styles").detail);
  }

  // 18b — a WHOLLY BOLD annotation is unbindable for the same reason a mixed-weight one is: the ladder is
  // all Lato Regular, so applying the style strips the bold. GUIDELINES.md prescribes size-without-binding
  // for bold country names, and the design team's own finished highlight map (`273:320`) ships nine of them
  // at 12px Lato Bold with an empty textStyleId. Before this exemption the row fired on all nine.
  {
    const boldAnn = annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3, styleId: "" });
    boldAnn.getStyledTextSegments = () => [{ fontName: { family: "Lato", style: "Bold" } }];
    const out = await run(buildFrame({ annotation: boldAnn }), {});
    check("18b wholly-bold unbound annotation is exempt", row(out, "named-styles").status === "ok", row(out, "named-styles").detail);
    check("18b and says why, naming the weight", /wholly-bold/.test(row(out, "named-styles").detail) && /Bold/.test(row(out, "named-styles").detail),
          row(out, "named-styles").detail);
    // and the exemption must not swallow the defect it sits next to
    const regularAnn = annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3, styleId: "" });
    regularAnn.getStyledTextSegments = () => [{ fontName: { family: "Lato", style: "Regular" } }];
    const still = await run(buildFrame({ annotation: regularAnn }), {});
    check("18b an unbound REGULAR annotation still FAILS", row(still, "named-styles").status === "FAIL", row(still, "named-styles").detail);
    check("18b and the message says REGULAR", /REGULAR/.test(row(still, "named-styles").detail), row(still, "named-styles").detail);
    // A heavier bold-family face is the same case as Bold, and exempt for the same reason.
    const semiAnn = annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3, styleId: "" });
    semiAnn.getStyledTextSegments = () => [{ fontName: { family: "Lato", style: "Semibold" } }];
    const semi = await run(buildFrame({ annotation: semiAnn }), {});
    check("18b Semibold is exempt too", row(semi, "named-styles").status === "ok", row(semi, "named-styles").detail);
  }

  // 18b-2 — the exemption is keyed on the weight being BOLD, not on it merely being "not Regular".
  // Written the loose way (`weights[0] !== "Regular"`) it swallowed every other single-weight face —
  // Light, Medium, Italic — none of which GUIDELINES.md licenses, and reported them back to the reader
  // as "wholly-bold", which is a false statement about the page. Binding is a real defect there.
  {
    for (const weight of ["Light", "Medium", "Italic"]) {
      const ann = annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3, styleId: "" });
      ann.getStyledTextSegments = () => [{ fontName: { family: "Lato", style: weight } }];
      const out = await run(buildFrame({ annotation: ann }), {});
      check(`18b-2 an unbound ${weight} annotation FAILS`, row(out, "named-styles").status === "FAIL",
            row(out, "named-styles").detail);
      check(`18b-2 ${weight} is NOT called wholly-bold`, !/wholly-bold/.test(row(out, "named-styles").detail),
            row(out, "named-styles").detail);
      check(`18b-2 ${weight} is named in the message`, new RegExp(weight).test(row(out, "named-styles").detail),
            row(out, "named-styles").detail);
    }
  }

  // 18c — label-contrast-on-background used to require `insidePlot`, which made it DEAD: annotations are
  // appended to the FRAME, so insidePlot is false for every one and `insidePlot && /^annotation__/` is a
  // contradiction. A nine-label map reported SKIPPED "no annotation text with a solid fill" while carrying
  // nine filled annotations. Same can't-fail family as the `annotations` walk this file already fixed.
  {
    const dark = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, fill: "#2d2e2d", stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("18c a frame-level annotation IS judged", row(dark, "label-contrast-on-background").status === "ok",
          row(dark, "label-contrast-on-background").detail);
    check("18c and the row is not skipped", row(dark, "label-contrast-on-background").status !== "SKIPPED",
          row(dark, "label-contrast-on-background").detail);
    const pale = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, fill: "#bbbbbb", stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("18c a pale annotation on white FAILS 4.5:1", row(pale, "label-contrast-on-background").status === "FAIL",
          row(pale, "label-contrast-on-background").detail);
    // A label in the frame's own colour is AMBIGUOUS, not settled: white-on-a-dark-mark is a correct
    // label (GUIDELINES.md → maps) and measuring it against a white frame reports 1:1, but white text
    // that landed on the white FRAME is unreadable and a real defect. Matching colours are no evidence
    // either way. It must not be routed to label-contrast-on-fill, which is a DECLARED gap — anything
    // sent there is reported by nobody, so an invisible annotation would leave no row at all.
    const onMark = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, fill: "#ffffff", stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("18c a label in the frame's own colour is not failed", row(onMark, "label-contrast-on-background").status !== "FAIL",
          row(onMark, "label-contrast-on-background").detail);
    check("18c nor silently dropped — it is REVIEW", row(onMark, "label-contrast-on-background").status === "REVIEW",
          row(onMark, "label-contrast-on-background").status);
    check("18c and names both readings", /inside a darker mark/.test(row(onMark, "label-contrast-on-background").detail)
          && /invisible text/.test(row(onMark, "label-contrast-on-background").detail),
          row(onMark, "label-contrast-on-background").detail);
    check("18c and tells the reader to check by eye", /by eye/.test(row(onMark, "label-contrast-on-background").detail),
          row(onMark, "label-contrast-on-background").detail);
    // a real contrast failure elsewhere still outranks the ambiguity
    check("18c a measurable failure still FAILS rather than REVIEWs", row(pale, "label-contrast-on-background").status === "FAIL",
          row(pale, "label-contrast-on-background").status);
  }

  // 18d — colour is only ONE of the two ways a label can be unjudgeable against the frame. The other is
  // GEOMETRY: an annotation sitting on a MAP SHAPE has a country's fill behind it, not the frame's, and a
  // country's bbox is deliberately not judged by annotation-overlap (a bbox is not its ink, maps.md) — so
  // dark text on a dark country was measured against the WHITE FRAME, scored well over 4.5:1 and passed,
  // with no other row looking. Over a NON-map mark the position is already illegal and annotation-overlap
  // FAILs it, so ordinary charts keep an informative ok/FAIL here instead of a blanket REVIEW.
  {
    // `mapCountries` puts France (#4c6a9c) at 40-100 x 160-200; the annotation is placed over it.
    const onCountry = await run(buildFrame({
      mapCountries: true,
      annotation: annotation({ x: 45, y: 165, w: 50, h: 16, fill: "#2d2e2d", stroke: "#ffffff", strokeWeight: 3 }),
    }), {});
    const rowA = row(onCountry, "label-contrast-on-background");
    check("18d a label over a map shape is not certified ok", rowA.status === "REVIEW", rowA.status + " " + rowA.detail);
    check("18d and the map shape it overlaps is named", /map shape/.test(rowA.detail), rowA.detail);
    check("18d and the country is identified", /country__FRA/.test(rowA.detail), rowA.detail);
    check("18d and the ratio against that fill is given", /#4c6a9c = /.test(rowA.detail), rowA.detail);
    // no map shape under it: the row stays informative rather than reviewing everything
    const clear = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, fill: "#2d2e2d", stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("18d a label clear of every mark is still judged ok", row(clear, "label-contrast-on-background").status === "ok",
          row(clear, "label-contrast-on-background").detail);
  }

  // 18e — the footer's source line: bold on the prefix ONLY. A real run shipped it bold throughout
  // and every other row passed, because nothing else inspects weight outside annotation__*.
  {
    const good = await run(buildFrame(), {});
    check("18e a correct source line passes", row(good, "source-line-weight").status === "ok", row(good, "source-line-weight").detail);
    const bad = await run(buildFrame({ boldSource: true }), {});
    check("18e a wholly-bold source line FAILS", row(bad, "source-line-weight").status === "FAIL", row(bad, "source-line-weight").detail);
    check("18e and names the collapse that causes it", /FIRST run/.test(row(bad, "source-line-weight").detail), row(bad, "source-line-weight").detail);
    // Not-bold is not the bar: the contract is Regular, so the weights BETWEEN Regular and Bold have
    // to fail too, or a tail nudged to Medium/Light/Italic gets certified.
    for (const w of ["Medium", "Light", "Italic"]) {
      const off = await run(buildFrame({ sourceTailWeight: w }), {});
      check(`18e a ${w} producer name FAILS`, row(off, "source-line-weight").status === "FAIL", row(off, "source-line-weight").detail);
      check(`18e and says Regular is what ${w} owes`, /prescribes Regular/.test(row(off, "source-line-weight").detail), row(off, "source-line-weight").detail);
    }
    // The prefix owes Bold exactly, for the same reason the tail owes Regular exactly. A substring
    // test on /bold|black/ certifies all four of these, so a footer nudged off the house weight
    // ships looking almost right.
    for (const w of ["Semibold", "ExtraBold", "Black", "Bold Italic"]) {
      const off = await run(buildFrame({ sourcePrefixWeight: w }), {});
      check(`18e a ${w} prefix FAILS`, row(off, "source-line-weight").status === "FAIL", row(off, "source-line-weight").detail);
      check(`18e and names Bold as the target`, /want Bold/.test(row(off, "source-line-weight").detail), row(off, "source-line-weight").detail);
    }
  }

  // 19 — the unimplemented half of the hierarchy is declared, not certified.
  {
    const out = await run(buildFrame(), {});
    check("19 text-hierarchy says CEILING ONLY", /CEILING ONLY/.test(row(out, "text-hierarchy").detail), row(out, "text-hierarchy").detail);
    check("19 ranks row declared SKIPPED", row(out, "text-hierarchy-ranks") && row(out, "text-hierarchy-ranks").status === "SKIPPED", "row missing");
  }

  // 20 — the small-format annotation clearance scales rather than applying the 540 constant.
  {
    const out = await run(buildFrame({ frameW: 302, frameH: 400, labelSize: 11,
      annotation: annotation({ x: 20, y: 130, w: 80, h: 14, size: 11, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("20 block-gap SKIPPED on 302", row(out, "annotation-block-gap").status === "SKIPPED", row(out, "annotation-block-gap").detail);
    // The annotation has to sit ABOVE the plot (which spans y 122-474) for this row to be in scope
    // at all — see the in-plot case below. At y=110 it clears the header by 2px, so the row runs and
    // fails, which is what "not SKIPPED" is asserting.
    const big = await run(buildFrame({ annotation: annotation({ x: 100, y: 110, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("20 540-wide still checks block-gap", row(big, "annotation-block-gap").status !== "SKIPPED", row(big, "annotation-block-gap").detail);
    check("20 and it reports which annotations put it in scope", /extend past the plot/.test(row(big, "annotation-block-gap").detail), row(big, "annotation-block-gap").detail);
    // An annotation INSIDE the plot makes the block the plot, so this row would demand 27px of the
    // very geometry `gap` requires to be 12-16 — the two are unsatisfiable together. It defers.
    const inPlot = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("20 an in-plot annotation defers to the gap row", row(inPlot, "annotation-block-gap").status === "SKIPPED", row(inPlot, "annotation-block-gap").detail);
    check("20 and says why it deferred", /inside the plot/.test(row(inPlot, "annotation-block-gap").detail), row(inPlot, "annotation-block-gap").detail);
    check("20 while the gap row still passes on that frame", row(inPlot, "gap").status === "ok", row(inPlot, "gap").detail);
  }

  // 21 — an annotation covering a filled bar segment, with no value label to catch it.
  {
    const out = await run(buildFrame({ barSegment: true,
      annotation: annotation({ x: 120, y: 390, w: 80, h: 16, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    const d = row(out, "annotation-overlap").detail;
    check("21 covering a bar segment FAILS", row(out, "annotation-overlap").status === "FAIL", d);
    check("21 names the segment", /bar__A/.test(d), d);
    const clear = await run(buildFrame({ barSegment: true,
      annotation: annotation({ x: 120, y: 150, w: 80, h: 16, stroke: null, strokeWeight: 0 }) }), {});
    check("21 an annotation clear of it passes", !/bar__A/.test(row(clear, "annotation-overlap").detail), row(clear, "annotation-overlap").detail);
  }

  // 22 — a scatter's point rings are not line halos (found by the all-chart-types sweep).
  {
    const out = await run(buildFrame({ scatterRings: true }), {});
    const d = row(out, "series-weight").detail;
    check("22 unpaired outline__ not judged as a halo", !/outline__India|outline__China/.test(d), d);
    check("22 and the exclusion is reported", /unpaired|point rings/.test(d), d);
  }

  // 23 — a slope chart's series is slope__X, and its paired halo still applies.
  {
    const out = await run(buildFrame({ slopeSeries: true }), {});
    const d = row(out, "series-weight").detail;
    check("23 slope__ series recognised", row(out, "series-weight").status !== "SKIPPED", d);
    check("23 slope at 3 with a 4 halo passes", row(out, "series-weight").status === "ok", d);
    const bad2 = await run(buildFrame({ slopeSeries: true, slopeWeight: 0.98 }), {});
    check("23 a thinned slope FAILS and names the series", row(bad2, "series-weight").status === "FAIL" && /slope__USA/.test(row(bad2, "series-weight").detail), row(bad2, "series-weight").detail);
    check("23 identity read from the GROUP, not the node", !/\bline\b 0\.98/.test(row(bad2, "series-weight").detail), row(bad2, "series-weight").detail);
  }

  // 24 — a top-level zero line is furniture (a discrete bar's only furniture).
  {
    const ok = await run(buildFrame({ zeroLineOnly: 1 }), {});
    check("24 zero line counted as furniture", row(ok, "furniture-weight").status === "ok" && !/no stroked node/.test(row(ok, "furniture-weight").detail), row(ok, "furniture-weight").detail);
    const bad3 = await run(buildFrame({ zeroLineOnly: 0.64 }), {});
    check("24 a thinned zero line FAILS", row(bad3, "furniture-weight").status === "FAIL" && /0\.64/.test(row(bad3, "furniture-weight").detail), row(bad3, "furniture-weight").detail);
  }

  // 25 — an antimeridian straddler's bbox must not count as a margin breach (found by measuring Fiji).
  {
    const out = await run(buildFrame({ straddler: true }), {});
    const d = row(out, "margins").detail;
    check("25 straddler excluded from margins", !/Fiji \d/.test(d) || /straddler\(s\) excluded/.test(d), d);
    check("25 and the exclusion is reported", /antimeridian straddler/.test(d), d);
    check("25 margins still ok", row(out, "margins").status === "ok", d);
    // a genuinely overflowing normal shape must still fail
    const f = buildFrame({ straddler: true });
    const mapg = f.children.find((c) => c.name === "chart").children.find((c) => c.name === "map");
    mapg.children[1].absoluteBoundingBox = { x: 480, y: 340, width: 80, height: 60 };
    const bad4 = await run(f, {});
    check("25 a real overflow still FAILS", row(bad4, "margins").status === "FAIL" && /Brazil/.test(row(bad4, "margins").detail), row(bad4, "margins").detail);
  }

  // 26 — a gridline whose dash was CLEARED. The target has to be derived from what a node is (its name
  // or its furniture container) and never from the dash it currently carries: classifying by the current
  // pattern put a cleared gridline in the "solid, nothing to compare" bucket, so the row returned ok on
  // precisely the defect it exists to catch.
  {
    const out = await run(buildFrame({ clearedGrid: true }), {});
    const d = row(out, "furniture-dash").detail;
    check("26 a cleared gridline dash FAILS", row(out, "furniture-dash").status === "FAIL", d);
    check("26 and names the gridline", /grid-1/.test(d), d);
    check("26 and calls out the empty pattern", /NO dash at all/.test(d), d);
    const clean = await run(buildFrame(), {});
    const dc = row(clean, "furniture-dash").detail;
    check("26 a solid zero line is NOT dragged to the grid target", row(clean, "furniture-dash").status === "ok", dc);
    check("26 and is judged as solid, not against [4,4]", /solid or at the slope's native/.test(dc), dc);
    // the mirror-image defect: a tick or zero line restyled TO the gridline target is not "native"
    const restyled = await run(buildFrame({ zeroLineOnly: 1, zeroLineDash: [4, 4] }), {});
    const dr = row(restyled, "furniture-dash").detail;
    check("26 a zero line restyled to [4,4] FAILS", row(restyled, "furniture-dash").status === "FAIL", dr);
    check("26 and is named as a should-be-solid node", /should be solid but are dashed/.test(dr) && /vertical-zero-line/.test(dr), dr);
    // the deliberate decision: a slope's native [3,2] zero line stays [3,2]. "A SLOPE chart's native
    // zero line" is two conditions, and this fixture used to assert only the node — it carried no
    // slope__* series at all, so it was really asserting that ANY chart may dash its zero line [3,2].
    const slopeZero = await run(buildFrame({ zeroLineOnly: 1, zeroLineDash: [3, 2], slopeSeries: true }), {});
    check("26 a SLOPE chart's native [3,2] zero line passes", row(slopeZero, "furniture-dash").status === "ok", row(slopeZero, "furniture-dash").detail);
    // ...and on any other chart type the same dash is a restyle
    const barZero32 = await run(buildFrame({ zeroLineOnly: 1, zeroLineDash: [3, 2] }), {});
    check("26 but the same [3,2] on a chart with no slope series FAILS",
          row(barZero32, "furniture-dash").status === "FAIL" && /no slope__\* series was found/.test(row(barZero32, "furniture-dash").detail),
          row(barZero32, "furniture-dash").detail);

    // grapher names each gridline after its TICK VALUE, so its zero line arrives as "0%" and matches
    // none of the zero/tick/axis words. Judged against [4,4] it reported a cleared dash on five of
    // eight real frames. It is reclassified by identity, and reclassified rather than exempted.
    const valZero = await run(buildFrame({ valueNamedZero: true }), {});
    const dv = row(valZero, "furniture-dash").detail;
    check("26 a solid gridline named for the value 0 passes", row(valZero, "furniture-dash").status === "ok", dv);
    check("26 and is reported as reclassified", /0%/.test(JSON.stringify(row(valZero, "furniture-dash").reclassifiedAsSolidByDesign || {})), JSON.stringify(row(valZero, "furniture-dash").reclassifiedAsSolidByDesign));
    const valZeroBad = await run(buildFrame({ valueNamedZero: true, zeroTickDash: [4, 4] }), {});
    check("26 but a value-named zero RESTYLED to [4,4] still FAILS",
          row(valZeroBad, "furniture-dash").status === "FAIL" && /should be solid but are dashed/.test(row(valZeroBad, "furniture-dash").detail),
          row(valZeroBad, "furniture-dash").detail);

    // a slope chart's two end verticals are not a grid; a small group of VERTICAL lines is an axis pair
    const slopePair = await run(buildFrame({ slopeAxisPair: true }), {});
    const dsp = row(slopePair, "furniture-dash").detail;
    check("26 a 2-member solid VERTICAL axis pair is not judged as a grid", row(slopePair, "furniture-dash").status === "ok", dsp);
    check("26 and the pair is reported as axis-only",
          /1980/.test(JSON.stringify(row(slopePair, "furniture-dash").reclassifiedAsSolidByDesign || {})),
          JSON.stringify(row(slopePair, "furniture-dash").reclassifiedAsSolidByDesign));

    // ...but the COUNT is not that shape. Exempting every group of fewer than three cut both ways on a
    // legitimate two-line HORIZONTAL grid: correctly dashed it FAILED as a dashed "axis" node, and with
    // its dash cleared it PASSED — the very defect this row exists to catch, hidden by the exemption.
    const twoOk = await run(buildFrame({ gridLines: 2, noZeroLine: true }), {});
    check("26 a 2-line HORIZONTAL grid correctly at [4,4] passes",
          row(twoOk, "furniture-dash").status === "ok" && /all 2 gridline\(s\) at \[4,4\]/.test(row(twoOk, "furniture-dash").detail),
          row(twoOk, "furniture-dash").detail);
    const twoCleared = await run(buildFrame({ gridLines: 2, noZeroLine: true, clearedGrid: true }), {});
    check("26 and a 2-line HORIZONTAL grid whose dash was CLEARED still FAILS",
          row(twoCleared, "furniture-dash").status === "FAIL" && /2 carry NO dash at all/.test(row(twoCleared, "furniture-dash").detail),
          row(twoCleared, "furniture-dash").detail);

    // the [3,2] exception belongs to a slope chart's native ZERO line, not to the whole solid-by-design
    // bucket: granted in bulk it also accepted an ordinary tick dashed [3,2], which CHECKS.md permits
    // nowhere.
    const tick32 = await run(buildFrame({ zeroAreaTick: true, tickDash: [3, 2] }), {});
    check("26 a TICK dashed [3,2] FAILS — the exception is the slope's zero line only",
          row(tick32, "furniture-dash").status === "FAIL" && /tick-0 \[3,2\]/.test(row(tick32, "furniture-dash").detail),
          row(tick32, "furniture-dash").detail);
    const tickSolid = await run(buildFrame({ zeroAreaTick: true }), {});
    check("26 while a solid tick still passes", row(tickSolid, "furniture-dash").status === "ok", row(tickSolid, "furniture-dash").detail);
  }

  // 32 — the UNGROUPED fallback has to find the whole plot, not the line-chart-shaped subset of it. A
  // whitelist of axis/grid/lines container names missed a map's `map`, a bar's `bars` and a scatter's
  // point container, which were then walked with insidePlot=false. That does not fail a row, it EMPTIES
  // one — and an empty row skips with a reason that is false: "no solid fills found in the plot" on a
  // map full of them, no marks for either annotation row, and `isMap` never set.
  {
    const grouped = await run(buildFrame({ mapCountries: true }), {});
    const ungrouped = await run(buildFrame({ mapCountries: true, ungrouped: true }), {});
    check("32 the ungrouped fallback names what it resolved",
          /ungrouped frame child\(ren\)/.test(ungrouped.resolved.chartBy) && /map/.test(ungrouped.resolved.chartBy),
          ungrouped.resolved.chartBy);
    check("32 an ungrouped map's fills reach off-palette instead of a false skip",
          row(ungrouped, "off-palette").status !== "SKIPPED" && row(ungrouped, "off-palette").status === row(grouped, "off-palette").status,
          `${row(ungrouped, "off-palette").status}: ${row(ungrouped, "off-palette").detail}`);
    check("32 and the map is still detected as a map",
          /^map:/.test(row(ungrouped, "gap").detail) && row(ungrouped, "gap").status === "SKIPPED",
          row(ungrouped, "gap").detail);
    check("32 the header, footer and logo are NOT taken for plot content",
          !/header|footer|logo/.test(ungrouped.resolved.chartBy), ungrouped.resolved.chartBy);
    const withAnn = await run(buildFrame({ mapCountries: true, ungrouped: true,
      annotation: annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("32 nor is one of our own annotations",
          !/annotation__/.test(withAnn.resolved.chartBy) && withAnn.rows.some((x) => x.check === "annotation-overlap" && x.status !== "SKIPPED"),
          withAnn.resolved.chartBy);
  }

  // 27 — a slope chart's stroked vector is called plain `line` and the series identity sits on its
  // `slope__<Entity>` group. The polyline filter read the node's own name only, so every slope segment
  // was absent from `polylines` and annotation-overlap could never fail on a slope.
  {
    const out = await run(buildFrame({ slopeSeries: true }), {});
    const pl = row(out, "polylines");
    const names = (pl.polylines || []).map((p) => p.name);
    check("27 slope segments sampled", pl.status === "ok", pl.detail);
    check("27 slope__USA reached polylines", names.indexOf("slope__USA") !== -1, names.join(","));
    check("27 its halo outline__USA too", names.indexOf("outline__USA") !== -1, names.join(","));
    check("27 point markers NOT sampled as series strokes", !names.some((n) => /point/.test(n)), names.join(","));
    const hit = await run(buildFrame({ slopeSeries: true,
      annotation: annotation({ x: 200, y: 250, w: 80, h: 18, stroke: null, strokeWeight: 0 }) }), {});
    const dh = row(hit, "annotation-overlap").detail;
    check("27 an annotation crossing a slope now FAILS", row(hit, "annotation-overlap").status === "FAIL", dh);
    check("27 and names the slope it crosses", /slope__USA/.test(dh), dh);
  }

  // 28 — a map country's BBOX IS NOT ITS INK (maps.md). Inventorying map shapes as filled data marks
  // reported an annotation over open ocean as covering a mark. Excluded and DECLARED, not silently
  // dropped — and a non-map filled mark (test 21) must still fail.
  {
    const out = await run(buildFrame({ mapCountries: true,
      annotation: annotation({ x: 50, y: 170, w: 40, h: 16, stroke: null, strokeWeight: 0 }) }), {});
    const d = row(out, "annotation-overlap").detail;
    check("28 a map shape is not judged as a covered mark", row(out, "annotation-overlap").status === "ok", d);
    check("28 no country reported as covered", !/covers country__FRA/.test(d), d);
    check("28 and the exclusion is DECLARED", /map shape\(s\) NOT judged/.test(d), d);
  }

  // 29 — a node with PER-RANGE font sizes. `fontSize` is figma.mixed (a SYMBOL) there, so a
  // `typeof === "number"` gate dropped the whole node from `texts`: an 8px run inside an otherwise
  // correct annotation was invisible to text-floor, ladder-sizes AND text-hierarchy at once.
  {
    const mixedSize = text("annotation__mix", "Note with a tiny tail", 14, 100, 195, 120, 18, "#2d2e2d", {
      fontSize: MIXED, strokes: [], strokeWeight: 0, textStyleId: "S:abc",
      segments: { fontSize: [[14, 0, 10], [8, 10, 21]], fontName: [["Regular", 0, 21]] },
    });
    const out = await run(buildFrame({ annotation: mixedSize }), {});
    const d = row(out, "text-floor").detail;
    check("29 an 8px RANGE breaches the floor", row(out, "text-floor").status === "FAIL", d);
    check("29 and the character range is named", /chars 10-21/.test(d), d);
    check("29 and mixed-size reading is declared", /MIXED-size node/.test(d), d);
    check("29 the off-ladder range is caught too", row(out, "ladder-sizes").status === "FAIL", row(out, "ladder-sizes").detail);
    // a mixed-size SUBTITLE must still resolve, or the hierarchy row loses its ceiling
    const f = buildFrame();
    const hdr = f.children.find((c) => c.name === "header");
    hdr.children[1] = text("subtitle", "A subtitle line", MIXED, 16, 51, 508, 19, null, {
      fontSize: MIXED, segments: { fontSize: [[16, 0, 8], [15, 8, 15]], fontName: [["Regular", 0, 15]] } });
    const sub = await run(f, {});
    check("29 a mixed-size subtitle still resolves", row(sub, "text-hierarchy").status !== "SKIPPED", row(sub, "text-hierarchy").detail);
    check("29 and the ceiling is its LARGEST range", /16px/.test(row(sub, "text-hierarchy").detail), row(sub, "text-hierarchy").detail);
  }

  // 30 — an axis-less bar: no polylines, no furniture, but filled segments ARE geometry. Leaving them
  // out of the skip guard silenced BOTH annotation rows on a chart type the skill ships regularly.
  {
    const f = buildFrame({ barSegment: true,
      annotation: annotation({ x: 120, y: 390, w: 80, h: 16, stroke: "#ffffff", strokeWeight: 3 }) });
    // strip the furniture and the series lines, leaving only the bar segment
    const chart = f.children.find((c) => c.name === "chart");
    chart.children = chart.children.filter((c) => /^bar__/.test(c.name));
    const out = await run(f, {});
    const d = row(out, "annotation-overlap").detail;
    check("30 the row does NOT skip with only filled marks", row(out, "annotation-overlap").status !== "SKIPPED", d);
    check("30 and it catches the covered segment", row(out, "annotation-overlap").status === "FAIL" && /bar__A/.test(d), d);
    check("30 the knockout row is not stranded either", row(out, "annotation-knockout").status !== "SKIPPED", row(out, "annotation-knockout").detail);
  }

  // 31 — gap symmetry is 0.5px, not 1.5. The fit sets the gap by construction, so 1.5px of slack only
  // ever hid a defect: measured across eight frames the asymmetry ran 0.34-2.24px and seven passed.
  {
    const f = buildFrame();
    const chart = f.children.find((c) => c.name === "chart");
    // header bottom 108, footer top 488 -> a 352-tall chart centred sits at 122 with 14/14. Offset by
    // half a pixel gives 14.5 top and 13.5 bottom: a 1.0px asymmetry, which the OLD 1.5px rule passed
    // and both ends of which are still inside the 12-16 target. So this fixture isolates the tolerance.
    chart.absoluteBoundingBox = { x: 16, y: 122.5, width: 508, height: 352 };
    const out = await run(f, {});
    const d = row(out, "gap").detail;
    check("31 a 1.0px asymmetry now FAILS", row(out, "gap").status === "FAIL", d);
    check("31 both ends are still inside the 12-16 target", /top 14.5, bottom 13.5/.test(d), d);
    check("31 and the bound is stated as 0.5px", /differ by more than 0.5px/.test(d), d);
    const even = buildFrame();
    even.children.find((c) => c.name === "chart").absoluteBoundingBox = { x: 16, y: 122, width: 508, height: 352 };
    check("31 a symmetric 14/14 still passes", row(await run(even, {}), "gap").status === "ok", row(await run(even, {}), "gap").detail);
  }

  // 33 — the colour-vision / grayscale-seams rows must hand over a RUNNABLE command, not a tool
  // name. They stay SKIPPED (no shell inside a Figma plugin), but a gap the operator has to
  // reconstruct by hand is the one that actually gets skipped.
  {
    const out = await run(buildFrame({ secondColour: true }), {});
    for (const name of ["colour-vision", "grayscale-seams"]) {
      const r = row(out, name);
      check(`33 ${name} still declares itself skipped`, r.status === "SKIPPED", r.status);
      check(`33 ${name} names its owner`, r.ownedBy === "scripts/color_audit.py", String(r.ownedBy));
      check(`33 ${name} emits the interpreter, not a bare script`,
            /\.venv\/bin\/python \.claude\/skills\/create-figma-chart\/scripts\/color_audit\.py/.test(r.detail), r.detail);
      check(`33 ${name} passes real hexes`, /'#[0-9a-f]{6}(,#[0-9a-f]{6})*'/.test(r.detail), r.detail);
      check(`33 ${name} declares an adjacency mode`, /--separated|--maps|--line/.test(r.detail), r.detail);
    }
    // Both rows must offer the SAME command — they are one run of one script.
    const cmdOf = (n) => (/color_audit\.py (.*?)(?: —|$)/.exec(row(out, n).detail) || [])[1];
    check("33 both rows hand over one identical command", cmdOf("colour-vision") === cmdOf("grayscale-seams"),
          `${cmdOf("colour-vision")} vs ${cmdOf("grayscale-seams")}`);
    // The mode flag also picks which palette a --suggest rerun searches, so it is not cosmetic.
    // A line/slope palette needs `--line`: `--separated` alone would have the search recommend FILL
    // colours for thin strokes. The default fixture is a line chart.
    check("33 a line series asks for the Line and Slope variants",
          / --line\b/.test(row(out, "colour-vision").detail), row(out, "colour-vision").detail);
    // A stacked chart is the case where order matters, so the warning must be present there — and only
    // there, since neither a map nor a line chart has seams. Strip the series lines to reach that branch.
    const noSeries = buildFrame({ barSegment: true, secondColour: true });
    const chartOf = (f) => f.children.find((c) => c.name === "chart");
    chartOf(noSeries).children = chartOf(noSeries).children.filter((c) => !/^(line|outline)__/.test(c.name));
    const dSep = row(await run(noSeries, {}), "colour-vision").detail;
    check("33 a fill-only palette gets --separated, not --line",
          / --separated\b/.test(dSep) && !/--line/.test(dSep), dSep);
    check("33 and that run warns that stack order matters", /STACKED or SEGMENTED/.test(dSep), dSep);
    // --- what the palette is BUILT FROM. The `fills` inventory was the wrong source: it carries every
    // solid paint on an area node in the plot, and a TEXT node has one, while a line chart's series
    // colour is a STROKE and is not in it at all. On this very fixture that meant the emitted command
    // audited `label__A`'s text fill and omitted the series colour — furniture in, data out.
    const offPaletteLabel = row(await run(buildFrame({ labelFill: "#123456" }), {}), "colour-vision").detail;
    check("33 a text label's fill is NOT audited as a category",
          !/#123456/.test(offPaletteLabel), offPaletteLabel);
    // The other half: the series colour must be PRESENT, and it exists only as a stroke.
    const strokeOnly = row(await run(buildFrame({ gappedLine: true }), {}), "colour-vision").detail;
    check("33 a line series' stroke colour IS audited", /#b13507/.test(strokeOnly), strokeOnly);
    // `outline__*` is the white halo under a line, shared by every series — not a category colour.
    const rings = row(await run(buildFrame({ scatterRings: true }), {}), "colour-vision").detail;
    check("33 the white outline halo is not taken for a category",
          !/#ffffff/.test(rings) && /#b13507/.test(rings), rings);
    // A colour carrying two categories is collapsed by the hex dedupe, so it is NAMED instead of
    // silently dropped — otherwise the severest clash there is (deltaE 0) produces a vacuous pass.
    const twoSeries = row(await run(buildFrame({ extraLine: 3 }), {}), "colour-vision").detail;
    check("33 two series on one colour are named, not silently merged",
          /judge them by eye/.test(twoSeries) && /A \+ B|B \+ A/.test(twoSeries), twoSeries);
    // The category name sits on the GROUP and the paint on its leaves — grapher's documented
    // `datapoints__<Entity>` shape, where the filled marker is called something like `Ellipse 12`.
    // Reading the category off the painted node's own name loses it on exactly that shape, so two
    // entities sharing a marker colour would be merged with no warning. The colour here is carried by
    // NOTHING else in the fixture, so this can only pass via the ancestor.
    const ancestry = buildFrame();
    const marker = (entity) => node({ name: `datapoints__${entity}`, x: 60, y: 150, width: 40, height: 40,
      children: [node({ type: "ELLIPSE", name: "Ellipse 12", x: 60, y: 150, width: 8, height: 8,
                        fills: solid("#58ac8c") })] });
    chartOf(ancestry).children.push(marker("Peru"), marker("Nepal"));
    const dAnc = row(await run(ancestry, {}), "colour-vision").detail;
    check("33 a marker's category is read from its group, not its leaf name",
          /#58ac8c carries (Peru \+ Nepal|Nepal \+ Peru)/.test(dAnc), dAnc);
    // And the category is what LABELS the audit too. Left as the leaf name, a failing pair reads
    // "Ellipse 12 vs Ellipse 12" and identifies nothing.
    check("33 and it labels the audit, so --names never says Ellipse 12",
          /--names '[^']*Peru/.test(dAnc) && !/Ellipse 12/.test(dAnc), dAnc);
    // But a choropleth puts every country in a bin into ONE colour by design, so the same note must
    // not fire on a map or it fires on every map.
    // Three countries, two of them in one bin: sharing a bin must not read as a clash, and the third
    // keeps a genuine PAIR in the palette so the map's own run is still emitted.
    const sameBin = buildFrame({ mapCountries: true });
    const sameBinMap = sameBin.children.find((c) => c.name === "chart").children.find((c) => c.name === "map");
    sameBinMap.children.find((c) => c.name === "country__DEU").fills = solid("#4c6a9c");
    sameBinMap.children.push(node({ type: "VECTOR", name: "country__BRA", x: 200, y: 160, width: 60, height: 40,
      fills: solid("#b13507") }));
    const binned = row(await run(sameBin, {}), "colour-vision").detail;
    check("33 map shapes sharing a bin colour are NOT reported as a clash",
          !/judge them by eye/.test(binned), binned);
    // A map is audited as a CATEGORICAL choropleth, which a sequential ramp is not: the deltaE 20 gate
    // fails a correct ramp by construction, so the row must say so rather than hand over the command flat.
    check("33 a map warns that a sequential ramp is out of scope",
          /SEQUENTIAL ramp/.test(binned) && /--maps/.test(binned), binned);
    // A frame can hold BOTH families at once — combination.md's exemplar is a line chart with an inset
    // locator map whose countries are filled with the same colours as their series. `isMap` is
    // frame-level, so the map won the single mode flag and the LINE STROKES were audited under
    // `--maps`, whose --suggest answers out of the lighter Categorical Maps set: fill colours
    // recommended for thin strokes, the exact swap `--line` exists to prevent. Worse, colour dedupe ran
    // across both families, so the series entry collapsed into the country that shares its colour and
    // the series disappeared from `--names` altogether. This fixture IS that chart: `mapCountries` adds
    // the inset to the default line series, and `country__FRA` reuses the series colour exactly as the
    // guidelines prescribe.
    const combo = row(await run(buildFrame({ mapCountries: true, secondColour: true }), {}), "colour-vision").detail;
    check("33 a combination frame audits the series as a LINE, not as a map",
          /--names 'B,A' --line/.test(combo), combo);
    check("33 and still audits its inset map as a map",
          /--names 'country__FRA,country__DEU' --maps/.test(combo), combo);
    check("33 and says why there are two runs, so the shared colour is not read as a clash",
          /two separate runs/.test(combo) && /expected/.test(combo), combo);
    // The split must not fire on a frame with only one family, or every ordinary chart grows a note
    // about a map it does not have. Both directions, since either alone can pass on a broken gate.
    const mapOnly = buildFrame({ mapCountries: true });
    chartOf(mapOnly).children = chartOf(mapOnly).children.filter((c) => c.name !== "line__A");
    const dMapOnly = row(await run(mapOnly, {}), "colour-vision").detail;
    check("33 a map with no series is still ONE run, in --maps",
          /--maps/.test(dMapOnly) && !/--line/.test(dMapOnly) && !/two separate runs/.test(dMapOnly), dMapOnly);
    const lineOnly = row(await run(buildFrame({ secondColour: true }), {}), "colour-vision").detail;
    check("33 a line chart with no map is still ONE run, in --line",
          /--line/.test(lineOnly) && !/--maps/.test(lineOnly) && !/two separate runs/.test(lineOnly), lineOnly);
    // Figma switches a paint off two independent ways, and only `visible: false` was tested on the
    // FILL side — so a mark left at `opacity: 0` handed the audit a category colour that paints no
    // pixels, and could be sent a reviewer to go and "fix". The stroke side already applied both
    // tests. This hex is carried by nothing else in the fixture, so its absence can only come from
    // the opacity test; the visible segment is asserted present so the case cannot pass by the
    // command going missing altogether.
    const ghost = buildFrame({ barSegment: true });
    chartOf(ghost).children.push(node({ type: "RECTANGLE", name: "bar__Ghost", x: 300, y: 380,
      width: 60, height: 40, fills: [Object.assign(solid("#12ab34")[0], { opacity: 0 })] }));
    const dGhost = row(await run(ghost, {}), "colour-vision").detail;
    check("33 a fully transparent mark fill is not a palette colour",
          !/#12ab34/.test(dGhost) && /#4c6a9c/.test(dGhost), dGhost);
    // PARTIAL opacity is the other half, and it is not the same case: the mark IS on the canvas, so it
    // keeps its box and its other rows — but its colour is the raw paint, not the composite the reader
    // sees, so auditing it asks about a colour that is not on the page. Held back and NAMED, since a
    // silently shorter palette is a subset audit reported as a whole one.
    const faded = buildFrame({ barSegment: true });
    chartOf(faded).children.push(node({ type: "RECTANGLE", name: "bar__Faded", x: 300, y: 380,
      width: 60, height: 40, fills: [Object.assign(solid("#12ab34")[0], { opacity: 0.5 })] }));
    const dFaded = row(await run(faded, {}), "colour-vision").detail;
    check("33 a translucent mark is held out of the palette",
          !/#12ab34/.test(dFaded) && /#4c6a9c/.test(dFaded), dFaded);
    check("33 and the held-back mark is named, not silently dropped",
          /translucent/.test(dFaded) && /Faded/.test(dFaded), dFaded);
    // NODE opacity dims every paint under it and accumulates down the tree, so a fully opaque leaf
    // inside a half-opacity group is just as unreportable. Read off the leaf's own paint alone, this
    // one looks perfectly measurable — which is why the group carries the opacity here, not the leaf.
    const dimGroup = buildFrame({ barSegment: true });
    chartOf(dimGroup).children.push(node({ name: "series__Muted", x: 300, y: 380, width: 60, height: 40,
      opacity: 0.4, children: [node({ type: "RECTANGLE", name: "Rectangle 7", x: 300, y: 380,
        width: 60, height: 40, fills: solid("#12ab34") })] }));
    const dDim = row(await run(dimGroup, {}), "colour-vision").detail;
    check("33 an ancestor's opacity disqualifies an opaque leaf's colour",
          !/#12ab34/.test(dDim) && /translucent/.test(dDim), dDim);
    // ZERO node opacity is NOT partial opacity, and the two must not share a verdict. A node at
    // `opacity: 0` paints no pixels at all — the same non-rendering state as `visible: false` and as a
    // zero-opacity PAINT, both of which are already dropped outright. Grouped with partial opacity it
    // came back as a held-back translucent mark, so the audit NAMED an invisible category and told the
    // operator to reset its opacity or judge it by eye: a verdict about something not on the canvas.
    // The group carries the zero and the leaf is fully opaque, so the leaf's own paint cannot excuse it.
    const zeroGroup = buildFrame({ barSegment: true });
    chartOf(zeroGroup).children.push(node({ name: "series__Invisible", x: 300, y: 380, width: 60, height: 40,
      opacity: 0, children: [node({ type: "RECTANGLE", name: "Rectangle 9", x: 300, y: 380,
        width: 60, height: 40, fills: solid("#12ab34") })] }));
    const dZero = row(await run(zeroGroup, {}), "colour-vision").detail;
    check("33 a node at opacity 0 is not a palette colour",
          !/#12ab34/.test(dZero) && /#4c6a9c/.test(dZero), dZero);
    check("33 and it is not reported as a translucent mark to go and check",
          !/translucent/.test(dZero) && !/Invisible/.test(dZero), dZero);
    // Opacity MULTIPLIES down the tree, so a pair of small values compounds to a very small one:
    // 0.05 inside 0.05 is 0.0025. That is FAINT, not absent — the non-rendering exit is exactly zero,
    // never a floor — so it takes the translucent treatment: out of the palette, and NAMED, because
    // "reset the opacity and re-run" is the one instruction that helps here. A cutoff would have
    // dropped it from every row in the file instead, including the rows that never look at colour.
    const compounded = buildFrame({ barSegment: true });
    chartOf(compounded).children.push(node({ name: "series__Vanished", x: 300, y: 380, width: 60, height: 40,
      opacity: 0.05, children: [node({ name: "inner", x: 300, y: 380, width: 60, height: 40, opacity: 0.05,
        children: [node({ type: "RECTANGLE", name: "Rectangle 10", x: 300, y: 380,
          width: 60, height: 40, fills: solid("#12ab34") })] })] }));
    const dComp = row(await run(compounded, {}), "colour-vision").detail;
    check("33 a mark compounded to near-zero is held out of the palette and named",
          !/#12ab34/.test(dComp) && /translucent/.test(dComp) && /Vanished/.test(dComp) && /#4c6a9c/.test(dComp), dComp);
    // And it stays in the rows that are not about colour at all. A cutoff took the whole subtree out of
    // the walk, so an 8px label at 0.005 left `text-floor` reporting that every range cleared the floor
    // — a row certifying a frame on input it had silently removed. Faint is not gone.
    const faintText = buildFrame();
    chartOf(faintText).children.push(node({ name: "faint", x: 60, y: 300, width: 80, height: 12, opacity: 0.005,
      children: [text("label__Faint", "8px and faint", 8, 60, 300, 80, 12, "#4c6a9c")] }));
    const outFaint = await run(faintText, {});
    check("33 a near-transparent 8px label is still judged by text-floor",
          row(outFaint, "text-floor").status === "FAIL" && /8px and faint/.test(row(outFaint, "text-floor").detail),
          row(outFaint, "text-floor").detail);
    // Exactly zero is the other side of that line, and it must stay dropped: the product reaches it
    // only through a factor of zero, so nothing here is a threshold judgement.
    const zeroText = buildFrame();
    chartOf(zeroText).children.push(node({ name: "gone", x: 60, y: 300, width: 80, height: 12, opacity: 0,
      children: [text("label__Gone", "8px and gone", 8, 60, 300, 80, 12, "#4c6a9c")] }));
    const outZeroText = await run(zeroText, {});
    check("33 while an 8px label at opacity 0 is not judged at all",
          row(outZeroText, "text-floor").status === "ok" && !/8px and gone/.test(row(outZeroText, "text-floor").detail),
          row(outZeroText, "text-floor").detail);
    // But a compounded value that is still ABOVE the floor is genuinely on the canvas: it keeps the
    // translucent treatment and stays NAMED, or the new zero gate would swallow every dimmed mark.
    const stillVisible = buildFrame({ barSegment: true });
    chartOf(stillVisible).children.push(node({ name: "series__Halved", x: 300, y: 380, width: 60, height: 40,
      opacity: 0.5, children: [node({ name: "inner", x: 300, y: 380, width: 60, height: 40, opacity: 0.5,
        children: [node({ type: "RECTANGLE", name: "Rectangle 11", x: 300, y: 380,
          width: 60, height: 40, fills: solid("#12ab34") })] })] }));
    const dStill = row(await run(stillVisible, {}), "colour-vision").detail;
    check("33 a compounded but still-visible mark stays translucent and named",
          !/#12ab34/.test(dStill) && /translucent/.test(dStill) && /Halved/.test(dStill), dStill);
    // The FRAME is the one ancestor every mark shares, and it sits ABOVE the walk — which was seeded
    // with a literal 1, so it was the only node whose opacity was never examined. A frame left at
    // reduced opacity dims every mark on the canvas, and the raw paints went on being emitted as a
    // paste-ready command. Nothing inside the frame is touched here, so only the frame can account for
    // the difference.
    const dimFrame = buildFrame({ barSegment: true });
    dimFrame.opacity = 0.4;
    const dDimFrame = row(await run(dimFrame, {}), "colour-vision").detail;
    check("33 a frame's own opacity disqualifies every mark under it",
          !/#4c6a9c/.test(dDimFrame) && /translucent/.test(dDimFrame), dDimFrame);
    // At zero the marks do not merely lose their colour, they leave every row — and so does the frame:
    // it paints no pixels, so it goes through the shared gate in case 35 rather than being explained
    // inside a colour row while thirty other rows go on certifying it.
    const goneFrame = buildFrame({ barSegment: true });
    goneFrame.opacity = 0;
    const outGone = await run(goneFrame, {});
    check("33 a frame at opacity 0 is not audited for colour at all",
          !row(outGone, "colour-vision") && !!row(outGone, "frame-not-rendered"),
          outGone.verdict);
    // And an ordinary frame must carry neither message, or both become noise on every run.
    // And the note must not fire on an ordinary opaque chart, or it becomes noise on every run.
    const opaque = row(await run(buildFrame({ barSegment: true }), {}), "colour-vision").detail;
    check("33 an opaque plot carries no translucency note",
          !/translucent/.test(opaque) && /#4c6a9c/.test(opaque), opaque);

    // --- when --names is safe to emit. Rename EVERY qualifying mark, not one: the emitter keeps the
    // FIRST name per distinct hex, so renaming an arbitrary node can leave its colour already
    // registered under a clean earlier name and silently test nothing. (That is exactly how this case
    // first passed vacuously.) `barSegment` supplies a filled RECTANGLE — a data mark, which is what
    // now feeds the palette; renaming text fills tests nothing.
    // `to` may be a function of the mark's index, for the cases where the names must differ from each
    // other — a palette of two colours under one name is a different defect, tested separately.
    const renameMarks = (frame, to) => {
      let renamed = 0;
      (function walk(n) {
        if (n.type !== "TEXT" && !(n.children && n.children.length)
            && n.fills && n.fills.length && n.fills.some((f) => f.type === "SOLID")) {
          n.name = typeof to === "function" ? to(renamed) : to; renamed++;
        }
        (n.children || []).forEach(walk);
      })(frame);
      return renamed;
    };
    // A comma in a name would misalign every --names entry after it, so the flag is dropped.
    const comma = buildFrame({ barSegment: true, secondColour: true });
    const renamed = renameMarks(comma, "series__Chile, mainland");
    check("33 the comma fixture actually renamed a data mark", renamed > 0, `renamed ${renamed}`);
    const d2 = row(await run(comma, {}), "colour-vision").detail;
    // Match the FLAG (`--names '…'`), not the substring: the explanatory note says "--names
    // omitted", so a bare /--names/ is satisfied by the very sentence proving it was dropped.
    check("33 a comma in a name drops --names rather than misaligning it",
          !/--names '/.test(d2) && /contains a comma/.test(d2), d2);
    // An apostrophe would end the single-quoted shell argument mid-name, so the advertised paste-ready
    // command would not parse. Same treatment, and the reason must name the apostrophe.
    const apos = buildFrame({ barSegment: true, secondColour: true });
    renameMarks(apos, "series__Women's employment");
    const d4 = row(await run(apos, {}), "colour-vision").detail;
    check("33 an apostrophe in a name drops --names rather than breaking the shell",
          !/--names '/.test(d4) && /apostrophe/.test(d4), d4);
    // The happy path, or the negatives above would pass on a build that never emits --names at all.
    const named = buildFrame({ barSegment: true, secondColour: true });
    renameMarks(named, (i) => `series__${["Chile", "Peru", "Nepal"][i] || "Other"}`);
    const d1 = row(await run(named, {}), "colour-vision").detail;
    // The label is the CATEGORY the mark belongs to, not the node's raw name — `--names` exists so the
    // audit's findings name the categories that need attention.
    check("33 clean names DO produce --names, carrying the category", /--names 'Chile[,']/.test(d1), d1);
    check("33 and then no omission note is attached", !/--names omitted/.test(d1), d1);
    // 33b — the two conditions measured on the REAL file, not invented. A `static_viz` import names
    // every series group `<kind>__<slug>` (the dataset, not the category) and every paint-bearing
    // leaf `Vector`, so distinct colours arrive sharing one name. That is worse than a missing name
    // because it looks right: the audit prints one row per colour, all under the same label.
    const shareName = buildFrame();
    const chartGrp = shareName.children.find((c) => c.name === "chart");
    chartGrp.children.push(node({
      name: "bars__agriculture-share", type: "GROUP", x: 100, y: 300, width: 120, height: 60,
      children: [
        node({ type: "RECTANGLE", name: "Vector", x: 100, y: 300, width: 40, height: 60, fills: solid("#00847e") }),
        node({ type: "RECTANGLE", name: "Vector", x: 150, y: 300, width: 40, height: 60, fills: solid("#883039") }),
      ],
    }));
    const dShare = row(await run(shareName, {}), "colour-vision").detail;
    check("33b two colours under one category name drop --names",
          !/--names '/.test(dShare) && /distinct name/.test(dShare), dShare);
    check("33b and both colours still reach the palette",
          /#00847e/.test(dShare) && /#883039/.test(dShare), dShare);

    // A generic import name labels nothing, even when it is distinct per colour.
    const generic = buildFrame();
    const chartGrp2 = generic.children.find((c) => c.name === "chart");
    chartGrp2.children.push(node({
      name: "series__one", type: "GROUP", x: 100, y: 300, width: 40, height: 60,
      children: [node({ type: "RECTANGLE", name: "Vector", x: 100, y: 300, width: 40, height: 60, fills: solid("#00847e") })],
    }));
    chartGrp2.children.push(node({ type: "RECTANGLE", name: "Rectangle 12", x: 150, y: 300, width: 40, height: 60, fills: solid("#883039") }));
    const dGen = row(await run(generic, {}), "colour-vision").detail;
    check("33b an import-default name drops --names and says so",
          !/--names '/.test(dGen) && /(import default|distinct name)/.test(dGen), dGen);

    // 33b-grapher — the OTHER naming convention, and the one --names exists for. Grapher's own SVG
    // export emits `line__<Entity>` / `outline__<Entity>` / `datapoints__<Entity>` per series, where
    // the second token IS the category. That must keep producing labels; the guards above are aimed
    // at `static_viz`'s `<slug>__<part>`, where it is not. One frame proves both halves are separable.
    const grapher = buildFrame();
    const gChart = grapher.children.find((c) => c.name === "chart");
    for (const [entity, hex] of [["Chile", "#883039"], ["Peru", "#00847e"]]) {
      gChart.children.push(node({
        name: `line__${entity}`, type: "GROUP", x: 100, y: 200, width: 200, height: 80,
        children: [node({ type: "VECTOR", name: "Vector", x: 100, y: 200, width: 200, height: 80,
                          strokes: solid(hex), strokeWeight: 3, strokeAlign: "CENTER", dashPattern: [] })],
      }));
    }
    const dGrapher = row(await run(grapher, {}), "colour-vision").detail;
    check("33b grapher line__<Entity> DOES produce --names",
          /--names '[^']*Chile[^']*Peru[^']*'/.test(dGrapher) || /--names '[^']*Peru[^']*Chile[^']*'/.test(dGrapher), dGrapher);
    check("33b and the leaf's generic `Vector` name is not what gets used",
          !/--names '[^']*Vector/.test(dGrapher), dGrapher);
    check("33b a line palette selects --line", /--line\b/.test(dGrapher), dGrapher);

    // 33c — a `static_viz` chart group is `chart__<slug>`, and an exact-only match called that
    // "ungrouped" while walking it anyway: a wrong explanation for a right answer.
    const slugged = buildFrame();
    slugged.children.find((c) => c.name === "chart").name = "chart__agriculture-share";
    const outSlug = await run(slugged, {});
    check("33c chart__<slug> resolves by name",
          /name "chart__agriculture-share"/.test(outSlug.resolved.chartBy), outSlug.resolved.chartBy);
    check("33c and is not reported as ungrouped",
          !/ungrouped/.test(outSlug.resolved.chartBy), outSlug.resolved.chartBy);
    // The third variant counted in the file: a two-format page names them `chart-desktop`/`chart-mobile`.
    const hyphen = buildFrame();
    hyphen.children.find((c) => c.name === "chart").name = "chart-desktop";
    const outHyphen = await run(hyphen, {});
    check("33c chart-desktop resolves by name",
          /name "chart-desktop"/.test(outHyphen.resolved.chartBy) && !/ungrouped/.test(outHyphen.resolved.chartBy),
          outHyphen.resolved.chartBy);
    check("33c plain `chart` still resolves exactly",
          /name "chart"$/.test((await run(buildFrame(), {})).resolved.chartBy),
          (await run(buildFrame(), {})).resolved.chartBy);

    // 34 — a LEGEND is a picture of the categories, not a set of categories. grapher draws it INSIDE
    // the chart group and OUTSIDE `map` (measured: `chart > numeric-color-legend > {lines, swatches,
    // labels, swatch-hit-areas}`, a sibling of `map`), so every swatch arrived as an ordinary filled
    // plot mark with `fromMap: false`. An ordinary choropleth then reported TWO palettes — its own
    // legend audited as chart marks, under `--separated`, against the wrong set.
    const legendGroup = (name, swatches) => node({ name, x: 40, y: 470, width: 300, height: 30, children: [
      node({ name: "swatches", x: 40, y: 470, width: 300, height: 12,
             children: swatches.map(([nm, hex], i) => node({ type: "RECTANGLE", name: nm, x: 40 + i * 70,
               y: 470, width: 60, height: 12, fills: solid(hex) })) }),
      node({ name: "labels", x: 40, y: 486, width: 300, height: 14,
             children: [text("0", "0", 12, 40, 486, 20, 14, "#2d2e2d")] })] });

    // 34a — a pure map with grapher's numeric legend. Two of the swatches repeat the countries' own
    // bin colours; the third is an empty bin, drawn in the legend and on no country.
    const legMap = buildFrame({ mapCountries: true });
    const legMapChart = legMap.children.find((c) => c.name === "chart");
    legMapChart.children = legMapChart.children.filter((c) => !/^(line|outline)__|^datapoints__/.test(c.name));
    legMapChart.children.push(legendGroup("numeric-color-legend", [
      ["Rectangle 3", "#4c6a9c"], ["Rectangle 4", "#b13507"], ["Rectangle 5", "#e56e5a"]]));
    const dLegMap = row(await run(legMap, {}), "colour-vision").detail;
    check("34a a map's own legend does not make it a two-palette frame",
          !/two separate runs/.test(dLegMap) && !/--separated/.test(dLegMap), dLegMap);
    check("34a and the map itself is still audited", /--maps/.test(dLegMap) && /#4c6a9c/.test(dLegMap), dLegMap);
    check("34a the excluded swatches are counted, not silently dropped",
          /legend swatch\(es\) are NOT in this palette/.test(dLegMap), dLegMap);
    check("34a and a colour that exists ONLY in the legend is named",
          /#e56e5a/.test(dLegMap) && /ONLY in the legend/.test(dLegMap), dLegMap);
    check("34a while a swatch's own node name never labels the audit",
          !/Rectangle 3/.test(dLegMap), dLegMap);

    // 34b — the same on an ordinary chart, where the harm lands on `--names`: a numeric legend's bins
    // are unnamed rects, so one swatch in a colour of its own put an import default into the palette
    // and dropped the flag for the whole run. The series here is `line__A`, which names itself.
    const legLine = buildFrame({ secondColour: true });
    legLine.children.find((c) => c.name === "chart").children
      .push(legendGroup("categorical-color-legend", [["Rectangle 8", "#e56e5a"]]));
    const dLegLine = row(await run(legLine, {}), "colour-vision").detail;
    check("34b a legend swatch does not cost an ordinary chart its --names",
          /--names 'B,A'/.test(dLegLine) && !/--names omitted/.test(dLegLine), dLegLine);
    check("34b and the swatch colour is reported as legend-only, not audited as a category",
          /#e56e5a/.test(dLegLine) && /ONLY in the legend/.test(dLegLine), dLegLine);

    // 34c — excluded from the PALETTE is not excluded from the page: an annotation dropped over the
    // legend still covers something the reader needs. It keeps its box and is named for what it is,
    // because "covers a filled data mark" sends the reader hunting for a bar that is not there.
    const legAnn = buildFrame({ annotation: annotation({ x: 45, y: 468, w: 40, h: 16, stroke: "#ffffff", strokeWeight: 3 }) });
    legAnn.children.find((c) => c.name === "chart").children
      .push(legendGroup("categorical-color-legend", [["Rectangle 8", "#e56e5a"]]));
    const dLegAnn = row(await run(legAnn, {}), "annotation-overlap");
    check("34c an annotation over a legend swatch still FAILS", dLegAnn.status === "FAIL", dLegAnn.detail);
    check("34c and it is called a legend swatch, not a data mark",
          /a legend swatch/.test(dLegAnn.detail) && !/Rectangle 8 — a filled data mark/.test(dLegAnn.detail), dLegAnn.detail);

    // 35 — the frame that paints no pixels. `collect` tests `visible` on every node it walks, but it
    // starts at the frame's CHILDREN: a `visible: false` on the frame itself, or on a section holding
    // it, was never read, and those children carry their own `visible: true`. The frame certified a
    // full sheet of rows about a deliverable that is switched off.
    const hiddenFrame = buildFrame({ barSegment: true });
    hiddenFrame.visible = false;
    const outHidden = await run(hiddenFrame, {});
    check("35 a hidden frame emits ONE row, not a sheet of verdicts",
          outHidden.rows.length === 1 && outHidden.rows[0].check === "frame-not-rendered", `${outHidden.rows.length} rows`);
    check("35 the row FAILS rather than passing quietly", outHidden.rows[0].status === "FAIL", outHidden.rows[0].status);
    check("35 the verdict says NOT CHECKED, never 'no mechanical row failed'",
          /NOT CHECKED/.test(outHidden.verdict) && !/no mechanical row failed/.test(outHidden.verdict), outHidden.verdict);
    check("35 and it names the switch that is off", /visible=false/.test(outHidden.rows[0].detail), outHidden.rows[0].detail);
    // The ancestor half. Nothing on the frame or under it is touched here, so only the climb can
    // account for the difference — and unhiding the frame would not help.
    const underSection = buildFrame({ barSegment: true });
    const outSection = await run(underSection, {}, (f) => node({ type: "SECTION", name: "WIP", visible: false, children: [f] }));
    check("35 a hidden SECTION above the frame counts too",
          outSection.rows.length === 1 && outSection.rows[0].check === "frame-not-rendered", `${outSection.rows.length} rows`);
    check("35 and the ancestor is named, since unhiding the frame would not help",
          /WIP/.test(outSection.rows[0].detail), outSection.rows[0].detail);
    // Zero effective opacity is the same state reached by the other switch, and takes the same route.
    const zeroFrame = buildFrame({ barSegment: true });
    zeroFrame.opacity = 0;
    const outZero = await run(zeroFrame, {});
    check("35 an effectively invisible frame takes the same gate",
          outZero.rows.length === 1 && /effective opacity/.test(outZero.rows[0].detail), outZero.verdict);
    // And the gate must not fire on a frame that merely CONTAINS something hidden, or every page with
    // a switched-off spare layer stops being checked at all.
    const hiddenChild = buildFrame({ barSegment: true });
    hiddenChild.children.find((c) => c.name === "chart").visible = false;
    const outChild = await run(hiddenChild, {});
    check("35 a hidden CHILD does not stop the frame being checked",
          !row(outChild, "frame-not-rendered") && outChild.rows.length > 20, `${outChild.rows.length} rows`);
    // A visible frame under a visible section is the other direction: the climb must not invent a
    // hidden ancestor out of a `visible: true` one.
    const outVisibleSection = await run(buildFrame({ barSegment: true }), {}, (f) => node({ type: "SECTION", name: "Live", children: [f] }));
    check("35 a visible section above the frame changes nothing",
          !row(outVisibleSection, "frame-not-rendered") && outVisibleSection.rows.length > 20,
          `${outVisibleSection.rows.length} rows`);

    // And the reason given must be the real one, not the comma message reused for a missing name.
    const anon = buildFrame({ barSegment: true, secondColour: true });
    renameMarks(anon, "");
    const d3 = row(await run(anon, {}), "colour-vision").detail;
    check("33 an unnamed mark reports THAT, not a phantom comma",
          /has no name/.test(d3) && !/contains a comma/.test(d3) && !/apostrophe/.test(d3), d3);

    // 36 — a palette of ONE colour has no pair, and both rows compare pairs. `color_audit.py` does not
    // say so: handed a single hex it prints an empty pair list, "overall: min dE inf", and exits 0,
    // which reads exactly like a clean audit. GUIDELINES.md rules on this case directly — one
    // categorical colour against neutral grays has nothing to check — so the command is withheld and
    // the two checks that ARE live are handed over instead.
    const oneColour = row(await run(buildFrame(), {}), "colour-vision").detail;
    check("36 a one-colour palette emits no command at all",
          !/Run: /.test(oneColour) && !/--names|--line|--maps|--separated/.test(oneColour)
          && /NOTHING TO RUN/.test(oneColour), oneColour);
    check("36 and it names the colour rather than reporting an empty plot",
          /#4c6a9c/.test(oneColour) && !/No data marks or series strokes found/.test(oneColour), oneColour);
    check("36 and hands over the two checks that are live",
          /contrast against the frame's background/.test(oneColour) && /grayscale/.test(oneColour), oneColour);
    check("36 and says why running it anyway would look clean",
          /inf/.test(oneColour) && /GUIDELINES/.test(oneColour), oneColour);
    // The negative control: two colours is a pair, so the command comes back.
    const twoColour = row(await run(buildFrame({ secondColour: true }), {}), "colour-vision").detail;
    check("36 two colours still produce a runnable command",
          /Run: \.venv\/bin\/python/.test(twoColour) && !/NOTHING TO RUN/.test(twoColour), twoColour);
    // And the clash note must SURVIVE the withheld command. Two categories painted one colour ARE a
    // one-colour palette — deltaE 0, the severest collision there is — so the branch that withholds
    // the run is the branch that most needs to report what it found. Computed before the exit.
    const twoOnOne = row(await run(buildFrame({ extraLine: 3 }), {}), "colour-vision").detail;
    check("36 a withheld run still names two categories sharing one colour",
          /NOTHING TO RUN/.test(twoOnOne) && /judge them by eye/.test(twoOnOne)
          && /(A \+ B|B \+ A)/.test(twoOnOne), twoOnOne);
    // GUIDELINES.md rules the same way on a declared highlight treatment: the muting grays are
    // furniture, so one highlight against them leaves no categorical pair. Which entries are muting
    // grays is not decidable here — a category legitimately painted gray is a category — so the run is
    // annotated rather than suppressed, and only when the treatment is DECLARED.
    const hi = row(await run(buildFrame({ secondColour: true }), { highlightTreatment: true }), "colour-vision").detail;
    check("36 a declared highlight treatment warns before the command",
          /highlightTreatment is set/.test(hi) && /Run: \.venv\/bin\/python/.test(hi), hi);
    check("36 and an undeclared frame carries no such warning",
          !/highlightTreatment is set/.test(twoColour), twoColour);
  }

  const bad = results.filter((x) => !x.ok);
  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  >> " + x.detail}`);
  console.log(bad.length ? `\n${bad.length} FAILURES` : `\nALL PASS (${results.length} checks)`);
  process.exit(bad.length ? 1 : 0);
})();
