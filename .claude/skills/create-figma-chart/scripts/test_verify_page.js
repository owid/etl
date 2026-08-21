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
  const src = text("source", "Data source: X", 13, 16, 488, contentW, 16);
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
  const chart = node({ name: "chart", type: "GROUP", x: 16, y: 122, width: contentW, height: 352, children: kids });

  // `ungrouped` models the documented rework case: the chart GROUP is gone and its subgroups sit as
  // direct frame children, so CONFIG.chartName resolves nothing and the fallback has to find the plot.
  const children = opts.ungrouped ? [header, footer, logo, ...kids] : [header, footer, logo, chart];
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

async function run(frame, config) {
  const byId = {};
  const index = (n) => { if (n.id) byId[n.id] = n; for (const c of n.children || []) index(c); };
  const page = node({ id: "P:1", type: "PAGE", name: "page", children: [frame] });
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
    check("1 no row silently absent", out.rows.length >= 24, `${out.rows.length} rows`);
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
    check("6 white knockout on cream FAILS", /not the frame's own #fffbf5/.test(row(out, "annotation-knockout").detail), row(out, "annotation-knockout").detail);
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
    const big = await run(buildFrame({ annotation: annotation({ x: 100, y: 195, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("20 540-wide still checks block-gap", row(big, "annotation-block-gap").status !== "SKIPPED", row(big, "annotation-block-gap").detail);
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

  // 32 — a text node whose SIZES COULD NOT BE READ is not a pass. `sizeRanges` catches a throwing or
  // non-numeric segment read and returns [], so the node contributes no ranges at all and `under.length`
  // stays 0 — the row said "ok" and the frame verdict read "no mechanical row failed" with an unmeasured
  // range sitting on it. The count was already in the detail; it was the STATUS that certified the frame.
  {
    // over empty space and strokeless, so this isolates the text rows: the knockout row must stay ok
    const unreadable = text("annotation__opaque", "Note nobody could measure", MIXED, 60, 140, 90, 18, "#2d2e2d", {
      fontSize: MIXED, strokes: [], strokeWeight: 0, textStyleId: "S:abc",
      // fontName only: asking for fontSize throws, exactly as an unsupported field would
      segments: { fontName: [["Regular", 0, 25]] },
    });
    const out = await run(buildFrame({ annotation: unreadable }), {});
    const tf = row(out, "text-floor");
    check("32 an unmeasurable text node is not certified ok", tf.status === "REVIEW", tf.status + " " + tf.detail);
    check("32 and the node is named as NOT judged", /NOT judged/.test(tf.detail) && /annotation__opaque/.test(tf.detail), tf.detail);
    const clean = await run(buildFrame(), {});
    const nrev = (o) => o.rows.filter((x) => x.status === "REVIEW").length;
    check("32 it adds a row to review on an otherwise identical frame", nrev(out) === nrev(clean) + 1, `${nrev(out)} vs ${nrev(clean)}`);
    check("32 the same frame with readable text is still a clean ok", row(clean, "text-floor").status === "ok", row(clean, "text-floor").detail);
    // and a REAL breach still outranks it — FAIL is not softened to REVIEW
    const both = await run(buildFrame({ labelSize: 9, annotation: unreadable }), {});
    check("32 a genuine sub-floor range still FAILS", row(both, "text-floor").status === "FAIL", row(both, "text-floor").detail);
  }

  // 33 — a knockout paint that RENDERS NOTHING. `strokes.length` counts a paint switched off or at zero
  // opacity, so an annotation crossing a gridline with a disabled stroke passed the weight, alignment and
  // colour rows: the row certified the missing knockout it exists to catch. Fills are read this way
  // everywhere above (`visible !== false`); strokes were not.
  {
    const off = annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 });
    off.strokes = [Object.assign({}, off.strokes[0], { visible: false })];
    const out = await run(buildFrame({ annotation: off }), {});
    const d = row(out, "annotation-knockout").detail;
    check("33 a switched-off knockout paint FAILS", row(out, "annotation-knockout").status === "FAIL", d);
    check("33 and is reported as NO knockout", /carries NO knockout/.test(d), d);
    const clear = annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 });
    clear.strokes = [Object.assign({}, clear.strokes[0], { opacity: 0 })];
    check("33 a fully transparent one FAILS by the same route",
          row(await run(buildFrame({ annotation: clear }), {}), "annotation-knockout").status === "FAIL",
          row(await run(buildFrame({ annotation: clear }), {}), "annotation-knockout").detail);
    // the COLOUR is read off the paint that renders, never off strokes[0]: an invisible white in front
    // of the frame's own cream used to be reported as a hardcoded-white knockout
    const layered = annotation({ x: 100, y: 195, w: 120, h: 18, stroke: "#ffffff", strokeWeight: 3 });
    layered.strokes = [Object.assign({}, layered.strokes[0], { visible: false }), solid("#fffbf5")[0]];
    const lay = await run(buildFrame({ frameFill: "#fffbf5", annotation: layered }), {});
    check("33 an invisible paint in front does not mask the real colour",
          row(lay, "annotation-knockout").status === "ok", row(lay, "annotation-knockout").detail);
  }

  const bad = results.filter((x) => !x.ok);
  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  >> " + x.detail}`);
  console.log(bad.length ? `\n${bad.length} FAILURES` : `\nALL PASS (${results.length} checks)`);
  process.exit(bad.length ? 1 : 0);
})();
