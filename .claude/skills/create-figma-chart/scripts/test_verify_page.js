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

let AUTO = 0;
function node(props) {
  const n = Object.assign({ visible: true, x: 0, y: 0, width: 0, height: 0, type: "FRAME", name: "" }, props);
  if (!n.id) n.id = `auto:${++AUTO}`;
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

const gridline = (name, y, dashed) => node({
  type: "VECTOR", name, x: 16, y, width: 508, height: 1,
  strokeWeight: 1, strokes: solid("#dddddd"), dashPattern: dashed ? [4, 4] : [], strokeAlign: "CENTER",
  absoluteTransform: [[1, 0, 16], [0, 1, y]],
  vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 508, y: 0 }], segments: [{ start: 0, end: 1 }] },
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

  const grid = node({ name: "horizontal-grid-lines", x: 16, y: 200, width: contentW, height: 260,
    children: [gridline("grid-1", 200, true), gridline("grid-2", 300, true), gridline("zero", 460, false)] });
  const kids = [
    grid,
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
      strokeWeight: 1, strokes: solid("#dddddd"), dashPattern: [], strokeAlign: "CENTER", fills: solid("#000000"),
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
  if (opts.zeroLineOnly) kids.push(node({ type: "VECTOR", name: "vertical-zero-line", x: 40, y: 160, width: 1, height: 300,
    strokeWeight: opts.zeroLineOnly, strokes: solid("#333333"), dashPattern: [], strokeAlign: "CENTER",
    absoluteTransform: [[1, 0, 40], [0, 1, 160]], vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 0, y: 300 }], segments: [{ start: 0, end: 1 }] } }));
  if (opts.mapCountries) kids.push(node({ name: "countries", x: 40, y: 160, width: 400, height: 200, children: [
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

  const children = [header, footer, logo, chart];
  if (opts.annotation) children.push(opts.annotation);

  return node({ id: "F:1", name: "test frame", x: 0, y: 0, width: W, height: opts.frameH || 540,
                fills: solid(opts.frameFill || "#ffffff"), children });
}

const annotation = (o) => text("annotation__test", o.chars || "Note", o.size || 14, o.x, o.y, o.w || 100, o.h || 18,
  o.fill || "#2d2e2d", {
    strokes: o.stroke ? solid(o.stroke) : [],
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

  // 3 — an arbitrary rescaled size must fail the ladder, style id or not (review finding 2).
  {
    const out = await run(buildFrame({ labelSize: 13.36 }), {});
    check("3 off-ladder 13.36 FAILS", row(out, "ladder-sizes").status === "FAIL", row(out, "ladder-sizes").detail);
    check("3 names the offending size", /13\.36/.test(row(out, "ladder-sizes").detail), row(out, "ladder-sizes").detail);
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

  const bad = results.filter((x) => !x.ok);
  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  >> " + x.detail}`);
  console.log(bad.length ? `\n${bad.length} FAILURES` : `\nALL PASS (${results.length} checks)`);
  process.exit(bad.length ? 1 : 0);
})();
