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
  vectorNetwork: { vertices: pts.map(([x, y]) => ({ x, y })) },
});

const gridline = (name, y, dashed) => node({
  type: "VECTOR", name, x: 16, y, width: 508, height: 1,
  strokeWeight: 1, strokes: solid("#dddddd"), dashPattern: dashed ? [4, 4] : [], strokeAlign: "CENTER",
  absoluteTransform: [[1, 0, 16], [0, 1, y]], vectorNetwork: { vertices: [{ x: 0, y: 0 }, { x: 508, y: 0 }] },
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

  const kids = [
    gridline("grid-1", 200, true),
    gridline("grid-2", 300, true),
    gridline("zero", 460, false),
    line("line__A", [[40, 440], [200, 300], [440, 260]], lineWeight),
    line("outline__A", [[40, 440], [200, 300], [440, 260]], lineWeight + 1),
    text("label__A", "Country A", labelSize, 450, 250, 60, 16, opts.labelFill || "#4c6a9c"),
    node({ name: "datapoints__A", x: 430, y: 250, width: 10, height: 10,
           children: [node({ type: "ELLIPSE", name: "dp", x: 430, y: 250, width: 8, height: 8 })] }),
  ];
  if (opts.extraLine) kids.push(line("line__B", [[40, 420], [200, 340], [440, 300]], opts.extraLine));
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

  // 8 — covering a dot is never legal.
  {
    const out = await run(buildFrame({ annotation: annotation({ x: 425, y: 245, w: 40, h: 18, stroke: "#ffffff", strokeWeight: 3 }) }), {});
    check("8 covering a dot FAILS", row(out, "annotation-overlap").status === "FAIL" && /a dot/.test(row(out, "annotation-overlap").detail), row(out, "annotation-overlap").detail);
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

  const bad = results.filter((x) => !x.ok);
  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  >> " + x.detail}`);
  console.log(bad.length ? `\n${bad.length} FAILURES` : `\nALL PASS (${results.length} checks)`);
  process.exit(bad.length ? 1 : 0);
})();
