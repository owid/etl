// test_replay_chart_edits.js — stubbed-figma harness for replay_chart_edits.js. Run: `node <this>`.
//
// The script's value is its ORDER, so most of these assert ordering rather than arithmetic: that
// strokes are set after the last scale (not multiplied by it), that hiding happens before measuring,
// and that a squeeze is refused rather than silently rewrapping labels.

const fs = require("fs");
const path = require("path");
const SRC = fs.readFileSync(path.join(__dirname, "replay_chart_edits.js"), "utf8");

let AUTO = 0;
function node(props) {
  const n = Object.assign({ visible: true, x: 0, y: 0, width: 0, height: 0, type: "FRAME", name: "" }, props);
  if (!n.id) n.id = `auto:${++AUTO}`;
  if (n.children) for (const c of n.children) c.parent = n;
  if (n.children === undefined) delete n.children;
  Object.defineProperty(n, "absoluteBoundingBox", {
    get() { return { x: n.x, y: n.y, width: n.width, height: n.height }; },
    configurable: true,
  });
  n.findAll = function (fn) { const o = []; (function rec(m) { for (const c of m.children || []) { if (!fn || fn(c)) o.push(c); rec(c); } })(this); return o; };
  n.findOne = function (fn) { return this.findAll(fn)[0] || null; };
  n.resize = function (w, h) { this.width = w; this.height = h; };
  // rescale about the node's own top-left: geometry, type and stroke weight all scale, which is why
  // the script must set strokes afterwards.
  n.rescale = function (f) {
    const ox = this.x, oy = this.y;
    (function walk(m) {
      m.x = ox + (m.x - ox) * f;
      m.y = oy + (m.y - oy) * f;
      m.width *= f;
      m.height *= f;
      if (typeof m.fontSize === "number") m.fontSize *= f;
      if (typeof m.strokeWeight === "number") m.strokeWeight *= f;
      if (Array.isArray(m.dashPattern) && m.dashPattern.length) m.dashPattern = m.dashPattern.map((v) => v * f);
      for (const c of m.children || []) walk(c);
    })(this);
  };
  return n;
}
const solid = () => [{ type: "SOLID", visible: true, color: { r: 0, g: 0, b: 0 } }];
const txt = (name, x, y, w, h, size) => node({ type: "TEXT", name, x, y, w: undefined, width: w, height: h, fontSize: size, characters: name });

// A 540x540 frame: header 16..108, footer top 488, so the band is 380 and the target chart box 352.
function build(opts = {}) {
  const header = node({ name: "header", layoutMode: "VERTICAL", x: 16, y: 16, width: 508, height: 92,
    children: [txt("title", 16, 16, 400, 30, 24), txt("subtitle", 16, 52, 508, 56, 17)] });
  const footer = node({ name: "footer", layoutMode: "VERTICAL", x: 16, y: 488, width: 508, height: 36,
    children: [txt("source", 16, 488, 300, 16, 13)] });
  const logo = node({ name: "logo", x: 460, y: 16, width: 64, height: 35 });

  const bars = node({ name: "bars", x: 100, y: 200, width: 500, height: 300,
    children: [node({ type: "VECTOR", name: "bar", x: 100, y: 200, width: 500, height: 300, fills: solid() })] });
  const gridlines = node({ name: "horizontal-grid-lines", x: 100, y: 200, width: 500, height: 300,
    children: [
      node({ type: "VECTOR", name: "g1", x: 100, y: 220, width: 500, height: 1, strokeWeight: 1, strokes: solid(), dashPattern: [4, 4] }),
      node({ type: "VECTOR", name: "g2", x: 100, y: 300, width: 500, height: 1, strokeWeight: 1, strokes: solid(), dashPattern: [4, 4] }),
      node({ type: "VECTOR", name: "g3", x: 100, y: 380, width: 500, height: 1, strokeWeight: 1, strokes: solid(), dashPattern: [4, 4] }),
    ] });
  const kids = [gridlines, bars, txt("label__A", 120, 210, 80, 18, 14)];

  // a zero line grapher draws far past the data — the single-entity stacked-discrete-bar shape
  if (opts.longZeroLine) kids.push(node({ type: "VECTOR", name: "vertical-zero-line", x: 100, y: 120, width: 0, height: 560, strokeWeight: 0.5, strokes: solid(), dashPattern: [] }));
  if (opts.connectors) kids.push(node({ name: "connectors", x: 80, y: 150, width: 700, height: 460 }));
  if (opts.sizeLegend) kids.push(node({ name: "size-legend", x: 400, y: 300, width: 60, height: 40, children: [txt("600M", 400, 300, 30, 8, 6)] }));
  if (opts.subpathNode) {
    kids.push(node({ type: "VECTOR", name: "United-States", x: 100, y: 200, width: 200, height: 100, fills: solid(),
      vectorPaths: [{ windingRule: "NONZERO", data: "M0 80L2 80L2 82L0 82Z M4 84L6 84L6 86L4 86Z M8 88L10 88L10 90L8 90Z M60 0L200 0L200 100L60 100Z M70 10L120 10L120 40L70 40Z M130 20L180 20L180 50L130 50Z" }] }));
  }
  // Grapher's real slope shape: `slope__<Entity>` and `outline__<Entity>` are GROUPS of
  // {start-point, end-point, line}, and the only stroked node is called plain `line`. Matching the
  // stroked node's own name finds no series here, which is why the identity has to travel down the walk.
  if (opts.slopeSeries) {
    const seg = (w) => node({ type: "VECTOR", name: "line", x: 100, y: 200, width: 300, height: 150,
      strokeWeight: w, strokes: solid(), dashPattern: [] });
    kids.push(node({ name: "slopes", x: 100, y: 200, width: 300, height: 150, children: [
      node({ name: "outline__USA", x: 100, y: 200, width: 300, height: 150, children: [
        node({ type: "VECTOR", name: "start-point", x: 100, y: 200, width: 6, height: 6 }), seg(opts.slopeHalo || 1.7)] }),
      node({ name: "slope__USA", x: 100, y: 200, width: 300, height: 150, children: [
        node({ type: "VECTOR", name: "end-point", x: 400, y: 350, width: 6, height: 6 }), seg(opts.slopeLine || 1.1)] })] }));
  }
  // A gridline nested one level deeper than its container — the parent-only check never saw it.
  if (opts.nestedGrid) kids.push(node({ name: "horizontal-grid-lines", x: 100, y: 200, width: 500, height: 300, children: [
    node({ name: "inner", x: 100, y: 200, width: 500, height: 300, children: [
      node({ type: "VECTOR", name: "gN", x: 100, y: 250, width: 500, height: 1, strokeWeight: 0.4, strokes: solid(), dashPattern: [4, 4] })] })] }));
  if (opts.legend) kids.push(node({ name: "numeric-color-legend", x: 200, y: 520, width: 300, height: 30, children: [txt("0 t", 200, 520, 20, 12, 12)] }));
  if (opts.mapBody) kids.push(node({ name: "map", x: 100, y: 200, width: 500, height: 250, children: [node({ type: "VECTOR", name: "France", x: 100, y: 200, width: 500, height: 250, fills: solid() })] }));

  // Width chosen so the height-first fit lands the box on 508 exactly: 508 * (480/352). A fixture with
  // an unsolved aspect makes the width step refuse to squeeze, which is correct behaviour and would
  // make case 7 assert the wrong thing.
  const chart = node({ name: "chart", type: "GROUP", x: 100, y: 120, width: 508 * (480 / 352), height: 480, children: kids });
  const frame = node({ id: "F:1", name: "clone", x: 0, y: 0, width: 540, height: 540, fills: solid(), children: [header, footer, logo, chart] });
  return frame;
}

async function run(frame, cfg) {
  const byId = {};
  (function index(n) { if (n.id) byId[n.id] = n; for (const c of n.children || []) index(c); })(node({ id: "P:1", type: "PAGE", name: "page", children: [frame] }));
  const page = frame.parent;
  const figma = { currentPage: page, getNodeByIdAsync: async (id) => byId[id] || null, setCurrentPageAsync: async () => {} };
  const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m, "const CONFIG = __CONFIG__;");
  const fn = new Function("figma", "__CONFIG__", `return (async () => { ${body} })();`);
  return fn(figma, Object.assign({ frameId: "F:1", chartName: "chart", contentL: 16, contentW: 508, gap: 14,
    hide: [], trimSubpaths: [], trimToExtentOf: [], furnitureWeight: 1, seriesWeights: null,
    restoreDashes: true, legend: null, bindAxis: "height", dryRun: false }, cfg));
}

const results = [];
const check = (name, ok, detail) => results.push({ name, ok: !!ok, detail: ok ? "" : String(detail).slice(0, 200) });

(async () => {
  // 1 — dryRun writes nothing
  {
    const f = build({ connectors: true });
    const before = JSON.stringify({ w: f.findOne((n) => n.name === "chart").width, vis: f.findOne((n) => n.name === "connectors").visible });
    const out = await run(f, { hide: [/^connectors$/], dryRun: true });
    const after = JSON.stringify({ w: f.findOne((n) => n.name === "chart").width, vis: f.findOne((n) => n.name === "connectors").visible });
    check("1 dryRun changes nothing", before === after, `${before} -> ${after}`);
    check("1 dryRun still reports a plan", out.plan.length > 0 && /DRY RUN/.test(out.verdict), out.verdict);
  }

  // 2 — hide by regex and by string, and a miss is reported rather than silently passing
  {
    const f = build({ connectors: true, sizeLegend: true });
    const out = await run(f, { hide: [/^connectors$/, "size-legend", "does-not-exist"] });
    check("2 hides by regex", f.findOne((n) => n.name === "connectors").visible === false, "connectors still visible");
    check("2 hides by exact string", f.findOne((n) => n.name === "size-legend").visible === false, "size-legend still visible");
    check("2 reports a name that matched nothing", out.plan.some((p) => p.what === "does-not-exist" && /NO MATCH/.test(p.detail)), JSON.stringify(out.plan));
  }

  // 3 — strokes are set AFTER the last scale. The fit here is ~0.73x, so a furniture stroke set before
  //     the scale would end at ~0.73px. This is the most-repeated mistake in the skill's history.
  {
    const f = build();
    const out = await run(f, {});
    const g1 = f.findOne((n) => n.name === "g1");
    check("3 furniture ends at exactly 1px, not thinned by the fit", Math.abs(g1.strokeWeight - 1) < 0.001, `strokeWeight ${g1.strokeWeight}`);
    check("3 and the fix is reported", out.strokeFixes.some((x) => x.name === "g1" && x.to === 1), JSON.stringify(out.strokeFixes));
    check("3 the dash is restored to its pre-scale value", JSON.stringify(g1.dashPattern) === "[4,4]", JSON.stringify(g1.dashPattern));
  }

  // 4 — a solid node must NOT gain a dash. Assigning one restyles the furniture instead of unscaling it.
  {
    const f = build({ longZeroLine: true });
    await run(f, {});
    const z = f.findOne((n) => n.name === "vertical-zero-line");
    check("4 a solid zero line stays solid", Array.isArray(z.dashPattern) && z.dashPattern.length === 0, JSON.stringify(z.dashPattern));
    check("4 and is taken to the furniture 1px", Math.abs(z.strokeWeight - 1) < 0.001, `strokeWeight ${z.strokeWeight}`);
  }

  // 5 — trimToExtentOf shortens over-long furniture to the data's own extent
  {
    const f = build({ longZeroLine: true });
    const zBefore = f.findOne((n) => n.name === "vertical-zero-line").height;
    const out = await run(f, { trimToExtentOf: [{ node: "vertical-zero-line", matchExtentOf: "bars", axis: "y" }] });
    const z = f.findOne((n) => n.name === "vertical-zero-line"), bars = f.findOne((n) => n.name === "bars");
    check("5 the zero line is trimmed to the bars", Math.abs(z.height - bars.height) < 0.01 && z.height < zBefore, `${zBefore} -> ${z.height} vs bars ${bars.height}`);
    check("5 and it is reported", out.plan.some((p) => p.step === "trimToExtentOf"), JSON.stringify(out.plan));
  }

  // 6 — trimSubpaths drops the specks and keeps the mainland; and refuses a cut that is too large
  {
    const f = build({ subpathNode: true });
    const out = await run(f, { trimSubpaths: [{ node: "United-States", keepRightOf: 130 }] });
    const us = f.findOne((n) => n.name === "United-States");
    const subpaths = us.vectorPaths[0].data.split(/(?=[Mm])/).filter((c) => c.trim().length);
    check("6 drops the three specks, keeps the three mainland subpaths", subpaths.length === 3, `${subpaths.length} subpaths left`);
    check("6 and reports the count", out.plan.some((p) => p.step === "trimSubpaths" && /dropped 3 of 6/.test(p.detail)), JSON.stringify(out.plan));

    const f2 = build({ subpathNode: true });
    const out2 = await run(f2, { trimSubpaths: [{ node: "United-States", keepRightOf: 1000 }] });
    check("6 refuses a cut that would drop nearly everything",
          out2.plan.some((p) => p.step === "trimSubpaths" && /REFUSED/.test(p.detail)) &&
          f2.findOne((n) => n.name === "United-States").vectorPaths[0].data.split(/(?=[Mm])/).filter((c) => c.trim().length).length === 6,
          JSON.stringify(out2.plan));
  }

  // 7 — the result lands on the content box with symmetric BOX gaps
  {
    const f = build();
    const out = await run(f, {});
    check("7 box gaps are symmetric", out.result.symmetric, `${out.result.gapAbove} / ${out.result.gapBelow}`);
    check("7 left and right edges are exact", out.result.edgesExact, JSON.stringify(out.result.box));
    check("7 no text rewrapped", out.fit.rewrappedTextNodes === 0, String(out.fit.rewrappedTextNodes));
  }

  // 8 — a squeeze past 0.5% is REFUSED, not applied. resize()/rescale() down rewraps labels, and the
  //     fix is a shorter re-export, never a squeeze.
  {
    const f = build();
    // make the chart much wider than the content box so the width step would have to squeeze hard
    const chart = f.findOne((n) => n.name === "chart");
    chart.width = 1400;
    const out = await run(f, {});
    check("8 a hard squeeze is refused and says why", /REFUSED/.test(out.fit.widthNote) && /re-export/i.test(out.fit.widthNote), out.fit.widthNote);
  }

  // 9 — a map's legend is centred on the content box and tucked under the plot
  {
    const f = build({ mapBody: true, legend: true });
    const out = await run(f, { legend: { name: /color-legend$/, plot: "map", gapToPlot: 16, centreOnContent: true } });
    const legend = f.findOne((n) => n.name === "numeric-color-legend"), map = f.findOne((n) => n.name === "map");
    const left = legend.x - 16, right = 16 + 508 - (legend.x + legend.width);
    check("9 the legend is centred on the content box", Math.abs(left - right) < 0.01, `left ${left}, right ${right}`);
    check("9 and sits the stated gap under the map", Math.abs(legend.y - (map.y + map.height + 16)) < 0.01, `${legend.y} vs ${map.y + map.height + 16}`);
    check("9 and it is reported", /centred/.test(out.legendNote || ""), out.legendNote);
  }

  // 10 — a renamed chart group is a hard error, not a silent no-op. A re-import is exactly when the
  //      name changes, which is the moment this script runs.
  {
    const f = build();
    f.findOne((n) => n.name === "chart").name = "chart-area";
    let threw = null;
    try { await run(f, {}); } catch (e) { threw = e.message; }
    check("10 a missing chart group throws with a usable message", threw && /no child named/.test(threw) && /re-import/.test(threw), String(threw));
  }

  // 11 — a refused width fit has to reach the VERDICT. It used to live only in `fit.widthNote` while
  //      the verdict said "wrote every edit" whenever no text happened to rewrap, which put a success
  //      line over a chart hanging 150px off the content box.
  {
    const f = build();
    f.findOne((n) => n.name === "chart").width = 1400;
    const out = await run(f, {});
    check("11 a refused width fit makes the verdict INCOMPLETE", /^INCOMPLETE/.test(out.verdict), out.verdict);
    check("11 and the verdict itself names the refusal", /refused/i.test(out.verdict), out.verdict);
    check("11 and names the box that missed the content edges", /does not sit on the content box/.test(out.verdict), out.verdict);
    check("11 with the same facts listed in result.problems", out.result.problems.length >= 2, JSON.stringify(out.result.problems));
    const clean = await run(build(), {});
    check("11 while a clean run still reads as a clean success", !/^INCOMPLETE/.test(clean.verdict) && clean.result.problems.length === 0, clean.verdict);
  }

  // 12 — a MAP binds on the WIDTH. Its ink aspect is the projection's, not the canvas's, so a
  //      height-first fit overflows the content width (measured 141px: Russia, Australia and the
  //      legend's last bin cut off). reference/FITTING.md and per-chart-type/maps.md fit it width-first
  //      and centre it in the band.
  {
    const mapFrame = () => { const f = build({ mapBody: true }); const c = f.findOne((n) => n.name === "chart"); c.width = 900; c.height = 480; return f; };
    const hf = await run(mapFrame(), {});
    check("12 height-first on a map overflows and is reported, not silently accepted",
          /^INCOMPLETE/.test(hf.verdict) && !hf.result.edgesExact && hf.result.box.w > 508,
          `${hf.verdict} | w=${hf.result.box.w}`);

    const wf = await run(mapFrame(), { bindAxis: "width" });
    check("12 width-bound puts the box exactly on the content width", Math.abs(wf.result.box.w - 508) < 0.01 && wf.result.edgesExact, JSON.stringify(wf.result.box));
    check("12 and needs no second rescale", /no second rescale/.test(wf.fit.widthNote) && wf.fit.bindAxis === "width", wf.fit.widthNote);
    check("12 leaving the height where the projection put it, centred in the band", wf.result.symmetric && wf.result.box.T > 14, JSON.stringify(wf.result));
    check("12 and the run reads as complete", !/^INCOMPLETE/.test(wf.verdict), wf.verdict);

    // a map too tall for the band is an overflow to REPORT, never a squeeze: squeezing one axis
    // distorts dots, arrowheads and text, so the fix is a re-export.
    const tall = build({ mapBody: true });
    const tc = tall.findOne((n) => n.name === "chart"); tc.width = 900; tc.height = 900;
    const to = await run(tall, { bindAxis: "width" });
    check("12 a map taller than the band reports the overflow in the verdict",
          /^INCOMPLETE/.test(to.verdict) && /overflows the band/i.test(to.verdict) && /never squeeze/.test(to.fit.heightNote),
          `${to.verdict} | ${to.fit.heightNote}`);
  }

  // 13 — a mistyped axis must throw. Silently fitting the wrong axis is the overflow the option exists
  //      to prevent, and an absent value keeps the height default every non-map chart wants.
  {
    let threw = null;
    try { await run(build(), { bindAxis: "Width" }); } catch (e) { threw = e.message; }
    check("13 a mistyped bindAxis throws", threw && /bindAxis must be "height" or "width"/.test(threw), String(threw));
    const noneGiven = await run(build(), { bindAxis: undefined });
    check("13 while an absent bindAxis defaults to the height", noneGiven.fit.bindAxis === "height" && noneGiven.result.edgesExact, JSON.stringify(noneGiven.fit));
  }

  // 16 — an edit that never got APPLIED reaches the verdict too. A re-import is exactly when a
  //      configured name stops matching, and these were recorded in `plan` only: a run whose geometry
  //      landed could return "wrote every edit" with a hide, a trim or the legend simply missing.
  {
    const miss = await run(build({ connectors: true }), { hide: [/^connectors$/, "does-not-exist"] });
    check("16 a hide target that matched nothing makes the verdict INCOMPLETE",
          /^INCOMPLETE/.test(miss.verdict) && /were NOT applied/.test(miss.verdict) && /does-not-exist/.test(miss.verdict), miss.verdict);

    const refused = await run(build({ subpathNode: true }), { trimSubpaths: [{ node: "United-States", keepRightOf: 1000 }] });
    check("16 a refused subpath cut does too", /^INCOMPLETE/.test(refused.verdict) && /REFUSED/.test(refused.verdict), refused.verdict);

    const noTarget = await run(build(), { trimToExtentOf: [{ node: "vertical-zero-line", matchExtentOf: "bars", axis: "y" }] });
    check("16 and a trim target that is not on the frame", /^INCOMPLETE/.test(noTarget.verdict) && /not found/.test(noTarget.verdict), noTarget.verdict);

    const noLegend = await run(build({ mapBody: true }), { legend: { name: /color-legend$/, plot: "map", gapToPlot: 16, centreOnContent: true } });
    check("16 an unresolvable legend is reported as incomplete, not as a note",
          /^INCOMPLETE/.test(noLegend.verdict) && /legend/.test(noLegend.verdict), noLegend.verdict);

    const ok = await run(build({ connectors: true, subpathNode: true }), { hide: [/^connectors$/], trimSubpaths: [{ node: "United-States", keepRightOf: 130 }] });
    check("16 while a run whose edits all applied still reads as a clean success",
          !/^INCOMPLETE/.test(ok.verdict) && ok.result.problems.length === 0, `${ok.verdict} | ${JSON.stringify(ok.result.problems)}`);
    // an already-trimmed node is a benign note, not an unapplied edit
    const already = await run(build({ subpathNode: true }), { trimSubpaths: [{ node: "United-States", keepRightOf: 0 }] });
    check("16 and 'already trimmed' is not counted as an unapplied edit",
          !/were NOT applied/.test(already.verdict), `${already.verdict} | ${JSON.stringify(already.plan)}`);
  }

  // 14 — the series identity is on an ANCESTOR on a slope export, so the configured house weights have
  //      to reach a stroked node called plain `line`. Read off the leaf name alone they never did, and
  //      the script reported a completed replay over grapher's own 1.1/1.7px.
  {
    const f = build({ slopeSeries: true });
    const out = await run(f, { seriesWeights: { line: 3, outline: 4 } });
    const slopeLine = f.findOne((n) => n.name === "slope__USA").children.find((c) => c.name === "line");
    const haloLine = f.findOne((n) => n.name === "outline__USA").children.find((c) => c.name === "line");
    check("14 a slope's series line reaches the house 3px", Math.abs(slopeLine.strokeWeight - 3) < 0.001, `strokeWeight ${slopeLine.strokeWeight}`);
    check("14 and its halo the house 4px", Math.abs(haloLine.strokeWeight - 4) < 0.001, `strokeWeight ${haloLine.strokeWeight}`);
    check("14 and both are reported with the reason", out.strokeFixes.filter((x) => x.why === "house series weight").length === 2, JSON.stringify(out.strokeFixes));
    // a series is never furniture, whatever container it sits in
    check("14 a series node is not dragged to the furniture 1px", !out.strokeFixes.some((x) => x.name === "line" && x.to === 1), JSON.stringify(out.strokeFixes));
    // with no seriesWeights configured, grapher's own weight is restored rather than thinned by the fit
    const f2 = build({ slopeSeries: true });
    const left = await run(f2, {});
    const leftLine = f2.findOne((n) => n.name === "slope__USA").children.find((c) => c.name === "line");
    check("14 with no seriesWeights the export's own weight is restored, not thinned",
          Math.abs(leftLine.strokeWeight - 1.1) < 0.001 && left.strokeFixes.some((x) => x.name === "line" && x.why === "un-thin to grapher's own"),
          `strokeWeight ${leftLine.strokeWeight} | ${JSON.stringify(left.strokeFixes)}`);
  }

  // 15 — the furniture container is carried too. Checking only the immediate parent left a gridline one
  //      level deeper at the export's own weight, which verify_page.js then fails.
  {
    const f = build({ nestedGrid: true });
    const out = await run(f, {});
    const gN = f.findOne((n) => n.name === "gN");
    check("15 a gridline nested below its container still reaches 1px", Math.abs(gN.strokeWeight - 1) < 0.001, `strokeWeight ${gN.strokeWeight}`);
    check("15 and is reported as furniture", out.strokeFixes.some((x) => x.name === "gN" && x.why === "furniture 1px"), JSON.stringify(out.strokeFixes));
  }

  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  >> " + x.detail}`);
  const bad = results.filter((x) => !x.ok);
  console.log(bad.length ? `\n${bad.length} FAILURES` : `\nALL PASS (${results.length} checks)`);
  process.exit(bad.length ? 1 : 0);
})();
