// Stubbed-figma harness for restyle_static_import.js — the script that had no test.
//
// Four consecutive review rounds found a real defect in that file, and three of them were in code the
// PREVIOUS round had just added: a crop computed before the legend reflow, an ink union that counted
// nodes which paint nothing, a clip intersection that nulled every zero-height gridline, and one that
// excluded overflow the script then un-hid by switching the frame's own clip off. Every one is a
// geometry or ordering bug that a reader can talk themselves into and a run cannot show them — the
// frame still looks right, because the failure is in the box rather than in the pixels.
//
// So this harness fakes the `figma` global, builds a frame per scenario, injects a test CONFIG into
// the committed file verbatim, runs it, and asserts on the resulting node tree.
//
//     node .claude/skills/create-figma-chart/scripts/test_restyle_static_import.js
//
// It is a MOCK: it validates control flow and arithmetic against the Plugin API's documented shapes,
// never Figma's actual behaviour. Keep it honest —
//   - `absoluteBoundingBox` is a GETTER that walks the parent chain, because the script reads absolute
//     boxes and writes relative `x`/`y`, and a mock that stores one number cannot catch a crop that
//     moves the ink;
//   - `absoluteBoundingBox` EXCLUDES strokes, so an open path is legitimately `w x 0` — the shape that
//     broke the clip walk;
//   - a leaf carries no `children` key at all, since the script tests `"children" in n`.

const fs = require("fs");
const path = require("path");

const SRC = fs.readFileSync(path.join(__dirname, "restyle_static_import.js"), "utf8");
const MIXED = Symbol("figma.mixed");

let AUTO = 0;
let failures = 0;
let checks = 0;

function detach(child) {
  const p = child.parent;
  if (p && p.children) p.children.splice(p.children.indexOf(child), 1);
  child.parent = null;
}

function node(props) {
  const n = Object.assign({ type: "FRAME", name: "", x: 0, y: 0, width: 0, height: 0, visible: true }, props);
  if (!n.id) n.id = `auto:${++AUTO}`;
  if (n.children) for (const c of n.children) c.parent = n;
  Object.defineProperty(n, "absoluteBoundingBox", {
    get() {
      let x = 0, y = 0;
      for (let a = this; a; a = a.parent) { x += a.x || 0; y += a.y || 0; }
      return { x, y, width: this.width, height: this.height };
    },
  });
  n.findAll = function (fn) {
    const out = [];
    const rec = (m) => { for (const c of m.children || []) { if (!fn || fn(c)) out.push(c); rec(c); } };
    rec(this);
    return out;
  };
  n.appendChild = function (child) { detach(child); child.parent = this; (this.children || (this.children = [])).push(child); };
  n.insertChild = function (i, child) { detach(child); child.parent = this; (this.children || (this.children = [])).splice(i, 0, child); };
  n.remove = function () { detach(this); this.removed = true; };
  n.rescale = function (s) {
    // Figma scales the node's own size and everything under it, keeping its own x/y.
    const self = this;
    const rec = (m) => {
      m.width *= s; m.height *= s;
      if (m !== self) { m.x *= s; m.y *= s; }
      if (typeof m.fontSize === "number") m.fontSize *= s;
      for (const c of m.children || []) rec(c);
    };
    rec(this);
  };
  n.resizeWithoutConstraints = function (w, h) { this.width = w; this.height = h; };
  n.setFillStyleIdAsync = async function (id) { this.fillStyleId = id; };
  return n;
}

const solid = (visible) => [{ type: "SOLID", visible: visible !== false, color: { r: 1, g: 1, b: 1 } }];

// A painted leaf — a filled vector with real area.
const leaf = (name, x, y, w, h, extra) =>
  node(Object.assign({ type: "VECTOR", name, x, y, width: w, height: h, fills: solid(), strokes: [] }, extra || {}));

// A stroked open path. `absoluteBoundingBox` excludes strokes, so a horizontal rule is `w x 0`.
const rule = (name, x, y, w, h) =>
  node({ type: "VECTOR", name, x, y, width: w, height: h, fills: [], strokes: solid(), strokeWeight: 1 });

const textNode = (name, chars, x, y, w, h, extra) =>
  node(Object.assign({
    type: "TEXT", name, characters: chars, fontSize: 12, x, y, width: w, height: h,
    fills: solid(), strokes: [],
    getStyledTextSegments: () => [{ fontName: { family: "Arial", style: "Regular" }, start: 0, end: chars.length }],
    setRangeFontName: function () {},
  }, extra || {}));

const CONFIG_BASE = {
  labelTintFactor: 0.4,
  textStyles: { dark: "dark-key", body: "body-key" },
  bodyTextParent: /__(label)$/,
  darkTextParent: /__(dark)$/,
  backgroundPatchParent: /^(figure|axes)_\d+$/,
  backgroundPatch: /^patch_\d+$/,
  slots: ["title", "subtitle", "note", "data-source", "tagline", "license"],
  families: [],
};

async function run(scenario) {
  const { page, cover } = scenario;
  const figma = {
    mixed: MIXED,
    root: { children: [page, cover].filter(Boolean) },
    currentPage: page,
    setCurrentPageAsync: async function (p) { figma.currentPage = p; },
    loadFontAsync: async function () {},
    importStyleByKeyAsync: async function (key) { return { id: "style:" + key }; },
    getNodeByIdAsync: async function (id) {
      const hunt = (m) => {
        if (m.id === id) return m;
        for (const c of m.children || []) { const hit = hunt(c); if (hit) return hit; }
        return null;
      };
      for (const p of figma.root.children) { const hit = hunt(p); if (hit) return hit; }
      return null;
    },
    createText: function () {
      // The reflow probe measures "nn" against "n n" to derive one space's width.
      const probe = node({ type: "TEXT", name: "probe", width: 0, height: 0 });
      Object.defineProperty(probe, "characters", {
        get: function () { return this._chars || ""; },
        set: function (v) { this._chars = v; this.width = v.length * 5; },
      });
      probe.textAutoResize = "WIDTH_AND_HEIGHT";
      return probe;
    },
  };
  const config = Object.assign({}, CONFIG_BASE, scenario.config, { pageId: page.id });
  const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m, "const CONFIG = __CONFIG__;");
  const fn = new Function("figma", "__CONFIG__", "return (async () => { " + body + " })();");
  return fn(figma, config);
}

function check(label, actual, expected) {
  checks++;
  const ok = typeof expected === "function" ? expected(actual) : JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) {
    failures++;
    const want = typeof expected !== "function" ? ", want " + JSON.stringify(expected) : "";
    console.log("  FAIL " + label + "\n       got " + JSON.stringify(actual) + want);
  } else {
    console.log("  ok   " + label);
  }
}

const near = (want, eps) => (got) => Math.abs(got - want) <= (eps === undefined ? 0.001 : eps);

// A page holding one template-shaped frame: a header auto-layout defining the content column (16..834),
// a footer, and optionally an existing chart to be replaced.
function scene(opts) {
  const inkNodes = opts.inkNodes;
  const canvasWidth = opts.canvasWidth === undefined ? 850 : opts.canvasWidth;
  const header = node({
    id: "header", name: "header", type: "FRAME", x: 16, y: 16, width: 818, height: 54,
    layoutMode: "VERTICAL", fills: [], children: [textNode("title", "Title", 0, 0, 400, 30)],
  });
  const footer = node({
    id: "footer", name: "footer", type: "FRAME", x: 16, y: 1000, width: 818, height: 60,
    layoutMode: "VERTICAL", fills: [], children: [textNode("note", "Note:", 0, 0, 400, 20)],
  });
  const frameChildren = [header, footer];
  if (opts.oldChart) {
    frameChildren.unshift(node({ id: "old", name: "chart", type: "FRAME", x: 0, y: 0, width: 850, height: 1095,
                                 fills: [], children: [leaf("stale", 0, 0, 10, 10)] }));
  }
  const frame = node({
    id: "frame", name: "how-do-people-spend-their-time", type: "FRAME", x: 1000, y: 0,
    width: 850, height: 1095, fills: solid(), fillStyleId: "cream-style", clipsContent: true,
    children: frameChildren,
  });
  const styled = node(Object.assign({
    id: "styled", name: "svg-import", type: "FRAME", x: 0, y: 0, width: canvasWidth, height: 1095,
    fills: [{ type: "SOLID", visible: false, color: { r: 1, g: 1, b: 1 } }], clipsContent: false,
    children: inkNodes,
  }, opts.styledExtra || {}));
  const reference = opts.referenceToo
    ? node({ id: "reference", name: "svg-import-2", type: "FRAME", x: 0, y: 0, width: canvasWidth, height: 1095,
             fills: [{ type: "SOLID", visible: false, color: { r: 1, g: 1, b: 1 } }], clipsContent: false,
             children: [leaf("ref-ink", 20, 90, 700, 800)] })
    : null;
  const cover = node({ id: "cover", name: "Cover", type: "PAGE", children: [styled, reference].filter(Boolean) });
  const page = node({ id: "page", name: "20260817 chart page", type: "PAGE", children: [frame] });
  return {
    page: page, cover: cover, frame: frame, styled: styled, reference: reference,
    config: { jobs: [{ frameId: "frame", styled: "styled", reference: reference ? "reference" : null,
                       canvasWidth: canvasWidth, frameWidth: 850, referenceGap: 80, reflowLegend: false }] },
  };
}

const box = (n) => ({ l: +n.x.toFixed(2), t: +n.y.toFixed(2), w: +n.width.toFixed(2), h: +n.height.toFixed(2) });

(async function () {
  console.log("\nbackground patches: unpainted stripped, stroke-only kept");
  {
    const figurePatch = node({ name: "patch_1", type: "GROUP", x: 0, y: 0, width: 850, height: 1095,
                               children: [leaf("bg", 0, 0, 850, 1095, { fills: [] })] });
    const spine = node({ name: "patch_1", type: "GROUP", x: 16, y: 88, width: 818, height: 900,
                         children: [rule("spine", 0, 0, 818, 0)] });
    const axes = node({ name: "axes_1", type: "FRAME", x: 0, y: 0, width: 850, height: 1095, fills: [],
                        children: [spine, leaf("bar", 16, 200, 300, 20)] });
    const figure = node({ name: "figure_1", type: "FRAME", x: 0, y: 0, width: 850, height: 1095, fills: [],
                          children: [figurePatch, axes] });
    const s = scene({ inkNodes: [figure] });
    const out = await run(s);
    check("figure patch stripped", out.report[0].strippedPatches, ["figure_1/patch_1 (unpainted — removed for its bbox)"]);
    check("axes spine survives", spine.removed !== true, true);
    check("crop ignores the stripped 850-wide patch", box(s.styled).w, near(818));
  }

  console.log("\ncrop: the frame becomes the ink, and the ink does not move");
  {
    const bar = leaf("bar", 40, 100, 400, 30);
    const s = scene({ inkNodes: [bar] });
    const before = bar.absoluteBoundingBox.x;
    await run(s);
    check("frame box is the ink box", box(s.styled), { l: 40, t: 100, w: 400, h: 30 });
    check("ink stayed put in absolute terms", bar.absoluteBoundingBox.x - before, 1000);
    check("chart sits at the BOTTOM of the z-order", s.page.children[0].children[0].name, "chart");
  }

  console.log("\ncrop counts only what renders");
  {
    const hidden = leaf("hidden", 0, 0, 850, 1095, { visible: false });
    const dimmed = node({ name: "dimmed", type: "GROUP", x: 0, y: 0, width: 850, height: 1095, opacity: 0, fills: [],
                          children: [leaf("ghost", 0, 0, 850, 1095)] });
    const clipArtifact = leaf("clip-path", 0, 0, 850, 1095, { vectorPaths: [{ windingRule: "NONE", data: "" }] });
    const strokedNone = node({ type: "VECTOR", name: "open-gridline", x: 500, y: 300, width: 100, height: 0,
                               fills: solid(), strokes: solid(), strokeWeight: 1,
                               vectorPaths: [{ windingRule: "NONE", data: "" }] });
    const bar = leaf("bar", 40, 100, 400, 30);
    const s = scene({ inkNodes: [hidden, dimmed, clipArtifact, strokedNone, bar] });
    await run(s);
    check("hidden, zero-opacity and clip-path artifacts excluded", box(s.styled).l, 40);
    check("a stroked zero-winding path still counts", box(s.styled).w, near(560));
    // 100..300: the bar's top to the zero-height rule. Nulling the rule would leave 30.
    check("... and its zero HEIGHT does not null it", box(s.styled).h, near(200));
  }

  console.log("\nclips: an inner clip trims, the frame's own clip does not");
  {
    const axesClip = node({ name: "axes_1", type: "FRAME", x: 16, y: 88, width: 818, height: 800, fills: [],
                            clipsContent: true, children: [leaf("overflowing-line", 0, 0, 2000, 20)] });
    const s = scene({ inkNodes: [axesClip] });
    await run(s);
    check("overflow past an inner clip does not widen the crop", box(s.styled).w, near(818));

    // A gridline INSIDE the clip: zero-height by construction, since absoluteBoundingBox excludes
    // strokes. Intersecting it with its clip must not null it — that is the whole F4 defect, and it
    // only bites a degenerate box that HAS a clipping ancestor.
    const clippedRule = node({ name: "axes_2", type: "FRAME", x: 16, y: 88, width: 818, height: 800, fills: [],
                               clipsContent: true, children: [rule("gridline", 0, 700, 818, 0)] });
    const s3 = scene({ inkNodes: [leaf("bar", 40, 100, 400, 30), clippedRule] });
    await run(s3);
    check("a zero-height gridline inside a clip survives the intersection", box(s3.styled).h, near(688));

    const overflow = leaf("past-the-canvas", 800, 100, 200, 30);
    const s2 = scene({ inkNodes: [leaf("bar", 40, 100, 400, 30), overflow], styledExtra: { clipsContent: true } });
    await run(s2);
    check("overflow past the FRAME's own clip is kept", box(s2.styled).w, near(960));
  }

  console.log("\nsnap to the content column");
  {
    const s = scene({ inkNodes: [leaf("bar", 15.92, 100, 817.96, 30)] });
    await run(s);
    check("a side within a pixel snaps exactly", [box(s.styled).l, +(s.styled.x + s.styled.width).toFixed(2)], [16, 834]);

    const s2 = scene({ inkNodes: [leaf("bar", 12, 100, 400, 30)] });
    await run(s2);
    check("a side 4px out does NOT snap", box(s2.styled).l, 12);
  }

  console.log("\nreflow runs BEFORE the crop");
  {
    const legend = node({ name: "header__leisure", type: "GROUP", x: 0, y: 40, width: 400, height: 14, fills: [],
                          children: [textNode("run-a", "Sports", 0, 0, 60, 14), textNode("run-b", "Events", 300, 0, 60, 14)] });
    const s = scene({ inkNodes: [leaf("bar", 40, 100, 400, 30), legend] });
    s.config.jobs[0].reflowLegend = true;
    const out = await run(s);
    check("runs were re-flowed", out.report[0].reflowed > 0, true);
    check("the crop reflects the POST-reflow ink", s.styled.x + s.styled.width < 440.001, true);
  }

  console.log("\nthe parked reference copy is left alone");
  {
    const s = scene({ inkNodes: [leaf("bar", 40, 100, 400, 30)], referenceToo: true, canvasWidth: 816 });
    await run(s);
    const r = s.reference;
    check("rescaled to the frame width", +r.width.toFixed(2), 850);
    check("kept at full canvas height, not cropped", r.height, near(1095 * 850 / 816, 0.01));
    check("its fill is still switched off", r.fills[0].visible, false);
    check("parked to the LEFT of the frame", r.x, near(70, 0.001));
    check("renamed for what it is", /— original SVG \(unstyled\)$/.test(r.name), true);
  }

  console.log("\nan existing chart is replaced, and the landing page is reported");
  {
    const s = scene({ inkNodes: [leaf("bar", 40, 100, 400, 30)], oldChart: true });
    const out = await run(s);
    check("the old chart is gone", s.page.children[0].children.filter((c) => c.name === "chart").length, 1);
    check("the landing page is named for the sweep", out.landingPages, ["cover Cover"]);
    check("the frame's own fill style is copied onto the chart", s.page.children[0].children[0].fillStyleId, "cream-style");
  }

  console.log("\n" + (checks - failures) + "/" + checks + " checks passed");
  process.exit(failures ? 1 : 0);
})();
