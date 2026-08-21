// Stubbed-figma harness for measure_fit.js — the only way to test that script off-canvas.
//
// measure_fit.js executes ONLY inside Figma (pasted as a `use_figma` call), so every review of it
// is reading-only and every edit is otherwise blind. This harness fakes the `figma` global — a
// synthetic template clone + chart group, with `getNodeByIdAsync` / `setCurrentPageAsync` /
// `findAll` stubbed — injects a test CONFIG into the committed file verbatim, and asserts on the
// returned JSON. Nineteen checks over seven scenarios, covering the guards eight Codex review
// rounds put into the script: the measured-inset nextPass, the outside-clone throw, the
// WIDTH_AND_HEIGHT reflow block, the unusable-inset fallback, the house-3/4 stroke verdict, the
// unmatched-hideIds report, and the no-declared guidance note. Its predecessors reproduced four of
// those findings as failures before their fixes and passed after.
//
// Run after ANY edit to measure_fit.js:
//     node .claude/skills/create-figma-chart/scripts/test_measure_fit.js
//
// What it cannot do: it is a MOCK. It validates control flow and arithmetic against the Plugin
// API's documented shapes, never Figma's actual behavior — insets, reflow, and rendering are still
// only testable in a real run. Keep the mock's node shapes honest (every node has an `id`; leaves
// have NO `children` key) — both were harness bugs once, and both masked real assertions.
const fs = require("fs");
const path = require("path");

const SRC = fs.readFileSync(path.join(__dirname, "measure_fit.js"), "utf8");

let AUTO_ID = 0;
function mkNode(props) {
  const n = Object.assign(
    { children: undefined, visible: true, x: 0, y: 0, width: 0, height: 0, type: "FRAME", name: "" },
    props,
  );
  if (!n.id) n.id = `auto:${++AUTO_ID}`; // every real Figma node has an id
  if (n.children) for (const c of n.children) c.parent = n;
  if (n.children === undefined) delete n.children; // real Figma leaves have no `children` key at all
  if (!("absoluteBoundingBox" in n)) {
    n.absoluteBoundingBox = { x: n.x, y: n.y, width: n.width, height: n.height };
  }
  n.findAll = function (fn) {
    const out = [];
    const rec = (m) => {
      for (const c of m.children || []) {
        if (!fn || fn(c)) out.push(c);
        rec(c);
      }
    };
    rec(this);
    return out;
  };
  return n;
}

// A template clone: header (auto-layout, AUTO, two HEIGHT texts), footer, logo.
function mkFrame(opts = {}) {
  const title = mkNode({
    type: "TEXT",
    name: "title",
    x: 16,
    y: 24,
    width: 508,
    height: 56,
    fontSize: 25,
    lineHeight: { unit: "PIXELS", value: 28 },
    textAutoResize: opts.titleAutoResize || "HEIGHT",
    layoutSizingVertical: "HUG",
    layoutGrow: 0,
  });
  const subtitle = mkNode({
    type: "TEXT",
    name: "subtitle",
    x: 16,
    y: 88,
    width: 508,
    height: 36,
    fontSize: 15,
    lineHeight: { unit: "PIXELS", value: 18 },
    textAutoResize: "HEIGHT",
    layoutSizingVertical: "HUG",
    layoutGrow: 0,
  });
  const header = mkNode({
    name: "header",
    layoutMode: "VERTICAL",
    primaryAxisSizingMode: "AUTO",
    itemSpacing: 8,
    x: 16,
    y: 24,
    width: 508,
    height: 104,
    children: [title, subtitle],
  });
  const footer = mkNode({
    name: "footer",
    layoutMode: "HORIZONTAL",
    x: 16,
    y: 537,
    width: 508,
    height: 40,
    children: [mkNode({ type: "TEXT", name: "source", x: 16, y: 537, width: 300, height: 16, fontSize: 11 })],
  });
  const logo = mkNode({ name: "logo", x: 468, y: 550, width: 56, height: 20 });
  const frame = mkNode({
    id: "F:1",
    name: "clone — IG square",
    x: 0,
    y: 0,
    width: 540,
    height: 600,
    children: [header, footer, logo],
  });
  return { frame, header, footer };
}

// A chart group: plot rect + connectors + datapoints__X + two label texts. Natural (probe) size.
function mkGroup(id, { withFurniture = true, scale = 1 } = {}) {
  const kids = [
    mkNode({ type: "RECTANGLE", name: "plot", x: 700, y: 100, width: 726.92 * scale, height: 615.96 * scale }),
    mkNode({ type: "TEXT", name: "label__Chile", x: 1300, y: 200, width: 60, height: 18, fontSize: 22.5 }),
    mkNode({ type: "TEXT", name: "value__0.78", x: 1300, y: 230, width: 40, height: 16, fontSize: 22.5 }),
    mkNode({
      type: "LINE",
      name: "line__chile",
      x: 710,
      y: 120,
      width: 700 * scale,
      height: 0.001,
      strokeWeight: 1.32,
    }),
  ];
  if (withFurniture) {
    kids.push(mkNode({ name: "connectors", x: 690, y: 90, width: 20, height: 640, children: [] }));
    kids.push(
      mkNode({
        name: "datapoints__Chile",
        x: 705,
        y: 105,
        width: 750,
        height: 620,
        children: [mkNode({ type: "ELLIPSE", name: "dp", x: 1450, y: 700, width: 8, height: 8 })],
      }),
    );
  }
  return mkNode({ id, name: "chart", type: "GROUP", x: 690, y: 90, width: 770, height: 640, children: kids });
}

async function run(config, { frame, extraNodes = [] }) {
  const page = mkNode({ id: "P:1", type: "PAGE", name: "page", children: [frame, ...extraNodes] });
  const byId = {};
  const index = (n) => {
    if (n.id) byId[n.id] = n;
    for (const c of n.children || []) index(c);
  };
  index(page);
  const figma = {
    currentPage: page,
    getNodeByIdAsync: async (id) => byId[id] || null,
    setCurrentPageAsync: async () => {},
  };
  const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m, "const CONFIG = __CONFIG__;");
  const fn = new Function("figma", "__CONFIG__", `return (async () => { ${body} })();`);
  return fn(figma, config);
}

const results = [];
const check = (name, cond, detail) => {
  results.push({ name, ok: !!cond, detail: cond ? "" : detail });
};

(async () => {
  // Case 1: happy path — furniture excluded by NAME, declared given → inset + exact-pass nextPass.
  {
    const { frame } = mkFrame();
    const group = mkGroup("G:1");
    frame.children.push(group);
    group.parent = frame;
    const out = await run(
      {
        frameId: "F:1",
        groupId: "G:1",
        hideNames: [/^connectors$/, /^datapoints__/],
        hideIds: [],
        declared: [791, 645],
        imFontSize: 30,
        targetGap: 14,
        targetLabel: 13.5,
        slug: "liberal-democracy",
        params: "country=~CHL",
        originalGroupId: null,
      },
      { frame },
    );
    check("1 band from header/footer", out.band && Math.abs(out.band.top - 128) < 0.01, JSON.stringify(out.band));
    check("1 hideNames excluded", out.group.excluded.byName.length === 2, JSON.stringify(out.group.excluded));
    check(
      "1 measured ink is the plot, not the furniture",
      Math.abs(out.group.measured.w - 726.92) < 0.01 && out.group.measured.w < 770,
      JSON.stringify(out.group.measured),
    );
    check("1 inset computed per axis", out.group.inset && out.group.inset.x > 0 && out.group.inset.y > 0, JSON.stringify(out.group.inset));
    check(
      "1 nextPass is the measured-inset pass 2",
      /--declared 791x645 --ink [\d.]+x[\d.]+ --im-font-size 30/.test(out.group.nextPass) &&
        out.group.nextPass.includes("--slug liberal-democracy") &&
        out.group.nextPass.includes("--params 'country=~CHL'") &&
        out.group.nextPass.startsWith(".venv/bin/python .claude/skills"),
      out.group.nextPass,
    );
    check("1 no reflection anywhere", !/content-aspect/.test(JSON.stringify(out)), "");
    check(
      "1 fitScale is height-first",
      Math.abs(out.group.fitScaleToBandH - (out.band.height - 28) / out.group.measured.h) < 1e-4,
      `${out.group.fitScaleToBandH}`,
    );
    check(
      "1 afterScale uses height-first factor",
      Math.abs(out.group.fontSizes[0].afterScale - 22.5 * out.group.fitScaleToBandH) < 0.01,
      JSON.stringify(out.group.fontSizes),
    );
  }

  // Case 2: group in a SIBLING frame → throw (round 7b guard).
  {
    const { frame } = mkFrame();
    const stray = mkGroup("G:2");
    let threw = null;
    try {
      await run(
        { frameId: "F:1", groupId: "G:2", hideNames: [], hideIds: [], declared: null, imFontSize: null },
        { frame, extraNodes: [stray] },
      );
    } catch (e) {
      threw = e.message;
    }
    check("2 outside-clone group throws", threw && threw.includes("is not inside frameId"), String(threw));
  }

  // Case 3: WIDTH_AND_HEIGHT title → reflows false with a named reason (round 4 guard).
  {
    const { frame } = mkFrame({ titleAutoResize: "WIDTH_AND_HEIGHT" });
    const out = await run(
      { frameId: "F:1", groupId: null, hideNames: [], hideIds: [], declared: null, imFontSize: null },
      { frame },
    );
    check("3 WIDTH_AND_HEIGHT blocks reflow", out.headerSizing.reflows === false, JSON.stringify(out.headerSizing.children[0]));
    check("3 note fires", out.notes.some((n) => n.includes("DOES NOT REFLOW")), JSON.stringify(out.notes));
  }

  // Case 4: rescaled group + declared → inset unusable, nextPass falls back to a probe solve.
  {
    const { frame } = mkFrame();
    const group = mkGroup("G:4", { scale: 0.66 });
    frame.children.push(group);
    group.parent = frame;
    const out = await run(
      {
        frameId: "F:1",
        groupId: "G:4",
        hideNames: [/^connectors$/, /^datapoints__/],
        hideIds: [],
        declared: [791, 645],
        imFontSize: 30,
        targetLabel: 15,
        slug: "x",
      },
      { frame },
    );
    check("4 rescaled inset unusable", out.group.inset && out.group.inset.unusable, JSON.stringify(out.group.inset));
    check(
      "4 fallback nextPass is a probe with target-label",
      out.group.nextPass.includes("--target-label 15") && !out.group.nextPass.includes("--declared"),
      out.group.nextPass,
    );
    check("4 nextPassNote explains", /unusable/.test(out.group.nextPassNote || ""), out.group.nextPassNote);
  }

  // Case 5: stroke comparison against a reference import (lessons-side feature preserved).
  {
    const { frame } = mkFrame();
    const group = mkGroup("G:5");
    frame.children.push(group);
    group.parent = frame;
    const orig = mkGroup("G:orig");
    orig.children.find((c) => c.name === "line__chile").strokeWeight = 4;
    const out = await run(
      {
        frameId: "F:1",
        groupId: "G:5",
        hideNames: [/^connectors$/, /^datapoints__/],
        hideIds: [],
        declared: null,
        imFontSize: null,
        originalGroupId: "G:orig",
      },
      { frame, extraNodes: [orig] },
    );
    check(
      "5 stroke mismatch prescribes the HOUSE target, not the reference",
      out.group.strokes && out.group.strokes.verdict.includes("OFF THE HOUSE 3/4") && out.group.strokes.verdict.includes("1.32 -> 3") && !out.group.strokes.verdict.includes("-> 4."),
      JSON.stringify(out.group.strokes),
    );
    check("5 stroke verdict in notes", out.notes.some((n) => n.includes("OFF THE HOUSE")), "");
  }

  // Case 6: bad hideIds reported as unmatched (round 2 guard preserved).
  {
    const { frame } = mkFrame();
    const group = mkGroup("G:6");
    frame.children.push(group);
    group.parent = frame;
    const out = await run(
      {
        frameId: "F:1",
        groupId: "G:6",
        hideNames: [/^connectors$/, /^datapoints__/],
        hideIds: ["NOPE:1"],
        declared: null,
        imFontSize: null,
      },
      { frame },
    );
    check("6 unmatched hideIds surfaced", out.group.excluded.unmatched.length === 1, JSON.stringify(out.group.excluded));
    check("6 note names the id", out.notes.some((n) => n.includes("NOPE:1")), JSON.stringify(out.notes));
  }

  // Case 7: declared unset → probe fallback + guidance note (merged behavior).
  {
    const { frame } = mkFrame();
    const group = mkGroup("G:7");
    frame.children.push(group);
    group.parent = frame;
    const out = await run(
      {
        frameId: "F:1",
        groupId: "G:7",
        hideNames: [/^connectors$/, /^datapoints__/],
        hideIds: [],
        declared: null,
        imFontSize: null,
      },
      { frame },
    );
    check(
      "7 no-declared fallback probe + note",
      out.group.nextPass.includes("--target-label") && /CONFIG\.declared/.test(out.group.nextPassNote),
      `${out.group.nextPass} | ${out.group.nextPassNote}`,
    );
  }

  // Case 8: a ZERO-WIDTH stroked line is ink. Grapher draws a single-entity stacked discrete bar's
  // vertical zero line at 1.54x the bar height, so an ink walk that drops zero-area nodes measures
  // only the bars and the fit leaves the rest of the stroke hanging off the artboard. The line here
  // is 200px taller than every other leaf, so if it were dropped the measured height would be 640.
  {
    const { frame } = mkFrame();
    const group = mkGroup("G:8");
    group.children.push(
      mkNode({ type: "VECTOR", name: "vertical-zero-line", x: 760, y: 60, width: 0, height: 840,
               strokeWeight: 0.5, strokes: [{ type: "SOLID", visible: true }] }),
    );
    // and a zero-area node with NO stroke stays excluded — it paints nothing
    group.children.push(
      mkNode({ type: "VECTOR", name: "phantom", x: 100, y: 60, width: 0, height: 2000, strokes: [] }),
    );
    frame.children.push(group);
    group.parent = frame;
    const out = await run(
      { frameId: "F:1", groupId: "G:8", hideNames: [/^connectors$/, /^datapoints__/], hideIds: [],
        declared: null, imFontSize: null, originalGroupId: null },
      { frame },
    );
    const h = out.group.measured.h;
    check("8 zero-width stroked line counts as ink", Math.abs(h - 840) < 0.5, `measured h=${h}, expected 840`);
    const l = out.group.measured.x0 !== undefined ? out.group.measured.x0 : null;
    check("8 zero-area node with no stroke stays excluded", h < 2000 && (l === null || l > 100),
          `measured=${JSON.stringify(out.group.measured)}`);
  }

  const bad = results.filter((x) => !x.ok);
  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  " + x.detail}`);
  console.log(bad.length ? `\n${bad.length} FAILURES` : "\nALL PASS");
  process.exit(bad.length ? 1 : 0);
})();
