// Stubbed-figma harness for diff_against_template.js — the same job test_verify_page.js does for
// verify_page.js, and for the same reason.
//
// diff_against_template.js executes ONLY inside Figma (pasted as a `use_figma` call), so every review
// of it is reading-only, and its whole purpose is to report drift that a screenshot survives. A script
// like that fails in one specific direction: it returns "matches the template" because it never looked.
// Three defects of exactly that shape were found by writing this file — a header that lost its subtitle
// reported as matching, four fingerprinted footer properties never compared, and a TypeError that
// killed the whole diff when one row changed node type.
//
// Run after ANY edit to diff_against_template.js:
//     node .claude/skills/create-figma-chart/scripts/test_diff_against_template.js
//
// It is a MOCK: it validates control flow against the Plugin API's documented shapes, never Figma's
// actual behaviour. Keep the mock honest — a TEXT row needs `getStyledTextSegments`, and the auto-layout
// bands need the same properties the real fingerprint reads.

const fs = require("fs");
const path = require("path");

const SRC = fs.readFileSync(path.join(__dirname, "diff_against_template.js"), "utf8");

let AUTO = 0;
function node(props) {
  const n = Object.assign({ visible: true, x: 0, y: 0, width: 0, height: 0, type: "FRAME", name: "" }, props);
  if (!n.id) n.id = `auto:${++AUTO}`;
  if (n.children) for (const c of n.children) c.parent = n;
  return n;
}
const solid = (hex) => [{ type: "SOLID", visible: true, color: {
  r: parseInt(hex.slice(1, 3), 16) / 255, g: parseInt(hex.slice(3, 5), 16) / 255, b: parseInt(hex.slice(5, 7), 16) / 255 } }];

// A TEXT row. Segments are given as [value, start, end] because that is the shape the real API returns
// and the offsets are load-bearing: `getStyledTextSegments` merges adjacent equal values, so a fully
// bound row with a bold prefix is ONE style run over 0-40 and TWO font runs (Bold 0-12, Regular 12-40).
// Only the character ranges say whether an unbound style run sits over bolded text.
//
// `textStyleId` is `figma.mixed` — a SYMBOL — on any node with a range override, which is why the
// node-level value can never be the thing that is compared.
const SEG = { fontPlain: [["Regular", 0, 40]] };
const txt = (name, o) => {
  const opt = o || {};
  const styles = opt.styleSegs || [[opt.styleId === undefined ? "S:abc" : opt.styleId, 0, 40]];
  const fonts = opt.fontSegs || SEG.fontPlain;
  return node({
    type: "TEXT", name, fontSize: opt.size || 16, fills: solid(opt.fill || "#000000"),
    textStyleId: styles.length > 1 ? Symbol("figma.mixed") : styles[0][0],
    textAutoResize: opt.autoResize || "HEIGHT",
    layoutSizingHorizontal: opt.lsH || "FILL", layoutSizingVertical: opt.lsV || "HUG",
    width: opt.w || 508, height: 19,
    getStyledTextSegments: (fields) =>
      (fields.indexOf("fontName") !== -1
        // A font segment is [style, start, end, family?]; the family defaults to the templates' Lato, so
        // a case can retype one row in another family without touching every other fixture.
        ? fonts.map(([v, start, end, family]) => ({ fontName: { family: family || "Lato", style: v }, start, end }))
        : styles.map(([v, start, end]) => ({ textStyleId: v, start, end }))),
  });
};
// The template's source row: bold "Data source:" prefix, the WHOLE row bound to one style. One style
// run, two font runs — the state TEXTS.md says is reachable in the UI only.
const BOLD_PREFIX = { fontSegs: [["Bold", 0, 12], ["Regular", 12, 40]] };
const tplSource = () => txt("source", Object.assign({ size: 13 }, BOLD_PREFIX));

// A template or a clone: header band, footer band, logo sibling. Matches the structural resolution the
// script uses — the logo is a SIBLING, and header/footer are the topmost and bottommost auto-layouts.
function buildFrame(o) {
  const opt = o || {};
  const footer = node({
    name: "footer", layoutMode: opt.footerMode || "VERTICAL",
    primaryAxisSizingMode: opt.footerSizing || "AUTO", itemSpacing: opt.footerSpacing === undefined ? 4 : opt.footerSpacing,
    primaryAxisAlignItems: opt.footerAlign || "MIN", counterAxisAlignItems: opt.footerCounterAlign || "MIN",
    constraints: { vertical: opt.footerConstraint || "MIN" },
    x: 16, y: opt.footerY === undefined ? 488 : opt.footerY, width: 508, height: 36,
    children: opt.footerRows || [txt("source", { size: 13 })],
  });
  return node({
    id: opt.id, name: opt.name || "frame", width: 540, height: 540, fills: solid(opt.fill || "#ffffff"),
    children: [
      node({ name: "header", layoutMode: "VERTICAL", primaryAxisSizingMode: opt.headerSizing || "AUTO",
             itemSpacing: opt.headerSpacing === undefined ? 6 : opt.headerSpacing,
             primaryAxisAlignItems: "MIN", counterAxisAlignItems: "MIN",
             x: 16, y: 16, width: 508, height: 92,
             children: opt.headerRows || [txt("title", { size: 25 }), txt("subtitle", { size: 16 })] }),
      footer,
      node({ name: "logo", x: 460, y: 16, width: 64, height: 35 }),
    ],
  });
}

async function run(tpl, clone, expected, opts) {
  const o = opts || {};
  tpl.id = "T:1";
  clone.id = "C:1";
  const tplPage = node({ id: "P:tpl", type: "PAGE", name: "Templates", children: [tpl] });
  const clonePage = o.samePage ? tplPage : node({ id: "P:clone", type: "PAGE", name: "20260821 Chart", children: [clone] });
  tpl.parent = tplPage;
  clone.parent = clonePage;
  if (o.samePage) tplPage.children.push(clone);
  const byId = { "T:1": tpl, "C:1": clone };
  let switches = 0;
  // `PageNode.loadAsync` loads a page's contents WITHOUT switching to it — the honest way to read the
  // template's page, and unmetered by the one-switch budget. It exists only under dynamic-page document
  // access, so `noLoadAsync` models the environment where it does not and the short-read gate is the
  // whole defence.
  const loaded = [];
  if (!o.noLoadAsync)
    for (const pg of [tplPage, clonePage]) pg.loadAsync = async () => { loaded.push(pg.name); };
  const figma = {
    currentPage: o.startOn === "clone" ? clonePage : tplPage,
    getNodeByIdAsync: async (id) => byId[id] || null,
    setCurrentPageAsync: async (p) => {
      switches++;
      if (switches > 1) throw new Error("Cannot switch pages more than once per call");
      figma.currentPage = p;
    },
  };
  const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m,
    `const CONFIG = { templateId: "T:1", frameIds: ["C:1"], expected: ${JSON.stringify(expected || [])} };`);
  const fn = new Function("figma", `return (async () => { ${body} })();`);
  const out = await fn(figma);
  return { out, switches, loaded };
}

const results = [];
const check = (name, cond, detail) => results.push({ name, ok: !!cond, detail: cond ? "" : String(detail).slice(0, 240) });
const drift = (res) => res.out.frames[0].drift;
const has = (res, re) => drift(res).some((d) => re.test(d));

(async () => {
  // 1 — an untouched clone is clean, and text CONTENT is excluded by design.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }));
    check("1 an untouched clone matches", res.out.frames[0].verdict === "matches the template", JSON.stringify(drift(res)));
    check("1 verdict says all frames match", /all 1 resolved frame\(s\) match/.test(res.out.verdict), res.out.verdict);
    check("1 exactly one page switch", res.switches === 1, `${res.switches} switches`);
  }

  // 2 — ONE page switch, and only one, and it does NOT matter which page the call starts on.
  //
  // This case used to assert the opposite: that the script refuses unless the template's page is already
  // current. That contract was unsatisfiable in practice — `figma.currentPage` resets to the file's FIRST
  // page at the start of every `use_figma` call (measured 2026-08-21: a call ending in
  // `setCurrentPageAsync(<working page>)` was followed by one reporting "Cover"), so "open that page and
  // re-run" is not something a session can arrange and the script could never run at all. It now reads
  // the template unswitched and gates on the read being COMPLETE instead, which is what the old guard was
  // really protecting against.
  {
    const off = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }), [], { startOn: "clone" });
    check("2 starting on the clone's page no longer refuses", off.out.frames[0].verdict === "matches the template", JSON.stringify(drift(off)));
    // At most one, ever. Starting on the clone's page costs ZERO — the script only switches if it has to.
    check("2 never more than one page switch", off.switches <= 1, `${off.switches} switches`);
    check("2 and none at all when already on the clone's page", off.switches === 0, `${off.switches} switches`);
    check("2 and the read declares the page was not current", off.out.templateFingerprintRead.pageWasCurrent === false,
          JSON.stringify(off.out.templateFingerprintRead));
    // clones on the template's own page need no switch at all
    const same = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }), [], { samePage: true });
    check("2 clones on the template's page cost zero switches", same.switches === 0, `${same.switches} switches`);
  }

  // 2b — the failure the old guard existed for, now asserted directly: a template whose subtree did not
  // load reads SHORT (no auto-layout children, so no header) and must STOP rather than diff against a
  // partial fingerprint. A check that cannot fail is worse than no check, so removing the page guard
  // obliges this one to exist.
  {
    let threw = null;
    try {
      await run(node({ name: "tpl-unloaded", width: 540, height: 540, fills: solid("#ffffff"), children: [] }),
                buildFrame({ name: "clone" }));
    } catch (e) { threw = e.message; }
    check("2b a short template read STOPS", threw && /read SHORT/.test(threw), threw);
    check("2b and names what was missing", threw && /header did not resolve/.test(threw), threw);
    check("2b and says it is a stop, not a diff", threw && /not a diff/.test(threw), threw);
  }

  // 2c — "the header resolved with rows in it" is an INFERENCE about completeness, not a proof: the
  // short list in GOTCHAS.md's first entry was NONEMPTY (a page read 4 children while current and 2
  // later), so a partial subtree can clear that gate and become the baseline. So the read is MADE
  // complete: `PageNode.loadAsync()` loads the template's page without switching to it, which does not
  // touch the one-switch budget the clones need.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }), [], { startOn: "clone" });
    check("2c the template's page is LOADED, not merely read", res.loaded.indexOf("Templates") !== -1, JSON.stringify(res.loaded));
    check("2c and reported as loaded", res.out.templateFingerprintRead.pageLoadedWithoutSwitch === true,
          JSON.stringify(res.out.templateFingerprintRead));
    check("2c and the note says the read is complete, not inferred", /not inferred complete/.test(res.out.templateFingerprintRead.note),
          res.out.templateFingerprintRead.note);
    check("2c loading costs no page switch", res.switches === 0, `${res.switches} switches`);
    check("2c and the diff is still clean", !drift(res).length, JSON.stringify(drift(res)));
  }

  // 2d — where loadAsync does not exist the script must still RUN (that is the whole point of dropping
  // the page guard), and must stop overclaiming: the gate is then an inference and the result says so.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }), [], { noLoadAsync: true });
    check("2d it still runs with no loadAsync", !drift(res).length, JSON.stringify(drift(res)));
    check("2d and does not claim the page was loaded", res.out.templateFingerprintRead.pageLoadedWithoutSwitch === false,
          JSON.stringify(res.out.templateFingerprintRead));
    check("2d and the note admits completeness is INFERRED", /INFERRED/.test(res.out.templateFingerprintRead.note),
          res.out.templateFingerprintRead.note);
    let threw = null;
    try {
      await run(node({ name: "tpl-unloaded", width: 540, height: 540, fills: solid("#ffffff"), children: [] }),
                buildFrame({ name: "clone" }), [], { noLoadAsync: true });
    } catch (e) { threw = e.message; }
    check("2d a short read still STOPS without loadAsync", threw && /read SHORT/.test(threw), threw);
    check("2d and says a lazy read is still possible", threw && /re-run once/.test(threw), threw);
  }

  // 2e — no gate can fully exclude a partial template read, and its signature is specific: EVERY clone
  // reports the SAME structural difference, because what is wrong is the baseline. Left unnamed, one bad
  // read reads as N frames each missing the same band, and a reader "fixes" correct clones.
  {
    const tpl = buildFrame({ name: "tpl" });
    const a = buildFrame({ name: "clone-a", headerRows: [txt("title", { size: 25 })] });
    const b = buildFrame({ name: "clone-b", headerRows: [txt("title", { size: 25 })] });
    const tplPage = node({ id: "P:tpl", type: "PAGE", name: "Templates", children: [tpl] });
    const clonePage = node({ id: "P:clone", type: "PAGE", name: "20260821 Chart", children: [a, b] });
    tpl.id = "T:1"; a.id = "C:1"; b.id = "C:2";
    tpl.parent = tplPage; a.parent = clonePage; b.parent = clonePage;
    for (const pg of [tplPage, clonePage]) pg.loadAsync = async () => {};
    const byId = { "T:1": tpl, "C:1": a, "C:2": b };
    const figma = { currentPage: tplPage, getNodeByIdAsync: async (id) => byId[id] || null,
                    setCurrentPageAsync: async (pg) => { figma.currentPage = pg; } };
    const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m,
      'const CONFIG = { templateId: "T:1", frameIds: ["C:1", "C:2"], expected: [] };');
    const out = await new Function("figma", `return (async () => { ${body} })();`)(figma);
    check("2e unanimous structural drift is named", /SUSPECT BASELINE/.test(out.verdict), out.verdict);
    check("2e and the shared difference is quoted", /rowCount 1 != 2/.test(out.verdict), out.verdict);
    check("2e and it is collected for the caller", out.unanimousStructuralDrift.length === 1,
          JSON.stringify(out.unanimousStructuralDrift));
    check("2e and it says to re-read the template first", /re-read the template/.test(out.verdict), out.verdict);
    // one frame cannot be unanimous with itself, and per-frame drift must not be misread as a bad baseline
    const solo = await run(buildFrame({ name: "tpl" }),
                           buildFrame({ name: "clone", headerRows: [txt("title", { size: 25 })] }));
    check("2e a single frame raises no baseline suspicion", !/SUSPECT BASELINE/.test(solo.out.verdict), solo.out.verdict);
    check("2e and its own drift is still reported", has(solo, /header rowCount 1 != 2/), JSON.stringify(drift(solo)));
  }

  // 3 — a header that LOST a row. The row loop only walks the overlap, so without a row-count compare
  // a clone shipped with no subtitle reported "matches the template".
  {
    const res = await run(buildFrame({ name: "tpl" }),
                          buildFrame({ name: "clone", headerRows: [txt("title", { size: 25 })] }));
    check("3 a header that lost a row is DRIFT", drift(res).length > 0, JSON.stringify(drift(res)));
    check("3 and it is named as a row count", has(res, /header rowCount 1 != 2/), JSON.stringify(drift(res)));
  }

  // 4 — every auto-layout property fingerprinted must be compared. Four of them were collected and
  // never diffed, which reads as coverage while passing everything.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone",
      footerSizing: "FIXED", footerSpacing: 99, footerAlign: "MAX", footerCounterAlign: "MAX", footerConstraint: "SCALE" }));
    check("4 footer sizing compared", has(res, /footer sizing FIXED != AUTO/), JSON.stringify(drift(res)));
    check("4 footer itemSpacing compared", has(res, /footer itemSpacing 99 != 4/), JSON.stringify(drift(res)));
    check("4 footer alignment compared", has(res, /primaryAxisAlignItems MAX != MIN/), JSON.stringify(drift(res)));
    check("4 footer counter-alignment compared", has(res, /counterAxisAlignItems MAX != MIN/), JSON.stringify(drift(res)));
    check("4 footer vertical constraint compared", has(res, /vertical constraint SCALE != MIN/), JSON.stringify(drift(res)));
    const hdr = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone", headerSizing: "FIXED", headerSpacing: 20 }));
    check("4 header sizing compared, with the reason", has(hdr, /header sizing FIXED != AUTO.*tracking the copy/), JSON.stringify(drift(hdr)));
    check("4 header itemSpacing compared", has(hdr, /header itemSpacing 20 != 6/), JSON.stringify(drift(hdr)));
  }

  // 5 — a row that changed NODE TYPE. Reading the text fields across that threw a TypeError on
  // `segFonts.join`, so one swapped row killed the entire diff instead of being reported.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone",
      headerRows: [txt("title", { size: 25 }), node({ type: "RECTANGLE", name: "blob", width: 508, height: 19 })] }));
    check("5 a swapped row type does not throw", Array.isArray(drift(res)), "the diff died");
    check("5 and is reported as a type change", has(res, /header\[1\] node type RECTANGLE != TEXT/), JSON.stringify(drift(res)));
  }

  // 6 — the drift that a run is MEANT to produce: sizes, fills, weights, sizing modes.
  {
    const res = await run(buildFrame({ name: "tpl" }),
                          buildFrame({ name: "clone", footerRows: [txt("source", { size: 11, lsV: "FIXED" })] }));
    check("6 a shrunken source line is DRIFT", has(res, /footer\[0\] size 11 != 13/), JSON.stringify(drift(res)));
    check("6 a FIXED row where the template HUGs is DRIFT", has(res, /layoutSizingVertical FIXED != HUG/), JSON.stringify(drift(res)));

    // The FAMILY is drift too, and it is the half that was never compared. Arial Regular and Lato
    // Regular have the same style runs, so a fingerprint holding only `style` reported a retyped row as
    // matching the template.
    const fam = await run(buildFrame({ name: "tpl" }),
                          buildFrame({ name: "clone", footerRows: [txt("source", { size: 13, fontSegs: [["Regular", 0, 40, "Arial"]] })] }));
    check("6 a row retyped in another FAMILY is DRIFT", has(fam, /footer\[0\] font family Arial != Lato/), JSON.stringify(drift(fam)));
    check("6 and the identical style runs are NOT reported as a weight change", !has(fam, /footer\[0\] weights/), JSON.stringify(drift(fam)));
    const sameFam = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }));
    check("6 while a clone in the template's own family reports no family drift", !has(sameFam, /font family/), JSON.stringify(drift(sameFam)));

    // A MOVED bold range leaves the value sequence identical — "Bold+Regular" either way — so only the
    // run boundaries can see it.
    const moved = await run(buildFrame({ name: "tpl", footerRows: [tplSource()] }),
                            buildFrame({ name: "clone", footerRows: [txt("source", { size: 13, fontSegs: [["Bold", 0, 14], ["Regular", 14, 40]] })] }));
    check("6 a bold range that MOVED is DRIFT", has(moved, /footer\[0\] font run boundaries 0,14 != 0,12/), JSON.stringify(drift(moved)));
    check("6 and the identical run values are not what caught it", !has(moved, /footer\[0\] weights/), JSON.stringify(drift(moved)));
    // ...but a longer source line is CONTENT, which is excluded by design: the last run's end is the
    // character count, so comparing it would report drift on every clone.
    const longer = await run(buildFrame({ name: "tpl", footerRows: [tplSource()] }),
                             buildFrame({ name: "clone", footerRows: [txt("source", { size: 13, fontSegs: [["Bold", 0, 12], ["Regular", 12, 96]] })] }));
    check("6 while a longer source line with the same boundaries is NOT drift",
          !has(longer, /font run boundaries/) && drift(longer).length === 0, JSON.stringify(drift(longer)));
  }

  // 7 — a DETACHED text style is the defect (assigning `characters` drops the binding); it must not be
  // waved through as the documented half-bound prefix.
  {
    const res = await run(buildFrame({ name: "tpl" }),
                          buildFrame({ name: "clone", footerRows: [txt("source", { size: 13, styleId: "" })] }));
    check("7 an unbound style is DRIFT", has(res, /footer\[0\] style \(unbound\)/), JSON.stringify(drift(res)));
    check("7 and is not filed as half-bound", res.out.frames[0].halfBound.length === 0, JSON.stringify(res.out.frames[0].halfBound));
  }

  // 8 — the bolded `Data source:` prefix CANNOT be both bold and style-bound through the plugin API.
  // That is a documented limitation (TEXTS.md), reported separately from real drift — and it is decided
  // by CHARACTER RANGE: the template is one bound style run, the clone is two runs, so segment counts
  // alone cannot tell this apart from a regular range losing its binding (test 11).
  {
    const res = await run(
      buildFrame({ name: "tpl", footerRows: [tplSource()] }),
      buildFrame({ name: "clone", footerRows: [txt("source", Object.assign({ size: 13,
        styleSegs: [["(unbound)", 0, 12], ["S:abc", 12, 40]] }, BOLD_PREFIX))] }));
    check("8 a half-bound prefix is not counted as drift", drift(res).length === 0, JSON.stringify(drift(res)));
    check("8 and is reported in halfBound", res.out.frames[0].halfBound.length === 1, JSON.stringify(res.out.frames[0].halfBound));
    check("8 with the API limitation named", /half-bound/.test(res.out.frames[0].halfBound[0] || ""), JSON.stringify(res.out.frames[0].halfBound));
    check("8 and the character ranges are shown", /@0-12/.test(res.out.frames[0].halfBound[0] || ""), JSON.stringify(res.out.frames[0].halfBound));
    // a real weight change is still drift
    const w = await run(buildFrame({ name: "tpl", footerRows: [tplSource()] }),
      buildFrame({ name: "clone", footerRows: [txt("source", { size: 13, fontSegs: [["Regular", 0, 40]] })] }));
    check("8 a lost bold prefix is still DRIFT", has(w, /weights Regular != Bold\+Regular/), JSON.stringify(drift(w)));
  }

  // 9 — CONFIG.expected reclassifies DECIDED drift as `accepted`, and nothing else.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone", footerMode: "HORIZONTAL" }),
                          ["footer mode HORIZONTAL"]);
    check("9 declared drift is accepted, not drift", drift(res).length === 0, JSON.stringify(drift(res)));
    check("9 and is listed as accepted", res.out.frames[0].accepted.length === 1, JSON.stringify(res.out.frames[0].accepted));
    const un = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone", footerMode: "HORIZONTAL" }));
    check("9 undeclared, the same change IS drift", has(un, /footer mode HORIZONTAL/), JSON.stringify(drift(un)));
  }

  // 10 — a footer that grew a row legitimately moves, but its BOTTOM edge must hold.
  {
    const res = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone", footerY: 440 }));
    check("10 a stranded footer is DRIFT", has(res, /footer bottom/), JSON.stringify(drift(res)));
    check("10 and says to re-pin it", has(res, /re-pin it/), JSON.stringify(drift(res)));
  }

  // 11 — a REGULAR range that lost its binding, which is the real defect (assigning `characters`
  // detaches the style). It is byte-for-byte the same SHAPE as test 8's documented exception — one
  // unbound run beside one bound run — and differs only in WHICH characters the unbound run covers. So
  // this is the assertion that a range-blind rule cannot satisfy at the same time as test 8.
  {
    const res = await run(
      buildFrame({ name: "tpl", footerRows: [tplSource()] }),
      buildFrame({ name: "clone", footerRows: [txt("source", Object.assign({ size: 13,
        styleSegs: [["S:abc", 0, 12], ["(unbound)", 12, 40]] }, BOLD_PREFIX))] }));
    check("11 an unbound REGULAR range is DRIFT, not the API limitation", drift(res).length > 0, JSON.stringify(drift(res)));
    check("11 and is NOT filed under halfBound", res.out.frames[0].halfBound.length === 0, JSON.stringify(res.out.frames[0].halfBound));
    check("11 and the segments are shown with ranges", has(res, /segments \[.*@12-40/), JSON.stringify(drift(res)));
    // mixed vs mixed with a different style id is drift too — both node-level values are the same SYMBOL
    const other = await run(
      buildFrame({ name: "tpl", footerRows: [txt("source", Object.assign({ size: 13,
        styleSegs: [["(unbound)", 0, 12], ["S:abc", 12, 40]] }, BOLD_PREFIX))] }),
      buildFrame({ name: "clone", footerRows: [txt("source", Object.assign({ size: 13,
        styleSegs: [["(unbound)", 0, 12], ["S:zzz", 12, 40]] }, BOLD_PREFIX))] }));
    check("11 two mixed nodes with different bindings are compared at all", drift(other).length > 0, JSON.stringify(drift(other)));
  }

  // 12 — a clone whose id does not resolve is an UNCHECKED deliverable. Counting only drift let the
  // aggregate verdict say everything matched while a frame had never been looked at.
  {
    const tpl = buildFrame({ name: "tpl" });
    const clone = buildFrame({ name: "clone" });
    const tplPage = node({ id: "P:tpl", type: "PAGE", name: "Templates", children: [tpl] });
    const clonePage = node({ id: "P:clone", type: "PAGE", name: "20260821 Chart", children: [clone] });
    tpl.id = "T:1"; clone.id = "C:1"; tpl.parent = tplPage; clone.parent = clonePage;
    const byId = { "T:1": tpl, "C:1": clone };
    const figma = { currentPage: tplPage, getNodeByIdAsync: async (id) => byId[id] || null,
                    setCurrentPageAsync: async (p) => { figma.currentPage = p; } };
    const body = SRC.replace(/^const CONFIG = \{[\s\S]*?^\};/m,
      'const CONFIG = { templateId: "T:1", frameIds: ["C:1", "C:stale"], expected: [] };');
    const out = await new Function("figma", `return (async () => { ${body} })();`)(figma);
    check("12 a stale id is not reported as matching", !/^all 2 frame\(s\) match/.test(out.verdict), out.verdict);
    check("12 the verdict says NOT CHECKED", /NOT CHECKED/.test(out.verdict), out.verdict);
    check("12 and names the unresolved id", /C:stale/.test(out.verdict), out.verdict);
    check("12 and counts it", out.unchecked === 1, String(out.unchecked));
  }

  // 13 — a logo squashed vertically keeps its x, y and width, so leaving `h` out of the comparison
  // reported a distorted logo as matching. `h` was fingerprinted the whole time.
  {
    const tpl = buildFrame({ name: "tpl" });
    const clone = buildFrame({ name: "clone" });
    clone.children.find((c) => c.name === "logo").height = 24;   // 35 in the template
    const res = await run(tpl, clone);
    check("13 a squashed logo is DRIFT", has(res, /logo 64x24/), JSON.stringify(drift(res)));
    check("13 and the template's size is quoted", has(res, /!= 64x35/), JSON.stringify(drift(res)));
    // a logo that has not moved or resized is still clean
    const same = await run(buildFrame({ name: "tpl" }), buildFrame({ name: "clone" }));
    check("13 an untouched logo is not drift", !has(same, /logo/), JSON.stringify(drift(same)));

    // 13b — a logo switched OFF keeps every coordinate, so no geometry test can see it. Same mistake
    // as the uncompared height above: fingerprinted, then not compared.
    const tpl2 = buildFrame({ name: "tpl" });
    const off = buildFrame({ name: "clone" });
    off.children.find((c) => c.name === "logo").visible = false;
    const res2 = await run(tpl2, off);
    check("13b a hidden logo is DRIFT", has(res2, /logo visible false/), JSON.stringify(drift(res2)));
    check("13b and says geometry cannot detect it", has(res2, /keeps its geometry/), JSON.stringify(drift(res2)));
  }

  const bad = results.filter((x) => !x.ok);
  for (const x of results) console.log(`${x.ok ? "PASS" : "FAIL"}  ${x.name}${x.ok ? "" : "  >> " + x.detail}`);
  console.log(bad.length ? `\n${bad.length} FAILURES` : `\nALL PASS (${results.length} checks)`);
  process.exit(bad.length ? 1 : 0);
})();
