// measure_fit.js — everything Step 7 needs to fit a chart, in ONE read-only use_figma call.
//
// Step 7 otherwise takes several separate probes: read the band off the filled clone, read the
// clone's content box, read the imported group's bbox, read the font-size histogram. At ~8-10s per
// MCP round trip that is a minute of latency for four numbers that come from one traversal.
//
// Read-only. It sets no property and creates no node, so it needs no approval to run against the
// shared Charts file (the skill's checkpoint rule covers writes).
//
// TESTED by scripts/test_measure_fit.js — a stubbed-figma harness, since this file executes only
// inside Figma. Run it after any edit here:  node .claude/skills/create-figma-chart/scripts/test_measure_fit.js
//
// USAGE
//   Fill in CONFIG and paste the whole file as one `use_figma` call.
//     frameId    — the TEMPLATE CLONE, after Step 6 has filled its texts. Measuring an unfilled
//                  clone gives you the placeholder band, which is the mistake reference/NODE-MAP.md
//                  warns about: the header hugs its text, so the band moves when the real title
//                  lands. A one-line title + one-line subtitle takes Static Vertical's band from
//                  118 to 70.
//     groupId    — optional; the imported chart group, once it exists. Give it and you also get the
//                  group's bbox, its ink aspect, and the scale needed to fit the band.
//     hideNames  — name patterns to treat as hidden furniture. Prefer this to hideIds: on a fresh
//                  import you do not have the ids yet, and grapher's names for these are stable
//                  where its node ids are not. `datapoints__<Entity>` is one marker per year (76 on
//                  a 1950-2025 line chart) — the pattern hides the container, not each child; the
//                  story dots are added by hand in Step 8.
//     hideIds    — extra ids to EXCLUDE from the measured bbox AND the font-size histogram, if
//                  something needs excluding that the names above don't catch. Both exclusions are
//                  computed as if the nodes were hidden, WITHOUT hiding them — the aspect you get is
//                  the one you will actually fit, and the file is untouched. reference/FITTING.md:
//                  hiding the elbows moved a measured aspect from 1.6026 to 1.5558, turning a 14px
//                  gap into 9.5px.
//     declared   — the probe export's declared SVG size, from
//                    grep -oE 'width="[0-9]+" height="[0-9]+"' embed.svg | head -1
//                  Give it (with imFontSize) and this returns the per-axis inset and the finished
//                  `solve_export.py` pass-2 command as `nextPass` — the export that lands exactly.
//     imFontSize — the imFontSize that probe was requested at, e.g. 30. The inset is only valid at
//                  that font, so the pass-2 command carries it.
//     slug       — optional, but needed for a runnable second pass: the slug the probe came from,
//     params       plus any query params it carried (a country selection, an MDim view). For an
//                  EXPLORER view pass the site-relative path (e.g. "explorers/natural-disasters"):
//                  solve_export.py routes a bare slug to /grapher/ and a path-carrying one as-is,
//                  because explorer views export from /explorers/<slug>.svg, a different endpoint.
//                  solve_export.py prints its `curl` only when it gets `--slug`, so without these
//                  `nextPass` solves the numbers and leaves you to rebuild the request by hand —
//                  which is where a selection or a view param gets dropped and the re-export quietly
//                  comes back with different data.
//     originalGroupId — the untouched reference import of the SAME format, left on the page beside
//                  the frame. Given it, this checks stroke widths: `rescale()` multiplies them, so
//                  a fitted chart's data line comes out thinner than grapher shipped it (measured
//                  1.32px against an original 4px) and reads as a weaker chart with nothing visibly
//                  broken. The reference DETECTS the change; the target is the house 3/4 (line 3,
//                  halo 4), not the export's own numbers — `imType=square` ships 4/5 where
//                  `uncaptioned` ships 2/3, and neither is what a finished frame carries.
//
// The `nextPass` field in the output is the finished second-pass command — run it as printed. It
// feeds solve_export.py the MEASURED per-axis inset (`--declared`/`--ink`/`--im-font-size`), which
// is what makes the second export exact instead of another guess: the `1.4 * imFontSize` model is
// symmetric and the real inset is not (64.1/29.0 measured at imFontSize 30 against the model's
// 42/42). Without CONFIG.declared there is nothing to compute the inset from, so `nextPass` falls
// back to a fresh probe solve and a note tells you what to set.

const CONFIG = {
  frameId: "5332:75", // the template clone
  groupId: null, // the imported chart group, or null
  hideNames: [/^connectors$/, /^datapoints__/], // furniture, matched by stable grapher names
  hideIds: [], // extra ids, e.g. ["I123:4;5:6"], for what the names above don't catch
  declared: null, // the probe's declared SVG size, e.g. [791, 645]
  imFontSize: null, // the imFontSize the probe was requested at, e.g. 30
  targetGap: 14, // px per end the fit aims for; 12-16 on 540-wide frames, 30 on the IG portrait
  targetLabel: 13.5, // final label px the probe was solved for; the portrait ladder uses 15
  slug: "", // slug the probe came from, e.g. "life-expectancy"; an explorer view needs its path, "explorers/<slug>"
  params: "", // the probe's extra query params, e.g. "country=USA~CHN" or an MDim view
  originalGroupId: null, // the untouched reference import of the same format, for stroke checks
};

// Normalized CONFIG reads, so deleting a line from the block above degrades to the house default
// rather than emitting `undefined` into the command this script prints.
const hideNames = CONFIG.hideNames || [];
const hideIds = CONFIG.hideIds || [];
const targetGap = CONFIG.targetGap ?? 14;
const targetLabel = CONFIG.targetLabel ?? 13.5;
const slug = CONFIG.slug || "";
const params = CONFIG.params || "";

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);
// Aspects and scale factors need more than 2dp: `rescale(1.14)` where 1.1433 was meant lands a
// 343px band height ~1px short, and the docs speak in 4dp aspects.
const r4 = (v) => (v === null || v === undefined ? null : Math.round(v * 10000) / 10000);

// How many lines a TEXT node actually renders on. `lineHeight` may be AUTO or a percentage, so
// resolve it to px first; fall back to 1.2x the font size, which is Figma's AUTO factor.
const renderedLines = (t) => {
  const fs = typeof t.fontSize === "number" ? t.fontSize : null;
  const lh = t.lineHeight;
  let px = null;
  if (lh && lh.unit === "PIXELS") px = lh.value;
  else if (lh && lh.unit === "PERCENT" && fs) px = (lh.value / 100) * fs;
  else if (fs) px = fs * 1.2;
  return px ? Math.max(1, Math.round(t.height / px)) : null;
};

const frame = await figma.getNodeByIdAsync(CONFIG.frameId);
if (!frame) throw new Error(`frameId ${CONFIG.frameId} not found`);

// A node is only readable once its page is loaded, and a script may switch pages once.
let page = frame;
while (page && page.type !== "PAGE") page = page.parent;
if (page && figma.currentPage !== page) await figma.setCurrentPageAsync(page);

const fb = frame.absoluteBoundingBox;

// --- header and footer, resolved STRUCTURALLY (topmost / bottommost auto-layout child).
// Names are not stable across design edits and a whole generation of them has already been
// replaced; verify_templates.js uses this same resolver for the same reason. Match ANY direction:
// DI's footer is HORIZONTAL, and a VERTICAL-only filter silently drops it.
const logo = frame.children.find((c) => /^logo/i.test(c.name) || /^Logos\//.test(c.name)) || null;
const autos = frame.children
  .filter((c) => "layoutMode" in c && c.layoutMode !== "NONE" && c !== logo)
  .sort((a, b) => a.y - b.y);
const header = autos[0] || null;
const footer = autos.length > 1 ? autos[autos.length - 1] : null;

// A source row raised inside its footer lifts the band above the footer's own y.
let footerTop = footer ? footer.y : null;
if (footer && "children" in footer && footer.children.length) {
  const minChildY = Math.min(...footer.children.map((c) => c.y));
  footerTop = footer.y + Math.min(0, minChildY);
}

const bandTop = header ? r(header.y + header.height) : null;
const band = bandTop !== null && footerTop !== null ? { top: bandTop, bottom: r(footerTop), height: r(footerTop - bandTop) } : null;

// --- content box: taken from the HEADER, exactly as verify_templates.js does it (`header.x` /
// `header.width`), because by Step 7 the imported chart is already a child of this frame — the docs
// require appending it before positioning — and a union over `frame.children` would then include the
// not-yet-fitted group. That inflates the box to the group's own width, so `xMapShortfall` comes out
// near 0, i.e. "nothing left to close", which is the one answer this script exists to produce.
const contentX = header ? r(header.x) : null;
const contentW = header ? r(header.width) : null;

// The union over the template's own rows, kept as a cross-check and NOT used for the scale. The
// group and the logo are excluded — the group via its top-level ancestor, since it is the ancestor,
// not the group itself, that sits in `frame.children`. On the 540-wide and 850-wide families this
// agrees with the header box (16/508 and 16/818); on the 302-wide small templates it does not, since
// their header hugs its own text width (206-278) — but that format has no fit step at all
// (reference/FITTING.md), so the header box is the right primary everywhere this script is used.
const groupNode = CONFIG.groupId ? await figma.getNodeByIdAsync(CONFIG.groupId) : null;
if (CONFIG.groupId && !groupNode) throw new Error(`groupId ${CONFIG.groupId} not found`);
// The group has to be INSIDE this frame, and failing to reach it is an error rather than a null to
// carry on from. Step 2 puts the original grapher chart on the page beside the template clone, so the
// page always holds at least two chart groups whose ids are copied by hand off `get_metadata` — and
// measuring the reference chart against the clone's band produces no error, no empty result and no
// visible clue: a plausible aspect, a plausible scale, and a `nextPass` that re-exports at the wrong
// aspect. Everything below this point measures against `frame`, so a group from anywhere else makes
// every number a mismatched pair.
let groupAncestor = null;
if (groupNode) {
  let n = groupNode;
  while (n && n.parent && n.parent !== frame) n = n.parent;
  groupAncestor = n && n.parent === frame ? n : null;
  if (!groupAncestor) {
    throw new Error(
      `groupId ${CONFIG.groupId} ("${groupNode.name}") is not inside frameId ${CONFIG.frameId} ` +
        `("${frame.name}") — walking up its parents never reached that frame, so the band, the ` +
        `content box and the fit scale would all describe a different node than the group. Check ` +
        `you did not copy the id of the ORIGINAL chart sitting beside the clone, or of a group on ` +
        `another page.`,
    );
  }
}
const rows = frame.children.filter(
  (c) => c !== logo && c !== groupAncestor && "absoluteBoundingBox" in c && c.absoluteBoundingBox,
);
const rowsX = rows.length ? r(Math.min(...rows.map((c) => c.x))) : null;
const rowsW = rows.length ? r(Math.max(...rows.map((c) => c.x + c.width)) - rowsX) : null;

// --- header sizing: does the band actually move with the copy?
// FIXED with a FILL child does NOT reflow — the band stays at the placeholder value however short
// the title, and the slack is absorbed by the flexible child's box instead, burying dead air under
// the subtitle. Nothing renders wrong, so it survives a screenshot.
//
// Every property that can pin the height has to be in the predicate, not just the parent's sizing
// mode: a child that is vertically FIXED, or a TEXT that does not auto-resize its height, keeps its
// box whatever the copy does, so an AUTO parent still hugs a constant. Reporting `reflows: true`
// there suppresses the warning in exactly the case it exists for.
//
// ABSOLUTE splits by type, and the distinction is the whole question this predicate answers — "does
// the band move with the copy?". An absolutely positioned TEXT is out of the auto-layout flow, so
// editing it cannot grow the header at all: the band stops tracking that copy entirely, which is
// worse than a pinned height, not safer. Any other absolute child is decoration that cannot pin the
// parent either way, so it is skipped rather than counted against.
const blocksReflow = (c) => {
  if ("layoutPositioning" in c && c.layoutPositioning === "ABSOLUTE") {
    return c.type === "TEXT" ? "layoutPositioning ABSOLUTE (text out of the flow)" : null;
  }
  if ("layoutGrow" in c && c.layoutGrow) return "layoutGrow";
  if ("layoutSizingVertical" in c && c.layoutSizingVertical === "FIXED") return "layoutSizingVertical FIXED";
  // HEIGHT exactly, which is the invariant reference/NODE-MAP.md documents and verify_templates.js
  // already enforces (`textAutoResize ${ar} != HEIGHT`). Listing the modes that block instead lets
  // WIDTH_AND_HEIGHT through, and that one is not reflow-safe: the box hugs the text on BOTH axes, so
  // a long title grows sideways on one line instead of wrapping to a second. The header height then
  // does not track the copy at all — the failure this predicate exists to catch — and the title runs
  // toward the logo and the frame edge while the band keeps its placeholder value.
  if (c.type === "TEXT" && c.textAutoResize !== "HEIGHT") {
    return `textAutoResize ${c.textAutoResize} (not HEIGHT)`;
  }
  return null;
};

const headerSizing = header
  ? {
      primaryAxisSizingMode: header.primaryAxisSizingMode,
      itemSpacing: header.itemSpacing,
      children: header.children.map((c) => ({
        name: c.name,
        type: c.type,
        h: r(c.height),
        // RENDERED lines, inferred from height / lineHeight. Counting `\n` in `characters` is the
        // trap: a wrapped title has no newline in it, so an explicit-break count reports a
        // two-line placeholder as 1 and the band arithmetic below looks wrong for no reason.
        lines: c.type === "TEXT" ? renderedLines(c) : null,
        textAutoResize: c.type === "TEXT" ? c.textAutoResize : null,
        layoutSizingVertical: "layoutSizingVertical" in c ? c.layoutSizingVertical : null,
        layoutGrow: "layoutGrow" in c ? c.layoutGrow : null,
        layoutPositioning: "layoutPositioning" in c ? c.layoutPositioning : null,
        // why this child pins the header's height, or null if it doesn't
        blocksReflow: blocksReflow(c),
      })),
      reflows: header.primaryAxisSizingMode === "AUTO" && header.children.every((c) => blocksReflow(c) === null),
    }
  : null;

// --- the imported group
let group = null;
if (groupNode) {
  const g = groupNode;
  const hide = new Set(hideIds);

  // Union the bboxes of visible leaves, skipping the excluded subtrees. Doing it from leaves is what
  // lets us answer "what would the aspect be with the connectors hidden" without hiding anything.
  let x0 = Infinity,
    y0 = Infinity,
    x1 = -Infinity,
    y1 = -Infinity;
  // Resolve the name patterns to ids in one walk, and record which hideIds actually named a node
  // under this group. A mistyped or copied-from-another-page id excludes nothing, and reporting the
  // requested count as `excluded` would then hide the one clue that the aspect still contains the
  // connectors — a silent no-op dressed up as a success.
  const seen = new Set();
  const hiddenByName = [];
  const collect = (n) => {
    seen.add(n.id);
    if (hideNames.some((re) => re.test(n.name))) {
      hide.add(n.id);
      hiddenByName.push(n.name);
    }
    if ("children" in n) n.children.forEach(collect);
  };
  collect(g);
  const unmatched = hideIds.filter((id) => !seen.has(id));

  // The font histogram is collected in THIS traversal, not a second findAllWithCriteria pass. A
  // separate pass sees neither the excluded subtrees nor an invisible ancestor — `visible` is a local
  // flag, so a text inside a hidden group still reads as visible — and would report label sizes for
  // text the fitted chart will not contain, which is the histogram's whole job to get right.
  const visibleTexts = [];
  const walk = (n) => {
    if (hide.has(n.id)) return;
    if ("visible" in n && !n.visible) return;
    if (n.type === "TEXT") visibleTexts.push(n);
    if ("children" in n && n.children.length) {
      n.children.forEach(walk);
      return;
    }
    const b = n.absoluteBoundingBox;
    if (!b || b.width === 0 || b.height === 0) return;
    x0 = Math.min(x0, b.x);
    y0 = Math.min(y0, b.y);
    x1 = Math.max(x1, b.x + b.width);
    y1 = Math.max(y1, b.y + b.height);
  };
  walk(g);

  if (!Number.isFinite(x0)) throw new Error(`groupId ${CONFIG.groupId} has no visible, non-excluded leaf to measure`);

  const raw = g.absoluteBoundingBox;
  const w = x1 - x0,
    h = y1 - y0;

  // What Step 7 actually asks for is the HEIGHT-first factor. reference/FITTING.md: "Fit to the
  // band's height, then map x to fill the width — not the other way round", and again in Step 8's
  // recipe, `chart.rescale(TARGET_H / chart.height)  // height-first; never resize()`. The
  // width-first `contentW / w` is the move those lines open by rejecting — it locks the width and
  // leaves the height wherever the export's aspect fell — so this reports TARGET_H / h instead. On
  // the docs' own near-miss (measured 1.4342 against a 1.4810 target) width-first comes out 3.3%
  // large, which would also land in every fontSizes.afterScale below: about 0.5px at 15px, enough
  // to pick the wrong rung off Step 8c's ladder.
  //
  // rescale(), never resize() — resize stretches children through their constraints and silently
  // rewraps every text box in the chart.
  const targetH = band ? band.height - 2 * targetGap : null;
  const fitScale = targetH !== null && targetH > 0 ? targetH / h : null;

  group = {
    id: g.id,
    name: g.name,
    declared: raw ? { w: r(raw.width), h: r(raw.height), aspect: r4(raw.width / raw.height) } : null,
    measured: { w: r(w), h: r(h), aspect: r4(w / h) },
    excluded: { byName: hiddenByName, requested: hideIds.length, matched: hideIds.length - unmatched.length, unmatched },
    fitScaleToBandH: r4(fitScale),
    // The height fit lands the targetGap per end BY CONSTRUCTION, so the gap is no longer the
    // diagnostic — the leftover width is. `xMapShortfall` is what the closed-form x-map has to
    // close, and it IS the aspect miss expressed in px; a value far from 0 means re-export.
    widthAtFitScale: fitScale === null ? null : r(w * fitScale),
    xMapShortfall: fitScale === null || !contentW ? null : r(contentW - w * fitScale),
  };

  // The second pass, solved rather than guessed: the per-axis inset, which is what makes the SECOND
  // export exact instead of another probe. `1.4 * imFontSize` is symmetric and the real inset is not
  // — 64.1/29.0 measured at imFontSize 30 on an uncaptioned line chart against the model's 42/42.
  // --slug/--params travel with the command because solve_export.py emits its curl only with --slug,
  // and rebuilding the URL by hand is where a country selection or an MDim view param gets dropped
  // and the re-export silently returns different data. Runnable as printed, from the repo root:
  // these scripts are committed non-executable like every other script in this directory, and the
  // repo rule is that Python goes through the venv.
  if (band && contentW) {
    const carry = (slug ? ` --slug ${slug}` : "") + (params ? ` --params '${params}'` : "");
    const cmd =
      ".venv/bin/python .claude/skills/create-figma-chart/scripts/solve_export.py" +
      ` --band ${contentW}x${r(band.height)} --gap ${targetGap}`;
    if (CONFIG.declared) {
      const [dw, dh] = CONFIG.declared;
      const insetX = r(dw - w);
      const insetY = r(dh - h);
      // The inset is only meaningful on a group still at its NATURAL size. Run this after the fit
      // and you are subtracting a rescaled width from the probe's declared one, which yields a large
      // plausible-looking number (282.95 against a true 64.08, measured) and would send the next
      // export badly wrong. A real inset is a small fraction of the canvas, so bound it.
      const rescaled = insetX <= 0 || insetY <= 0 || insetX > 0.25 * dw || insetY > 0.25 * dh;
      if (rescaled) {
        group.inset = {
          unusable:
            `inset would be ${insetX} / ${insetY} against a declared ${dw}x${dh} — implausible, so this ` +
            "group is not at its natural size (already rescaled, or CONFIG.declared belongs to a " +
            "different export). Measure the inset on the freshly-imported probe, before the fit.",
        };
        group.nextPass = `${cmd} --target-label ${targetLabel}${carry}`;
        group.nextPassNote =
          "the inset was unusable (see group.inset), so this is a fresh PROBE solve, not the exact " +
          "second pass. Re-import the probe at its natural size and re-run.";
      } else {
        group.inset = {
          x: insetX,
          y: insetY,
          modelWouldSay: CONFIG.imFontSize ? r(1.4 * CONFIG.imFontSize) : null,
        };
        group.nextPass =
          `${cmd} --declared ${dw}x${dh} --ink ${r(w)}x${r(h)}` +
          (CONFIG.imFontSize ? ` --im-font-size ${CONFIG.imFontSize}` : " --im-font-size <the one you exported at>") +
          carry;
        if (!CONFIG.imFontSize) {
          group.nextPassNote =
            "CONFIG.imFontSize is unset — fill in the imFontSize the probe was requested at before " +
            "running nextPass; the inset is only valid at that font.";
        }
      }
    } else {
      group.nextPass = `${cmd} --target-label ${targetLabel}${carry}`;
      group.nextPassNote =
        "CONFIG.declared is unset, so no inset was computed and this is a fresh PROBE solve. For the " +
        "exact second pass, read the declared size off the probe SVG " +
        "(grep -oE 'width=\"[0-9]+\" height=\"[0-9]+\"' embed.svg | head -1) and re-run with " +
        "CONFIG.declared and CONFIG.imFontSize set.";
    }
  }

  // Stroke widths against the untouched reference import: rescale() multiplied them, and the data
  // line is the one that matters. Compare by node NAME, since the two groups are separate imports.
  if (CONFIG.originalGroupId) {
    const orig = await figma.getNodeByIdAsync(CONFIG.originalGroupId);
    if (!orig) {
      group.strokes = { error: `originalGroupId ${CONFIG.originalGroupId} not found` };
    } else {
      const widths = (node) => {
        const m = {};
        for (const n of node.findAll(() => true)) {
          if (/^(line|outline)__/.test(n.name) && "strokeWeight" in n && typeof n.strokeWeight === "number") {
            m[n.name] = r(n.strokeWeight);
          }
        }
        return m;
      };
      const o = widths(orig);
      const f = widths(g);
      // The reference tells you the rescale CHANGED a stroke. It does not tell you the target:
      // imType=square ships 4/5 and uncaptioned ships 2/3, and the house weight is 3 with a 4px halo
      // — neither of them (reference/GOTCHAS.md). So the comparison detects, and the HOUSE value is
      // what the repair sets, after the scale.
      const house = (k) => (/^outline__/.test(k) ? 4 : 3);
      const strokeRows = Object.keys(o).map((k) => ({
        name: k,
        reference: o[k],
        fitted: f[k] ?? null,
        house: house(k),
        ok: f[k] != null && Math.abs(f[k] - house(k)) < 0.05,
      }));
      group.strokes = {
        rows: strokeRows,
        verdict: strokeRows.every((x) => x.ok)
          ? "ok — strokes sit at the house 3/4"
          : "STROKES ARE OFF THE HOUSE 3/4 — set them after the scale: " +
            strokeRows.filter((x) => !x.ok).map((x) => `${x.name} ${x.fitted} -> ${x.house}`).join(", ") +
            ". The reference column only shows what the export shipped (square 4/5, uncaptioned 2/3) " +
            "— it is the detector, not the target; the house weight is 3 with a 4px halo regardless " +
            "of frame width (reference/GOTCHAS.md).",
      };
    }
  }

  // font-size histogram, and what each size becomes once scaled in. Built from the bbox traversal's
  // own text nodes, so it describes exactly the labels the measured aspect was taken over.
  const sizes = {};
  for (const t of visibleTexts) {
    const fs = typeof t.fontSize === "number" ? t.fontSize : "mixed";
    sizes[fs] = (sizes[fs] || 0) + 1;
  }
  group.fontSizes = Object.entries(sizes)
    .sort((a, b) => b[1] - a[1])
    .map(([size, n]) => ({
      size: size === "mixed" ? "mixed" : Number(size),
      count: n,
      // the height-first fit factor, so these are the sizes Step 8c actually picks its rung from
      afterScale: size === "mixed" || fitScale === null ? null : r(Number(size) * fitScale),
    }));
}

return {
  frame: { id: frame.id, name: frame.name, w: fb ? r(fb.width) : null, h: fb ? r(fb.height) : null },
  contentBox: { x: contentX, w: contentW, from: header ? "header" : null },
  contentBoxFromRows: { x: rowsX, w: rowsW },
  header: header ? { id: header.id, name: header.name, y: r(header.y), h: r(header.height) } : null,
  footer: footer ? { id: footer.id, name: footer.name, y: r(footer.y), h: r(footer.height), layoutMode: footer.layoutMode } : null,
  band,
  headerSizing,
  group,
  // Read these before trusting the numbers above.
  notes: [
    headerSizing && headerSizing.reflows === false
      ? "HEADER DOES NOT REFLOW — the band is a constant, not a measurement. Check each child's `blocksReflow` for which property pins it. Fix the CLONE (primaryAxisSizingMode AUTO, both children HUG + HEIGHT, layoutGrow 0), never the shared template, and say so in your report."
      : null,
    contentW !== null && rowsW !== null && Math.abs(contentW - rowsW) > 1
      ? `contentBox from the header (${contentX}/${contentW}) disagrees with the union of the template's other rows (${rowsX}/${rowsW}). The header is the one to trust on any template with a fit step; a difference here means either a header that hugs its text (the 302-wide small templates do, and they have no fit step) or a row that has been moved — look before you scale.`
      : null,
    group && group.excluded.byName.length === 0 && group.excluded.matched === 0
      ? "Nothing was excluded — if this chart has `connectors` or per-year markers, the measured aspect includes them and will move once you hide them. Check the names in the tree against CONFIG.hideNames."
      : null,
    group && group.excluded.unmatched.length
      ? `hideIds NOT FOUND under the group: ${group.excluded.unmatched.join(", ")}. Nothing was excluded for them, so the measured aspect still contains whatever they were meant to remove. Re-read the ids off this group — an id from another page or another chart looks identical and excludes nothing.`
      : null,
    // 6px of leftover width is the old "2px off the target gap" threshold re-expressed for the
    // height-first fit: |gap error| = |xMapShortfall| / (2 x measured aspect), so at the aspects
    // these templates run (~1.4-1.6) the two trip at very nearly the same aspect miss.
    group && group.xMapShortfall !== null && Math.abs(group.xMapShortfall) > 6
      ? `the height-fitted group lands ${group.xMapShortfall}px off the ${contentW}px content width — that leftover IS the aspect miss in px, and it is more than the x-map should be asked to close. Re-export with the \`nextPass\` command above (read \`nextPassNote\` first if it is set). The ${targetGap}px gap per end is already correct by construction, so do not judge this fit by the gap.`
      : null,
    group && group.nextPass && !slug
      ? "CONFIG.slug is empty, so `nextPass` solves the numbers but prints no curl — solve_export.py emits the re-export command only with `--slug`. Set `slug` (and `params`, for a country selection or an MDim view) to what the probe used, so the second pass runs as printed instead of being rebuilt by hand."
      : null,
    group && !CONFIG.originalGroupId
      ? "originalGroupId is unset, so stroke widths were not checked against the reference import. The rescale thins them and the render still looks fine — pass it."
      : null,
    group && group.strokes && group.strokes.verdict && !group.strokes.verdict.startsWith("ok")
      ? group.strokes.verdict
      : null,
  ].filter(Boolean),
};
