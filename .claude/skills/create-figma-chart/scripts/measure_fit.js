// measure_fit.js — everything Step 7 needs to fit a chart, in ONE read-only use_figma call.
//
// Step 7 otherwise takes several separate probes: read the band off the filled clone, read the
// clone's content box, read the imported group's bbox, read the font-size histogram. At ~8-10s per
// MCP round trip that is a minute of latency for four numbers that come from one traversal.
//
// Read-only. It sets no property and creates no node, so it needs no approval to run against the
// shared Charts file (the skill's checkpoint rule covers writes).
//
// USAGE
//   Fill in CONFIG and paste the whole file as one `use_figma` call.
//     frameId  — the TEMPLATE CLONE, after Step 6 has filled its texts. Measuring an unfilled
//                clone gives you the placeholder band, which is the mistake reference/NODE-MAP.md
//                warns about: the header hugs its text, so the band moves when the real title
//                lands. A one-line title + one-line subtitle takes Static Vertical's band from
//                118 to 70.
//     groupId  — optional; the imported chart group, once it exists. Give it and you also get the
//                group's bbox, its content aspect, and the scale needed to fit the band.
//     hideIds  — optional; nodes to EXCLUDE from the group's measured bbox AND from the font-size
//                histogram (grapher's `connectors` and year markers extend past the plot). This
//                computes both as if they were hidden, WITHOUT hiding them — so the aspect you get
//                is the one you will actually fit, the histogram covers only text that survives,
//                and the file is untouched. reference/FITTING.md: hiding the elbows moved a
//                measured aspect from 1.6026 to 1.5558, turning a 14px gap into 9.5px.
//     slug     — optional, but needed for a runnable second pass: the grapher slug the first export
//     params     came from, plus any query params it carried (a country selection, an MDim view).
//                solve_export.py prints its `curl` only when it gets `--slug`, so without these
//                `nextPass` solves the numbers and leaves you to rebuild the request by hand — which
//                is where a selection or a view param gets dropped and the re-export quietly comes
//                back with different data.
//
// The `nextPass` field in the output is the finished second-pass command — run it as printed. Do
// NOT hand-build it by feeding the measured aspect back as `--content-aspect`: the solve would then
// aim at the aspect you already got, which moves the export further from the target rather than onto
// it. `nextPass` passes the REFLECTION `2*target - measured` instead, so the same model error that
// put the group off-target cancels. Worked through on the docs' own case (solved 1.6026, measured
// 1.5558): feeding 1.5558 back doubles the error, the reflection lands on the target.

const CONFIG = {
  frameId: "5332:75", // the template clone
  groupId: null, // the imported chart group, or null
  hideIds: [], // e.g. ["I123:4;5:6"] for connectors / year markers
  targetGap: 14, // px per end the fit aims for; 12-16 on 540-wide frames, 30 on the IG portrait
  targetLabel: 13.5, // final label px the first export was solved for; the portrait ladder uses 15
  slug: "", // grapher slug the first export came from, e.g. "life-expectancy"
  params: "", // the first export's extra query params, e.g. "country=USA~CHN" or an MDim view
};

// Normalized CONFIG reads, so deleting a line from the block above degrades to the house default
// rather than emitting `undefined` into the command this script prints.
const hideIds = CONFIG.hideIds || [];
const targetGap = CONFIG.targetGap ?? 14;
const targetLabel = CONFIG.targetLabel ?? 13.5;
const slug = CONFIG.slug || "";
const params = CONFIG.params || "";

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);
// Aspects and scale factors need more than 2dp: `rescale(1.14)` where 1.1433 was meant lands a
// 343px band height ~1px short, and the docs and `--content-aspect` both speak in 4dp aspects.
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
let groupAncestor = null;
if (groupNode) {
  let n = groupNode;
  while (n && n.parent && n.parent !== frame) n = n.parent;
  groupAncestor = n && n.parent === frame ? n : null;
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

  // Union the bboxes of visible leaves, skipping the hideIds subtrees. Doing it from leaves is what
  // lets us answer "what would the aspect be with the connectors hidden" without hiding anything.
  let x0 = Infinity,
    y0 = Infinity,
    x1 = -Infinity,
    y1 = -Infinity;
  // Which hideIds actually named a node under this group. A mistyped or copied-from-another-page id
  // excludes nothing, and reporting the requested count as `excluded` would then hide the one clue
  // that the aspect still contains the connectors — a silent no-op dressed up as a success.
  const seen = new Set();
  const collect = (n) => {
    seen.add(n.id);
    if ("children" in n) n.children.forEach(collect);
  };
  collect(g);
  const unmatched = hideIds.filter((id) => !seen.has(id));

  // The font histogram is collected in THIS traversal, not a second findAllWithCriteria pass. A
  // separate pass sees neither the hideIds subtrees nor an invisible ancestor — `visible` is a local
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
    excluded: { requested: hideIds.length, matched: hideIds.length - unmatched.length, unmatched },
    fitScaleToBandH: r4(fitScale),
    // The height fit lands the targetGap per end BY CONSTRUCTION, so the gap is no longer the
    // diagnostic — the leftover width is. `xMapShortfall` is what the closed-form x-map has to
    // close, and it IS the aspect miss expressed in px; a value far from 0 means re-export.
    widthAtFitScale: fitScale === null ? null : r(w * fitScale),
    xMapShortfall: fitScale === null || !contentW ? null : r(contentW - w * fitScale),
  };

  // The second pass, solved rather than guessed. `target` is the aspect the group has to have for
  // the gap to come out at targetGap; `measured` is what it actually came back as; the export to
  // request next is the one solved for the reflection of the measured aspect about the target.
  const gap = targetGap;
  const usable = targetH;
  if (contentW && usable > 0) {
    const target = contentW / usable;
    const measured = w / h;
    // --target-label has to travel with the correction: solve_export.py defaults to 13.5, so a
    // portrait solved at 15 would come back with smaller text purely from re-solving the aspect.
    // --slug/--params travel for the same reason one step further out: solve_export.py emits its
    // curl only with --slug, and rebuilding the URL by hand is where a country selection or an MDim
    // view param gets dropped and the re-export silently returns different data.
    // Runnable as printed, from the repo root: these scripts are committed non-executable like every
    // other script in this directory, and the repo rule is that Python goes through the venv.
    const cmd =
      ".venv/bin/python .claude/skills/create-figma-chart/scripts/solve_export.py" +
      ` --band ${contentW}x${r(band.height)} --gap ${gap} --target-label ${targetLabel}` +
      (slug ? ` --slug ${slug}` : "") +
      (params ? ` --params '${params}'` : "");
    group.target = { aspect: r4(target), gap, usableHeight: r(usable) };
    // The reflection only cancels a small model error. A group that is far off the target is not a
    // near-miss to correct but something else — the wrong export, or furniture still in the bbox —
    // and reflecting it would ask for an absurd aspect, so fall back to a plain first-pass solve.
    if (Math.abs(measured - target) / target <= 0.15) {
      group.nextPass = `${cmd} --content-aspect ${(2 * target - measured).toFixed(4)}`;
    } else {
      group.nextPass = cmd;
      group.nextPassNote =
        `measured aspect ${r(measured)} is more than 15% off the ${r(target)} target, too far for a ` +
        "one-step correction — solve it fresh (command above), and check the export and the hideIds first.";
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
    CONFIG.groupId && hideIds.length === 0
      ? "hideIds is empty — if the chart has `connectors` or year markers, the measured aspect includes them and will move once you hide them. Pass their ids."
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
      ? "CONFIG.slug is empty, so `nextPass` solves the numbers but prints no curl — solve_export.py emits the re-export command only with `--slug`. Set `slug` (and `params`, for a country selection or an MDim view) to what the first export used, so the second pass runs as printed instead of being rebuilt by hand."
      : null,
  ].filter(Boolean),
};
