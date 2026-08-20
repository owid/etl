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
//     hideIds  — optional; nodes to EXCLUDE from the group's measured bbox (grapher's `connectors`
//                and year markers extend past the plot). This computes the bbox as if they were
//                hidden, WITHOUT hiding them — so the aspect you get is the one you will actually
//                fit, and the file is untouched. reference/FITTING.md: hiding the elbows moved a
//                measured aspect from 1.6026 to 1.5558, turning a 14px gap into 9.5px.
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
};

const hideIds = CONFIG.hideIds || [];

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);

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
// not-yet-fitted group. That inflates the box to the group's own width and `scaleToContentW` comes
// out near 1, i.e. "no scaling needed", which is the one answer this script exists to produce.
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
// there suppresses the warning in exactly the case it exists for. An ABSOLUTE child is out of the
// auto-layout flow entirely and cannot pin anything, so it is skipped rather than counted against.
const blocksReflow = (c) => {
  if ("layoutPositioning" in c && c.layoutPositioning === "ABSOLUTE") return null;
  if ("layoutGrow" in c && c.layoutGrow) return "layoutGrow";
  if ("layoutSizingVertical" in c && c.layoutSizingVertical === "FIXED") return "layoutSizingVertical FIXED";
  if (c.type === "TEXT" && (c.textAutoResize === "NONE" || c.textAutoResize === "TRUNCATE")) {
    return `textAutoResize ${c.textAutoResize}`;
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
  const walk = (n) => {
    if (hide.has(n.id)) return;
    if ("visible" in n && !n.visible) return;
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
  group = {
    id: g.id,
    name: g.name,
    declared: raw ? { w: r(raw.width), h: r(raw.height), aspect: r(raw.width / raw.height) } : null,
    measured: { w: r(w), h: r(h), aspect: r(w / h) },
    excluded: hideIds.length,
    // What Step 7 actually asks for: the uniform factor that makes the group span the content
    // width. rescale(), never resize() — resize stretches children through their constraints and
    // silently rewraps every text box in the chart.
    scaleToContentW: contentW ? r(contentW / w) : null,
    heightAtThatScale: contentW ? r((h * contentW) / w) : null,
    gapPerEnd: contentW && band ? r((band.height - (h * contentW) / w) / 2) : null,
  };

  // The second pass, solved rather than guessed. `target` is the aspect the group has to have for
  // the gap to come out at targetGap; `measured` is what it actually came back as; the export to
  // request next is the one solved for the reflection of the measured aspect about the target.
  const gap = CONFIG.targetGap;
  const usable = band ? band.height - 2 * gap : null;
  if (contentW && usable > 0) {
    const target = contentW / usable;
    const measured = w / h;
    const cmd = `scripts/solve_export.py --band ${contentW}x${r(band.height)} --gap ${gap}`;
    group.target = { aspect: r(target), gap, usableHeight: r(usable) };
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

  // font-size histogram, and what each size becomes once scaled in
  const sizes = {};
  const texts = g.findAllWithCriteria ? g.findAllWithCriteria({ types: ["TEXT"] }) : [];
  for (const t of texts) {
    if (!t.visible) continue;
    const fs = typeof t.fontSize === "number" ? t.fontSize : "mixed";
    sizes[fs] = (sizes[fs] || 0) + 1;
  }
  group.fontSizes = Object.entries(sizes)
    .sort((a, b) => b[1] - a[1])
    .map(([size, n]) => ({
      size: size === "mixed" ? "mixed" : Number(size),
      count: n,
      afterScale: size === "mixed" || !group.scaleToContentW ? null : r(Number(size) * group.scaleToContentW),
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
    group && group.gapPerEnd !== null && Math.abs(group.gapPerEnd - CONFIG.targetGap) > 2
      ? `gapPerEnd ${group.gapPerEnd} is more than 2px off the ${CONFIG.targetGap}px target — re-export with the \`nextPass\` command above (read \`nextPassNote\` first if it is set).`
      : null,
  ].filter(Boolean),
};
