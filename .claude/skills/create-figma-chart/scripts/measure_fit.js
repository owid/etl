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
// Feed `contentAspect` from the output straight into
//   scripts/solve_export.py --band <W>x<H> --content-aspect <A>
// which then emits the corrected curl. Those two together are what remove the re-export per page.

const CONFIG = {
  frameId: "5332:75", // the template clone
  groupId: null, // the imported chart group, or null
  hideIds: [], // e.g. ["I123:4;5:6"] for connectors / year markers
};

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

// --- content box: the horizontal inset the template's own rows sit at.
const rows = frame.children.filter((c) => c !== logo && "absoluteBoundingBox" in c && c.absoluteBoundingBox);
const contentX = rows.length ? r(Math.min(...rows.map((c) => c.x))) : null;
const contentW = rows.length ? r(Math.max(...rows.map((c) => c.x + c.width)) - contentX) : null;

// --- header sizing: does the band actually move with the copy?
// FIXED with a FILL child does NOT reflow — the band stays at the placeholder value however short
// the title, and the slack is absorbed by the flexible child's box instead, burying dead air under
// the subtitle. Nothing renders wrong, so it survives a screenshot.
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
      })),
      reflows:
        header.primaryAxisSizingMode === "AUTO" &&
        header.children.every((c) => !("layoutGrow" in c) || c.layoutGrow === 0),
    }
  : null;

// --- the imported group
let group = null;
if (CONFIG.groupId) {
  const g = await figma.getNodeByIdAsync(CONFIG.groupId);
  if (!g) throw new Error(`groupId ${CONFIG.groupId} not found`);
  const hide = new Set(CONFIG.hideIds || []);

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

  const raw = g.absoluteBoundingBox;
  const w = x1 - x0,
    h = y1 - y0;
  group = {
    id: g.id,
    name: g.name,
    declared: { w: r(raw.width), h: r(raw.height), aspect: r(raw.width / raw.height) },
    measured: { w: r(w), h: r(h), aspect: r(w / h) },
    excluded: CONFIG.hideIds.length,
    // What Step 7 actually asks for: the uniform factor that makes the group span the content
    // width. rescale(), never resize() — resize stretches children through their constraints and
    // silently rewraps every text box in the chart.
    scaleToContentW: contentW ? r(contentW / w) : null,
    heightAtThatScale: contentW ? r((h * contentW) / w) : null,
    gapPerEnd: contentW && band ? r((band.height - (h * contentW) / w) / 2) : null,
  };

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
  frame: { id: frame.id, name: frame.name, w: r(fb.width), h: r(fb.height) },
  contentBox: { x: contentX, w: contentW },
  header: header ? { id: header.id, name: header.name, y: r(header.y), h: r(header.height) } : null,
  footer: footer ? { id: footer.id, name: footer.name, y: r(footer.y), h: r(footer.height), layoutMode: footer.layoutMode } : null,
  band,
  headerSizing,
  group,
  // Read these before trusting the numbers above.
  notes: [
    headerSizing && headerSizing.reflows === false
      ? "HEADER DOES NOT REFLOW — the band is a constant, not a measurement. Fix the CLONE (primaryAxisSizingMode AUTO, both children HUG + HEIGHT, layoutGrow 0), never the shared template, and say so in your report."
      : null,
    CONFIG.groupId && CONFIG.hideIds.length === 0
      ? "hideIds is empty — if the chart has `connectors` or year markers, the measured aspect includes them and will move once you hide them. Pass their ids."
      : null,
    group && group.gapPerEnd !== null && (group.gapPerEnd < 12 || group.gapPerEnd > 16)
      ? `gapPerEnd ${group.gapPerEnd} is outside the 12-16px target — re-solve the export with scripts/solve_export.py --content-aspect ${group.measured.aspect}`
      : null,
  ].filter(Boolean),
};
