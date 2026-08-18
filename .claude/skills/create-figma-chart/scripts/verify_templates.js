// Measure every template in SKILL.md's node map, in one `use_figma` call.
//
// Paste as the `code` argument with fileKey = the yearly Charts file. Read-only: it switches to the
// Templates page (once, per the figma-use page rule) and reports geometry. Diff the output against
// the node map and the Step 7 band table before cloning anything — the design team edits these
// frames in place, and the values that move are the ones a page gets laid out against.
//
// What it reports per template, and why each field is here:
//   size          — the frame, which is what picks the template in the first place
//   fill          — one of the "which template is this?" tells (DI white, static cream, IG beige)
//   contentX/W    — the box the chart and any annotation must match. On the 850-wide pair this
//                   comes back 0/850 because their header wrapper spans the frame and pads 16px
//                   inward; the content box there is 16/818. That is expected, not drift.
//   headerBottom  — the band's top edge, after the header's own auto-layout has settled
//   footer        — id, name, y, height, layoutMode and rows. `layoutMode` is the one Step 6's
//                   structural check turns on: VERTICAL reflows when a row's line count changes,
//                   NONE does not. Both Instagram footers changed from NONE to VERTICAL (and to
//                   new ids) between 2026-08-14 and 2026-08-17, so check structure, not just numbers.
//
// The band to fit a chart into is `headerBottom → footerTop`, where footerTop is
// `footer.y + Math.min(0, sourceRow.y)` — a source row raised inside its footer lifts the band.

const TEMPLATES = {
  "IG square": "798:161",
  "IG portrait": "6689:8",
  "IG reel": "7336:8",
  DI: "6799:1859",
  "static mobile 1": "24590:20",
  "static mobile 2": "24590:32",
  "static horizontal": "5332:75",
  "static vertical": "5332:93",
  "small guided": "25344:1357",
  "small pull": "25344:1391",
};

const templatesPage = figma.root.children.find((p) => p.id === "798:54");
if (!templatesPage) throw new Error("Templates page 798:54 not found — new year, new file? Ask for the link.");
await figma.setCurrentPageAsync(templatesPage);

const r = (v) => Math.round(v * 100) / 100;
const hex = (node) => {
  const f = node.fills && node.fills[0];
  if (!f) return "none";
  if (f.type !== "SOLID") return f.type;
  const c = [f.color.r, f.color.g, f.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0"));
  return (f.visible === false ? "hidden " : "") + "#" + c.join("");
};

const out = {};
for (const [label, id] of Object.entries(TEMPLATES)) {
  const n = await figma.getNodeByIdAsync(id);
  if (!n) {
    out[label] = { missing: id };
    continue;
  }
  // Resolve header and footer structurally: the topmost and bottommost vertical auto-layout
  // children. Names differ per template ("Frame 14", "Frame 5", "header"), ids change.
  const autos = n.children.filter((c) => "layoutMode" in c && c.layoutMode === "VERTICAL").sort((a, b) => a.y - b.y);
  const header = autos[0] || null;
  const footer = autos.length > 1 ? autos[autos.length - 1] : null;
  out[label] = {
    id: n.id,
    name: n.name,
    size: [r(n.width), r(n.height)],
    fill: hex(n),
    contentX: header ? r(header.x) : null,
    contentW: header ? r(header.width) : null,
    headerBottom: header ? r(header.y + header.height) : null,
    footer: footer
      ? {
          id: footer.id,
          name: footer.name,
          y: r(footer.y),
          h: r(footer.height),
          layoutMode: footer.layoutMode,
          rows: footer.children.map((c) => ({ name: c.name.slice(0, 30), y: r(c.y), h: r(c.height), w: r(c.width) })),
        }
      : null,
    // Everything at the top level, so a footer that is NOT auto-layout (DI's `Frame 12`) still shows up.
    topLevel: n.children.map((c) => ({
      name: c.name.slice(0, 26),
      type: c.type,
      y: r(c.y),
      h: r(c.height),
      layoutMode: c.layoutMode || null,
    })),
  };
}
return out;
