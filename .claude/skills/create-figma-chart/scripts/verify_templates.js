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
//   contentX/W    — the box the chart and any annotation must match. No wrapper carries inner
//                   padding any more, so this reads the content box directly — 16/818 on the
//                   850-wide pair. A 0/850 here means a wrapper was re-padded to span the frame.
//   headerBottom  — the band's top edge, after the header's own auto-layout has settled
//   headerSizing  — `primaryAxisSizingMode` plus each child's sizing. AUTO + HUG means the header
//                   reflows when the text gets shorter; FIXED + a FILL/grow child means it does
//                   NOT, and a short subtitle silently inflates its own box instead (the 850-wide
//                   pair). This is the single most consequential property on the frame and it is
//                   invisible until you fill the texts.
//   footer        — id, name, y, height, layoutMode, constraints and rows. `layoutMode` says
//                   whether the rows reflow; `constraints.vertical` says which way the footer
//                   grows when they do — MAX keeps the bottom (grows into the band), MIN keeps
//                   the top (grows out of the frame, off the artboard).
//   logo          — the logo is a SIBLING of the header, so it does not affect header height.
//                   Reported so a future move back inside a title row is visible immediately.
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
  // The logo is a sibling of the header, not a child, so it contributes nothing to headerBottom.
  // Resolved first so the header/footer filter can exclude it — an INSTANCE carries its own
  // auto-layout. Reported so a move back inside a title row is visible immediately.
  const logo = n.children.find((c) => /^logo/i.test(c.name) || /^Logos\//.test(c.name)) || null;
  // Resolve header and footer structurally: the topmost and bottommost auto-layout children.
  // Names differ per template ("Frame 14", "Frame 5", "header"), ids change. Match ANY direction —
  // DI's footer is HORIZONTAL, so a VERTICAL-only filter drops it, leaves `footer` null, and
  // silently skips every footer check below.
  const autos = n.children
    .filter((c) => "layoutMode" in c && c.layoutMode !== "NONE" && c !== logo)
    .sort((a, b) => a.y - b.y);
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
    // AUTO + HUG children => the header reflows when the text is shorter. FIXED + a FILL/grow
    // child => it does not, and the slack silently inflates that child's box instead.
    headerSizing: header
      ? {
          primaryAxisSizingMode: header.primaryAxisSizingMode,
          reflows: header.primaryAxisSizingMode === "AUTO",
          children: header.children.map((c) => ({
            n: c.name.slice(0, 24),
            ar: c.textAutoResize || null,
            lsV: c.layoutSizingVertical,
            grow: c.layoutGrow,
            h: r(c.height),
          })),
        }
      : null,
    logo: logo ? { name: logo.name, type: logo.type, x: r(logo.x), y: r(logo.y), w: r(logo.width), h: r(logo.height) } : "NOT A SIBLING — check whether it moved back inside the header",
    footer: footer
      ? {
          id: footer.id,
          name: footer.name,
          y: r(footer.y),
          h: r(footer.height),
          bottom: r(footer.y + footer.height),
          layoutMode: footer.layoutMode,
          // MAX keeps the bottom edge (grows up into the band); MIN keeps the top edge and
          // grows down, out of the frame. Almost all of them are MIN — re-pin by hand.
          constraintV: footer.constraints.vertical,
          growsOutOfFrame: footer.constraints.vertical === "MIN",
          rows: footer.children.map((c) => ({ name: c.name.slice(0, 30), y: r(c.y), h: r(c.height), w: r(c.width) })),
        }
      : null,
    // Everything at the top level, so a footer left with no auto-layout at all still shows up.
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
