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
//                   NOT, and a short subtitle silently inflates its own box instead. As of
//                   2026-08-20 all nine templates are AUTO + HUG; the 850-wide pair used to be the
//                   FIXED case and the design team has since converted it, so a `false` here is now
//                   a regression rather than a shipped state. This is the single most consequential
//                   property on the frame and it is invisible until you fill the texts.
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

// The load-bearing geometry, as this skill records it. Mirrors SKILL.md's node map and the Step 7
// band table, so a DRIFT verdict means EITHER the design team moved the frame OR this skill's table
// is wrong — both are stop-and-report, and you cannot tell which without looking.
//
// `VERIFIED` is provenance, not a gate: it says when these numbers were last measured, which is what
// lets you judge a drift report (a day-old table plus a mismatch is probably your error; a
// three-month-old one plus a mismatch is probably the design team). The gate is this script's own
// verdict. Update both together — a stale expectation with a fresh date is worse than no date.
//
// `null` means "don't check": the 302-wide pair has a free height, and its bottom-most auto-layout
// child is a source row rather than a footer, so footer fields don't apply.
const VERIFIED = "2026-08-20";
const EXPECT = {
  "IG square":        { size: [540, 540],   contentX: 16, contentW: 508, headerBottom: 118,    reflows: true,  footerY: 488,     footerMode: "VERTICAL",   footerConstraintV: "MIN" },
  "IG portrait":      { size: [560, 700],   contentX: 26, contentW: 508, headerBottom: 135,    reflows: true,  footerY: 640,     footerMode: "VERTICAL",   footerConstraintV: "MIN" },
  "IG reel":          { size: [616, 1096],  contentX: null, contentW: null, headerBottom: null, reflows: null,  footerY: null,    footerMode: null,         footerConstraintV: null },
  DI:                 { size: [540, 540],   contentX: 16, contentW: 508, headerBottom: 118,    reflows: true,  footerY: 508,     footerMode: "HORIZONTAL", footerConstraintV: "MIN" },
  "static mobile 1":  { size: [540, 540],   contentX: 16, contentW: 508, headerBottom: 118,    reflows: true,  footerY: 486,     footerMode: "VERTICAL",   footerConstraintV: "MAX" },
  "static mobile 2":  { size: [540, 824],   contentX: 16, contentW: 508, headerBottom: 118,    reflows: true,  footerY: 770,     footerMode: "VERTICAL",   footerConstraintV: "MIN" },
  "static horizontal":{ size: [850, 638],   contentX: 16, contentW: 818, headerBottom: 118,    reflows: true,  footerY: 559,     footerMode: "VERTICAL",   footerConstraintV: "MIN" },
  "static vertical":  { size: [850, 1095],  contentX: 16, contentW: 818, headerBottom: 118,    reflows: true,  footerY: 1015.81, footerMode: "VERTICAL",   footerConstraintV: "MIN" },
  // The 302-wide pair carries no logo and its bottom-most auto-layout child is a source row, not a
  // footer — so `logo` and every footer field are skipped rather than expected.
  "small guided":     { size: [302, null],  contentX: 12, contentW: 278, headerBottom: 44,     reflows: true,  footerY: null,    footerMode: null,         footerConstraintV: null, noLogo: true },
  "small pull":       { size: [302, null],  contentX: 12, contentW: 278, headerBottom: 44,     reflows: true,  footerY: null,    footerMode: null,         footerConstraintV: null, noLogo: true },
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

// Compare against EXPECT. Tolerance 0.5px: the templates carry sub-pixel values (1015.81, 35.23)
// and a rounding difference is not drift.
const near = (a, b) => typeof a === "number" && typeof b === "number" && Math.abs(a - b) < 0.5;
const drifted = [];
for (const [label, e] of Object.entries(EXPECT)) {
  const g = out[label];
  if (!g) continue;
  if (g.missing) {
    g.verdict = "MISSING";
    drifted.push(label);
    continue;
  }
  const d = [];
  if (e.size) {
    if (!near(g.size[0], e.size[0])) d.push(`width ${g.size[0]} != ${e.size[0]}`);
    if (e.size[1] !== null && !near(g.size[1], e.size[1])) d.push(`height ${g.size[1]} != ${e.size[1]}`);
  }
  if (e.contentX !== null && !near(g.contentX, e.contentX)) d.push(`contentX ${g.contentX} != ${e.contentX}`);
  if (e.contentW !== null && !near(g.contentW, e.contentW)) d.push(`contentW ${g.contentW} != ${e.contentW}`);
  if (e.headerBottom !== null && !near(g.headerBottom, e.headerBottom)) d.push(`headerBottom ${g.headerBottom} != ${e.headerBottom}`);
  if (e.reflows !== null && g.headerSizing && g.headerSizing.reflows !== e.reflows) {
    d.push(`reflows ${g.headerSizing.reflows} != ${e.reflows} (primaryAxisSizingMode ${g.headerSizing.primaryAxisSizingMode})`);
  }
  if (e.footerY !== null) {
    if (!g.footer) d.push("footer not resolved");
    else {
      if (!near(g.footer.y, e.footerY)) d.push(`footer.y ${g.footer.y} != ${e.footerY}`);
      if (e.footerMode && g.footer.layoutMode !== e.footerMode) d.push(`footer.layoutMode ${g.footer.layoutMode} != ${e.footerMode}`);
      if (e.footerConstraintV && g.footer.constraintV !== e.footerConstraintV) d.push(`footer.constraintV ${g.footer.constraintV} != ${e.footerConstraintV}`);
    }
  }
  if (!e.noLogo && typeof g.logo === "string") d.push(g.logo);
  g.verdict = d.length ? "DRIFT" : "ok";
  if (d.length) {
    g.drift = d;
    drifted.push(label);
  }
}

return {
  summary: {
    expectationsVerified: VERIFIED,
    verdict: drifted.length
      ? `DRIFT on ${drifted.length} template(s) — STOP and report before cloning: ${drifted.join(", ")}`
      : "ok — every checked template matches the recorded geometry",
    drifted,
  },
  templates: out,
};
