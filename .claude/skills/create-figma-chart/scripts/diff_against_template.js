// diff_against_template.js — check a finished clone BACK against the template it came from.
//
// The workflow is start from the template, modify it, and check back with the template again. That
// last step is the one that gets skipped, and skipping it is how a page accumulates drift nobody
// chose: on one run of eight frames it hid a source line whose text style had been silently detached
// by assigning `characters`, a source dropped to 13px, and a zero line whose dash had been forced to
// the gridline target. Every one of those renders plausibly and survives a screenshot.
//
// Read-only. It fingerprints the TEMPLATE at runtime and diffs each clone against it, so it works for
// any of the templates rather than the numbers of whichever one it was written on. Text CONTENT is
// excluded on purpose — that is what a run is meant to change. Everything else is the template's law.
//
// USAGE
//   Fill in CONFIG and paste the whole file as one `use_figma` call.
//     templateId — the template the clones came from (reference/NODE-MAP.md has the ids).
//     frameIds   — the finished clones. All must be on ONE page: a script may switch pages only once
//                  (GOTCHAS.md), and this one spends its single switch on the CLONES' page.
//
// RUN IT WITH THE TEMPLATE'S PAGE OPEN. That is a precondition, not a detail. Both halves need their
// page current — an unswitched page's contents load lazily and come back short without erroring
// (GOTCHAS.md) — and the connector allows exactly one `setCurrentPageAsync` per call. Starting on the
// template's page costs zero switches to fingerprint it and leaves the one switch for the clones;
// starting anywhere else needs two, and the second throws, so the script says so up front instead.
//     expected   — drift you have decided on, as an array of substrings. A footer converted to
//                  VERTICAL to give a long source its own row is a deliberate change, not a defect, so
//                  it belongs here and is reported as `accepted` instead of `DRIFT`. Anything not
//                  listed is drift you did not choose.
//
// Two readings this deliberately gets right, because both were wrong in an earlier hand-rolled pass:
//   - `textStyleId` is `figma.mixed` (a SYMBOL) on any node with a per-range style override. A
//     truthiness test calls that "bound" and a `=== id` test calls it "unbound"; both are wrong. This
//     reports the per-segment styles so a half-bound node is visible as exactly that.
//   - The bold `Data source:` prefix CANNOT be both bold and style-bound through the plugin API —
//     `setRangeFontName` clears the range's binding, and re-binding the range clears the bold. The
//     template's own state is reachable in the UI only. So `sourceHalfBound` is called out separately
//     from real drift; see reference/TEXTS.md.

const CONFIG = {
  templateId: "6799:1859",
  frameIds: ["26033:6"],
  expected: ["footer mode VERTICAL"],
};

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);
const hex = (p) =>
  p && p.type === "SOLID"
    ? "#" + [p.color.r, p.color.g, p.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("")
    : p
      ? p.type
      : null;
const sid = (v) => (typeof v === "string" ? v || "(unbound)" : String(v));
const firstFill = (n) => (Array.isArray(n.fills) ? hex(n.fills[0]) : null);

// Structural resolution, the same rule verify_templates.js and measure_fit.js use: the logo is a
// SIBLING of the header, and header/footer are the topmost and bottommost auto-layout children.
const parts = (frame) => {
  const logo = frame.children.find((c) => /^logo/i.test(c.name) || /^Logos\//.test(c.name)) || null;
  const autos = frame.children
    .filter((c) => "layoutMode" in c && c.layoutMode !== "NONE" && c !== logo)
    .sort((a, b) => a.y - b.y);
  return { logo, header: autos[0] || null, footer: autos.length > 1 ? autos[autos.length - 1] : null };
};

const textRow = (n) => ({
  size: n.fontSize,
  style: sid(n.textStyleId),
  segStyles: n.getStyledTextSegments(["textStyleId"]).map((s) => sid(s.textStyleId)),
  segFonts: n.getStyledTextSegments(["fontName"]).map((s) => s.fontName.style),
  fill: firstFill(n),
  autoResize: n.textAutoResize,
  lsH: n.layoutSizingHorizontal,
  lsV: n.layoutSizingVertical,
  w: r(n.width),
});

const fingerprint = (frame) => {
  const { logo, header, footer } = parts(frame);
  return {
    size: [r(frame.width), r(frame.height)],
    fill: firstFill(frame),
    header: header
      ? { mode: header.layoutMode, sizing: header.primaryAxisSizingMode, spacing: header.itemSpacing,
          x: r(header.x), w: r(header.width), rows: header.children.map((c) => (c.type === "TEXT" ? textRow(c) : { nonText: c.type })) }
      : null,
    footer: footer
      ? { mode: footer.layoutMode, sizing: footer.primaryAxisSizingMode, spacing: footer.itemSpacing,
          align: footer.primaryAxisAlignItems, counterAlign: footer.counterAxisAlignItems,
          x: r(footer.x), w: r(footer.width), rowCount: footer.children.length,
          constraintV: footer.constraints.vertical, bottom: r(footer.y + footer.height),
          rows: footer.children.map((c) => (c.type === "TEXT" ? textRow(c) : { nonText: c.type })) }
      : null,
    logo: logo ? { x: r(logo.x), y: r(logo.y), w: r(logo.width), h: r(logo.height) } : null,
  };
};

// --- the template's own fingerprint. NO page switch: the template's page must already be current.
const tpl = await figma.getNodeByIdAsync(CONFIG.templateId);
if (!tpl) throw new Error(`templateId ${CONFIG.templateId} not found — new yearly file? ask for the link`);
let tplPage = tpl;
while (tplPage && tplPage.type !== "PAGE") tplPage = tplPage.parent;
if (tplPage && figma.currentPage !== tplPage)
  throw new Error(
    `open the template's page ("${tplPage.name}") in Figma, then re-run. Currently on "${figma.currentPage.name}". ` +
    `Only ONE setCurrentPageAsync is allowed per call and this one is spent on the clones' page, so the template ` +
    `must already be current — a fingerprint read off an unswitched page comes back short without erroring. ` +
    `(If the clones are on the template's own page, no switch is needed at all.)`);
const T = fingerprint(tpl);

// --- the clones. The one and only page switch.
const first = await figma.getNodeByIdAsync(CONFIG.frameIds[0]);
if (!first) throw new Error(`frameIds[0] ${CONFIG.frameIds[0]} not found`);
let clonePage = first;
while (clonePage && clonePage.type !== "PAGE") clonePage = clonePage.parent;
if (clonePage && figma.currentPage !== clonePage) await figma.setCurrentPageAsync(clonePage);

const cmpText = (label, got, want, push) => {
  if (!got || !want) return;
  if (got.size !== want.size) push(`${label} size ${got.size} != ${want.size}`);
  if (got.fill !== want.fill) push(`${label} fill ${got.fill} != ${want.fill}`);
  if (got.autoResize !== want.autoResize) push(`${label} textAutoResize ${got.autoResize} != ${want.autoResize}`);
  if (got.lsH !== want.lsH) push(`${label} layoutSizingHorizontal ${got.lsH} != ${want.lsH}`);
  if (got.lsV !== want.lsV) push(`${label} layoutSizingVertical ${got.lsV} != ${want.lsV}`);
  if (got.segFonts.join("+") !== want.segFonts.join("+")) push(`${label} weights ${got.segFonts.join("+")} != ${want.segFonts.join("+")}`);
  // width is only law where the template FIXES it; a FILL row's width follows its parent
  if (want.lsH === "FIXED" && got.w !== want.w) push(`${label} width ${got.w} != ${want.w}`);
  if (got.style !== want.style) {
    const halfBound = got.style === "Symbol(figma.mixed)" && want.segStyles.length && got.segStyles.some((s) => s !== "(unbound)");
    push(`${label} style ${got.style.startsWith("S:") ? got.style.slice(0, 16) + "…" : got.style} != template` +
         (halfBound ? "  [half-bound: a range override cleared the binding — expected for a bolded prefix, see TEXTS.md]" : ""),
         halfBound);
  }
};

const results = [];
for (const id of CONFIG.frameIds) {
  const frame = await figma.getNodeByIdAsync(id);
  if (!frame) { results.push({ frame: id, error: "not found" }); continue; }
  const G = fingerprint(frame);
  const drift = [], accepted = [], halfBound = [];
  const push = (msg, isHalfBound) => {
    if (isHalfBound) { halfBound.push(msg); return; }
    (CONFIG.expected.some((e) => msg.indexOf(e) !== -1) ? accepted : drift).push(msg);
  };

  if (String(G.size) !== String(T.size)) push(`frame size ${G.size} != ${T.size}`);
  if (G.fill !== T.fill) push(`frame fill ${G.fill} != ${T.fill}`);
  if (!!G.logo !== !!T.logo) push("logo present/absent differs");
  else if (G.logo && (G.logo.x !== T.logo.x || G.logo.y !== T.logo.y || G.logo.w !== T.logo.w)) push(`logo moved to ${G.logo.x}/${G.logo.y}`);

  for (const key of ["header", "footer"]) {
    const g = G[key], w = T[key];
    if (!g !== !w) { push(`${key} present/absent differs`); continue; }
    if (!g) continue;
    if (g.mode !== w.mode) push(`${key} mode ${g.mode} != ${w.mode}`);
    if (g.x !== w.x || g.w !== w.w) push(`${key} box ${g.x}/${g.w} != ${w.x}/${w.w}`);
    if (g.rowCount !== undefined && g.rowCount !== w.rowCount) push(`${key} rowCount ${g.rowCount} != ${w.rowCount}`);
    // The header's sizing mode is load-bearing: FIXED stops the band tracking the copy (NODE-MAP.md).
    if (key === "header" && g.sizing !== w.sizing) push(`header sizing ${g.sizing} != ${w.sizing} — the band stops tracking the copy`);
    if (key === "header" && g.spacing !== w.spacing) push(`header itemSpacing ${g.spacing} != ${w.spacing}`);
    // A footer that grew a row legitimately moves; what must hold is its BOTTOM edge.
    if (key === "footer" && Math.abs(g.bottom - w.bottom) > 0.5) push(`footer bottom ${g.bottom} != ${w.bottom} — re-pin it, MIN constraints grow off the artboard`);
    const n = Math.min(g.rows.length, w.rows.length);
    for (let i = 0; i < n; i++) cmpText(`${key}[${i}]`, g.rows[i], w.rows[i], push);
  }

  results.push({
    frame: frame.name, id: frame.id,
    verdict: drift.length ? `DRIFT on ${drift.length}` : "matches the template",
    drift, accepted, halfBound,
  });
}

const totalDrift = results.reduce((s, x) => s + (x.drift ? x.drift.length : 0), 0);
return {
  template: { id: CONFIG.templateId, name: tpl.name, size: T.size, fill: T.fill },
  verdict: totalDrift ? `${totalDrift} unintended difference(s) across ${results.length} frame(s)` : `all ${results.length} frame(s) match the template`,
  note: "text CONTENT and any added chart are excluded by design; `accepted` is drift declared in CONFIG.expected; `halfBound` is the API limitation on a bolded prefix, not a defect",
  frames: results,
};
