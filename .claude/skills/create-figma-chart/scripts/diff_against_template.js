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
//     expected   — drift you have decided on, as an array of substrings. A footer converted to
//                  VERTICAL to give a long source its own row is a deliberate change, not a defect, so
//                  it belongs here and is reported as `accepted` instead of `DRIFT`. Anything not
//                  listed is drift you did not choose.
//
// RUN IT WITH THE TEMPLATE'S PAGE OPEN. That is a precondition, not a detail. Both halves need their
// page current — an unswitched page's contents load lazily and come back short without erroring
// (GOTCHAS.md) — and the connector allows exactly one `setCurrentPageAsync` per call. Starting on the
// template's page costs zero switches to fingerprint it and leaves the one switch for the clones;
// starting anywhere else needs two, and the second throws, so the script says so up front instead.
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

// Segments are captured WITH their character offsets. `getStyledTextSegments` merges adjacent equal
// values, so the style runs and the font runs of one node do not line up one-to-one — a fully bound row
// with a bold prefix is ONE style run and TWO font runs. Without `start`/`end` there is no way to tell
// "the bolded prefix lost its binding" (the documented API limitation) from "a regular range lost its
// binding" (a real defect): both read as one unbound run next to one bound run.
const segs = (n, field, read) => n.getStyledTextSegments([field]).map((s) => ({ v: read(s), start: s.start, end: s.end }));
const vals = (list) => list.map((s) => s.v);

const textRow = (n) => ({
  size: n.fontSize,
  style: sid(n.textStyleId),
  segStyles: segs(n, "textStyleId", (s) => sid(s.textStyleId)),
  segFonts: segs(n, "fontName", (s) => s.fontName.style),
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
          align: header.primaryAxisAlignItems, counterAlign: header.counterAxisAlignItems,
          x: r(header.x), w: r(header.width), rowCount: header.children.length,
          rows: header.children.map((c) => (c.type === "TEXT" ? textRow(c) : { nonText: c.type })) }
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
  // A row can change TYPE — a designer replaces the subtitle with a group, or a text row becomes an
  // image. That is drift worth naming, and reading the text fields across it threw
  // `Cannot read properties of undefined (reading 'join')` on `segFonts`, killing the whole diff on
  // one swapped row rather than reporting it.
  if (got.nonText || want.nonText) {
    if (got.nonText !== want.nonText) push(`${label} node type ${got.nonText || "TEXT"} != ${want.nonText || "TEXT"}`);
    return;
  }
  if (got.size !== want.size) push(`${label} size ${got.size} != ${want.size}`);
  if (got.fill !== want.fill) push(`${label} fill ${got.fill} != ${want.fill}`);
  if (got.autoResize !== want.autoResize) push(`${label} textAutoResize ${got.autoResize} != ${want.autoResize}`);
  if (got.lsH !== want.lsH) push(`${label} layoutSizingHorizontal ${got.lsH} != ${want.lsH}`);
  if (got.lsV !== want.lsV) push(`${label} layoutSizingVertical ${got.lsV} != ${want.lsV}`);
  if (vals(got.segFonts).join("+") !== vals(want.segFonts).join("+")) push(`${label} weights ${vals(got.segFonts).join("+")} != ${vals(want.segFonts).join("+")}`);
  // width is only law where the template FIXES it; a FILL row's width follows its parent
  if (want.lsH === "FIXED" && got.w !== want.w) push(`${label} width ${got.w} != ${want.w}`);
  // Style bindings are compared PER SEGMENT, never by the node-level value alone. `textStyleId` is
  // `figma.mixed` on any node with a range override, so two nodes with different per-range bindings both
  // stringify to "Symbol(figma.mixed)" and a node-level compare finds them equal — which let a clone
  // whose bold prefix had come unbound while the rest stayed bound report as matching the template. That
  // is the exact half-bound state this script exists to expose.
  const gseg = vals(got.segStyles).join(" | "), wseg = vals(want.segStyles).join(" | ");
  if (got.style !== want.style || gseg !== wseg) {
    // The ONE acceptable difference: a bolded range cannot be both bold and style-bound through the
    // plugin API (TEXTS.md). Decided by CHARACTER RANGE, not by segment index — an unbound style run
    // counts as the documented limitation only when the characters it covers are entirely non-Regular.
    // A REGULAR range that lost its binding is the real defect (assigning `characters` detaches the
    // style), and by index alone the two are indistinguishable.
    const boldAt = (start, end) => got.segFonts.some((f) => f.v !== "Regular" && f.start <= start && f.end >= end);
    const wantIds = vals(want.segStyles).filter((v) => v !== "(unbound)");
    const gotUnbound = got.segStyles.filter((s) => s.v === "(unbound)");
    const halfBound =
      wantIds.length > 0 && !vals(want.segStyles).includes("(unbound)") &&   // the template is fully bound
      gotUnbound.length > 0 &&                                                // the clone has lost some binding
      gotUnbound.every((s) => boldAt(s.start, s.end)) &&                      // …only over bolded characters
      got.segStyles.every((s) => s.v === "(unbound)" || wantIds.includes(s.v)); // and the rest is the template's own style
    const shown = (v) => (v.startsWith("S:") ? v.slice(0, 16) + "…" : v);
    const range = (s) => `${shown(s.v)}@${s.start}-${s.end}`;
    push(`${label} style ${shown(got.style)} != template${gseg !== wseg ? `; segments [${got.segStyles.map(range).join(", ")}] != [${want.segStyles.map(range).join(", ")}]` : ""}` +
         (halfBound ? "  [half-bound: a bolded range cannot also be style-bound — expected, see TEXTS.md]" : ""),
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
  // Height included: a logo resized vertically while keeping its x, y and width is a DISTORTED logo, and
  // leaving `h` out of the comparison reported that as matching the template.
  else if (G.logo && (G.logo.x !== T.logo.x || G.logo.y !== T.logo.y || G.logo.w !== T.logo.w || G.logo.h !== T.logo.h))
    push(`logo ${G.logo.w}x${G.logo.h} at ${G.logo.x}/${G.logo.y} != ${T.logo.w}x${T.logo.h} at ${T.logo.x}/${T.logo.y}`);

  for (const key of ["header", "footer"]) {
    const g = G[key], w = T[key];
    if (!g !== !w) { push(`${key} present/absent differs`); continue; }
    if (!g) continue;
    if (g.mode !== w.mode) push(`${key} mode ${g.mode} != ${w.mode}`);
    if (g.x !== w.x || g.w !== w.w) push(`${key} box ${g.x}/${g.w} != ${w.x}/${w.w}`);
    // Row count is compared on BOTH bands. Guarding this on `rowCount !== undefined` while only the
    // footer carried the field meant a header that LOST a row — a clone shipped with no subtitle at all
    // — was reported as matching the template: the row loop below only walks the overlap.
    if (g.rowCount !== w.rowCount) push(`${key} rowCount ${g.rowCount} != ${w.rowCount}`);
    // Every auto-layout property fingerprinted here is compared. Collecting one and not diffing it is a
    // check that cannot fire, and it reads as coverage: a footer switched to FIXED sizing, re-aligned,
    // re-spaced and re-constrained came back "matches the template" on all four.
    if (g.sizing !== w.sizing) push(`${key} sizing ${g.sizing} != ${w.sizing}` + (key === "header" ? " — the band stops tracking the copy (NODE-MAP.md)" : " — the band stops tracking its text"));
    if (g.spacing !== w.spacing) push(`${key} itemSpacing ${g.spacing} != ${w.spacing}`);
    if (g.align !== w.align) push(`${key} primaryAxisAlignItems ${g.align} != ${w.align}`);
    if (g.counterAlign !== w.counterAlign) push(`${key} counterAxisAlignItems ${g.counterAlign} != ${w.counterAlign}`);
    if (key === "footer" && g.constraintV !== w.constraintV) push(`footer vertical constraint ${g.constraintV} != ${w.constraintV} — MIN keeps the TOP edge, so the band grows off the artboard (GOTCHAS.md)`);
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
// A frame that could not be resolved is an UNCHECKED deliverable, and it must never sit behind a verdict
// that says everything matches. A stale id is the normal way this happens — a re-import replaces the
// group and the id captured earlier returns null (GOTCHAS.md) — so it is the likely case, not the exotic
// one, and counting only drift let it pass as a clean run.
const missing = results.filter((x) => x.error);
return {
  template: { id: CONFIG.templateId, name: tpl.name, size: T.size, fill: T.fill },
  verdict: (totalDrift ? `${totalDrift} unintended difference(s) across ${results.length} frame(s)` : `all ${results.length - missing.length} resolved frame(s) match the template`) +
           (missing.length ? ` — ${missing.length} of ${results.length} frame(s) NOT CHECKED, id not found: ${missing.map((x) => x.frame).join(", ")}. Resolve the clone by NAME, not by a captured id (GOTCHAS.md).` : ""),
  unchecked: missing.length,
  note: "text CONTENT and any added chart are excluded by design; `accepted` is drift declared in CONFIG.expected; `halfBound` is the API limitation on a bolded prefix, not a defect",
  frames: results,
};
