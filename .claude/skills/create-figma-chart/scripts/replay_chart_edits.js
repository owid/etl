// replay_chart_edits.js — re-apply every chart-local edit after a re-import, in one `use_figma` call.
//
// A re-import replaces the chart group, and with it destroys every edit made on top: hidden
// connectors, trimmed geometry, unscaled stroke weights, the fit itself, a repositioned legend. Step 8
// says to expect a re-import "more than once per run" and to keep the edits as one scripted function
// you re-run rather than redoing them by hand. This is that function.
//
// Declarative on purpose: CONFIG says WHAT the frame needs, and the script owns the ORDER, which is
// where the real knowledge is. Every ordering constraint below cost a broken frame to learn:
//
//   1. hide            — before measuring anything. Connectors and year markers extend past the plot,
//                        so hiding them narrows the group and makes it relatively taller.
//   2. trim            — subpaths and over-long furniture, also before measuring.
//   3. fit             — ONE rescale() on whichever axis BINDS (CONFIG.bindAxis: the band's height
//                        normally, the content width for a map). rescale() scales type with geometry,
//                        so nothing rewraps.
//   4. width           — close the residual left by a height-bound fit. Only ever a STRETCH; see the
//                        direction rule below. A width-bound fit has no residual to close.
//   5. strokes         — LAST, because rescale() multiplies strokeWeight. Setting them before step 3
//                        is the single most repeated mistake in this skill's history.
//   6. legend + centre — after the geometry is final.
//
// Read-only unless CONFIG.dryRun is false. Run it with dryRun first and read the plan.

const CONFIG = {
  frameId: "26033:83",
  chartName: "chart",

  contentL: 16, // the content box, from the header (reference/NODE-MAP.md)
  contentW: 508,
  gap: 14, // per end, box-to-box (reference/FITTING.md — the box, not the ink)

  // Which axis binds the fit. "height" is right for a chart whose aspect the export solve controls, so
  // the height binds and the width step closes a sub-pixel remainder. A MAP breaks that premise: its
  // ink aspect is set by the projection, not by the canvas requested, so height-fitting it overflows
  // the content width by a measured 141px — Russia, Australia and the legend's last bin cut off at the
  // frame edge. reference/FITTING.md and per-chart-type/maps.md fit a map WIDTH-first and centre it in
  // the band, which leaves ~49px gaps by construction. Set this to "width" for any map.
  bindAxis: "height", // "height" | "width"

  // Names to hide. Strings match exactly; use a RegExp for a family. Hiding is reversible; deleting
  // is not, so this never deletes a node.
  hide: [],

  // Drop subpaths from ONE vector without hiding the whole node — the Hawaii case, where five ~1px
  // specks live inside the single `United-States` path and pin the map's left edge. `keepRightOf` is
  // in FRAME coordinates.
  trimSubpaths: [], // e.g. [{ node: "United-States", keepRightOf: 30 }]

  // Shorten furniture that grapher draws past the data — a single-entity stacked discrete bar's zero
  // line is 1.54x the bar height at every canvas size, so it hangs off the artboard after a fit.
  trimToExtentOf: [], // e.g. [{ node: "vertical-zero-line", matchExtentOf: "bars", axis: "y" }]

  furnitureWeight: 1, // CHECKS.md: gridlines, zero lines and ticks are all 1px. NOT grapher's value.
  seriesWeights: null, // { line: 3, outline: 4 } — only on a highlight treatment; null leaves them
  restoreDashes: true, // re-assign each node's pre-scale dashPattern (rescale multiplies it)

  // A map's legend is laid out for the map grapher exported, so it is wrong the moment the map moves.
  legend: null, // e.g. { name: /color-legend$/, plot: "map", gapToPlot: 16, centreOnContent: true }

  dryRun: true,
};

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 1000) / 1000);
const asMatcher = (x) => (x instanceof RegExp ? (n) => x.test(n) : (n) => n === x);
const plan = [];
const note = (step, what, detail) => plan.push({ step, what, detail });

const frame = await figma.getNodeByIdAsync(CONFIG.frameId);
if (!frame) throw new Error(`frameId ${CONFIG.frameId} not found`);
let page = frame;
while (page && page.type !== "PAGE") page = page.parent;
if (page && figma.currentPage !== page) await figma.setCurrentPageAsync(page);
const fb = frame.absoluteBoundingBox;

// Structural resolution, the same rule the sibling scripts use: the logo is a SIBLING of the header,
// and header/footer are the topmost and bottommost auto-layout children. Never name-based.
const logo = frame.children.find((c) => /^logo/i.test(c.name) || /^Logos\//.test(c.name)) || null;
const autos = frame.children.filter((c) => "layoutMode" in c && c.layoutMode !== "NONE" && c !== logo).sort((a, b) => a.y - b.y);
if (autos.length < 2) throw new Error("could not resolve a header and a footer (need two auto-layout children)");
const header = autos[0], footer = autos[autos.length - 1];
const headerB = header.absoluteBoundingBox.y + header.height;
// The band's bottom edge is the footer's TOPMOST INK, not its box origin: a footer child may sit above
// its own container's y (grapher's source row does), and `verify_page.js` and `measure_fit.js` both lift
// the edge by the most negative child offset. This script used the raw box origin, so it centred the
// chart against one edge while the two scripts that check it measured another — and because its own
// `symmetric` verdict reads the same edge it centred on, it certified its own disagreement. Three
// scripts, one rule.
const footerLift = footer.children && footer.children.length
  ? Math.min(0, Math.min(...footer.children.map((c) => c.y)))
  : 0;
const footerT = footer.absoluteBoundingBox.y + footerLift;
const bandH = footerT - headerB;
const targetH = bandH - 2 * CONFIG.gap;

const chart = frame.children.find((c) => c.name === CONFIG.chartName);
if (!chart) throw new Error(`no child named "${CONFIG.chartName}" — a re-import may have renamed it; check the frame's children`);
const findAll = (pred) => chart.findAll(pred);
const findOne = (pred) => chart.findOne(pred);

// ---------------------------------------------------------------- 1. hide
const hidden = [];
for (const spec of CONFIG.hide) {
  const match = asMatcher(spec);
  const found = findAll((n) => match(n.name));
  if (!found.length) { note("hide", String(spec), "NO MATCH — the export may have renamed it"); continue; }
  for (const n of found) {
    hidden.push(n.name);
    if (!CONFIG.dryRun) n.visible = false;
  }
  note("hide", String(spec), `${found.length} node(s)`);
}

// ---------------------------------------------------------------- 2. trim
for (const spec of CONFIG.trimSubpaths) {
  const node = findOne((n) => n.name === spec.node);
  if (!node || !node.vectorPaths || !node.vectorPaths.length) { note("trimSubpaths", spec.node, "not found, or carries no vectorPaths"); continue; }
  const bb0 = node.absoluteBoundingBox;
  const path0 = node.vectorPaths[0];
  const chunks = path0.data.split(/(?=[Mm])/).filter((c) => c.trim().length);
  const geom = chunks.map((c) => {
    const nums = (c.match(/-?\d*\.?\d+(?:e-?\d+)?/g) || []).map(Number);
    const xs = [];
    for (let i = 0; i + 1 < nums.length; i += 2) xs.push(nums[i]);
    return { c, lx0: Math.min(...xs), lx1: Math.max(...xs) };
  });
  const LX0 = Math.min(...geom.map((g) => g.lx0)), LX1 = Math.max(...geom.map((g) => g.lx1));
  const sx = bb0.width / (LX1 - LX0 || 1);
  const relL = (g) => bb0.x + (g.lx0 - LX0) * sx - fb.x;
  const kept = geom.filter((g) => relL(g) >= spec.keepRightOf);
  const dropped = geom.length - kept.length;
  if (!dropped) { note("trimSubpaths", spec.node, `nothing left of x=${spec.keepRightOf} — already trimmed`); continue; }
  if (kept.length < 3) { note("trimSubpaths", spec.node, `REFUSED: would drop ${dropped} of ${geom.length} subpaths`); continue; }
  const keepLeftRel = Math.min(...kept.map(relL));
  const keepTopRel = bb0.y - fb.y;
  if (!CONFIG.dryRun) {
    node.vectorPaths = [{ windingRule: path0.windingRule, data: kept.map((g) => g.c).join("") }];
    // Figma re-origins a vector when its geometry changes, so put the survivors back where they were
    node.x += (fb.x + keepLeftRel) - node.absoluteBoundingBox.x;
    node.y += (fb.y + keepTopRel) - node.absoluteBoundingBox.y;
  }
  note("trimSubpaths", spec.node, `dropped ${dropped} of ${geom.length} subpath(s) left of x=${spec.keepRightOf}`);
}

for (const spec of CONFIG.trimToExtentOf) {
  const node = findOne((n) => n.name === spec.node);
  const ref = findOne((n) => n.name === spec.matchExtentOf);
  if (!node || !ref) { note("trimToExtentOf", spec.node, `node or reference "${spec.matchExtentOf}" not found`); continue; }
  const nb = node.absoluteBoundingBox, rb = ref.absoluteBoundingBox;
  const axis = spec.axis === "x" ? "x" : "y";
  const before = axis === "y" ? [r(nb.y - fb.y), r(nb.y + nb.height - fb.y)] : [r(nb.x - fb.x), r(nb.x + nb.width - fb.x)];
  if (!CONFIG.dryRun) {
    // a zero-width vertical line cannot be resized to width 0; keep it at its own size on the other axis
    if (axis === "y") { node.resize(Math.max(node.width, 0.01), rb.height); node.y += rb.y - node.absoluteBoundingBox.y; }
    else { node.resize(rb.width, Math.max(node.height, 0.01)); node.x += rb.x - node.absoluteBoundingBox.x; }
  }
  note("trimToExtentOf", spec.node, `${axis} extent ${before.join("..")} -> that of "${spec.matchExtentOf}"`);
}

// ---------------------------------------------------------------- record pre-scale stroke state
// Captured AFTER hiding and trimming, BEFORE any scale — these values are what step 5 restores.
// Both identities travel DOWN the walk, because on some chart types neither is on the stroked node.
// On a SLOPE export `slope__<Entity>` and `outline__<Entity>` are GROUPS of {start-point, end-point,
// line} and the only stroked node is called plain `line` — so matching the leaf name finds no series at
// all, `CONFIG.seriesWeights` is silently not applied, and the script reports a completed replay over
// grapher's own weights. verify_page.js carries the naming ancestor for exactly this reason. The
// furniture container is carried for the same reason: checking only the immediate parent misses a
// gridline nested more than one level under its container, which is then left un-un-thinned.
const strokeState = [];
const SERIES_ANY = /^(line|slope|outline)__/;
const FURNITURE_ANY = /grid|axis|tick|zero-line/i;
(function collect(n, seriesOf, furnitureGroup) {
  if (n.visible === false) return;
  const sm = SERIES_ANY.exec(n.name);
  if (sm) seriesOf = sm[1];
  if (FURNITURE_ANY.test(n.name)) furnitureGroup = n.name;
  if ("strokeWeight" in n && typeof n.strokeWeight === "number" && n.strokes && n.strokes.length) {
    strokeState.push({ node: n, name: n.name, w: n.strokeWeight, dash: n.dashPattern ? [...n.dashPattern] : [],
                       seriesKind: seriesOf || null, furnitureGroup: furnitureGroup || null });
  }
  if ("children" in n) n.children.forEach((c) => collect(c, seriesOf, furnitureGroup));
})(chart, null, null);

// ---------------------------------------------------------------- 3. fit, and 4. width
const box0 = chart.absoluteBoundingBox;
// Fit on whichever axis BINDS. Height-first is correct only because the export solve guarantees the
// aspect; where the aspect is not ours to set — a map — the axis that would overflow is the one to fit.
// Absent means the height, which is what every non-map chart wants; a value that is neither throws,
// because a typo silently fitting the wrong axis is the overflow this option exists to prevent.
const bindAxis = CONFIG.bindAxis == null ? "height" : CONFIG.bindAxis;
if (bindAxis !== "height" && bindAxis !== "width") throw new Error(`CONFIG.bindAxis must be "height" or "width", got ${JSON.stringify(CONFIG.bindAxis)}`);
const widthBound = bindAxis === "width";
const fitFactor = widthBound ? CONFIG.contentW / box0.width : targetH / box0.height;
// A rewrap is a change in LINE COUNT, not in height. `rescale` scales every text height in proportion,
// so comparing raw heights reports a rewrap on every uniform scale — which made this warning fire on
// a run where nothing had rewrapped at all. height/fontSize is invariant under rescale and changes
// only when the text actually re-flows.
const textLines = () => { const o = []; (function tw(n) { if (n.type === "TEXT" && typeof n.fontSize === "number" && n.fontSize > 0) o.push(Math.round((n.height / n.fontSize) * 20) / 20); if ("children" in n) n.children.forEach(tw); })(chart); return o; };
const hBefore = textLines();
let widthNote = "not attempted (dryRun)";
let heightNote = "not attempted (dryRun)";
if (!CONFIG.dryRun) {
  chart.rescale(fitFactor);
  if (widthBound) {
    // The one rescale already put the box on the content width, so there is no residual to close — and
    // the height is deliberately left where the projection puts it. Never squeeze it into the band:
    // maps.md centres the map and accepts the larger gaps. Report an overflow rather than hiding it.
    widthNote = `width-bound fit x${r(fitFactor)} — the box spans the content width at ${r(chart.width)}; no second rescale`;
    const over = chart.height - bandH;
    heightNote = over > 0.5
      ? `OVERFLOWS the band by ${r(over)}px (${r(chart.height)} into ${r(bandH)}) — re-export at a flatter aspect; never squeeze one axis`
      : `${r(chart.height)} into a ${r(bandH)} band, centred (a map carries larger gaps by construction)`;
  } else {
    // close the width residual. A STRETCH is free; a SQUEEZE rewraps labels (measured: 0.999x rewrapped
    // 5 of 80 on a scatter, 0.99x rewrapped 78). If the ink is wider than the box, re-export.
    for (let i = 0; i < 3; i++) {
      const w = chart.width;
      if (Math.abs(w - CONFIG.contentW) < 0.005) break;
      const f = CONFIG.contentW / w;
      if (f < 0.995) { widthNote = `REFUSED: would squeeze x${r(f)} and rewrap labels; left at ${r(w)}. Re-export shorter instead.`; break; }
      chart.rescale(f); // uniform, so type scales with the box and nothing rewraps
      widthNote = `closed by uniform rescale to ${r(chart.width)}`;
    }
    heightNote = `height-bound fit x${r(fitFactor)} onto a ${r(targetH)} target`;
  }
}
const hAfter = CONFIG.dryRun ? hBefore : textLines();
const rewrapped = hBefore.filter((h, i) => h !== hAfter[i]).length;

// ---------------------------------------------------------------- 5. strokes — after the LAST scale
const strokeFixes = [];
for (const s of strokeState) {
  // A node we can name as a SERIES is never furniture, whatever container it sits in: the identity on
  // its own group is more specific than a container name inherited from an ancestor.
  const seriesKind = s.seriesKind;
  const isFurniture = !seriesKind &&
                      (/gridline|grid-lines|zero-line|^ticks?$|^tick|^axis/i.test(s.name) ||
                       (s.furnitureGroup !== null && FURNITURE_ANY.test(s.furnitureGroup)));
  // `why` is decided by the branch actually taken, not re-derived afterwards: read off `seriesKind`
  // alone it labelled a series left at grapher's own weight (because no seriesWeights were configured)
  // as "house series weight", which is the one thing that did not happen.
  let want = null, why = "";
  if (isFurniture) { want = CONFIG.furnitureWeight; why = "furniture 1px"; }
  else if (seriesKind && CONFIG.seriesWeights) { want = seriesKind === "outline" ? CONFIG.seriesWeights.outline : CONFIG.seriesWeights.line; why = "house series weight"; }
  else { want = s.w; why = "un-thin to grapher's own"; } // leave grapher's own weight, un-thinned
  if (want !== null && Math.abs((CONFIG.dryRun ? s.w : s.node.strokeWeight) - want) > 0.01) {
    strokeFixes.push({ name: s.name, now: r(CONFIG.dryRun ? s.w : s.node.strokeWeight), to: r(want), why });
    if (!CONFIG.dryRun) s.node.strokeWeight = want;
  }
  // Only re-dash a node that HAD a dash: assigning one to a solid zero line or tick restyles the
  // furniture instead of unscaling it (CHECKS.md — the dash target is per node type).
  if (CONFIG.restoreDashes && s.dash.length && !CONFIG.dryRun) s.node.dashPattern = s.dash;
}

// ---------------------------------------------------------------- 6. legend, then centre
let legendNote = null;
if (CONFIG.legend) {
  const match = asMatcher(CONFIG.legend.name);
  const legend = findOne((n) => match(n.name));
  const plot = CONFIG.legend.plot ? findOne((n) => n.name === CONFIG.legend.plot) : null;
  if (!legend) legendNote = "legend not found";
  else if (CONFIG.legend.plot && !plot) legendNote = `plot "${CONFIG.legend.plot}" not found`;
  else {
    const lb = legend.absoluteBoundingBox, pb = plot ? plot.absoluteBoundingBox : null;
    if (!CONFIG.dryRun) {
      if (CONFIG.legend.centreOnContent) legend.x += (fb.x + CONFIG.contentL + (CONFIG.contentW - lb.width) / 2) - lb.x;
      if (pb && CONFIG.legend.gapToPlot != null) legend.y += (pb.y + pb.height + CONFIG.legend.gapToPlot) - legend.absoluteBoundingBox.y;
    }
    legendNote = `centred on the content box${pb ? ` and tucked ${CONFIG.legend.gapToPlot}px under "${CONFIG.legend.plot}"` : ""}`;
  }
}

if (!CONFIG.dryRun) {
  chart.x += (fb.x + CONFIG.contentL) - chart.absoluteBoundingBox.x;
  const bb = chart.absoluteBoundingBox;
  chart.y += (headerB + (footerT - headerB - bb.height) / 2) - bb.y;
}

const b = chart.absoluteBoundingBox;
const gapAbove = b.y - headerB, gapBelow = footerT - (b.y + b.height);
const symmetric = Math.abs(gapAbove - gapBelow) < 0.005;
const edgesExact = Math.abs(b.x - fb.x - CONFIG.contentL) < 0.005 && Math.abs(b.x + b.width - fb.x - (CONFIG.contentL + CONFIG.contentW)) < 0.005;

// Every way this run can come out incomplete has to reach the VERDICT, not just a nested note. A
// refused squeeze leaves the chart overflowing the content box and `edgesExact` false, and the verdict
// still said "wrote every edit" whenever no text happened to rewrap — a prominent success line over a
// broken layout, which the caller has to go digging in `fit.widthNote` to disbelieve.
// And an edit that never got APPLIED is the other half. The whole point of this script is to restore
// the chart-local edits a re-import destroys, and a re-import is exactly when a configured name stops
// matching — so "hide: NO MATCH", a trim target that is not there, a refused subpath cut and an
// unresolvable legend are the expected failures, not exotic ones. They were recorded in `plan` only, so
// a run whose geometry happened to land could still return "wrote every edit" with an edit missing.
const problems = [];
if (!CONFIG.dryRun) {
  const unapplied = plan.filter((p) => /NO MATCH|REFUSED|not found|carries no vectorPaths/.test(p.detail));
  if (unapplied.length) problems.push(`${unapplied.length} configured edit(s) were NOT applied: ` +
    unapplied.map((p) => `${p.step} "${p.what}" — ${p.detail}`).join("; "));
  if (legendNote && /not found/.test(legendNote)) problems.push(`the legend was configured but could not be resolved (${legendNote})`);
  if (rewrapped) problems.push(`${rewrapped} text node(s) changed height — a label rewrapped`);
  if (/^REFUSED/.test(widthNote)) problems.push(`the width fit was refused (${widthNote})`);
  if (/^OVERFLOWS/.test(heightNote)) problems.push(heightNote.replace(/^OVERFLOWS/, "the chart overflows"));
  if (!edgesExact) problems.push(`the box does not sit on the content box (L ${r(b.x - fb.x)}, R ${r(b.x + b.width - fb.x)}, want ${CONFIG.contentL}..${CONFIG.contentL + CONFIG.contentW})`);
  if (!symmetric) problems.push(`the band gaps are not symmetric (${r(gapAbove)} above, ${r(gapBelow)} below)`);
}
return {
  dryRun: CONFIG.dryRun,
  frame: { id: frame.id, name: frame.name },
  band: { headerBottom: r(headerB - fb.y), footerTop: r(footerT - fb.y), h: r(bandH), targetChartH: r(targetH) },
  plan,
  hiddenCount: hidden.length,
  fit: { bindAxis, factor: r(fitFactor), widthNote, heightNote, rewrappedTextNodes: rewrapped },
  strokeFixes,
  legendNote,
  result: {
    box: { L: r(b.x - fb.x), R: r(b.x + b.width - fb.x), T: r(b.y - fb.y), B: r(b.y + b.height - fb.y), w: r(b.width) },
    gapAbove: r(gapAbove), gapBelow: r(gapBelow),
    symmetric,
    edgesExact,
    problems,
  },
  verdict: CONFIG.dryRun
    ? "DRY RUN — nothing was written. Read `plan`, then set CONFIG.dryRun = false."
    : problems.length
      ? `INCOMPLETE — ${problems.join("; ")}. The edits were written, but the layout is not finished; fix these before checking the frame off.`
      : "wrote every edit; no text rewrapped, the box is on the content edges and the band gaps are symmetric",
};
