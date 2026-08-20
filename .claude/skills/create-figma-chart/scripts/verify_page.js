// verify_page.js — the MECHANICAL rows of Step 8c, in one read-only `use_figma` call.
//
// reference/CHECKS.md is the gate before anyone sees a frame, and most of its rows are property
// reads of a single page: font sizes, stroke weights, dash patterns, gaps, box alignment, margins,
// bound-vs-raw fills, annotation knockouts. Run one at a time that is a dozen MCP round trips at
// ~8-10s each; run as one traversal it is one. This is the same move `verify_templates.js` makes for
// the template geometry, applied to a finished page.
//
// Read-only. It sets no property and creates no node.
//
// USAGE
//   Fill in CONFIG and paste the whole file as one `use_figma` call.
//     frameId       — the finished frame (the template clone, after Step 8).
//     frameIds      — optional array, for a chart-rows SET: every frame is checked and the result is
//                     one entry per frame. All of them must be on the SAME page, since a script may
//                     switch pages only once.
//     chartName     — the chart group's name. Resolved by NAME, never by a captured id: a re-import
//                     replaces the group and a stale id returns null (reference/GOTCHAS.md). If a
//                     designer has ungrouped it, pass null and the plot subgroups are used instead.
//     gapTarget     — [min, max] px per end. 12-16 on the 540-wide frames, 30 on the IG portrait.
//                     Left null it is derived from the frame width, which is right for the nine
//                     in-scope templates and wrong for anything else — so pass it when you know it.
//     tightlyMeasured — set true for an axis-less chart whose furniture was trimmed and label boxes
//                     hugged (Step 8). CHECKS.md's gap row does not apply as written there; the row
//                     reports SKIPPED with the reason rather than failing a correct chart.
//     highlightTreatment — true when the chart uses the muting-gray highlight treatment, which
//                     changes the mark-weight bar (context 1px, protagonist 3px, halo 2x), makes
//                     the muting grays a standing palette exception, and makes a muted CONTEXT line
//                     legal to cross with an annotation (CHECKS.md allows exactly that).
//     xlAnnotations — true when a lead annotation deliberately carries the message and takes
//                     Annotation XL 16, level with the subtitle. That is the documented ceiling, spent
//                     only when the annotation IS the message — so it has to be declared rather than
//                     inferred, and left false a 16px annotation is a defect.
//     textFloor     — px. Left null it is derived from the frame width: the 302-wide small and pull
//                     templates run an 11px floor, because their own subtitle, source and year text
//                     is 11px by design (SMALL-CHARTS.md overrides it) — a pull chart ALWAYS carries
//                     an 11px source line, so the 12px bar reports every one of them as broken.
//
// WHAT IT DOES NOT COVER, and never silently passes: every row it cannot judge is returned as
// SKIPPED with the reason and the tool that owns it. Colour-vision and grayscale seams are
// `scripts/color_audit.py`; spelling is `codespell`; "the text is true of the indicator" is
// `/adversarial-data-review`; the entity-completeness row needs the EFFECTIVE selection from
// outside Figma (Step 1's table); the arrow and leader-on-map rows need rendered pixels (CHECKS.md's
// four-render protocol). A check that cannot fail is worse than no check, so those are reported as
// gaps in coverage, not as passes.

const CONFIG = {
  frameId: "26000:6",
  frameIds: null,
  chartName: "chart",
  gapTarget: null,
  tightlyMeasured: false,
  highlightTreatment: false,
  textFloor: null,
  xlAnnotations: false,
};

const LADDER_FULL = [12, 13, 14, 15, 16]; // Annotation XS..XL; the ceiling is L 15, XL only when
const LADDER_CEILING = 15;                // the annotation IS the message (GUIDELINES.md)
const SMALL_FRAME_W = 302;                // the small/pull templates, whose floor is 11 not 12
const HOUSE_LINE = 3, HOUSE_HALO = 4;
const FURNITURE_W = 1, FURNITURE_DASH = [4, 4];
const BLOCK_CLEARANCE = 27;          // annotation block vs header/footer on the 540x540 pages
const GRAPHER_RESIDUAL = "#585c64";  // emitted for residual categories; in no library group

const r = (v) => (v === null || v === undefined ? null : Math.round(v * 100) / 100);
// WCAG relative luminance and contrast, so the "direct labels readable as text" row is computed
// rather than declared unchecked. Pure function of two hexes; no geometry needed.
const lin = (c) => (c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4));
const lum = (hex) => { const n = parseInt(hex.slice(1), 16); const R = ((n >> 16) & 255) / 255, G = ((n >> 8) & 255) / 255, B = (n & 255) / 255;
  return 0.2126 * lin(R) + 0.7152 * lin(G) + 0.0722 * lin(B); };
const contrast = (a, b) => { const la = lum(a), lb = lum(b); return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05); };
let rows = [];
const add = (name, status, detail, extra) => rows.push({ check: name, status, detail, ...(extra || {}) });
const skip = (name, why, owner) => add(name, "SKIPPED", why, owner ? { ownedBy: owner } : null);

// One frame's worth of checking. Split out so a CHART-ROWS SET — 3-5 frames that ship as one
// deliverable (SMALL-CHARTS.md) — sweeps in a single call instead of one per frame. All frames
// must live on the same page: a script may switch pages only once (the figma-use page rule).
const checkFrame = async (frameId) => {
  rows = [];
  const frame = await figma.getNodeByIdAsync(frameId);
  if (!frame) throw new Error(`frameId ${frameId} not found`);
  let page = frame;
  while (page && page.type !== "PAGE") page = page.parent;
  if (page && figma.currentPage !== page) await figma.setCurrentPageAsync(page);

  const fb = frame.absoluteBoundingBox;
  const isSmall = fb ? Math.round(fb.width) <= SMALL_FRAME_W : false;
  const TEXT_FLOOR = CONFIG.textFloor !== null && CONFIG.textFloor !== undefined ? CONFIG.textFloor : (isSmall ? 11 : 12);
  const LADDER = isSmall ? [11, ...LADDER_FULL] : LADDER_FULL;
  const frameFill = (() => { const f = frame.fills && frame.fills[0];
    if (!f || f.type !== "SOLID" || f.visible === false) return null;
    return "#" + [f.color.r, f.color.g, f.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join(""); })();
  const rel = (n) => {
    const b = n.absoluteBoundingBox;
    return b ? { l: b.x - fb.x, t: b.y - fb.y, rr: b.x - fb.x + b.width, bb: b.y - fb.y + b.height, w: b.width, h: b.height } : null;
  };

  // --- structural resolution, the same rule verify_templates.js and measure_fit.js use. Names are not
  // stable across design edits and the logo is a SIBLING of the header, not a child.
  const logo = frame.children.find((c) => /^logo/i.test(c.name) || /^Logos\//.test(c.name)) || null;
  const autos = frame.children.filter((c) => "layoutMode" in c && c.layoutMode !== "NONE" && c !== logo).sort((a, b) => a.y - b.y);
  const header = autos[0] || null;
  const footer = autos.length > 1 ? autos[autos.length - 1] : null;
  const footerTop = footer && footer.children.length ? footer.y + Math.min(0, Math.min(...footer.children.map((c) => c.y))) : footer ? footer.y : null;
  const bandTop = header ? header.y + header.height : null;
  const contentL = header ? header.x : null;
  const contentR = header ? header.x + header.width : null;

  // The chart, by NAME. A designer's rework ungroups it, leaving the plot subgroups as direct frame
  // children — so fall back to those rather than reporting nothing.
  let chart = CONFIG.chartName ? frame.children.find((c) => c.name === CONFIG.chartName) : null;
  let plotRoots = chart ? [chart] : frame.children.filter((c) => /^(horizontal-axis|vertical-axis|horizontal-grid-lines|vertical-labels|lines)$/.test(c.name));
  const chartResolvedBy = chart ? `name "${CONFIG.chartName}"` : plotRoots.length ? `plot subgroups (${plotRoots.length}) — the group looks ungrouped` : "NOT RESOLVED";

  // --- one traversal collects everything the rows below read.
  // ONE walk over the whole frame, with `insidePlot` set by which top-level child we descended from.
  // Walking only [chart, header, footer] is the trap: Step 8's annotations are appended to the FRAME,
  // so they are none of those three — `annotations` then comes back empty on every real page and its
  // four rows report "no annotation__* nodes" forever, which reads as "nothing to check" rather than
  // "I never looked". Caught by planting an annotation and watching the rows stay silent.
  const texts = [], stroked = [], fills = [], leaves = [], annotations = [], vectors = [], markBoxes = [];
  // Grapher groups its axis furniture under stable container names. Identifying furniture as
  // "stroked and not a series line" instead is wrong the moment the chart is not a line chart: on a
  // map EVERY country vector is a stroked non-series plot node, so the prescribed 0.22px borders and
  // 0.3-0.35px highlight outlines (per-chart-type/maps.md) all report as rescale defects.
  // Zero lines are furniture and are NOT inside an axis or grid container: on a discrete bar the plot's
  // only furniture is a top-level `vertical-zero-line`, and on a slope a `horizontal-zero-line`. Left
  // out, those charts report "no furniture" and the 1px rule never runs on the one node it applies to.
  const FURNITURE_GROUPS = /^(horizontal|vertical)-(axis|grid-lines|zero-line)$|^grid|^axis|^ticks?$|zero-line$/i;
  // The series identity can sit on a GROUP rather than on the stroked node. On a SLOPE chart
  // `slope__<Entity>` and `outline__<Entity>` are groups of {start-point, end-point, line}, and the
  // only stroked node is called plain `line` — so matching names on the stroked node alone finds no
  // series at all and the weight row skips in silence. Same shape as `datapoints__<Entity>`: name on
  // the group, properties on the child. So carry the nearest naming ancestor down the walk.
  const SERIES_ANY = /^(line|slope|outline)__(.+)$/;
  const collect = (n, insidePlot, inFurniture, seriesOf) => {
    if ("visible" in n && !n.visible) return;
    if (FURNITURE_GROUPS.test(n.name)) inFurniture = true;
    const sm = SERIES_ANY.exec(n.name);
    if (sm) seriesOf = { kind: sm[1], series: sm[2] };
    if (n.type === "TEXT" && typeof n.fontSize === "number") {
      const tf = Array.isArray(n.fills) && n.fills[0] && n.fills[0].type === "SOLID" && n.fills[0].visible !== false
        ? "#" + [n.fills[0].color.r, n.fills[0].color.g, n.fills[0].color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("")
        : null;
      let mixedWeight = false;
      try { mixedWeight = n.getStyledTextSegments(["fontName"]).length > 1; } catch (e) { mixedWeight = false; }
      texts.push({ node: n, name: n.name, chars: (n.characters || "").slice(0, 30), size: n.fontSize,
                   styleId: n.textStyleId || "", box: rel(n), insidePlot, fill: tf, mixedWeight });
    }
    if (/^annotation__/.test(n.name)) annotations.push({ node: n, name: n.name, box: rel(n), type: n.type });
    if ("strokeWeight" in n && typeof n.strokeWeight === "number" && n.strokes && n.strokes.length) {
      stroked.push({ node: n, name: n.name, type: n.type, w: n.strokeWeight,
                     dash: "dashPattern" in n && n.dashPattern ? [...n.dashPattern] : [],
                     align: n.strokeAlign, insidePlot, inFurniture,
                     seriesKind: seriesOf ? seriesOf.kind : null, seriesName: seriesOf ? seriesOf.series : null });
    }
    // Zero-area nodes are EXCLUDED from the fill inventory and KEPT for the stroke rows (CHECKS.md).
    // A grapher import's tick vectors are zero-width and carry a default black fill that paints no
    // pixels, so counting them reports phantom #000000 and sends a reviewer to bind a mark that is not
    // there. Their strokes are real, which is why the exclusion is per-row rather than per-node.
    const areaBox = rel(n);
    const hasArea = areaBox && areaBox.w > 0 && areaBox.h > 0;
    if (hasArea && "fills" in n && Array.isArray(n.fills)) {
      for (const f of n.fills) {
        if (f.type === "SOLID" && f.visible !== false) {
          const hex = "#" + [f.color.r, f.color.g, f.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("");
          fills.push({ name: n.name, type: n.type, hex, styleId: n.fillStyleId || "", insidePlot });
        }
      }
    }
    // Marker groups and value labels. The NAME is on the group and the GEOMETRY is on its children, and
    // both halves matter. `collect` returns after descending, so a GROUP is never a leaf — and grapher's
    // stable marker name sits on the `datapoints__<Entity>` GROUP while its descendants are unnamed, so
    // filtering leaves by name matches nothing and the dot rule never fires. But the group's own bbox is
    // the whole series (measured: 210x114 and 401x290 on a two-line chart), so testing against it would
    // flag every annotation placed anywhere over the plot. So take the name from the group and a box per
    // marker from its leaf descendants.
    // Filled DATA MARKS — a bar segment, a stacked band, a map shape. CHECKS.md forbids an annotation
    // covering "a bar segment carrying a number", and a strokeless filled rect appears in neither the
    // stroke inventory nor the dot/value one, so on a segmented bar an annotation could cover a coloured
    // segment while missing its `value__*` text and the row still returned ok. Anything inside the plot
    // that carries a visible solid fill, is not furniture, and is not text is a mark the reader sees.
    if (insidePlot && !inFurniture && n.type !== "TEXT" && !(("children" in n) && n.children.length)) {
      const mb0 = rel(n);
      const filled = Array.isArray(n.fills) && n.fills.some((f) => f.type === "SOLID" && f.visible !== false);
      if (filled && mb0 && mb0.w > 0 && mb0.h > 0) markBoxes.push({ name: n.name, box: mb0, why: "a filled data mark", insidePlot });
    }
    if (/^datapoints__|^dot__|^value__/.test(n.name)) {
      const why = /^value__/.test(n.name) ? "a value label" : "a dot";
      const pushLeafBoxes = (m) => {
        if ("visible" in m && !m.visible) return;
        if ("children" in m && m.children.length) { m.children.forEach(pushLeafBoxes); return; }
        const b = rel(m);
        if (b && b.w > 0 && b.h > 0) markBoxes.push({ name: n.name, box: b, why, insidePlot });
      };
      pushLeafBoxes(n);
    }
    if (n.type === "VECTOR" && insidePlot) vectors.push(n);
    if ("children" in n && n.children.length) { n.children.forEach((c) => collect(c, insidePlot, inFurniture, seriesOf)); return; }
    const b = rel(n);
    if (b && b.w > 0 && b.h > 0) leaves.push({ name: n.name, type: n.type, box: b, insidePlot });
  };
  for (const child of frame.children) {
    if (child === logo) continue;
    collect(child, plotRoots.indexOf(child) !== -1, false, null);
  }

  // ---------------------------------------------------------------- rows
  // Text floor (CHECKS.md: nothing below 12px)
  {
    const under = texts.filter((t) => t.size < TEXT_FLOOR - 0.01);
    add("text-floor", under.length ? "FAIL" : "ok",
        (under.length ? `${under.length} text node(s) below ${TEXT_FLOOR}px: ` + under.map((t) => `"${t.chars}" ${r(t.size)}px`).join(", ")
                      : `all ${texts.length} text nodes at or above ${TEXT_FLOOR}px`) +
        ` (floor ${TEXT_FLOOR}px, ${CONFIG.textFloor != null ? "from CONFIG" : isSmall ? "302-wide format — SMALL-CHARTS.md overrides 12 to 11" : "540/850-wide format"})`);
  }

  // Annotation ladder + ceiling. Only annotation__* nodes are ours; an imported chart's label sizes
  // come from the export and are governed by the floor row above, not by the ladder.
  {
    const ann = texts.filter((t) => /^annotation__/.test(t.name));
    if (!ann.length) skip("annotation-ladder", "no annotation__* text nodes on this frame");
    else {
      // The ceiling is L 15, and XL 16 is legal only when the annotation IS the message — a deliberate
      // choice, so it is declared via CONFIG rather than inferred from the number being present.
      const ceiling = CONFIG.xlAnnotations ? 16 : LADDER_CEILING;
      const off = ann.filter((t) => !LADDER.some((L) => Math.abs(t.size - L) < 0.01));
      const over = ann.filter((t) => t.size > ceiling + 0.01);
      const bad = [...off.map((t) => `"${t.chars}" ${r(t.size)}px off-ladder`),
                   ...over.map((t) => `"${t.chars}" ${r(t.size)}px above the ${CONFIG.xlAnnotations ? "XL 16" : "L " + LADDER_CEILING} ceiling` + (CONFIG.xlAnnotations ? "" : " — set CONFIG.xlAnnotations if this annotation IS the message"))];
      add("annotation-ladder", bad.length ? "FAIL" : "ok",
          bad.length ? bad.join(", ") : `all ${ann.length} annotation(s) on the ladder and at or below ${ceiling}` + (CONFIG.xlAnnotations ? " (XL declared)" : ""));
    }
  }

  // Sizes are named styles — CHECKS.md's bar is "no arbitrary sizes left over from scaling the export
  // (13.7, 16.8)", which is about the NUMBER, not about carrying a Figma style id. An SVG import can
  // never carry an id, so judging only bound nodes lets exactly the defect this row exists for through:
  // a fitted chart's labels land wherever the rescale puts them (13.36 measured on a live run) and the
  // row still reported ok. So check the numeric size of every plot and annotation text against the
  // ladder, and keep the style-id question as its own row below.
  {
    const subject = texts.filter((t) => t.insidePlot || /^annotation__/.test(t.name));
    if (!subject.length) skip("ladder-sizes", "no plot or annotation text to size");
    else {
      const off = subject.filter((t) => !LADDER.some((L) => Math.abs(t.size - L) < 0.01));
      const distinct = [...new Set(off.map((t) => r(t.size)))].sort((a, b) => a - b);
      add("ladder-sizes", off.length ? "FAIL" : "ok",
          off.length ? `${off.length} of ${subject.length} text node(s) off the ${LADDER.join("/")} ladder: ${distinct.join(", ")}px. A rescaled export leaves arbitrary sizes — set them to the nearest rung by rank.`
                     : `all ${subject.length} plot/annotation text node(s) on the ${LADDER.join("/")} ladder`,
          { offLadderSizes: distinct });
    }
  }

  // Style BINDING, separately from the numbers above. An SVG import cannot carry a style id, so this
  // row judges OUR nodes only and reports the imported ones as context.
  {
    const ann = texts.filter((t) => /^annotation__/.test(t.name));
    // An annotation that bolds its key phrase — the prescribed recipe — is MIXED-WEIGHT, and Figma
    // drops the node-level textStyleId when it is (reference/GOTCHAS.md says so and says not to treat
    // it as a defect). Failing those means a correctly-built annotation can never pass, so they are
    // judged on their ladder size instead, which the ladder-sizes row above already enforces.
    const unbound = ann.filter((t) => !t.styleId && !t.mixedWeight);
    const mixed = ann.filter((t) => t.mixedWeight);
    const importedRaw = texts.filter((t) => t.insidePlot && !/^annotation__/.test(t.name) && !t.styleId).length;
    if (!ann.length) skip("named-styles", "no annotation__* text nodes; an imported chart's text cannot carry a style id");
    else add("named-styles", unbound.length ? "FAIL" : "ok",
             (unbound.length ? `${unbound.length} annotation(s) with no textStyleId — setting fontSize looks like the ladder and is not it: ` + unbound.map((t) => `"${t.chars}"`).join(", ")
                             : `all ${ann.length - mixed.length} single-weight annotation(s) bound to a text style`) +
             (mixed.length ? ` ${mixed.length} mixed-weight annotation(s) exempted — Figma drops the node-level style id when a phrase is bolded, which is the prescribed recipe (GOTCHAS.md); their sizes are covered by ladder-sizes.` : "") +
             ` ${importedRaw} imported chart text node(s) are raw, which is expected.`);
  }

  // Text hierarchy: nothing may exceed the subtitle (CHECKS.md row 26).
  {
    // Structurally: the header's second TEXT child. Picking index 1 out of a list of collected texts
    // sorted by y is fragile — anything that lands two nodes at the same top, or collects a node
    // twice, silently promotes the TITLE into the subtitle's place, and a 25px bar passes everything.
    const headerTexts = header ? header.children.filter((c) => c.type === "TEXT" && typeof c.fontSize === "number") : [];
    const subtitle = headerTexts.length > 1 ? { size: headerTexts[1].fontSize, chars: headerTexts[1].characters.slice(0, 24) } : null;
    if (!subtitle) skip("text-hierarchy", "could not resolve the subtitle (header has fewer than two TEXT children)");
    else {
      const over = texts.filter((t) => (t.insidePlot || /^annotation__/.test(t.name)) && t.size > subtitle.size + 0.01);
      add("text-hierarchy", over.length ? "FAIL" : "ok",
          (over.length ? `${over.length} in-plot text node(s) exceed the subtitle's ${r(subtitle.size)}px: ` + over.map((t) => `"${t.chars}" ${r(t.size)}px`).join(", ")
                      : `nothing in the plot exceeds the subtitle's ${r(subtitle.size)}px`) +
          ". CEILING ONLY — the rest of CHECKS.md's hierarchy (annotations outranking supporting text and labels, same-rank items sharing a size) needs a rank per node, which nothing here supplies, so an inverted lower order is NOT covered. See text-hierarchy-ranks.",
          { distinctPlotSizes: [...new Set(texts.filter((t) => t.insidePlot).map((t) => r(t.size)))].sort((a, b) => a - b) });
    }
  }

  // Mark weight — the series lines and their halos.
  //
  // Two things vary by chart type and both were wrong when this only knew line charts. The series line
  // is `line__X` on a line/stacked chart and `slope__X` on a slope chart, so a slope's series were
  // never checked at all — the row reported "no line__*" and skipped in silence. And `outline__X` is a
  // halo ONLY when a series line of the same name exists: on a SCATTER, `outline__<Entity>` is the ring
  // around a point, which measured 3.5-4.1px here and was being judged against a line's halo bar.
  // So a halo is paired or it is not a halo.
  {
    const lineNames = new Set();
    for (const s of stroked) if (s.seriesKind && s.seriesKind !== "outline") lineNames.add(s.seriesName);
    const unpairedOutlines = stroked.filter((s) => s.seriesKind === "outline" && !lineNames.has(s.seriesName));
    const series = stroked.filter((s) => s.seriesKind && (s.seriesKind !== "outline" || lineNames.has(s.seriesName)));
    if (!series.length) skip("series-weight", `no series stroke found — looked for line__*/slope__* names on the stroked node OR on an ancestor group, plus their paired outline__*` +
        (unpairedOutlines.length ? `. ${unpairedOutlines.length} outline__* node(s) have no paired series line, so they are point rings or shape outlines rather than halos and are not judged here (a scatter's point rings run 3.5-4.1px by design)` : ""));
    else if (CONFIG.highlightTreatment) {
      // A shared set of allowed numbers is the wrong shape: it rejects a valid 1px context line whose
      // halo is the required 2px, and accepts nonsense like a 4px `line__` beside a 1px `outline__`.
      // The bar is a RELATIONSHIP — context 1, protagonist 3, halo 2x (or line+1 where nothing crosses).
      const lineW = {};
      for (const s of series) if (s.seriesKind !== "outline") lineW[s.seriesName] = s.w;
      const bad = [];
      for (const s of series) {
        if (!s.seriesKind) continue;
        if (s.seriesKind !== "outline") {
          if (!(Math.abs(s.w - 1) < 0.05 || Math.abs(s.w - HOUSE_LINE) < 0.05)) bad.push(`${s.name} ${r(s.w)} — a series line is 1 (muted context) or ${HOUSE_LINE} (protagonist)`);
        } else {
          const lw = lineW[s.seriesName];
          if (lw === undefined) { bad.push(`${s.name} has no paired series line for ${s.seriesName} to size its halo against`); continue; }
          const ok = Math.abs(s.w - lw * 2) < 0.05 || Math.abs(s.w - (lw + 1)) < 0.05;
          if (!ok) bad.push(`${s.name} ${r(s.w)} against a ${r(lw)}px line — a halo is 2x (${r(lw * 2)}) or line+1 (${r(lw + 1)})`);
        }
      }
      add("series-weight", bad.length ? "FAIL" : "ok",
          bad.length ? bad.join("; ") : `all ${series.length} series stroke(s) hold the highlight relationship (line 1 or ${HOUSE_LINE}; halo 2x or line+1)`);
    } else {
      const bad = series.filter((s) => Math.abs(s.w - (s.seriesKind === "outline" ? HOUSE_HALO : HOUSE_LINE)) >= 0.05);
      add("series-weight", bad.length ? "FAIL" : "ok",
          (bad.length ? `off the house ${HOUSE_LINE}/${HOUSE_HALO}: ` + bad.map((s) => `${s.seriesKind}__${s.seriesName} ${r(s.w)} -> ${s.seriesKind === "outline" ? HOUSE_HALO : HOUSE_LINE}`).join(", ") + ". rescale() multiplies stroke weight — set these AFTER the last scale."
                      : `all ${series.length} series stroke(s) at the house ${HOUSE_LINE}/${HOUSE_HALO}`) +
          (unpairedOutlines.length ? ` (${unpairedOutlines.length} unpaired outline__* node(s) excluded — point rings, not halos)` : ""));
    }
  }

  // Furniture weight and dash — the two rows that are easiest to miss because you never set them.
  {
    const furn = stroked.filter((s) => s.insidePlot && s.inFurniture && !s.seriesKind);
    if (!furn.length) skip("furniture-weight", "no stroked node sits under an axis/gridline group (" + FURNITURE_GROUPS + ") — the 1px rule covers gridlines, zero lines and tick marks only, so it is not applied to whatever else this chart draws (a map's country borders run 0.22px by design)");
    else {
      const bad = furn.filter((s) => Math.abs(s.w - FURNITURE_W) >= 0.05);
      add("furniture-weight", bad.length ? "FAIL" : "ok",
          bad.length ? `${bad.length} of ${furn.length} furniture stroke(s) off ${FURNITURE_W}px (rescale() thinned them): ` + [...new Set(bad.map((s) => r(s.w)))].join(", ") + `px seen`
                     : `all ${furn.length} furniture stroke(s) at ${FURNITURE_W}px`);
      // Dash target is PER NODE TYPE: a node that already had a dash keeps [4,4]; the zero line and
      // tick marks are solid and must keep an EMPTY pattern. One blanket target restyles the grid.
      const dashed = furn.filter((s) => s.dash.length);
      const solid = furn.filter((s) => !s.dash.length);
      const badDash = dashed.filter((s) => s.dash.length !== FURNITURE_DASH.length || s.dash.some((v, i) => Math.abs(v - FURNITURE_DASH[i]) >= 0.05));
      add("furniture-dash", badDash.length ? "FAIL" : "ok",
          badDash.length ? `${badDash.length} dashed node(s) off [${FURNITURE_DASH}]: ` + [...new Set(badDash.map((s) => JSON.stringify(s.dash.map(r))))].join(", ")
                         : `${dashed.length} dashed node(s) at [${FURNITURE_DASH}], ${solid.length} solid node(s) keeping an empty pattern`);
    }
  }

  // Box alignment — the chart's edges against the header box, to the pixel.
  {
    const boxes = plotRoots.map(rel).filter(Boolean);
    if (isSmall) skip("box-alignment", "302-wide format: the header HUGS its own text (206-278 against a 278 content box), so the chart's width is not meant to match it — SMALL-CHARTS.md");
    else if (!boxes.length || contentL === null) skip("box-alignment", "chart or header box not resolved");
    else {
      const l = Math.min(...boxes.map((b) => b.l)), rr = Math.max(...boxes.map((b) => b.rr));
      const dl = l - contentL, dr = rr - contentR;
      const bad = Math.abs(dl) > 1 || Math.abs(dr) > 1;
      add("box-alignment", bad ? "FAIL" : "ok",
          `chart ${r(l)}..${r(rr)} against the header's ${r(contentL)}..${r(contentR)} (left ${r(dl) >= 0 ? "+" : ""}${r(dl)}, right ${r(dr) >= 0 ? "+" : ""}${r(dr)})`);
    }
  }

  // Gap — top and bottom, against the band of the template actually filled.
  {
    const boxes = plotRoots.map(rel).filter(Boolean);
    const target = CONFIG.gapTarget || (fb && Math.round(fb.width) === 560 ? [30, 30] : [12, 16]);
    if (isSmall) skip("gap", "302-wide format: free frame height and no fit step, so the 12-16px band rule does not apply as written — SMALL-CHARTS.md");
    else if (CONFIG.tightlyMeasured) skip("gap", "tightlyMeasured: CHECKS.md's band figure does not apply to a trimmed, hugged group — match the reference page's own measurement (typically 20-30px) and record it");
    else if (!boxes.length || bandTop === null || footerTop === null) skip("gap", "band or chart not resolved");
    else {
      const t = Math.min(...boxes.map((b) => b.t)) - bandTop;
      const b2 = footerTop - Math.max(...boxes.map((b) => b.bb));
      const within = (v) => v >= target[0] - 0.5 && v <= target[1] + 0.5;
      const bad = !within(t) || !within(b2) || Math.abs(t - b2) > 1.5;
      add("gap", bad ? "FAIL" : "ok",
          `top ${r(t)}, bottom ${r(b2)} against a ${target[0]}-${target[1]}px target${Math.abs(t - b2) > 1.5 ? " — and the two ends differ by more than 1.5px" : ""}`);
    }
  }

  // Nothing in the margins.
  {
    // On the 302-wide format the header hugs its text, so a header-derived right edge would reject ink
    // that is legitimately inside the format's own 12..290 content box. Take the bounds from the FORMAT
    // there and from the header everywhere else (SMALL-CHARTS.md -> Checks).
    const marginL = isSmall ? 12 : contentL;
    const marginR = isSmall && fb ? fb.width - 12 : contentR;
    if (marginL === null || marginR === null) skip("margins", "content box not resolved");
    else {
      const out = leaves.filter((x) => (x.insidePlot || /^annotation__/.test(x.name)) && (x.box.l < marginL - 0.5 || x.box.rr > marginR + 0.5));
      add("margins", out.length ? "FAIL" : "ok",
          (out.length ? `${out.length} mark(s) outside ${r(marginL)}..${r(marginR)}: ` + out.slice(0, 6).map((x) => `${x.name} at ${r(x.box.l)}..${r(x.box.rr)}`).join(", ")
                      : `no ink outside ${r(marginL)}..${r(marginR)} across ${leaves.filter((x) => x.insidePlot).length} plot leaves`) +
          ` (bounds from ${isSmall ? "the 302-wide FORMAT, not its hugging header" : "the header box"})`);
    }
  }

  // Off-palette fills. Two standing exceptions are listed rather than flagged.
  {
    const plotFills = fills.filter((f) => f.insidePlot);
    if (!plotFills.length) skip("off-palette", "no solid fills found in the plot");
    else {
      const unbound = plotFills.filter((f) => !f.styleId);
      const residual = plotFills.filter((f) => f.hex.toLowerCase() === GRAPHER_RESIDUAL);
      const distinct = [...new Set(unbound.map((f) => f.hex))];
      // An imported chart arrives with raw fills by construction, so this row REPORTS rather than
      // fails unless grapher's residual gray is present, which is never a library colour.
      add("off-palette", residual.length ? "FAIL" : "REVIEW",
          (residual.length ? `grapher's residual-category ${GRAPHER_RESIDUAL} is present (${residual.length} fill(s)) — it is in no library group. ` : "") +
          `${unbound.length} of ${plotFills.length} plot fills carry no style id, across ${distinct.length} distinct colour(s): ${distinct.slice(0, 10).join(", ")}. ` +
          "An SVG import cannot bind a style, so bind the ones you keep and confirm each is a library colour. Standing exceptions, not defects: a highlight treatment's muting grays" +
          (CONFIG.highlightTreatment ? " (declared for this frame)" : "") + ", and a grapher-managed sequential map ramp.",
          { distinctUnboundFills: distinct });
    }
  }

  // ---- Crossings, computed BEFORE the two rows that read them. Vertices are LOCAL to their node, so
  // map them through absoluteTransform — a bbox is not a substitute for a line (CHECKS.md), and an
  // untransformed read puts the geometry somewhere else entirely (reference/GOTCHAS.md).
  //
  // What an annotation may sit on is a CLASSIFICATION, not a yes/no: gridlines, empty space and a
  // muted context line are legal; a protagonist line, a dot and a value label are not. Failing every
  // intersection alike reports a correct highlighted chart as broken, because crossing the muted
  // context is exactly what the treatment is for.
  let crossings = null;
  {
    const map = (n, pt) => { const m = n.absoluteTransform; return { x: m[0][0] * pt.x + m[0][1] * pt.y + m[0][2] - fb.x, y: m[1][0] * pt.x + m[1][1] * pt.y + m[1][2] - fb.y }; };
    const segHitsRect = (p, q, b) => {
      const inside = (pt) => pt[0] >= b.l && pt[0] <= b.rr && pt[1] >= b.t && pt[1] <= b.bb;
      if (inside(p) || inside(q)) return true;
      const cr = (ax, ay, bx, by) => ax * by - ay * bx;
      const edges = [[[b.l, b.t], [b.rr, b.t]], [[b.rr, b.t], [b.rr, b.bb]], [[b.rr, b.bb], [b.l, b.bb]], [[b.l, b.bb], [b.l, b.t]]];
      for (const [e0, e1] of edges) {
        const d1 = cr(q[0] - p[0], q[1] - p[1], e0[0] - p[0], e0[1] - p[1]);
        const d2 = cr(q[0] - p[0], q[1] - p[1], e1[0] - p[0], e1[1] - p[1]);
        const d3 = cr(e1[0] - e0[0], e1[1] - e0[1], p[0] - e0[0], p[1] - e0[1]);
        const d4 = cr(e1[0] - e0[0], e1[1] - e0[1], q[0] - e0[0], q[1] - e0[1]);
        if (((d1 > 0) !== (d2 > 0)) && ((d3 > 0) !== (d4 > 0))) return true;
      }
      return false;
    };
    const overlaps = (a, b) => a.l < b.rr && a.rr > b.l && a.t < b.bb && a.bb > b.t;

    // Muted-or-not is a property of the SERIES, not of the node. Under the highlight treatment a 1px
    // `line__X` is the muted context — and so is its halo, `outline__X`, which sits at 2x (CHECKS.md:
    // "halo 2x, or line+1 where nothing crosses"). Classifying each node by its own weight therefore
    // reports the halo of a legally-crossed context line as a protagonist. So read the weight of each
    // series' `line__` node and let its `outline__` inherit the verdict.
    const lineWeightBySeries = {};
    for (const v of vectors) {
      const m = /^(line|slope)__(.+)$/.exec(v.name);
      if (m && typeof v.strokeWeight === "number") lineWeightBySeries[m[2]] = v.strokeWeight;
    }
    const polylines = [];
    for (const v of vectors) {
      const m = /^(line|slope|outline)__(.+)$/.exec(v.name);
      if (!m) continue;
      let net = null;
      try { net = v.vectorNetwork; } catch (e) { net = null; }
      if (!net || !net.vertices || !net.vertices.length) continue;
      const w = typeof v.strokeWeight === "number" ? v.strokeWeight : null;
      const seriesW = lineWeightBySeries[m[2]];
      const muted = CONFIG.highlightTreatment && seriesW !== undefined && Math.abs(seriesW - 1) < 0.05;
      const pts = net.vertices.map((pt) => { const q = map(v, pt); return [r(q.x), r(q.y)]; });
      // Connectivity lives in `segments`, NOT in vertex order. A series with a gap (a missing interval
      // in a time range) has disconnected subpaths, and joining consecutive vertices invents a stroke
      // across the gap — which then reports an annotation sitting in that empty space as crossing the
      // line, and demands a knockout for it. Fall back to vertex order only when segments are absent.
      const segs = Array.isArray(net.segments) && net.segments.length
        ? net.segments.filter((s) => pts[s.start] && pts[s.end]).map((s) => [pts[s.start], pts[s.end]])
        : pts.slice(1).map((q, i) => [pts[i], q]);
      polylines.push({ name: v.name, muted, w, seriesLineW: seriesW === undefined ? null : r(seriesW),
                       points: pts, segments: segs, fromSegments: !!(Array.isArray(net.segments) && net.segments.length) });
    }
    if (!polylines.length) skip("polylines", "no line__*/outline__* VECTOR carried a readable vectorNetwork");
    else add("polylines", "ok", `${polylines.length} series polyline(s) sampled, ${polylines.reduce((s, p) => s + p.points.length, 0)} vertices and ${polylines.reduce((s, p) => s + p.segments.length, 0)} segments (connectivity ${polylines.every((p) => p.fromSegments) ? "from vectorNetwork.segments" : "PARTLY from vertex order — a gapped series may report a phantom crossing"}), in frame coordinates` +
          (CONFIG.highlightTreatment ? `; ${polylines.filter((p) => p.muted).length} classified as muted context (legal to cross)` : ""),
          { polylines: polylines.map((p) => ({ name: p.name, n: p.points.length, muted: p.muted, first: p.points[0], last: p.points[p.points.length - 1] })) });

    // Furniture and forbidden marks, by bbox. A gridline, axis or tick is axis-aligned, so its bbox IS
    // its geometry; dots and value labels are small and compact, so a bbox is right for them too.
    const furnitureBoxes = stroked.filter((s) => s.insidePlot && !/^(line|outline)__/.test(s.name))
      .map((s) => ({ name: s.name, box: rel(s.node) })).filter((x) => x.box);
    const forbiddenBoxes = markBoxes.filter((x) => x.insidePlot);

    if (!annotations.length) { skip("annotation-overlap", "no annotation__* nodes on this frame"); }
    else if (!polylines.length && !furnitureBoxes.length) { skip("annotation-overlap", "nothing to test against: no readable polylines and no furniture"); }
    else {
      crossings = {};
      const illegal = [];
      for (const a of annotations) {
        if (!a.box) continue;
        const hits = [];
        for (const pl of polylines) {
          for (const [p, q] of pl.segments) {
            if (segHitsRect(p, q, a.box)) {
              hits.push(pl.name);
              if (!pl.muted) illegal.push(`${a.name} crosses ${pl.name}` + (CONFIG.highlightTreatment ? ` (its series line is ${pl.seriesLineW}px — protagonist, not the 1px muted context)` : ""));
              break;
            }
          }
        }
        for (const f of furnitureBoxes) if (overlaps(a.box, f.box)) hits.push(f.name);
        for (const f of forbiddenBoxes) if (overlaps(a.box, f.box)) { hits.push(f.name); illegal.push(`${a.name} covers ${f.name} — ${f.why}`); }
        crossings[a.name] = [...new Set(hits)];
      }
      const uniq = [...new Set(illegal)];
      add("annotation-overlap", uniq.length ? "FAIL" : "ok",
          uniq.length ? uniq.join("; ") + ". Gridlines, empty space and a muted context line are legal; a protagonist line, a dot or a value label is not."
                      : `no annotation covers a prohibited mark (${annotations.length} annotation(s) vs ${polylines.length} line(s), ${furnitureBoxes.length} furniture node(s), ${forbiddenBoxes.length} individual dot/value mark(s))`,
          { crossingsPerAnnotation: crossings, approximate: "segment-vs-rect on sampled vertices; a near-miss is settled by CHECKS.md's four-render pixel probe." });
    }
  }

  // Annotation knockout tier. The TIER IS DECIDED BY WHAT IS CROSSED, so this row cannot be a
  // property check alone: an annotation crossing a gridline with NO stroke is a missing knockout, and
  // judging only the nodes that already have one certifies exactly that defect. Crossings are computed
  // below (furniture bboxes + series polylines) and this row reads them.
  {
    if (!annotations.length) skip("annotation-knockout", "no annotation__* nodes on this frame");
    else if (!crossings) skip("annotation-knockout", "crossings not computed (no furniture and no readable polylines) — cannot decide the tier, so not judging the strokes either");
    else {
      const bad = [];
      for (const a of annotations) {
        const n = a.node;
        if (!("strokeWeight" in n) || typeof n.strokeWeight !== "number") continue;
        const hasStroke = n.strokes && n.strokes.length && n.strokeWeight > 0;
        const crosses = crossings[a.name] || [];
        if (crosses.length && !hasStroke) {
          bad.push(`${a.name} crosses ${crosses.length} thing(s) (${crosses.slice(0, 3).join(", ")}) but carries NO knockout — CHECKS.md requires a 3px OUTSIDE stroke whenever furniture is crossed`);
          continue;
        }
        if (!crosses.length && hasStroke) {
          bad.push(`${a.name} crosses nothing yet carries a ${r(n.strokeWeight)}px knockout — an annotation over empty space takes no stroke and no frame`);
          continue;
        }
        if (hasStroke && Math.abs(n.strokeWeight - 3) >= 0.05) {
          bad.push(`${a.name} knockout ${r(n.strokeWeight)}px (want 3)` + (n.strokeWeight < 1 ? " — sub-pixel means the stroke was set before a rescale()" : ""));
        }
        if (hasStroke && n.strokeAlign !== "OUTSIDE") bad.push(`${a.name} strokeAlign ${n.strokeAlign} (want OUTSIDE)`);
        if (hasStroke && frameFill) {
          const sf = n.strokes[0];
          if (sf.type === "SOLID") {
            const hex = "#" + [sf.color.r, sf.color.g, sf.color.b].map((x) => Math.round(x * 255).toString(16).padStart(2, "0")).join("");
            if (hex.toLowerCase() !== frameFill.toLowerCase()) bad.push(`${a.name} knockout is ${hex}, not the frame's own ${frameFill} — read the colour off the frame, never hardcode white`);
          }
        }
      }
      add("annotation-knockout", bad.length ? "FAIL" : "ok", bad.length ? bad.join("; ") : `all ${annotations.length} annotation(s) carry the tier their crossings require`);
    }
  }

  // Annotation block gap — the block's outer edges, not the plot's.
  {
    if (!annotations.length) skip("annotation-block-gap", "no annotation__* nodes on this frame");
    else if (isSmall) skip("annotation-block-gap", "302-wide format: SMALL-CHARTS.md replaces the 27px constant with 'scale to the frame', so the 540x540 figure would reject a valid scaled layout. Measure it against that page's own rule and record the number");
    else if (bandTop === null || footerTop === null) skip("annotation-block-gap", "band not resolved");
    else {
      const all = [...annotations.map((a) => a.box), ...plotRoots.map(rel)].filter(Boolean);
      const top = Math.min(...all.map((b) => b.t)), bot = Math.max(...all.map((b) => b.bb));
      const cTop = top - (header ? header.y + header.height : bandTop), cBot = footerTop - bot;
      const bad = cTop < BLOCK_CLEARANCE - 0.5 || cBot < BLOCK_CLEARANCE - 0.5;
      add("annotation-block-gap", bad ? "FAIL" : "ok", `block clears header by ${r(cTop)} and footer by ${r(cBot)} (want >= ${BLOCK_CLEARANCE})`);
    }
  }

  // Direct labels readable as text — computed, not declared. CHECKS.md wants 4.5:1 against the
  // background for every category label drawn on it; that is a pure function of two hexes.
  {
    const onBg = texts.filter((t) => t.insidePlot && t.fill && /^(label|annotation)__/.test(t.name));
    if (!frameFill) skip("label-contrast-on-background", "frame carries no solid fill to measure against");
    else if (!onBg.length) skip("label-contrast-on-background", "no label__*/annotation__* text with a solid fill");
    else {
      const bad = onBg.map((t) => ({ t, c: contrast(t.fill, frameFill) })).filter((x) => x.c < 4.5);
      add("label-contrast-on-background", bad.length ? "FAIL" : "ok",
          bad.length ? bad.map((x) => `"${x.t.chars}" ${x.t.fill} on ${frameFill} = ${r(x.c)}:1 (want 4.5)`).join(", ")
                     : `all ${onBg.length} label(s) clear 4.5:1 against ${frameFill} (lowest ${r(Math.min(...onBg.map((t) => contrast(t.fill, frameFill))))}:1)`);
    }
  }

  // ---------------------------------------------------------------- declared gaps in coverage
  skip("colour-vision", "all-pairs deltaE 20 for deuteranopia/protanopia on CATEGORICAL fills", "scripts/color_audit.py");
  skip("grayscale-seams", "adjacent pairs above ~1.6:1; needs --separated for non-stacked charts", "scripts/color_audit.py");
  skip("spelling-and-prose", "American spelling, typos, style-guide breaches", ".venv/bin/codespell + /check-metadata-style");
  skip("text-true-of-indicator", "every claim in every string checked against the producer's documentation", "/adversarial-data-review");
  skip("entities-all-render", "needs the EFFECTIVE selection (URL country=, or the MDim view's resolved list from the DB) — never the SVG's own labels, which makes the check unable to fail", "Step 1's table + /query-grapher-db");
  skip("year-stated-not-stale", "a single-time image must name its year; a time series must not gain a caption", "/check-hardcoded-years");
  skip("legend-agreement", "swatch->label pairing by geometry; not attempted here because a direct-labelled chart has no legend — run it by hand if this frame has one");
  skip("text-hierarchy-ranks", "annotations must outrank supporting text and labels, and same-rank items must share a size. Needs a rank per text node; the ceiling half is enforced by text-hierarchy above", "CHECKS.md");
  skip("direct-label-pairing", "each category label's fill and x against the segment it names, in the reference row");
  // The OTHER 4.5:1 row. Declared rather than computed because it needs the label->segment pairing the
  // row above owns: the bar's own fill is the background here, and picking it by geometry is that
  // pairing problem. The contrast arithmetic is already in this file (see label-contrast-on-background)
  // and can be reused the moment pairing exists.
  skip("label-contrast-on-fill", "4.5:1 for every label drawn INSIDE a fill, at 13.5px regular — the 3:1 large-text allowance does not apply. Needs label->segment pairing to know which fill is behind each label", "CHECKS.md + the direct-label-pairing row");
  skip("arrow-clearance", "arrow pixels vs target pixels; needs 3N+1 renders (the four-render protocol, pair-specific)", "CHECKS.md");
  skip("leader-on-map", "terminal vertex against the country's PIXELS, not its bounding box", "CHECKS.md + per-chart-type/maps.md");

  const fails = rows.filter((x) => x.status === "FAIL");
  const review = rows.filter((x) => x.status === "REVIEW");
  const skipped = rows.filter((x) => x.status === "SKIPPED");
  return {
    frame: { id: frame.id, name: frame.name, w: fb ? r(fb.width) : null, h: fb ? r(fb.height) : null },
    resolved: { chartBy: chartResolvedBy, contentBox: [r(contentL), r(contentR)], band: [r(bandTop), r(footerTop)],
                counts: { texts: texts.length, stroked: stroked.length, plotLeaves: leaves.filter((x) => x.insidePlot).length, annotations: annotations.length } },
    verdict: fails.length ? `FAIL on ${fails.length} row(s): ${fails.map((f) => f.check).join(", ")}`
                          : `no mechanical row failed (${review.length} to review, ${skipped.length} not covered here)`,
    rows,
  };

};

// frameIds wins when present; frameId stays the single-frame spelling.
const targets = Array.isArray(CONFIG.frameIds) && CONFIG.frameIds.length ? CONFIG.frameIds : [CONFIG.frameId];
if (targets.length === 1) return await checkFrame(targets[0]);
const results = [];
for (const id of targets) {
  try { results.push(await checkFrame(id)); }
  catch (e) { results.push({ frame: { id }, verdict: `THREW: ${e.message}`, rows: [] }); }
}
return {
  swept: results.length,
  summary: results.map((x) => `${x.frame && x.frame.name ? x.frame.name : x.frame.id}: ${x.verdict}`),
  frames: results,
};