// restretch_plot.js — make an axis chart (line, stacked area, slope, column, scatter) taller inside a
// frame that was just made taller (540×540 → 540×675), without distorting anything that must keep
// its shape. Run through `use_figma` with CONFIG filled in. One write, one page.
//
// The plot group's box is mapped linearly from its current y-range onto the new band:
//     mapY(y) = bandTop + (y − plotTop) × s,   s = band / plotHeight
// and every descendant is handled by what it is, not by its name:
//   - GROUP            → recurse (a group's box follows its children);
//   - TEXT             → moved, never resized. A label within `edgeZone` px of the plot's top or
//                        bottom edge (x tick labels, a label sitting on the top gridline) keeps its
//                        distance from that edge; every other label maps by its centre (y tick labels
//                        land on their gridlines because both map with the same function);
//   - ELLIPSE, or any leaf ≤ `dotMax` px in both dimensions (dots, tick marks, arrowheads) → moved
//                        by its centre, never stretched;
//   - horizontal hairline (h < 1)  → moved;
//   - vertical hairline (w < 1)    → moved and lengthened by rewriting its path (resize on a
//                        zero-width vector throws);
//   - every other VECTOR / RECTANGLE / LINE / BOOLEAN (polylines, areas, columns) → height × s, then
//                        moved. Stroke weights are untouched by resize, so lines keep their weight.
// Siblings of the plot group whose centre lies in its y-range (end dots, value labels, annotations and
// their leaders) are TRANSLATED as rigid wholes — each sibling, group or leaf, moves by the map of its
// own centre and nothing inside it is resized — unless listed in `skipIds`.
//
// It changes no text, font, fill or stroke. It returns everything it touched, and `unmatched` for
// leaves it did not know how to treat — treat a non-empty `unmatched` as a stop.

const CONFIG = {
  frameId: "<clone id>",       // the clone (already 540×675)
  plotId: "<plot group id>",   // the group holding axes, gridlines and marks (often named chart-area)
  bandTop: 0,                  // new top of the plot box: old plot top + (new header bottom − old header bottom)
  bandBottom: 0,               // new bottom of the plot box: new footer top − (old footer top − old plot bottom)
  skipIds: [],                 // title, subtitle, logo, footer — never moved by this script
  dotMax: 12,                  // leaves at most this big in both dimensions are moved, not stretched
  edgeZone: 30,                // TEXT within this many px of the plot's top/bottom edge keeps its edge offset
};

const frame = await figma.getNodeByIdAsync(CONFIG.frameId);
if (!frame) throw new Error("frame not found: " + CONFIG.frameId);
let page = frame; while (page.type !== "PAGE") page = page.parent;
await figma.setCurrentPageAsync(page);
const plot = await figma.getNodeByIdAsync(CONFIG.plotId);
if (!plot || !("children" in plot)) throw new Error("plot group not found: " + CONFIG.plotId);

// Coordinates: children of a FRAME are relative to it; children of a GROUP share the group's
// parent's space. Every node here is inside `frame`, and groups are transparent, so node.y is in
// frame space for everything the script touches unless a nested FRAME intervenes (then it is moved
// as one leaf, so its children need no mapping).
const plotTop = plot.y, plotBottom = plot.y + plot.height;
const oldH = plotBottom - plotTop, newH = CONFIG.bandBottom - CONFIG.bandTop;
const s = newH / oldH;
const mapY = y => CONFIG.bandTop + (y - plotTop) * s;
const skip = new Set(CONFIG.skipIds);
const moved = [], stretched = [], unmatched = [];

const moveCentre = n => { const c = n.y + n.height / 2; n.y = mapY(c) - n.height / 2; moved.push({ id: n.id, type: n.type, name: n.name.slice(0, 24), y: +n.y.toFixed(1) }); };

const leaf = n => {
  if (skip.has(n.id)) return;
  if (n.type === "TEXT") {
    const c = n.y + n.height / 2;
    if (plotBottom - c <= CONFIG.edgeZone) n.y = CONFIG.bandBottom - (plotBottom - n.y);
    else if (c - plotTop <= CONFIG.edgeZone) n.y = CONFIG.bandTop + (n.y - plotTop);
    else n.y = mapY(c) - n.height / 2;
    moved.push({ id: n.id, type: "TEXT", name: n.characters.slice(0, 24), y: +n.y.toFixed(1) });
    return;
  }
  const small = n.width <= CONFIG.dotMax && n.height <= CONFIG.dotMax;
  if (n.type === "ELLIPSE" || small || n.type === "FRAME" || n.type === "INSTANCE") return moveCentre(n);
  if (n.type === "VECTOR" && n.height < 1) { n.y = mapY(n.y); moved.push({ id: n.id, type: "hline", name: n.name.slice(0, 24), y: +n.y.toFixed(1) }); return; }
  if (n.type === "VECTOR" && n.width < 1) {
    const h = n.height * s;
    n.vectorPaths = [{ windingRule: "NONZERO", data: `M 0 0 L 0 ${h}` }];
    n.y = mapY(n.y);
    stretched.push({ id: n.id, type: "vline", name: n.name.slice(0, 24), y: +n.y.toFixed(1), h: +h.toFixed(1) });
    return;
  }
  if (["VECTOR", "RECTANGLE", "LINE", "BOOLEAN_OPERATION", "STAR", "POLYGON"].includes(n.type)) {
    const top = n.y;
    n.resize(n.width, n.height * s);
    n.y = mapY(top);
    stretched.push({ id: n.id, type: n.type, name: n.name.slice(0, 24), y: +n.y.toFixed(1), h: +n.height.toFixed(1) });
    return;
  }
  unmatched.push({ id: n.id, type: n.type, name: n.name.slice(0, 24) });
};
const walk = n => { if (skip.has(n.id)) return; if (n.type === "GROUP") { for (const c of n.children) walk(c); } else leaf(n); };
for (const c of plot.children) walk(c);

// siblings that live in the plot's y-range: end dots, value labels, annotations and their leaders.
// Rigid translation only — an annotation's curved leader or backdrop must never be stretched.
const siblings = [];
for (const c of frame.children) {
  if (c === plot || skip.has(c.id)) continue;
  const cy = c.y + c.height / 2;
  if (cy < plotTop || cy > plotBottom) continue;
  c.y += mapY(cy) - cy;
  siblings.push({ id: c.id, type: c.type, name: c.name.slice(0, 24), y: +c.y.toFixed(1) });
  moved.push({ id: c.id, type: c.type, name: c.name.slice(0, 24), y: +c.y.toFixed(1) });
}

return {
  mutatedNodeIds: [...moved.map(m => m.id), ...stretched.map(m => m.id)],
  scale: +s.toFixed(4), plotBefore: { top: plotTop, bottom: plotBottom, h: oldH },
  plotAfter: { top: +plot.y.toFixed(1), bottom: +(plot.y + plot.height).toFixed(1), h: +plot.height.toFixed(1) },
  band: { top: CONFIG.bandTop, bottom: CONFIG.bandBottom }, siblingsTranslated: siblings,
  counts: { moved: moved.length, stretched: stretched.length, unmatched: unmatched.length },
  stretched, unmatched, movedSample: moved.slice(0, 12),
};
