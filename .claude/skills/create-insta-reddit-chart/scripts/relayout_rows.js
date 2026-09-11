// relayout_rows.js — re-space the rows of a horizontal bar chart inside a frame that was just made
// taller (540×540 → 540×675). Run through `use_figma` with CONFIG filled in. One write, one page.
//
// What it does, in order:
//   1. finds the bars: VECTOR children whose path is a plain rectangle, sharing one x (the axis) and
//      one height; sorts them top to bottom into rows;
//   2. computes the band it may fill: [headerBottom + gapBelowHeader, footerTop - gapAboveFooter];
//   3. resizes each bar to barHeight and places row i at bandTop + inset + i * pitch, with
//      pitch = (band - 2*inset - barHeight) / (rows - 1);
//   4. moves every other non-skipped child (labels, flags, value texts) with its nearest row, adding
//      half the bar-height increase so it stays centred on the bar;
//   5. redraws the axis (the zero-width VECTOR) to span the band;
//   6. returns what it found and moved. Anything it could not assign to a row is listed under
//      `unmatched` — treat that as a stop, not a warning.
//
// It never touches text content, fonts or fills, and never resizes anything but the bars.

const CONFIG = {
  frameId: "<clone id>",       // the clone (already 540×675), e.g. "1234:5"
  headerBottom: 108,           // subtitle.y + subtitle.height, read after the header restyle
  footerTop: 623,              // 675 - 16 - 36 for the two-row Instagram footer
  gapBelowHeader: 8,           // subtitle bottom → axis top, MEASURED on the source frame
  gapAboveFooter: 10,          // axis bottom → footer top, MEASURED on the source frame
  inset: 7,                    // bar clearance from each end of the axis
  barHeight: 30,               // new bar height (was 24 on the DI)
  skipIds: [],                 // title, subtitle, logo, footer — anything that is not a chart row
  axisId: null,                // the zero-width VECTOR; found automatically when null
};

const frame = await figma.getNodeByIdAsync(CONFIG.frameId);
if (!frame) throw new Error("frame not found: " + CONFIG.frameId);
let page = frame; while (page.type !== "PAGE") page = page.parent;
await figma.setCurrentPageAsync(page);

const isRect = v => v.type === "VECTOR" && v.vectorPaths.length === 1 &&
  /^M 0 0 L [\d.]+ 0 L [\d.]+ [\d.]+ L 0 [\d.]+ L 0 0 Z$/.test(v.vectorPaths[0].data.trim());
const mode = arr => { const m = new Map(); for (const a of arr) m.set(a, (m.get(a) || 0) + 1);
  return [...m.entries()].sort((a, b) => b[1] - a[1])[0][0]; };

// 1. bars
const rects = frame.children.filter(isRect);
if (rects.length < 2) throw new Error("fewer than two rectangle vectors — not a bar chart I recognise");
const axisX = mode(rects.map(r => Math.round(r.x)));
const oldH = mode(rects.map(r => Math.round(r.height)));
const bars = rects.filter(r => Math.round(r.x) === axisX && Math.round(r.height) === oldH).sort((a, b) => a.y - b.y);
const oldBarY = bars.map(b => b.y);
const n = bars.length;

// 2. band
const bandTop = CONFIG.headerBottom + CONFIG.gapBelowHeader;
const bandBottom = CONFIG.footerTop - CONFIG.gapAboveFooter;
const band = bandBottom - bandTop;
const pitch = (band - 2 * CONFIG.inset - CONFIG.barHeight) / (n - 1);
const newBarY = oldBarY.map((_, i) => bandTop + CONFIG.inset + i * pitch);
const shift = (CONFIG.barHeight - oldH) / 2;

// 3 + 4. move
const skip = new Set(CONFIG.skipIds);
const barIds = new Set(bars.map(b => b.id));
const moved = [], unmatched = [];
let axis = CONFIG.axisId ? await figma.getNodeByIdAsync(CONFIG.axisId) : null;
const rowOf = c => {
  const cy = c.y + c.height / 2;
  let best = -1, bd = Infinity;
  oldBarY.forEach((by, i) => { const d = Math.abs(cy - (by + oldH / 2)); if (d < bd) { bd = d; best = i; } });
  return bd <= oldH ? best : -1;
};
const visit = c => {
  if (skip.has(c.id)) return;
  if (!axis && c.type === "VECTOR" && c.width < 1 && c.height > band * 0.5) { axis = c; return; }
  if (c.type === "GROUP" && !barIds.has(c.id) && c.children.some(k => k.type === "VECTOR" && k.width < 1)) {
    for (const k of c.children) visit(k); return;                       // the `axes` group
  }
  if (barIds.has(c.id)) {
    const i = bars.findIndex(b => b.id === c.id);
    c.resize(c.width, CONFIG.barHeight); c.y = newBarY[i];
    moved.push({ id: c.id, row: i, bar: true, y: +c.y.toFixed(1), h: c.height });
    return;
  }
  const i = rowOf(c);
  if (i < 0) { unmatched.push({ id: c.id, name: c.name, type: c.type, y: c.y, h: c.height }); return; }
  c.y = newBarY[i] + (c.y - oldBarY[i]) + shift;
  moved.push({ id: c.id, row: i, name: c.name.slice(0, 30), y: +c.y.toFixed(1) });
};
for (const c of frame.children) visit(c);

// 5. axis
if (axis) { axis.vectorPaths = [{ windingRule: "NONZERO", data: `M 0 0 L 0 ${band}` }]; axis.y = bandTop; }

return {
  mutatedNodeIds: [...moved.map(m => m.id), ...(axis ? [axis.id] : [])],
  rows: n, axisX, oldBarHeight: oldH, newBarHeight: CONFIG.barHeight,
  oldPitch: n > 1 ? +((oldBarY[n - 1] - oldBarY[0]) / (n - 1)).toFixed(1) : null,
  newPitch: +pitch.toFixed(1), band: { top: bandTop, bottom: bandBottom, height: band },
  axis: axis ? { id: axis.id, y: axis.y, h: axis.height } : "NOT FOUND",
  moved, unmatched,
};
