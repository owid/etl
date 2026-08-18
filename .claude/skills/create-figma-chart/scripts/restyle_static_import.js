// Rebuild a static-viz frame's chart from a freshly imported SVG: place it, restyle it to OWID's
// fonts and palette, put every label back on its anchor, and keep an unstyled copy beside the frame.
//
// HOW TO USE
//   1. Render the step, then `upload_assets` TWO copies of each SVG — one to style, one to keep as the
//      untouched reference. Note each returned `placedOnNodeId`.
//   2. Fill in CONFIG below.
//   3. Paste this whole file as the `code` argument of one `use_figma` call.
//
// It is written to be pasted, not imported: `use_figma` runs a single script with the `figma` global,
// so there is no module system to require it from. Keep the edits to CONFIG so the pass itself stays
// the same one that has been verified.
//
// `node --check` will not pass on this file, and that is expected of every script here: `use_figma`
// bodies use top-level `await` (invalid in a script) *and* a top-level `return` (invalid in a module),
// so no extension satisfies both. Don't "fix" it by wrapping the body in a function — the return value
// is how the script reports back.
//
// WHY EACH STEP IS HERE (all of these were bugs first — see SKILL.md for the long version):
//   - an upload lands on the file's *current* page, so the node is fetched by id and moved, and the
//     page it came from is left clean;
//   - the import arrives as a FRAME sized to the SVG canvas (0.96x the template), so the rescale
//     factor is exact and independent of the ink's bbox;
//   - an opaque matplotlib figure/axes patch is stripped BEFORE anything else, because it is
//     frame-sized and hides the clone's background, logo and footer;
//   - the step's own copies of the template's text slots are dropped BY PREFIX, because a slot the
//     step had to emit as runs is `license-0 … license-5`;
//   - tints are derived from each family's base rather than listed, so a family keeps its internal
//     steps and the base can be swapped in one place;
//   - changing a face moves every label by half its width change, so the font pass is bracketed by an
//     anchor pass keyed on each node's own `textAlignHorizontal`;
//   - a line built from several runs is re-flowed afterwards, since independent boxes in a narrower
//     face leave the gaps between them uneven.

// ---------------------------------------------------------------------------
// CONFIG — the only part that changes per chart
// ---------------------------------------------------------------------------
const CONFIG = {
  pageId: "0:0", // the dated chart page
  jobs: [
    // One per frame. `styled` is consumed into the frame; `reference` is parked beside it.
    { frameId: "0:0", styled: "0:0", reference: "0:0", canvasWidth: 816, frameWidth: 850, referenceGap: 80, reflowLegend: false },
  ],
  // Each family is one palette base plus members at the tint weights the step gives them. Weight 0
  // means "the base itself". Style keys come from the Chart colors library (read them off a page that
  // already uses them — `Plugin / Bar charts` enumerates most of the palette).
  families: [
    {
      category: "example-category", // matches the step's `category__<slug>` gid
      base: "#d73c50",
      styleKey: "1c9f24e98b86e3823ee604c7d7b2ea6553124a8a", // OWID Distinct/Coral
      members: [["example-column", 0], ["example-other", 0.45]],
    },
  ],
  // A pale fill is unreadable as a name, so the step keeps a member's label at this share of its tint.
  labelTintFactor: 0.4,
  // House text styles, for the row labels and any column header.
  textStyles: { dark: "565a425eb6bd33b26d09f38a605dfa0ae1fd1e58", body: "5a7394f8beea1df86bd3ad44cd77182c6625f525" },
  // Which parents get the house text styles rather than a family color.
  bodyTextParent: /__(label|total-leisure)$/,
  darkTextParent: /^header__total-leisure/,
  slots: ["title", "subtitle", "note", "data-source", "tagline", "license"],
  // matplotlib's figure and axes background patches: the FIRST `patch_N` inside each of these
  // parents. A step on the current contract emits them as `fill: none`, so this finds nothing; an
  // older export fills them white and they land opaque over the template.
  backgroundPatch: /^patch_\d+$/,
  backgroundPatchParent: /^(figure|axes)_\d+$/,
};

// ---------------------------------------------------------------------------
const round = (v) => Math.round(v * 100) / 100;
const toRgb = (h) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16) / 255);
const toHex = (rgb) => "#" + rgb.map((c) => Math.round(c * 255).toString(16).padStart(2, "0")).join("");
const tint = (h, w) => toHex(toRgb(h).map((c) => c + (1 - c) * w));
const rgbOf = (h) => {
  const [r, g, b] = toRgb(h);
  return { r, g, b };
};

// Style binding goes through the async setters because that is the form SKILL.md's API note
// prescribes, and it is valid in every access mode. Measured in this file, on a bound mark, both forms
// work: `fillStyleId =` returned and read back correctly, and so did `setFillStyleIdAsync`. So prefer
// the async one on principle, not out of fear — if you see a pass die at its first bound mark, the
// cause is somewhere else.
//
// The stroke branch is for a member whose mark carries its color in `strokes` rather than `fills` —
// matplotlib writes line marks as `fill:none; stroke:…`, and such a node is invisible to a fills-only
// pass. Note it does not apply to `category__…-line1`, which is the *second line of a wrapped category
// name* (a TEXT node with fills), nor to the bracket rules: those are stroke-only VECTORs, but they are
// a neutral rule rather than a category color and are deliberately never selected here.
async function paint(node, hexColor, styleId) {
  const apply = async (n) => {
    if ("fills" in n && n.fills !== figma.mixed && Array.isArray(n.fills) && n.fills.length) {
      n.fills = [{ ...n.fills[0], type: "SOLID", color: rgbOf(hexColor) }];
      if (styleId) await n.setFillStyleIdAsync(styleId);
    } else if ("strokes" in n && n.strokes !== figma.mixed && Array.isArray(n.strokes) && n.strokes.length) {
      // Second in the chain, not alongside, so a filled bar keeps its own edge.
      n.strokes = [{ ...n.strokes[0], type: "SOLID", color: rgbOf(hexColor) }];
      if (styleId) await n.setStrokeStyleIdAsync(styleId);
    }
    if ("children" in n) for (const child of n.children) await apply(child);
  };
  await apply(node);
}

const page = figma.root.children.find((p) => p.id === CONFIG.pageId);
await figma.setCurrentPageAsync(page);
await figma.loadFontAsync({ family: "Lato", style: "Regular" });
await figma.loadFontAsync({ family: "Lato", style: "Bold" });

const styleIds = {};
for (const family of CONFIG.families) styleIds[family.base] = (await figma.importStyleByKeyAsync(family.styleKey)).id;
for (const [role, key] of Object.entries(CONFIG.textStyles)) styleIds[role] = (await figma.importStyleByKeyAsync(key)).id;

const report = [];
const landingPages = new Set();
for (const job of CONFIG.jobs) {
  const frame = page.children.find((f) => f.id === job.frameId);
  const styled = await figma.getNodeByIdAsync(job.styled);
  const reference = await figma.getNodeByIdAsync(job.reference);
  for (const node of [styled, reference]) {
    let ancestor = node;
    while (ancestor && ancestor.type !== "PAGE") ancestor = ancestor.parent;
    if (ancestor) landingPages.add(`${ancestor.id} ${ancestor.name}`);
    page.appendChild(node);
  }

  const scale = job.frameWidth / job.canvasWidth;

  // The untouched import, parked to the LEFT of the frame so the page reads original → edited in
  // reading order. (To its right it reads as an afterthought, and the eye lands on the raw export
  // last, when it is the thing you are comparing against.)
  reference.rescale(scale);
  reference.name = `${frame.name} — original SVG (unstyled)`;
  reference.x = frame.x - reference.width - job.referenceGap;
  reference.y = frame.y;

  styled.rescale(scale);

  // The background goes first, because it is the one that ruins the page: an opaque figure patch is
  // frame-sized and lands *above* the clone's cream background, logo and footer, hiding all three.
  // The patches are their own groups, so dropping the duplicate text below does not uncover them.
  //
  // Three things this has to get right, and each was wrong in an earlier version:
  //   - matplotlib writes a patch as `<g id="patch_1"><path/></g>`, so it imports as a GROUP, which
  //     has no `fills` of its own (see SKILL.md → Gotchas). The paint is on the descendant vector, so
  //     that is what decides whether the patch covers anything.
  //   - only a *fill* counts. The spine group is a `patch_N` too and is stroke-only, so testing fills
  //     rather than any paint leaves it alone.
  //   - the number says nothing: a bar chart's rectangles are `patch_N` groups parented to the axes
  //     just like the background is. What identifies the background is position — matplotlib emits the
  //     figure and axes patch *before* any data artist — so only the first patch of `figure_N`/`axes_N`
  //     is a candidate. Match on the number alone and a pass deletes the chart's marks.
  const strippedPatches = [];
  const fillsOf = (n) => ("fills" in n && n.fills !== figma.mixed && Array.isArray(n.fills) ? n.fills : []);
  const painted = (n) =>
    fillsOf(n).some((f) => f.visible !== false && (f.opacity === undefined || f.opacity > 0)) ||
    ("children" in n && n.children.some(painted));
  for (const parent of styled.findAll((n) => CONFIG.backgroundPatchParent.test(n.name))) {
    const first = ("children" in parent ? parent.children : []).find((c) => CONFIG.backgroundPatch.test(c.name));
    if (!first || first.removed || !painted(first)) continue;
    strippedPatches.push(`${parent.name}/${first.name}`);
    first.remove();
  }

  for (const slot of CONFIG.slots) {
    for (const node of styled.findAll((n) => n.name === slot || n.name.startsWith(slot + "-"))) node.remove();
  }

  for (const family of CONFIG.families) {
    for (const [column, weight] of family.members) {
      const fill = weight ? tint(family.base, weight) : family.base;
      const label = weight ? tint(family.base, weight * CONFIG.labelTintFactor) : family.base;
      for (const node of styled.findAll((n) => n.name.endsWith("__" + column) && !/^(header|category)__/.test(n.name))) {
        await paint(node, fill, weight ? null : styleIds[family.base]);
      }
      for (const node of styled.findAll((n) => n.name === "header__" + column)) {
        await paint(node, label, weight ? null : styleIds[family.base]);
      }
    }
    for (const node of styled.findAll(
      (n) => n.name === "category__" + family.category || n.name.startsWith("category__" + family.category + "-line")
    )) {
      await paint(node, family.base, styleIds[family.base]);
    }
  }

  // Fonts, bracketed by the anchor pass.
  const texts = styled.findAll((n) => n.type === "TEXT");
  const anchors = texts.map((node) => {
    const box = node.absoluteBoundingBox;
    return { node, align: node.textAlignHorizontal, left: box.x, right: box.x + box.width, center: box.x + box.width / 2 };
  });
  const fonts = new Set();
  for (const node of texts) {
    for (const segment of node.getStyledTextSegments(["fontName"])) fonts.add(JSON.stringify(segment.fontName));
  }
  for (const font of fonts) await figma.loadFontAsync(JSON.parse(font));
  for (const node of texts) {
    for (const segment of node.getStyledTextSegments(["fontName"])) {
      const bold = /bold|black|heavy/i.test(segment.fontName.style);
      node.setRangeFontName(segment.start, segment.end, { family: "Lato", style: bold ? "Bold" : "Regular" });
    }
  }
  let recentred = 0;
  for (const record of anchors) {
    const box = record.node.absoluteBoundingBox;
    const target =
      record.align === "CENTER"
        ? record.center - box.width / 2
        : record.align === "RIGHT"
          ? record.right - box.width
          : record.left;
    if (Math.abs(box.x - target) > 0.01) {
      record.node.x += target - box.x;
      recentred++;
    }
  }

  for (const node of styled.findAll((n) => n.type === "TEXT")) {
    const parent = node.parent ? node.parent.name : "";
    if (CONFIG.bodyTextParent.test(parent) || CONFIG.darkTextParent.test(parent)) {
      const dark = CONFIG.darkTextParent.test(parent);
      await node.setFillStyleIdAsync(styleIds[dark ? "dark" : "body"]);
    }
  }

  const old = frame.children.find((c) => c.name === "chart");
  styled.name = "chart";
  styled.clipsContent = false;
  frame.appendChild(styled);
  styled.x = 0;
  styled.y = 0;
  if (old) old.remove();

  // A flowed line of runs needs laying out again, one measured space apart. Rows are keyed on y
  // alone because each run of one line sits under its own `header__…`/`category__…` parent, so
  // keying on the parent would put every run in a row of its own and reflow nothing. That makes
  // "the selected runs form one line per y" a precondition, which is why this is per-job opt-in.
  let reflowed = 0;
  if (job.reflowLegend) {
    const runs = styled.findAll(
      (n) => n.type === "TEXT" && /^(header|category)__/.test(n.parent.name) && !CONFIG.darkTextParent.test(n.parent.name)
    );
    if (runs.length) {
      const probe = figma.createText();
      styled.appendChild(probe);
      probe.fontName = { family: "Lato", style: "Regular" };
      probe.fontSize = runs[0].fontSize;
      probe.textAutoResize = "WIDTH_AND_HEIGHT";
      probe.characters = "nn";
      const tight = probe.width;
      probe.characters = "n n";
      const space = probe.width - tight;
      probe.remove();

      const rows = new Map();
      for (const node of runs) {
        const key = Math.round((node.absoluteBoundingBox.y - frame.y) * 2) / 2;
        if (!rows.has(key)) rows.set(key, []);
        rows.get(key).push(node);
      }
      for (const [, nodes] of rows) {
        nodes.sort((a, b) => a.absoluteBoundingBox.x - b.absoluteBoundingBox.x);
        let cursor = nodes[0].absoluteBoundingBox.x;
        for (const node of nodes) {
          node.x += cursor - node.absoluteBoundingBox.x;
          cursor += node.absoluteBoundingBox.width + space;
          reflowed++;
        }
      }
    }
  }

  report.push({
    frame: frame.name,
    size: [round(styled.width), round(styled.height)],
    strippedPatches,
    recentred,
    reflowed,
    reference: reference.name,
  });
}

// Anything of ours still sitting on the page the uploads landed on is litter in a shared file.
return { report, landingPages: [...landingPages], reminder: "check the landing pages listed above are clean" };
