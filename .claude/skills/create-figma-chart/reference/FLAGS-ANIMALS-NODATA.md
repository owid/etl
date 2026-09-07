# Flags, animals, and the no-data pattern

> Read only when the chart carries country flags, animal icons, or a map with missing data.
> Part of [`/create-figma-chart`](../SKILL.md); [GUIDELINES.md](../GUIDELINES.md) has the rest
> of the design vocabulary.

## Flags, animals, no-data pattern

- **Flags** next to country labels or bars help when space allows (small multiples, ranked bars). Copy from the cheat sheet (Charts file node `2654:5`); flag height matches bar height; if a flag edge blends into the background (white stripes), add a **1px `#DBE5F0` outside stroke**. The Flags *plugin* the team uses is manual — and it has a known bug with the US flag's stars; the file provides correct US flags to copy.
- **Animals** (node `5336:5`): chicken, rooster, turkey, fish, cow, egg-laying hen, pig — for livestock/food topics.
- **"No data" hashed pattern — Figma drops it on import, and you can now restore it in script instead of by hand.** The imported no-data shapes arrive with an **empty `fills` array** (the SVG's `noDataPatternForMap` does not survive), which is why they render as nothing until someone paints them. The design team's manual route is the **Hero Patterns plugin** (instructions at node `4162:5`): select the no-data shapes — *each shape individually, not their group* — plus the legend's "No data" pill, pattern color `#C9C9C9`, diagonal stripes, tile 50%.

  What that produces is reproducible without the plugin: an **`IMAGE` fill, `scaleMode: "TILE"`, `scalingFactor` exactly `0.5`**, sourced from a **12×12 RGBA PNG** tile. **The 50% tile is the house value, not an approximation — assert it, don't inherit it.** Rescaling the map drifts the factor (a `rescale()` left every country at `0.508337` here while the legend pill stayed at `0.5`, so the hatch was fractionally coarser on the map than in its own key), and nothing warns you. After any map rescale, sweep every no-data shape *and* the legend pill and set `scalingFactor: 0.5` back; then confirm a single value across the whole set. So:
  - **Inside a file that already has it, copy the paint** — `target.fills = source.fills.map(f => ({...f}))`, looped over every no-data shape and the legend pill. This is the whole job, it needs no selection, and it inherits whatever tile scale the designer settled on, so re-imported or hand-added countries match instead of landing as flat grey.
  - **In a fresh yearly file, rebuild it from the saved tile** — `assets/no-data-hatch-tile.png` in this skill, inlined as bytes (`figma.createImage(data: Uint8Array)` is available; `createImageAsync` is not). The byte array is kept ready to paste in `assets/no-data-hatch-tile.bytes.js`, and the same tile as base64 in `assets/no-data-hatch-tile.b64.txt` — the plugin sandbox cannot read files, so the bytes have to travel inside the script:
    ```js
    const bytes = new Uint8Array([/* the 188 bytes of assets/no-data-hatch-tile.png */]);
    const img = figma.createImage(bytes);
    const hatch = { type: "IMAGE", imageHash: img.hash, scaleMode: "TILE", scalingFactor: 0.5, rotation: 0, opacity: 1 };
    for (const n of noDataGroup.children) n.fills = [hatch];
    ```
  - **Re-assert the tile scale after every rescale** — the same one-liner, cheap to run and easy to forget:
    ```js
    for (const n of [...noDataGroup.children, legendNoDataPill]) {
      const f = n.fills[0];
      if (f && f.type === "IMAGE" && f.scalingFactor !== 0.5) n.fills = [{ ...f, scalingFactor: 0.5 }];
    }
    ```
  - **Never flatten it to a solid `#C9C9C9`.** A flat grey reads as a real category rather than absence, and it will not match the shapes the designer has already patterned. Group the no-data shapes by fill signature (`imageHash` + `scaleMode` + `scalingFactor`) to confirm a single canonical value; two groups means the file has drifted, and the designer's version is the one to spread.
**The same hatch means "not observed", not only "no data".** The archive reuses it for projections and preliminary periods — `511:159` (a 2040 projection column), `355:154` (a remaining carbon budget), `223:5` (a decade with only partial years). In those uses it is the category's own color hatched, with a **dashed outline** rather than a solid one, and the note says why.
