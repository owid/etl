# Gotchas

> Read on an error, or grep it by symptom. Worth skimming once before your first `use_figma` call.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


- **`page.children` on a page you have NOT switched to is lazily loaded, and returns a short list without erroring.** This is the nastiest read in the skill, because it looks like evidence and it is used for exactly the check that matters: "did I leave litter on someone else's page?" The same page reported **4 children while it was current and 2 later without a switch** — which read as "I deleted two of a colleague's frames" in a shared design file. Nothing had been deleted. So: any claim about a page's contents needs `await figma.setCurrentPageAsync(page)` first (that is what loads it), and since a script may switch pages only once, checking N pages means N calls, fanned out in parallel. Never report a page as clean *or* as damaged from an unswitched read — and if you catch yourself about to tell the user you removed something, re-read it properly first.
- **`figma.currentPage` resets to the file's FIRST page at the start of every `use_figma` call — it does not persist between calls, and `upload_assets` does not follow it.** Measured 2026-08-21: a call ending in `setCurrentPageAsync(<working page>)` was followed by one whose first statement reported `figma.currentPage.name === "Cover"`. Two consequences worth holding onto. **A script must never require a particular page to be current on entry** — `diff_against_template.js` used to throw "open the template's page in Figma, then re-run", which is unsatisfiable from a session and made it unrunnable; it now reads the template unswitched instead. **`PageNode.loadAsync()` is the way to do that honestly** — it loads a page's contents *without* switching to it, which is the half of `setCurrentPageAsync` that matters and the half you are free to spend on a second page. Gating on "the read looks complete" is only the fallback for where `loadAsync` is unavailable, and it is an inference, not a proof: the short list in the point above was *nonempty*, so a resolved header with rows in it does not prove the rest of the subtree arrived. And **`upload_assets` places its asset on whatever page the FILE has open**, which is a third thing again: on one run three consecutive uploads all landed on the working page even though every script had started on Cover. So log the `placedOnNodeId`'s PAGE ancestor on every upload rather than predicting it, and reparent unconditionally.
- **A sub-pixel stroke is invisible at 1:1 and solid in the export — so never judge one on a 540px preview.** A map leader at the prescribed 0.3px measured **1.26:1** against white in a 540px render (the line spreads over two pixels at ~12% coverage each) and **4.2–6.6:1** at 4×, which is the scale a DI is exported at. The first number reads as a defect and is an artefact. To measure at export scale you need a real 4× render, and neither obvious route gives you one: `get_screenshot`'s `maxDimension` only ever *downscales* and clamps at the node's natural size, so a 540 frame returns 540px however large a number you pass, and `node.screenshot({scale: 4})` returns an inline image you cannot pixel-probe. Clone the frame, `rescale(4)`, `get_screenshot` the clone at `maxDimension: 2160` (its natural size is now 2160, so the clamp allows it), measure the PNG, then delete the clone. Same trick for any hairline, dot or dash you are asked about.
- **`get_screenshot` on a PAGE id can include nodes from other pages.** A page render came back 2860px wide for a page whose two nodes span 1149px, with a neighbouring page's frames in the image. Screenshot the frames you care about individually and compose the comparison yourself; use the page render only for a rough look, and never to decide what a page contains (see the previous point for the reliable way).
- **`get_metadata` page listing lies — and now quantified: it reports ONE page where the Charts file has 198.** Called with no `nodeId` the hosted connector answered `- 778:2684: Cover` and stopped, with no error, no truncation notice and nothing to suggest 197 pages were missing (measured 2026-08-24; it returned only "Cover" for the Guidelines file too). Same lazy loading as `page.children` above, one level up: it sees the loaded page. **Enumerate pages via `use_figma` → `figma.root.children`** — that returned all 198 in one call — and access known nodes directly by id. Two refinements worth having. *Scoped to a node it is fine*: asked for `798:161`, the hosted connector and the desktop server returned byte-identical XML in well under a second, so this is a document-level listing bug, not a reason to distrust `get_metadata` on a frame. And *the desktop server does not have the bug* — the same no-`nodeId` call there listed all 198 pages. This is also the diagnosis of what was previously filed as a size-related `get_metadata` failure on this file: it is not size, and it does not fail; it under-reports.
- **And its node tree is lossy: a childless-looking frame usually isn't.** Every bar segment whose group held only a fill vector and no value label came back as an empty `<frame …/>`, while segments with both were listed in full — so reading the XML alone would say the small segments have no bar drawn at all. The tell is in the ids: consecutive siblings numbered `…494` and `…496` have a `495` that was dropped. Use it for structure and names, and confirm anything you intend to *assert* (a missing label, an unpainted mark) with a `use_figma` read.
- **An empty `fills` array is NOT a reliable marker for "no-data shape".** It is the marker the no-data hatch rule leans on, and it over-matches: grapher's map export also contains an invisible `swatch-hit-areas` group — full-size rectangles over each legend bin, with no fill, there for mouse targeting. A blanket "every empty-fill vector gets the hatch" pass therefore painted diagonal stripes across all three legend bins while correctly hatching one country. Scope the sweep by parent instead — `countries-without-data` for the map and the legend's own `swatches` group for the key — and hide `swatch-hit-areas` outright, since a static image has nothing to target.
- **A path with negative coordinates needs `x`/`y` at its bounding-box minimum, not at its first vertex.** Figma normalizes a vector's bbox, so `M 0 0 L 24 -104` assigned `v.y = startY` puts the box's *top* at `startY` and the line draws downward — the opposite of the intent. One leader aimed up at Chad ran down through the legend into the footer instead. Compute `min(y1,y2)` and offset the path data by it (snippet in [ANNOTATIONS-AND-ARROWS.md](ANNOTATIONS-AND-ARROWS.md) → Straight elbowed arrows).
- **`useColumnShortNames` suffixes every CSV column with the chart's slug, and the slug contains the other series' names.** On `elec-fossil-nuclear-renewables` every column ends `..._chart_elec_fossil_nuclear_renewables`, so `next(c for c in cols if "nuclear" in c)` returns the **fossil** column and every share you compute is wrong — in a way that looks plausible (61.7% "nuclear" in 1985). Match on the prefix (`c.startswith("nuclear_")`), and sanity-check one number against the rendered chart before writing it into an annotation.
- **On a rotated node, `x`/`y`/`width`/`height` are NOT the visual bounding box — and the curvy arrows are all rotated.** Their `x` is the untransformed origin, so an arrow group reporting `x: 534.3, width: 29.6` in a 560-wide frame actually paints at `494.9`, 40px wide. Everything downstream inherits the error silently: a pixel probe over that box found *zero* arrow pixels, and — worse — the path→frame mapping used everywhere in this skill (normalize `vectorPaths` numbers by the path bbox, scale onto `node.width/height`) is only valid when rotation is 0, so every arrow-to-line distance measured that way was fiction. Use `absoluteBoundingBox` minus the frame's for anything that might be rotated, and check `node.rotation` before trusting a bbox-normalized path mapping. Imported chart geometry (lines, bars, ticks) is unrotated, which is why the mapping works there.
- **`clone()` copies a node's own transform and drops its parents' — and `rotation` won't tell you.** Assets in the finished pages sit inside groups that are themselves mirrored or rotated, so a cloned child arrives with the group's half of the orientation missing: a solid arrowhead renders as a hollow chevron. The `rotation` getter is no help, because for a mirrored node it reports the un-mirrored angle — the source read `169.9` and its own clone read `10.1`, and only `absoluteTransform` (linear part `[[-1,0],[0,1]]` on the parent group) showed the flip. When cloning out of a group, set `clone.relativeTransform` from the source's `absoluteTransform` linear part, then translate. And seat the result by a **transformed vertex**, not the bbox: for a rotated shape the visual tip is a couple of pixels off the box centre in both axes, which is the difference between an arrow that touches its target and one that looks detached.
- **Per-vertex `strokeCap` needs `setVectorNetworkAsync`; the node-level `strokeCap` caps both ends.** A leader that ends in a dot — the house treatment on maps — is one `CIRCLE_FILLED` vertex and the rest `NONE`, which `vectorPaths` cannot express. Build the whole path as a network (`vertices` + `segments`, `regions: []`), and re-assert `x`/`y` afterwards, since the call can re-origin the node.
- **Rewriting `vectorPaths` to drop subpaths moves the node.** Trimming Hawaii out of the US shape, or an antimeridian half out of Fiji, changes the geometry's bbox and Figma re-origins the vector — so the surviving shape lands somewhere else on the map. Compute the kept subpaths' union *before* the write and correct `x`/`y` after it (snippet in `reference/per-chart-type/maps.md`).
- **The plot's edge is where the gridlines stop, not where the last tick label sits.** Grapher insets the first and last x-axis labels so they don't clip — ~17px on a 540px frame — so a year→x mapping fitted through them is wrong everywhere, and "point at the last year" aimed at the label centre lands well inside the plot. Fit on interior ticks (residuals ±0.1px; the two edge labels appear as equal opposite outliers) and take the plot extent from the `horizontal-grid-lines` boxes. Note the group is **plural**: an equality test against `"horizontal-grid-line"` matches nothing, and `Math.max(...[])` then yields `-Infinity`, which surfaces as `Invalid command at Infinity` from `set_vectorPaths` rather than as an empty-selection error.
- **A comma in the upload filename silently loses the asset.** `upload_assets` names the layer from the multipart filename, and a POST of `…(original, with World).svg` returned `{"success":true}` with a `placedOnNodeId` — but no such node existed and only the *other* upload had landed. Keep upload filenames free of commas (parentheses are fine), then rename the node in Figma. And **verify after every batch**: list the page's children and count them, rather than trusting N success responses.
- **Local file styles cannot be imported by key; library styles cannot be applied by id.** The two kinds look identical in a harvest and need opposite handling. `Data Insights/*` and `Instagram/*` are **local** to the Charts file — `importStyleByKeyAsync` throws `Style with key "…" not found`, and you apply them by passing the id straight through (`"S:e06b99…,"`, note the trailing comma). `Default Palette/*` and `Line and Slope Charts/*` come from the **[Chart Colors] Library** and must be imported by key first. Tell them apart by the id shape: a library style's id carries a node suffix (`S:28466fa…,2401:49`), a local one ends at the comma. Get every local id in one call with `figma.getLocalPaintStylesAsync()` / `getLocalTextStylesAsync()`; get library keys from `search_design_system` — and note that a query for the *group* name (`"Default Palette"`, `"Line and Slope Charts"`) is far cheaper than one query per color, **but it is a partial harvest, not an enumeration**: the call caps at ~14 results (gotcha below) while the Default Palette alone runs to 24 fills plus `Gray` (GUIDELINES.md → Colors). Take what it returns, then query the colors still missing by name.
- **Load the fonts you are about to *write*, not only the ones already on the node.** Scanning `getStyledTextSegments(['fontName'])` over the imported chart loads what the export used — and then `label.fontName = {family:"Lato", style:"Bold"}` throws, because nothing in the chart was bold. Two variants of the same trap: `set_fontSize` also throws on a node that merely *contains* an unloaded weight (a template's `Data source:` line is Bold + Regular), so a size sweep over template text needs both weights loaded. Load `Lato Regular`, `Lato Bold` and `Playfair Display SemiBold` unconditionally at the top of any script that touches text.
- **A text node carries a *scaled* `strokeWeight` after `rescale()`, even with no strokes.** So adding the tier-2 white outside stroke to an annotation on a chart you height-fitted gives a 0.65px halo unless you set the weight — and a sub-pixel halo is indistinguishable from none, which reads as "the knockout didn't work" rather than "the weight is wrong". Set `strokeWeight = 3` explicitly and read it back.
- **A hugging annotation frame clips its own descenders — a tier-3-only trap, and a reason to prefer tier 2.** Frames have `clipsContent = true` by default, and `leadingTrim = "CAP_HEIGHT"` puts the baseline *at* the box bottom — so every descender is cut and "today" renders as "todav", "very" as "verv". It is invisible in a node listing and easy to miss in a thumbnail. Set `box.clipsContent = false` on every annotation frame you create; keep the trim. Clipping is only half of it — the opaque fill still stops at the baseline, so pair it with `paddingBottom ≈ 0.22 × the last line's font size` or the recovered descenders sit outside their own knockout. A bare text node with an outside stroke has neither failure, which is most of why [ANNOTATIONS-AND-ARROWS.md](ANNOTATIONS-AND-ARROWS.md) makes the stroke the default.
- **`entity-labels` children are not always TEXT.** When a bar's entity name wraps, grapher groups the two lines, so `node.fontSize = 14` throws `no such property 'fontSize' on GROUP` — and because `use_figma` is atomic you lose the whole pass. Iterate `group.query("TEXT")` for styling and `group.children` for per-row layout.
- **Exceeding the concurrency ceiling inflates every call in the batch, silently.** The pass that wrote GUIDELINES.md screenshotted 272 chart-library nodes at **peak 14 in flight**, and its calls averaged **35.5 s** against ~10 s everywhere else. That is the ceiling being exceeded, not the connector being slow: 8.2 s at one in flight, 13.2 s at eight queued, 35.5 s at fourteen.

- **A manifest row longer than the ceiling splits across messages — the ceiling wins.** SKILL.md's batch manifest says to issue each row's calls in one message, but that instruction exists to stop a row being dribbled out one call per turn; it is not licence to exceed 4–6 in flight. Where a row is longer than the ceiling, send it as consecutive batches of 4–6 in a row of messages, and never as one oversized message. The palette harvest is the row this actually bites — `search_design_system` caps at ~14 results against a 24-fill palette, so it needs about twelve queries, which is two or three batches rather than one. A page survey covering many pages is the other candidate. Batching all twelve at once would land squarely in the regime measured above, where every call in the batch pays for the queue.

- **A calls-per-message histogram cannot tell you whether calls were batched — use interval overlap.** The transcript writes one entry per tool call whether or not the calls were batched, so a calls-per-assistant-message histogram reports `{1: N}` for a provably concurrent run — checked against an 8-call probe that measured 4.12×, which the histogram scored as eight singletons. Sweep `tool_use` → `tool_result` timestamps for peak simultaneous in-flight calls instead, and count how many calls start before the previous one finished.

- **`use_figma` calls do not run concurrently — plugin runs serialize per file, so batching them
  buys only the shared turn.** Measured locally on an identical read-only script (count the file's
  198 pages, no page switch), batches of four against a serial arm run before *and* after them:

  | | per call | wall for 4 | against 4× serial |
  |---|---|---|---|
  | one call per message (n=11 over two runs) | 3.49–5.75 s, mean **4.4 s** | 17.5–17.8 s | — |
  | four in one message — four such batches | 3.83–17.86 s | **16.67–21.50 s** | **0.82–1.05×** |

  Four in flight every time, and the wall is `4 × 4.4 s` plus overhead — the shape of a queue, not of
  parallelism. **The completion timestamps are the proof:** inside a batch the calls finish one every
  ~3.8–4.7 s, each gap a whole serial execution, having all been dispatched within ~4 s of one
  another. Client-side overlap is not server-side concurrency — the calls all show as in flight
  while the file's single plugin context runs them one at a time. So a batch's `sum/wall` (2.0–2.6×)
  is pure artifact: the honest figure straddles 1.0×, meaning **the server time does not compress at
  all**.

  Two runs, four batches, and the spread is worth reading honestly: 0.82× and 0.83× and 0.83× against
  one 1.05×. The high one was the *second* batch of its session and opened with a 3.83 s call where
  the cold ones opened at ~9.6 s, so some of the loss is a one-off warm-up rather than a property of
  batching. Do not read a reliable 17% penalty into it; read "no gain on the calls".

  **This does not mean stop batching writes** — a call costs the turn *and* the hop, and only the hop
  serializes, so batching still collects every turn gap. Measured end to end including turns: four
  calls one per message took **29.89 s** against **16.67 s** batched, **1.79×**, on a session whose
  turns ran 3.7 s. In a cloud session the turn is an even larger share of the cost — because the
  *call* is cheap there, not because the turn is dear — so batching pays *more*, which the block
  below now measures at **2.26×**. The serialization is a property of the file's plugin context and
  does not change. What does change is the stopping rule: an extra `use_figma` in a batch costs
  roughly a **whole call** (measured 4.10 s against a 4.37 s serial mean, within 6%), not the
  0.75–2.1 s an extra `get_screenshot` costs. So collapsing work into one bigger script — which is
  free, script size doesn't move the latency — beats spreading it across a batch every time.

  Two caveats on the numbers. A batched call's *own* duration is queue-inclusive, so only the fastest
  call in a batch approximates real server time; and all of this is one file, hence one plugin
  context — a batch spanning *different* files might genuinely parallelize, untested. Still untested:
  mutating scripts and heavier scripts.

  **The cloud case is now measured, and it changes the arithmetic without changing the advice.** Same
  four-arm probe from a verified cloud sandbox: serial `use_figma` **0.701 s** (n=6, σ 0.10) — six
  times cheaper than local — and a turn of **2.79 s**, so the *call* is cheap and the *turn* is most
  of the cost. Serialization is confirmed far more sharply than locally: a single-server queue model
  (a call cannot start until the previous finishes) reproduces every batched duration **to the
  millisecond**, and `sum/wall` never exceeds 1.14. Arm A's four ~6 s calls turned out to be one
  **5.9 s cold start** — plugin context plus the 198-page file load — that all four queued behind,
  then a 0.65–1.17 s staircase. So batch **or** don't, that cold start is paid once per session.

  Honest speedup on the calls is *worse* in the cloud (0.30× and 0.56×) precisely because the calls
  are so cheap that overhead dominates — and the overhead is the interesting part: **a cloud batch's
  wall is mostly the model emitting the tool calls, not Figma running them.** One four-call batch
  spent **3.9 s of its 5.0 s** wall on dispatch spread. Two consequences: keep batched call payloads
  terse (short scripts, short `description`s), and read a batch's own `sum/wall` as meaningless here.
  End to end including turns, batching still wins and wins bigger than locally — **2.26× against
  1.79×** on the matched framing (arm wall to arm wall; the same cloud data reads as 1.79× if you
  charge the batch its entry turn, which is how much the framing matters — pin it before comparing).
  At the margin an extra call costs **1.44 s inside a batch against 3.49 s as its own turn**, so
  batch it.

- **`sum/wall` is not the speedup — measure a batch against a *serial arm run in the same session*.**
  A queued call's own duration includes the time it spent waiting, so summing the durations inside a
  batch and dividing by the wall clock counts that wait as work and flatters batching. An independent
  local replication (six-call fixed probe, peak 6 of 6 in flight, Figma's desktop app running):
  batch wall **25.85 s** where four one-per-message calls averaged **12.10 s** — so **2.81×** honestly,
  against the **3.81×** the same batch scores on `sum/wall`. That reproduces the ~3.84× `sum/wall`
  figure in the budget almost exactly while putting the real local gain at **2.8–3.2×**, and it
  reproduces the mechanism too: per-call latency inflated to 11.5–21.3 s inside the batch against
  10.8–13.9 s serial, and the completions ended over a **14.35 s** spread having been dispatched over
  **4.57 s** — a local session pipelines rather than parallelizing. Batching still wins by a wide
  margin; just don't quote `sum/wall` as the win.

- **Only the *reading* tools are metered, so a screenshot-heavy run is the one that can hit a wall.**
  Figma's [rate limits](https://developers.figma.com/docs/figma-mcp-server/rate-limits-access/)
  (checked 2026-08-24) apply "to Figma MCP server tools that read data from Figma" — a per-day and a
  per-minute cap, both varying by plan and seat, which is why the numbers are not copied here. Writes
  are not documented as counting, so `use_figma` and `upload_assets` are effectively free of the
  quota while `get_screenshot` and `get_metadata` spend it: a survey of N nodes is N against the cap,
  and the surveys are what to trim if a run ever gets throttled. Two things to expect if it does —
  the message is a plain refusal with no retry hint from the connector, and forum reports have the
  limiter misreading the seat, so a refusal is not proof the quota was really spent. Figma has also
  signposted `use_figma` becoming "a usage-based paid feature", which would change this arithmetic.

- **For a *bulk* render, one Figma REST call replaces N `get_screenshot`s — but only for settled
  state.** `GET https://api.figma.com/v1/images/<fileKey>?ids=<id1>,<id2>,…&format=png&scale=<n>`
  renders **many nodes in one request** and returns a URL per node, where `get_screenshot` is one
  node per 8–20 s call; it also takes `scale` up to 4, which `get_screenshot` cannot do at all
  (`maxDimension` only downscales). It needs a personal access token in `X-Figma-Token`, which this
  skill does not carry — the grapher server holds one as `FIGMA_API_KEY` for its own `/api/figma/image`
  route, so an ETL-side use means asking for a token rather than reusing that. Untested from here;
  treat the note as a lead, not a recipe, and check a render against a `get_screenshot` of the same
  node before trusting a batch of them. **Never point it at a node you just wrote to.** The pixel
  probes read state that a `use_figma` call created moments earlier, and a server-side render that
  lags by a beat returns a plausible-looking image of the *old* state — the same silent-wrong-state
  failure as batching the four-render arrow protocol. Bulk-render surveys and delivery renders of
  finished frames; keep every post-write probe on `get_screenshot`. It also returns no
  `original_width`/`original_height`, so the size-only survey still wants `get_screenshot`.

- **The Figma *desktop* MCP server cannot do the writes — but it serves the reads ~30× faster, and
  on a local run that is the biggest lever there is.** Two different servers are in play: the
  hosted connector this skill normally uses, and one the desktop app serves on
  `http://127.0.0.1:3845/mcp`. Enumerated live (2026-08-24) the desktop one offers **six tools,
  all `readOnlyHint: true`** — `get_screenshot`, `get_metadata`, `get_design_context`,
  `get_variable_defs`, `get_figjam`, `get_motion_context` — and **nothing this skill writes with**:
  no `use_figma`, `upload_assets`, `search_design_system` or `download_assets`. That also settles
  which server a session is on: if `use_figma` is in your tool list, you are on the hosted
  connector and the desktop app is not in that path at all. So the desktop app cannot be the
  reason hosted calls are slower locally — a hypothesis the budget section used to float.

  What it *can* do is every read, at a different order of magnitude. Same six template nodes, same
  session, measured both ways:

  | | per call | six calls |
  |---|---|---|
  | hosted connector | 10.8–13.9 s (mean 12.10 s) | 25.85 s batched |
  | desktop server | **0.27–0.60 s** (mean 0.37 s) | **0.51 s**, six fired at once |

  Roughly **33× per call and 50× on a batch**, and it *does* parallelize where the hosted
  connector queues. Renders verified genuine, not placeholders: `798:161` came back a real
  540×540 PNG of the template. Four constraints, all load-bearing:

  1. **No `fileKey` parameter** (`additionalProperties: false`) — it renders from whatever document
     is the **active tab** in the running app. Its own error says so: *"Make sure the Figma desktop
     app is open and the document containing the node is the active tab."* So it cannot touch an
     arbitrary file, and a cloud session cannot use it at all.
  2. **No `maxDimension`, and a silent 1024 px cap on the longer edge.** You cannot ask for a 256 px
     thumbnail, and you do not reliably get natural size either: the Reel template
     (`7336:8`, natural 616×1096) came back **576×1024**, scaled to fit. Anything whose longer edge
     is ≤ 1024 does arrive at natural size — the four 540×540 templates and the 850×638 horizontal
     all did, matching NODE-MAP.md exactly.
  3. **The response is an inline base64 PNG**, with none of the hosted tool's
     `original_width`/`original_height` JSON. Read the dimensions out of the PNG header instead
     (big-endian uint32 pair at byte 16) — but per the cap above that is the *rendered* size, so it
     is only the natural size below 1024 px. **For a size-only survey of anything taller or wider
     than that, stay on the hosted `get_screenshot`**, which reports the true natural size whatever
     it renders.
  4. **Claude Code is not connected to it.** Either add it
     (`claude mcp add --transport http figma-desktop http://127.0.0.1:3845/mcp`) or call it from
     Bash over JSON-RPC, which needs no config change —
     `scripts/figma_desktop_read.py` does the handshake, fans out the calls and writes the PNGs.
     One handshake serves many calls, so amortize it.

  Metering is the open question: these calls never reach the hosted connector, and Figma documents
  no desktop exemption either way. Untested in a real chart build.

- **Downloading N screenshot URLs: parallelize, and pair each URL with its own output file.** Six
  serially is 2.7 s through a cloud sandbox's egress proxy, 0.8 s in parallel:

  ```bash
  printf '%s %s\n' "$U1" 1.png "$U2" 2.png | xargs -P6 -n2 sh -c 'curl -sSL -o "$2" "$1"' _
  ```

  **Don't use `-I{}` with a single `-o`:** `{}` expands only in the URL, so every parallel `curl`
  writes the same file and you Read one screenshot six times — a wrong verdict from a real render,
  which is the worst shape of bug this skill has. Same trap as the `upload_assets` pattern below,
  and the same fix. Locally you can skip this leg entirely: `scripts/figma_desktop_read.py shot`
  returns the PNGs directly, no URLs to fetch (see the desktop-server entry above).

- **`upload_assets` gives you N `submitUrl`s so the POSTs can overlap — run them that way.** The
  Step 5 snippet shows one `curl`, and a two-format run that copies it twice pays two full uploads of
  a ~165 KB SVG back to back. Pair each URL with its file and fan them out, exactly as the screenshot
  downloads do:

  ```bash
  printf '%s %s\n' "$URL1" "$DIR/original.svg" "$URL2" "$DIR/original-square.svg" \
    | xargs -P4 -n2 sh -c 'curl -s -X POST "$1" -F "file=@$2;type=image/svg+xml"' _
  ```

  Two ways to get this wrong, both silent. `xargs -I{}` with a single `-F` uploads the *same file*
  to every URL, because `{}` expands only in the URL — the same trap as the screenshot downloads. And
  with a trailing `_`, `sh -c` binds `$0` to the `_`, so the pair arrives as `$1` and `$2`: a snippet
  reading `$0` posts to nothing. Expand the paths in the `printf` rather than referencing `$DIR`
  inside the single quotes, where the child shell never sees it. Keep filenames comma-free (see above), and verify by counting the page's children
  rather than trusting N `success` responses.

- **`upload_assets`, never `createNodeFromSvg`** — the plugin sandbox has no `fetch`, and inlining an SVG into `use_figma` blows the 50k-char cap. `upload_assets` handles up to 10 MB and yields an editable vector tree.
- **`rescale()`, never `resize()`** on imported charts — `resize` crops instead of scaling children.
- **Figma plugins can't be run from here — but the no-data hatch no longer needs one.** Imported no-data shapes arrive with an **empty `fills` array**, and the hatch the design team applies by hand is just an `IMAGE` fill, `scaleMode: "TILE"`, `scalingFactor ≈ 0.5` from a 12×12 tile. Reproduce it by copying `fills` from a shape that already has it, or rebuild it from `assets/no-data-hatch-tile.png` via `figma.createImage(bytes)` — and apply it to **every** no-data shape *and* the legend's "No data" pill, never a flat `#C9C9C9` ([FLAGS-ANIMALS-NODATA.md](FLAGS-ANIMALS-NODATA.md)). The Flags plugin (`2654:5`) is still manual.
- **Fonts**: every text edit needs `loadFontAsync` first; the templates use Playfair Display and Lato — if a font is missing in the user's Figma, text edits throw.
- **Before blaming a stale width, check that your setter changed anything.** Assigning a property the value it already holds is a no-op, and the unchanged geometry then looks exactly like the pinning below — so the diagnosis goes to the wrong place while the real cause sits untouched. Read the property back (`getStyledTextSegments(['fontSize', 'fontName'])` for text) and compare against what you meant to set.
- **A text node pinned to a fixed width does not resize when you set its `characters`, and that reads exactly like a stale width.** SVG-imported text arrives **fixed** — the clone of a `22px` value label stays 22px wide and wraps "Poultry" onto two lines — so every width you read back is the old one and any layout computed from them lands wrong. **This silently invalidates a placement search**, which is the worst version of it: the candidate rectangles are built from the wrong widths, every collision test passes, and the script reports `forced: 0` while the labels sit on top of each other on the canvas. A search that returns a suspiciously clean result on a crowded chart is the tell. **The fix is the sizing mode, not a second call:** set `textAutoResize = "WIDTH_AND_HEIGHT"` *before* measuring. Measured 2026-08-18 on both a fresh node and a clone of an imported one: while a node is auto-resizing, `characters` → `width` reads true in the **same** call, on every ordering and across repeated reassignment (`71 → 233 → 12`), and releasing a pinned node's sizing mode updates it in that same call too (`71 → 233`). A node left at `textAutoResize = "NONE"` holds its 71 px however long the string gets — that is the pinning, not a cache, so don't spend a second call waiting for it to settle.
- **`imType=square` and `imType=uncaptioned` don't render the same chart.** The square re-layout drops per-segment value labels that the uncaptioned crop keeps (and the uncaptioned crop keeps the legend, which is inside the chart area, not the header). Export both and look before deciding which one to embed.
- **`/admin/charts/<id>.svg` doesn't exist**; narrative charts have no public slug — both go through `by-uuid/<uuid>.svg`.
- **Texts come from `.metadata.json`, not `.config.json`** — the latter has no source attribution, omits inherited subtitles/notes, and 404s on MDim slugs. Carry the view's query params on the request.
- **`x`/`y` are parent-relative** — reparent the embed into the template clone before applying the Step 7 coordinates.
- **`?tab=table` silently renders the default tab**; `imSquareSize` is PNG-only; `imWidth`/`imHeight` can't enlarge an SVG (renormalized to ~510k px²).
- **Line charts with >500 points render no dots** (grapher performance cutoff) — don't hunt for dots that were never exported.
- **Never stretch one axis** of the imported chart — dots, squares, and arrowheads distort. Re-export at the right aspect ratio instead. The one sanctioned exception is the scripted plot-only x-map in Step 8, which skips text and preserves marker sizes by construction; verify the markers are still square afterwards rather than trusting that.
- **A sweep over mixed node types must guard every property read.** `dashPattern`, `strokes` and `fills` don't exist on `GROUP`, so one un-guarded read aborts the whole script — and `use_figma` is atomic, so you lose the entire pass, not just that node. Wrap each read in its own `try`, and remember `fontSize`/`fontName`/`lineHeight` can come back as `figma.mixed` rather than a value.
- **To draw a dashed leader or guide line, create a VECTOR with an explicit path** — `figma.createLine()` gives you a horizontal line you then have to rotate, which is fiddly to place. `v.vectorPaths = [{windingRule:"NONE", data:`M 0 0 L 0 ${len}`}]` then `v.dashPattern = [2,2]` is exact and needs no rotation math.
- **The line-chart export's group names are `text-labels`, `connectors`, `lines`, `datapoints__<Entity>`, `outline__<Entity>`, `tick-marks`, `horizontal-grid-lines`** — worth knowing before you go hunting, and worth re-checking per chart type, since the tree is grapher's and it changes.
- **A local SVG from an `export://static_viz` step has none of those names.** matplotlib names its own nodes `figure_1` / `axes_1` / `line2d_3`, and the step supplies meaningful ones through `gid=` — `<subject>__<role>`, e.g. `boys__median`, `girls__stunting-threshold`, plus `title` / `subtitle` / `note` / `data-source` / `tagline` / `license` on the text blocks. So every named-group lookup in Steps 7–8 needs the step's own scheme, which `/create-static-viz` hands over with the file; enumerate the ids from the SVG if it doesn't (`grep -oE 'id="[a-z0-9_-]+"'`). Two consequences: there is no `connectors` group to hide, because a matplotlib chart has no elbow leaders to begin with; and the x-map stretch in Step 7 is unnecessary, because the step already drew the frame at the template's aspect — though it still needs the single uniform rescale to the template's *size*, which the aspect does not give you (Step 7).
- **Restyling an imported chart's text to Lato: enumerate the ranks, don't bucket by threshold.** A size map like `size > 13 ? 14 : 12` silently collapses two ranks into one — 16px facet titles landed at 14px alongside the tick labels, erasing a distinction grapher makes deliberately. Write the map from the ranks you expect to see (facet title / ticks / annotation), then assert the resulting size histogram has that many entries. Same for weight: a sweep that sets one `fontName` for every node strips bold from whatever had it (the facet titles and the axis label both), so drive it off a set of the strings that are bold rather than off a single default.
- **Never patch a node's absolute `x`/`y` in a later call from anchors recorded before a fit.** Fitting moves the whole chart group, so anchors captured at import time are stale by that delta the moment the fit runs — reapplying them yanks those nodes back out of the group and the bbox explodes (a 635px chart reported 878px, and its band gap went negative). Re-anchor and fit **in one call**, in that order. If a later change is needed, rebuild the working copy from the untouched reference on the page rather than patching coordinates.
- **Bind the series color as a library style; derive the band tints from it.** `setStrokeStyleIdAsync` for a line, `setFillStyleIdAsync` for a fill — and note the gid names an SVG **group**, not the vector, so descend to the `VECTOR` children first (`node.query('VECTOR')`) or the setter throws `no such property on GROUP`. The library carries no tints, so a banded chart's fills stay computed blends of the bound color towards white; record that as an accepted deviation from "every fill is a bound style", same as text fills.
- **Choose between candidate palette pairs on grayscale separation, not ΔE alone — then ask whether it gates.** Every plausible two-color pair clears the ΔE 20 bar comfortably (70–100 here), so the number that actually differs between candidates is the grayscale seam: `Camel`/`Denim` 1.86:1 against `Copper`/`Blue` 1.18:1. But the seam only *gates* when the reader has to tell the colors apart. With the two series in separate, text-titled facets, nothing depends on it, which is why `Rusty Orange`/`Denim` at 1.14:1 is fine here. Report the figure either way.
- **A step's desktop and mobile SVGs are meant to stay paired**, so a hand edit to one is almost always an edit to both. `/create-static-viz` builds them from one code path precisely so their explanatory blocks match — the `diagram__*` layers, the same label wording, the same conventions. Landing a wording or styling change in only one frame silently breaks the pairing, so make the change twice, then crop both frames side by side and compare. Anything worth doing to both is worth pushing back into the step instead.
- **Raising `imFontSize` makes grapher drop labels it can no longer fit.** Bigger type means narrow segments lose their value entirely — Brazil's 7.3% fish label vanished between two exports, and a chart can come back with fewer labels than the one you measured. After changing the font size, check that the specific values an annotation or a recommendation relies on are still present.
- **The Plugin API's shape is not uniform, and guessing costs a round trip.** `figma.getLocalVariableCollectionsAsync` does not exist — variables live under `figma.variables.*`, and this file has paint and text styles but **no color variables at all**, so a variables sweep comes back empty and means nothing. The range setters are **synchronous** (`setRangeFontName`, `setRangeFillStyleId`) while the node-level ones are async (`setFillStyleIdAsync`, `setTextStyleIdAsync`); `setRangeFontNameAsync` is not a method. Read the typings rather than pattern-matching the `Async` suffix.
- **The SVG import renames nodes: spaces become hyphens.** A category displayed as "Beef and buffalo" is the node `Beef-and-buffalo`, so `query('[name=Beef and buffalo]')` finds nothing while the legend text still reads with spaces. Query by the hyphenated node name and map to the label text explicitly — that mismatch is also why the legend has to be paired by geometry rather than by name.
- **`query('[name=…]')` also breaks on punctuation the selector parses** — `Micronesia-(country)` returns nothing because of the parentheses, and a template's `"This is a title on two lines, lorem ipsum…"` fails on the spaces and comma. Both failures are silent `null`s that surface later as `cannot read property of null`. For anything whose name you don't fully control, **build a name→node map by walking `children` once** and look up in that instead of trusting the selector.
- **Figma deletes a group the moment it becomes empty, so never touch it afterwards.** Moving the last child out and then reading `group.children.length` throws `The node with id … does not exist` — and because `use_figma` is atomic you lose the whole script, not just that line. Drain the group and simply don't refer to it again.
- **A mixed-weight text node cannot hold a text-style binding.** The annotation ladder is all Lato Regular, so the moment you bold the country name Figma drops `textStyleId` — `setTextStyleIdAsync` then reads back as unbound, before *and* after. That is expected, not a failure to fix: take the **size** from the ladder value and bind the **fill** style (which does survive), and don't chase the text-style binding. Report it that way rather than as a defect.
- **`insertCharacters`/`deleteCharacters` need every font on the node loaded, not just the one you're writing.** Editing a mixed-weight note throws `Cannot write to node with unloaded font "Lato Bold"` even when the inserted text is Regular. Loop `getStyledTextSegments(['fontName'])` and load each before any character surgery.
- **To re-centre after a block's height changes, translate everything by the same delta rather than re-solving the layout.** A shorter legend leaves the map+legend block off-centre; shifting the map, every annotation and every leader by one shared `dy` preserves all relative geometry exactly — labels stay over the same water, leaders stay valid, and no placement search has to run again. Verify afterwards that the leaders still end inside their countries; that check is cheap and catches a mistranslation immediately.
- **`search_design_system` returns about 14 styles per query.** It cannot enumerate a library group in one call, so query each color by name (or query several times with different wording) and resolve hexes with `importStyleByKeyAsync`. Never conclude a group is small because one search returned few results.
- **`get_screenshot` hands back a URL, not an image.** Download it with `curl` and open it with Read — an inline base64 response costs far more context for the same picture.
- **`get_screenshot`'s `maxDimension` never upscales.** It clamps at the node's natural size, so a 540×540 frame comes back 540×540 whether you ask for 1024 or 65536. Any `@2x`/`@3x` export has to come from Figma's own export UI or the admin's `/api/figma/image?...` endpoint (which uses `scale: 3`).
- **`imType=thumbnail` is a different renderer, not a smaller one.** It selects `GrapherVariant.Thumbnail` and its own per-type components (`LineChartThumbnail`, `SlopeChartThumbnail`, …), which emit direct labels and drop the y-axis on their own. Don't reason about it from the `uncaptioned` route's behaviour: `imWidth`/`imHeight` set the size outright rather than the aspect, `imFontSize` is in rendered pixels (15 → 11px, 16 → 12px at 302 wide; the default 14 gives 10px), and `imMinimal=1` swaps entity names for values rather than removing furniture.
- **A thumbnail export emits every label twice.** Grapher paints a white halo behind each label, so `United States` appears as two text nodes. A text edit that touches one copy leaves the other in place, and a text-node count is double what the picture shows.
- **A missing view param renders an axis-only chart at HTTP 200.** An explorer requested with no view params came back with two texts and no series; an MDim slug with no params rendered its *default* view (a map, for `energy-mix`). Nothing errors — assert the text count and the rendered tab against the view you asked for.
- **A bespoke component mounted outside a Shadow DOM renders unstyled, silently.** Each bundle injects its own CSS scoped to `:host`, so those rules never match in a plain element — and because the serializer reads `getComputedStyle`, you get a structurally plausible SVG with the wrong paint, weights and geometry. BESPOKE-SVG.md has the correct mount.
- **New year, new file** — ask for the link and re-verify every node id in the map above before the first run of a new year.

- **Once the chart is fitted it is a frame child, so measuring the content box from "all children" includes it.** The chart lands 508.05px wide against a 508px content box, so a content-box measurement taken after the fit reports 508.05 — it drifts the very number the chart was fitted to, and each re-fit compounds it. Exclude the chart group (and the logo) when reading `contentX`/`contentW`; `scripts/measure_fit.js` does.
- **An inset computed against an already-rescaled group is garbage that looks plausible.** `declared − ink` is only the inset while the import is at its natural size. Run it after the fit and you subtract a rescaled width from the probe's declared one: measured 282.95 / 264.28 against a true 64.08 / 29.04, which would send the next export badly wrong. A real inset is a small fraction of the canvas — `measure_fit.js` bounds it at 25% and reports `unusable` rather than a number.
- **Never place a curvy arrow by any bounding box — neither the group's nor the head's.** These arrows
  are a long curved tail plus a small arrowhead, so the group's box is mostly tail and the head's box
  has the tip in one corner. Aiming the group's box left the heads **13–21px off their dots**; aiming
  the head's box centre put one tip *inside* its dot and another 5.3px off-axis. **The full recipe is
  in [LABELING.md](LABELING.md) → Placing an arrow**; it works from the head's real vertices, picks
  the arrow whose span fits, and checks both ends.
- **Resizing a template clone displaces EVERY child it already holds, each by its own constraint —
  and nothing in the response says so.** Two distinct failures from one `clone.resize()`:
  - **The chart got squashed non-uniformly**, 122.7px to **94.26px** with its width untouched, through
    the group's top-and-bottom constraint. A **GROUP has no `constraints` property** (`node.constraints:
    no such property on GROUP`), so it cannot be pinned — it must be placed *after* the resize.
  - **The header moved, and the frame clipped it.** The 302-wide headers are constrained
    `vertical: CENTER` on the pull template and `SCALE` on the guided one — neither is `MIN` — so
    shrinking the frame 233 → 184 put the pull header at **y = −15**, with its title sliced off by
    `clipsContent`. Ordering cannot help here: the header is in the template before you touch it.

  So: resize first, then **re-pin every child** to its designed position (the 302-wide header belongs
  at `y = 10`, bottom flush with the 44px band top), and assert nothing has a negative `y` — a
  clipping frame hides the evidence.
- **`curl -F "file=@..."` fails with exit 26 on a path containing spaces or non-ASCII** — and since
  the upload filename becomes the Figma layer name, the temptation is to name the file exactly what
  you want the layer called. Don't: use a plain ASCII filename and rename the node in Figma.
- **Route parallel uploads by declared size, never by response order.** Two POSTs fired concurrently
  come back in arbitrary order, so pairing the *n*th response with the *n*th target silently swaps
  them. Match each import to its target by dimensions and assert no two route to the same
  destination — the sizes differ per template, so it is exact.
- **`resize()` resets a text node's sizing to FIXED, so `textAutoResize` must come AFTER it — and
  getting that backwards makes the node lie about its height while rendering perfectly.** Setting
  `textAutoResize = "HEIGHT"` and *then* `resize(w, 10)` left four annotations with **10px-tall boxes
  around 38–76px of ink**. Figma text overflows rather than clipping, so the render was correct and
  nothing errored — but every gap, centring and overlap test computed against those boxes was
  meaningless and each one silently "passed". The tell is a text node whose `height` is far smaller
  than its line count justifies. Order it `resize()` → `textAutoResize`, and read the height back on
  the *next* call before positioning anything against it.
- **Figma's `rotation` is counter-clockwise, while screen angles from `atan2(dy, dx)` are y-down.** So
  for a node you are rotating to a computed heading, `achieved = natural − rotation`, and the value to
  set is `natural − required`. Getting the sign wrong put an arrow at 29° when 173° was wanted, with
  no error and a plausible-looking number in the response. Always set, re-measure, and correct by
  `(achieved − required)` — one iteration converges exactly.
- **A chart's node id does not survive a re-import — resolve it by NAME.** Every corrected export
  replaces the group, so an id captured earlier goes stale and `getNodeByIdAsync` returns `null`; the
  next `.findAll` throws `cannot read property 'findAll' of null`. On a run with two refitted frames
  out of five, three ids were still good and two were dead, which is the worst version of this. Use
  `clone.children.find((c) => c.name === "chart")`.
- **A PAGE does not resolve through `getNodeByIdAsync` — use `figma.root.children`.** It returns
  `null`, and passing that to `setCurrentPageAsync` throws `cannot read property 'id' of null` rather
  than anything mentioning pages. Find pages with
  `figma.root.children.find((p) => p.id === "…")`, and search by name when an id may be stale — a page
  someone has deleted looks identical to a page id you got wrong.
- **One `setCurrentPageAsync` per call, including in read-only loops.** Iterating two pages to compare
  them throws on the second switch. Split it into one call per page — and since these are reads, emit
  them in the same message so they run concurrently.
- **`x`/`y` on a frame child are PARENT-relative, while `absoluteBoundingBox` is not.** After `clone.appendChild(chart)`, setting `chart.x = clone.x + contentX` sends the chart off-canvas by exactly the frame's own page offset — 1590px on a real run, which reads as the import having vanished. Read absolute geometry from `absoluteBoundingBox`, but *write* `x`/`y` in the parent's coordinates: `chart.x = contentX`. The tell is a reported right edge far outside the content box.
- **Removing a footer row leaves the footer stranded, because every footer but one is constrained `MIN`.** MIN keeps the *top* edge, so deleting the `Note:` row from a Static Horizontal clone collapsed the footer 63 → 31 but left `y` at 559: its bottom rose from 622 to 590 and the frame gained 48px of dead space against its own 16px margin. Nothing errors and the band bottom does not move, so it survives a band measurement. Re-pin with `footer.y = frame.height − bottomMargin − footer.height`, and do it **before** measuring the band.

**Four mechanics below were each found on one chart type and bite on all of them.** They are here,
not in the type file, for that reason — the worked examples stay in `reference/per-chart-type/`.

- **`rescale()` multiplies every stroke width.** A 0.22px hairline becomes 0.9px after a 4× scale, and
  a 1px annotation stroke becomes 4px — so **set every stroke after the final scale, never before**.
  Found on maps (`reference/per-chart-type/maps.md`, where the whole treatment is hairlines), but it
  applies to any chart whose group is scaled into a band, which is all of them in Step 7.
  - **It also thins the data line, which is the case you will actually ship.** Fitting a square DI
    chart scaled by 0.657 left `line__Chile` at **1.32px** and its white halo at 1.98. Nothing looks
    broken — it reads as a weaker chart than grapher's own, and a designer spots it before you do.
    `scripts/measure_fit.js` detects it when given `CONFIG.originalGroupId`, comparing the fitted
    strokes against the untouched reference import of the same format. **It reads the series identity
    from the naming ANCESTOR** — on a slope export `slope__<Entity>` and `outline__<Entity>` are groups
    and the stroked vector is called plain `line`, so matching the node's own name inventoried *nothing*
    and `[].every()` reported "strokes sit at the house 3/4" without inspecting one. An empty inventory
    now reports `NOT CHECKED`, because an empty comparison is a gap in coverage and not a pass.
  - **It goes the other way too, on any template whose band is WIDER than the export.** Static
    Vertical fits at **1.30x**, an upscale, so the same multiplier *thickens*: its line arrived at 2.61
    and its halo at 3.91 and both had to come **down** to 2/3. So the rule is not "restore what the
    rescale thinned" but "set every stroke after the scale, whichever way the scale went".
  - **The reference tells you the rescale changed it. It does not tell you the target.** The two export
    routes ship different weights, and neither is a reliable stand-in for the house value — measured on
    one chart, `imType=square` came back **line 3 / outline 4** and `imType=uncaptioned` **line 1.5 /
    outline 2.5**, before any fit. So the square route happens to arrive at the house weight and the
    uncaptioned route at 40% of it: read the fitted frame and set **3 with a 4px halo** explicitly.
    Use the comparison to *notice*, never to source the number — and note that a square reference will
    agree with the house value and so cannot tell you whether the repair ran.
  - **3/4 holds across the static and IG templates regardless of frame width.** I set the 850-wide
    Static Vertical to 2/3 on a rule I invented — "a wider plot wants a relatively thinner line" — and
    on a 1095-tall frame it read thin. Don't derive a weight from the frame size; 3/4 is the value.
  - This entry existed before the run that hit it. Documenting a multiplier is not enough; the check is
    what catches it — and a check that asserts the wrong target is its own bug.
- **Text widths only settle on the next `use_figma` call.** Same class as the `leadingTrim` rule in
  the Round-trip budget: bolding a label, changing its font, or editing its characters does not
  update `width` within the call that does it. Any placement search that runs in the same call is
  searching against the pre-edit width. Bold first, measure next call, then place.
- **The API exposes no per-line width for a multi-line text node, and a probe clone cannot answer
  it.** Setting `characters` on a clone resets the bold ranges, and bold is wider, so the
  measurement comes back short — which puts anything you anchor to it on top of a glyph. Measure the
  line's *ink* off the render instead: screenshot the frame and scan that line's row band for the
  rightmost non-canvas pixel (`sum(abs(c - 255) for c in px[x, y]) > 36` against a white canvas).
- **A label moved on top of a fill can be invisible with nothing wrong in the node tree.** grapher
  orders its `text-labels` group *before* the fills, which is harmless while the labels sit in a
  reserved margin and fatal the moment you move one into the plot — the area paints over it.
  Re-append the label to the chart group after moving it (`chart.appendChild(label)`), and **check
  the render, not the node list**: the tree looks correct either way. Found on stacked areas, and it
  is the same failure as a map leader hidden by the annotation it starts from.

## Running the scripts

- **`use_figma`'s `code` parameter caps at 50,000 bytes**, and `verify_page.js` is ~59KB with its
  comments. It has to be comment-stripped to run: drop the header block and every whole-line `//`
  comment (never inline ones — a URL or a regex can contain `//`), which took it to ~37KB. Worth
  knowing before you plan a run around it, and worth remembering when adding to any of these scripts:
  past the cap, a script cannot be executed at all.
- **`node --check` rejects these scripts, and that is not a syntax error.** They use top-level `await`
  and `return`, which are valid inside the async wrapper `use_figma` provides and invalid in a plain
  CommonJS file. The harnesses are the real gate — they wrap the source the same way the tool does —
  so run `node test_<name>.js`, never `node --check <name>.js`.
- **`verify_docs.py --against` takes a git ref, not a path.** `--against HEAD` is the useful form while
  a move is still uncommitted: it diffs the working tree against the committed text and reports
  anything that went missing as `LOST`.

## Re-checking a row outside its script

**Copy the predicate verbatim, or run the script.** Re-implementing a check compactly to spot-check a
few frames is how a check becomes vacuous: dropping half of one predicate — `isGridByName` tests the
node's name *or* its furniture container, and only the name got copied — meant nothing was classified as
a gridline, so the row reported `ok` having judged **zero** nodes, on frames that genuinely had the
defect a moment earlier.

**So make every ad-hoc check report the COUNT it judged**, not just its verdict. `status: "ok"` and
`gridsJudged: 0` in the same object is instantly recognisable as vacuous; `status: "ok"` alone is not.
This is the same failure the harnesses exist to prevent, arriving through the side door.
