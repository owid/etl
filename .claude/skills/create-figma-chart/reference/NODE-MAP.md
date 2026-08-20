# The yearly Charts file — node map (2026)

> Read before you clone anything, in Step 5 — and run `scripts/verify_templates.js` from here on every run.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


Each year gets a new file. For **2026** the file key is `s6Sv60bakebRRW2TxsMQbF` ([Charts (2026)](https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-)). **If the current year is not 2026, ask the user for that year's file link and re-verify every node id below** (the templates page is named " 📑 Templates" — note the leading space).

**Geometry last verified: 2026-08-20.** Re-verify at the start of every run rather than trusting this table — the design team edits these frames in place, and edits that move a chart area's edge have landed days apart. A page laid out against stale numbers still renders and still passes every check in Step 8c; it just doesn't match the frame it was cloned from. Three days was enough for both Instagram footers to be rebuilt as auto-layout under new ids, so re-verify **structure**, not only numbers.

**Verify on every run. The date above never licenses skipping it** — it is not a staleness trigger, and "the table was verified recently" is not a reason to lay a page out against it. The edits that have actually broken this file landed within days of a verification, so a fresh date is the case where a check is *most* likely to be skipped and *no* less likely to be needed.

**Run [`scripts/verify_templates.js`](../scripts/verify_templates.js) — this is the check.** It carries the expected geometry for all ten templates and returns a per-template `ok`/`DRIFT` verdict plus a `summary.verdict`, so the comparison is mechanical rather than an eyeball diff of two tables. **A `DRIFT` verdict stops the run**: report which field moved and let the user decide, because you cannot tell from the output whether the design team changed the frame or this file is wrong — and both matter. Do not work around a drift by adjusting your clone to match.

**If the script cannot run** — no Figma access, rate-limited, or a new yearly file whose ids don't resolve — you still verify, by hand: `get_metadata` on the template you are about to clone, checked against its row in the node map and the Step 7 band table. The date's only job then is to tell you how far to distrust the table while you do that: **two weeks or older and treat every number here as suspect**, including ones the hand check doesn't cover.

So the date is provenance, not a gate. What it is genuinely good for is judging a drift report — a day-old table plus a mismatch is probably your error; a three-month-old one plus a mismatch is probably the design team. **When you re-measure, update the date and the script's `EXPECT` block together**; a fresh date over stale expectations is worse than no date at all.

**The header is a flat auto-layout of `[title, subtitle]`, and the logo is its SIBLING, not its child** — a `logo` FRAME on the 540-wide set and Instagram, a `Logos/Our World in Data/36px` INSTANCE on the 850-wide pair. So the logo does **not** contribute to the header's height, and the band moves with the text alone. What still keeps the title clear of it is the title node's own width: it is sized narrower than the content box (**737.84** on Static Vertical, **428** on the 540-wide set, against content widths of 818 and 508). Measure a candidate title against *that* number, never against the content width — see the orphan rule in Step 6.

**Resolve the header and footer structurally — topmost/bottommost auto-layout child — never by name.** Frame names here are not stable across design edits; a whole generation of them (`Frame 14`, `Frame 5`, `header`) has already been replaced by `Frame 20`/`23`/`26`/`27`/`28`/`29`/`36`. The structural resolver in `verify_templates.js` and in the Step 7 snippet is immune to that; every name-based lookup silently returns `null` and takes a `cannot read property of null` with it.

Run [`scripts/verify_templates.js`](../scripts/verify_templates.js) through `use_figma` — it measures every template in the table below in one call (size, frame fill, content box, header band, footer id/name/`layoutMode`/rows) and is what the notes here were last written from. Diff its output against this table before you clone anything.

| What | Node | Size | Notes |
|---|---|---|---|
| Templates page | `798:54` | — | all templates + instruction frames live here. **The arrows, flags, animals, no-data and checklist ids below are *pages*, not nodes on this one** — they sit at page indices 2–7 |
| InstagramPost_Template_English | `798:161` | 540×540 | two-row footer, **`Frame 17` (`25518:14`)** @ y=488, h=36 — source (y=0, h=16) then `Frame 16` (y=20) carrying `OurWorldinData.org/[Topic]` and CC BY. VERTICAL auto-layout. Header `Frame 28` |
| InstagramPost_Template_Portrait_English | `6689:8` | 560×700 | footer **`Frame 19` (`25518:16`)** @ y=640, h=36 — a Note row (y=0, h=17) then `Frame 18` (y=20), VERTICAL auto-layout. Header `Frame 29`, h=114, hugs its text |
| InstagramReel_template | `7336:8` | 616×1096 | has top/bottom no-go zones; contains a worked small-multiples example |
| DI_Template | `6799:1859` | 540×540 | **one**-row footer, **`Frame 37`** @ y=508, h=16: source + CC BY. HORIZONTAL auto-layout |
| Static Chart Template_Mobile (example 1) | `24590:20` | 540×540 | **two**-row footer (`Frame 15` `25343:276` @ y=486, h=38): `Data source:` then `Licensed under CC-BY by the author […]`, both full width |
| Static Chart Template_Mobile (example 2) | `24590:32` | 540×824 | taller variant — use when the chart needs vertical room. Same two-row `Frame 15` (`25343:275` @ y=770) |
| Static Chart Template_Horizontal | `5332:75` | 850×638 | header `Frame 20`; footer **`Frame 22` (`25808:13`)** @ y=559, **h=63**: Note (y=0, h=28), Data source (y=32, h=14), then `Frame 21` (y=50, h=13) carrying the OWID tagline and "Licensed under CC-BY by the author [Name]". No inner padding on either wrapper |
| Static Chart Template_Vertical | `5332:93` | 850×1095 | header `Frame 23`; footer **`Frame 25` (`25808:16`)** @ y=1015.81, h=63, same three rows. Header bands identical to the Horizontal at **118**, and the header and footer are both **818** wide. (An earlier pass recorded the header at 817.57, a 0.43px mismatch that produced a phantom right-hand breach in ink-span checks; re-measured 2026-08-20 it is 818, so the phantom is gone — if you see 817.57 again, it has come back.) |
| **`small-chart-template-guided`** | **`25344:1357`** | 302 × free | title + optional subtitle, no source row — see [SMALL-CHARTS.md](../SMALL-CHARTS.md) |
| **`small-chart-template-pull`** | **`25344:1391`** | 302 × free | the same plus a mandatory source row — see [SMALL-CHARTS.md](../SMALL-CHARTS.md) |
| `"SMALL" Charts` section heading | `25344:1235` | — | "featured on the OWID website as guided and PULL charts" |
| Curvy arrows | `798:773` | — | copy/paste into the chart; scaling rules in GUIDELINES.md |
| "No data" hashed-pattern instructions | `4162:5` | — | Hero Patterns plugin (manual route). Scriptable instead — TILE `IMAGE` fill from `assets/no-data-hatch-tile.png`, see GUIDELINES.md → Flags, animals, no-data pattern |
| Flags | `2654:5` | — | Flags **plugin** — manual; US flags provided in the file |
| Animals | `5336:5` | — | chicken, rooster, turkey, fish, cow, egg-laying hen, pig |
| Good Data Viz Checklist | `20729:1027` | — | distilled in GUIDELINES.md |

### Header sizing, and the one property that decides whether the band moves

**A header that hugs its text moves the band with the copy you write** — `primaryAxisSizingMode: "AUTO"`, with both the title and the subtitle at `textAutoResize: "HEIGHT"` and `layoutSizingVertical: "HUG"`. **Every in-scope template ships that way** — the nine the band table covers, verified 2026-08-20 (the IG reel is out of scope and is not banded by this skill; `verify_templates.js` says why beside its row): on both 850-wide frames the header is `AUTO`, `itemSpacing: 6`, zero padding, and both children `HUG` + `HEIGHT` with `layoutGrow: 0`. So a one-line title plus a one-line subtitle takes Static Vertical's `headerBottom` from the placeholder's 118 down to **70** with no preparation at all.

**That is a change, and it resolves a documented open question.** The 850-wide pair previously shipped `FIXED` with a `layoutGrow: 1` child, which this file recorded as a trap and worked around by making the clone hug. The design team has since converted them, so **the workaround is gone — do not apply it.** If you find yourself setting `primaryAxisSizingMode = "AUTO"` on a clone, stop and re-verify, because that is now either a regression in the file or a stale note.

Confirmed empirically, not just from the properties: on throwaway clones of both 850-wide templates (2026-08-20), writing a one-line title and a one-line subtitle with **no hug intervention** took the header from 102 to 54 and `headerBottom` from 118 to **70** on each, `primaryAxisSizingMode` staying `AUTO` throughout — **48px of chart recovered for free**. That 48px is the same dead air the old trap described; it now comes back on its own rather than needing a fix.

Still check it on every clone, because it is the one property that silently costs a lot of chart. A header left at `FIXED` with a `FILL` / `layoutGrow: 1` child does **not** reflow: `headerBottom` stays at the placeholder's value however short the copy is, and the slack is absorbed by the flexible child's *box* instead — a one-line title leaves the subtitle's box 67px tall for 19px of ink, and fitting to the frame band then buries ~48px of dead air under the subtitle. Nothing renders wrong, so it survives a screenshot; the tell is a subtitle whose `height` is much larger than its line count justifies. `scripts/verify_templates.js` now gates on this, so a reversion shows up as a DRIFT verdict rather than as a chart that quietly lost 48px.

```js
// what a hugging header reports — assert this on the clone before measuring the band
header.primaryAxisSizingMode === "AUTO"
header.children.every(c => c.layoutSizingVertical === "HUG" && c.textAutoResize === "HEIGHT")
```

`verify_templates.js` reports this as `headerSizing.reflows`, which is the cheapest place to catch it: a `false` there means the band you are about to fit into is a constant, not a measurement. If you meet one, fix your **clone** (set the three properties above), never the shared template, and say so in your report.

### Slot sizes per family, measured 2026-08-17

The per-slot *positions* for the four static templates live in [`/create-static-viz`'s TEMPLATES.md](../../create-static-viz/TEMPLATES.md); these are the type sizes you are filling into, which Step 6 needs and which no other file records for the non-static families. They do **not** move together — the same row is 14px on one family and 11px on another, so read the family you are filling rather than carrying a number across.

| Family | Title | Subtitle | Note | Source | Tagline | License / CC BY |
|---|---|---|---|---|---|---|
| Static Horizontal / Vertical (850) | 25 | 16 | **12** | **12** | **11** | **11** (263px slot at x=571) |
| Static mobile 1 & 2 (540) | 25 | 16 | — | 14 | — | 14 (own row, full 508) |
| DI (540) | 25 | 16 | — | 14 | — | 14 |
| IG square (540) | 25 | 16 | — | 14 | 14 | 14 |
| IG portrait (560) | **28** | **18** | 14 | 14 | — | 14 |
| IG reel (616) | 28 | 18 | — | 15 | — | 15 |
| Small guided / pull (302) | **16** | **11** | — | 11 (pull only) | — | — |

Two traps in that table. The 850-wide pair's **12px note and source** are the smallest body text of any family, so a source line that overruns there cannot be fixed by dropping a size the way a 14px one can. And its **license shares a row with the 467px tagline inside 818px** — the slot holds roughly fifty characters, so a two-author credit overruns the tagline and prints on top of it; the phrasing gives (`by <names>` rather than `by the authors <names>`), never the names. When even that overruns, break the line yourself with `\n` after `by` rather than letting it wrap or dropping a size — the wrap point is then a decision instead of an accident, and both lines carry real content. TEMPLATES.md carries the measured widths.

**In that credit, bold the names and nothing else.** The template ships one bold run (`[Name of author]`), so writing several names into it bolds the connectives too and the line reads as `Pablo Arriagada, Hannah Ritchie **and** Pablo Rosado`. Set the joining word back to an unbolded weight, the same way `Licensed under` and `by` are unbolded — the bold is doing one job, marking who is credited, and a bold `and` makes it look like part of a name.

**Two Spanish Instagram post templates sit beside the English ones on that page. They are no longer used and may be deleted** — never target one, and don't read their absence from this table as an omission to fix.

The templates' left-to-right arrangement on the page is not load-bearing anywhere in this skill: everything is addressed by node id, so the design team can regroup the sections freely. Resolve by id or by structure, never by position or sibling index.

The last five ids are **pages of their own**, which is why they don't appear in a `get_metadata` dump of `798:54`: `↪️ Curvy Arrows` (index 2), `🌎 No data on maps and hashed pattern` (3), `🎌 Flags` (4), `🐖 Animals` (5), `✅ The Good Data Viz Checklist` (7). Reach them with `figma.root.children`, not by looking inside the Templates page. The dated chart pages start at index 9, immediately below the `-----------------------------------------` divider page at index 8.

Shared styles in the file: text styles `Data Insights/Title` (Playfair Display SemiBold 25) and `Data Insights/Subtitle` (Lato 16); paint styles `Data Insights/Title` #2D2E2D, `Data Insights/Subtitle` #5B5B5B, `Data Insights/Source` #858585; color variables `Text/Gray 100` #2D2E2D, `Text/Gray 80` #5B5B5B, `Website/Text/Blue 100` #002147, `Instagram/Beige Background` #FBF9F3; plus the **Chart colors** library (see GUIDELINES.md → Colors). Note the text and paint styles share names — `Data Insights/Title` is both a 25px Playfair text style and a #2D2E2D fill.

The DI Charts Guidelines file (`8gxqkVmZ9x3MK3ky5oigrJ`) is the source of truth behind GUIDELINES.md — six pages: line `0:1`, stacked area `130:35045`, bar/stacked bar `130:35046`, slope `130:35047`, scatter `130:35048`, map `130:35049`. Re-read the relevant page if GUIDELINES.md seems stale.
