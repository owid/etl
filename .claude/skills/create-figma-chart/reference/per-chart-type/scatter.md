# Scatter plots

> Chart-type conventions from the DI Charts Guidelines. Read **only** the file for the chart in
> hand — see [GUIDELINES.md](../../GUIDELINES.md) → Per chart type for the index.

- Ask whether the continent color-coding adds anything; if not, one color for all dots, then highlight the countries the story needs — different color and/or a circle drawn around them.
- Grapher's auto-chosen labels are cluttered and hard to attach to dots: prune to the entities that matter, give kept labels the white outside stroke.
- If you stretch the chart, dots deform — select all circles in the layers panel and set equal width/height in one edit.
- For binary/divided axes, annotate the two sides ("countries above this line …") so the divider explains itself.

- **Annotate a parity or divider line on *both* sides, with the text rotated parallel to the line and a small arrow pointing away from it** — `1:3615`, `91:1118`, `210:710`. That is the form; horizontal callouts are not what the archive does here.
- **If one side of the divider is empty, say so.** `210:710`: "No dots are on this side of the line: there are no countries with reported data where female suicides are more common." Without the sentence a reader assumes the dots were cropped.
- **Highlight = saturated dot plus a bold label in the same color; context = pale muted dots.** `80:382`, `205:275`, `210:710`, `89:344`. This, not the continent palette, is the default — and a **ring around the dot** is the second channel when several highlighted dots must also be told apart (`89:344`, which additionally recolors the protagonist).
- **Keep the continent legend only when "it holds across every continent" is the claim** — `91:1118` is the one chart on the page that does.
- Annotations name the entity bold in its highlight color and carry the values inline — `205:275`.
- `186:185` is the page's outlier and worth a look: a five-step diverging ramp shown as a horizontal gradient bar with the category names above it, the title's words colored to match, and paired dots joined by a vertical line with a curved arrow showing the year-on-year move.

Exemplars: `210:710` (divider annotated both sides, empty side stated), `80:382` (highlight vs muted field), `89:344` (ringed dots), `91:1118` (the one kept legend), `186:185` (ramp legend, change arrows).

## The bubble-size legend does not survive a downscale — and it is a decoder, not decoration

Grapher sizes the `size-legend` block ("Circles sized by / Population / 1.4B / 600M") as a fraction of
the export canvas, so a 0.6x fit takes its type to **6.96px and 5.99px** against a 12px floor — a third
of the minimum, and the only text on the frame that broke it. It cannot be enlarged in place without
colliding with the circles it annotates.

**But it is not "the one element carrying no data" — it is the key to one.** Circle area encodes a
second variable, and that block is the only thing on the frame telling a reader what the areas mean.
Hiding it leaves visibly different bubbles with nothing to read them by, which is a worse defect than
the small type was. So the question is which chart you have:

- **The size dimension is part of the claim** — then the legend stays, and it has to become legible.
  It cannot grow in place, so rebuild it *outside* the plot: retype its labels at a ladder size and sit
  it under the chart the way a map's legend does (see maps.md), keeping map→legend clearly tighter than
  legend→footer so it reads as belonging to the chart.
- **The size dimension is irrelevant to the claim** — then the bubbles should not be sized by it at all.
  Re-export without the size encoding rather than hiding its key; equal-area dots and no legend is
  honest, differently sized dots and no legend is not.

Only hide the whole `size-legend` group once you have established the second case. When you do: on the
measured frame it sat well inside the plot, so the chart's box did not change and no refit was needed —
check that rather than assuming it, and refit if the group did set an extreme.

**Find it by lowest common ancestor, never by walking up from one text.** Walking up from the
"Circles sized by" node until the parent is the chart lands on a `Clip path group` holding the *entire*
chart — hiding that took out every country label, the continent legend and all the points, leaving axes
on an empty frame. Collect the legend's own texts, walk each one's ancestor chain, take the **deepest
common** node, then verify its text descendants are *only* the legend's before hiding anything. On the
real frame that resolved to a group actually named `size-legend`, with exactly six texts.

The recovery is worth knowing too: the fit is **defined** by the box, so re-deriving it undoes a bad
intermediate scale without needing to know what that scale was — un-hide, loop `rescale(508 / width)`
until the width settles, align the left edge, re-centre. That restored the frame to 16→524 with 14/14
gaps after a 1.264x mis-scale.

## Labeling a dense field: the ring is the association channel, not proximity

On a scatter with a few hundred points, **proximity does not identify a dot** — and this is measurable
rather than a matter of taste. For each label, rank *every* dot by its distance to the label's box; if
the label's own dot is not the nearest, the label is pointing at the wrong country. Run it before
shipping:

```js
const distToBox = (px, py, b) => Math.hypot(Math.max(b.x - px, 0, px - (b.x + b.width)),
                                            Math.max(b.y - py, 0, py - (b.y + b.height)));
// rank(own dot) === 1, and a margin over the runner-up, or the label is ambiguous
```

Measured on an 11-label / 167-dot scatter with every label tucked 6–8px from its dot: **seven of eight
context labels sat nearer another country's dot than their own** — China's own dot ranked **7th**
(Moldova was 5.3px away against China's 22.6), South Africa's **8th**. Nothing in the render looks
wrong, which is exactly why this needs measuring.

**Leader lines do not fix it in a dense field, and the reason is structural.** A leader needs the label
to move somewhere clear, and in the crowded quadrant of a scatter there is nowhere clear to move to —
every relocation lands on different dots. Tested at three minimum leader lengths on the same chart:
natural spacing gave 2px leaders (invisible), ≥11px hid 12 dots, ≥16px hid 13, against 8 for the tucked
labels with no leaders at all. Each leader bought association at the price of 4–5 newly hidden dots.

**Ring the labelled dot instead.** A thin circle centred on the dot, in the dot's own color, says
*which* dot the nearby label names without moving the label or adding a line across the plot — the
"second channel" this page already recommends for highlights, applied to every labelled point. It also
frees the label to sit wherever it hides fewest dots, since it no longer has to be the thing that
identifies the mark. Same chart: 26 dots hidden → **6**.

**Size the ring against the nearest-neighbour distance, not by eye.** A ring that encloses a *second*
dot makes the ambiguity worse than no ring. The largest safe radius is `nearestNeighbourCentreDist −
dotRadius`; take the minimum across all labelled dots and use one size for all of them.

**Some dots cannot be disambiguated by any treatment — drop the label and say so.** Where two dots sit
closer than about one dot diameter, no ring, label or leader can point at one and not the other.
Measured: India's dot lay **4.6px** from Ukraine's (dots were 6px), giving a maximum safe ring radius
of 1.6px — smaller than the dot itself. The honest move is to drop that label and report why, choosing
a substitute that keeps the coverage the chart needs (Asia kept Indonesia and China).

## A channel applied to everything stops being a channel

Once every labelled dot carries a ring, the ring means "this dot is labelled", not "this dot is the
story" — so a *second* ring size to mark the protagonists encodes a distinction the reader has no key
for. The test is whether the difference is both **perceptible** and **exclusive**: 14.78px against
11px is a 3.8px difference on a 540px frame, which is neither.

When the protagonists already sit inside a shaded region that an annotation names, that region *is*
the highlight; ring size and bold labels on top of it are a third and fourth encoding of the same
fact. Drop them — and note that bold in particular works against a "look how many" chart, because it
pulls the eye toward the handful of exceptions and away from the mass the title is about.

## Colors: the palette's light end fails as a thin mark

Rings and direct labels are thin marks on white, so the Default Palette's light entries fail there
even though they read fine as a filled dot. Measure contrast against the canvas and swap to the **Line
and Slope Charts** variants below 4.5:1 rather than assuming the dot's fill will do:

| Continent fill | on white | line variant | on white |
|---|---|---|---|
| Peach `#e56e5a` (North America) | **3.13:1** | Peach\* `#c4523e` | 4.54:1 |
| Turquoise `#38aaba` (Oceania) | **2.75:1** | Turquoise\* `#008291` | 4.56:1 |
| Teal, Mauve, Denim, Maroon | 4.56–8.30:1 | — | already pass |

The symptom is a reviewer saying they cannot see one ring while every other ring is fine; the cause is
the color, not z-order or a missing node. Check the fill's contrast before hunting for the node.

## The subtitle has to say what the two axes compare

A same-unit scatter describes *two* quantities, and a subtitle inherited from a single indicator
describes one — so it silently omits the comparison the chart exists to make. Say "share of **men and
women** who …", not "share of people who …", and add **"Each dot is a country."**, which nothing else
on the frame states once most dots are unlabelled. Both fit inside the usual two lines.

Do **not** also spell out what each side of the divider means when annotations already sit in those
territories: a sentence positioned above the line and another inside the shaded region below teach the
rule by placement, and repeating it in the subtitle states the same fact three times on one frame —
for the price of a third subtitle line and a re-fit.

## A shaded region has three or four boundaries — test them all

Tinting one side of a divider is a strong way to make "which side am I on?" legible without touching
the dot colors, and it costs no legibility when the shaded side is nearly empty — which on a parity
scatter is exactly the side the story is about. Prefer it to muting the majority field: shading the
*minority* region keeps every dot at full strength, and a large, clearly-bounded, almost-empty area is
itself the evidence for an "almost every country" claim.

But an annotation placed inside it is not contained just because it sits below the divider. The region
is a **triangle**, bounded by the divider, the plot's right edge and the plot's floor:

```js
const inside = (b) => b.l >= X0 && b.rr <= X1 && b.bb <= Y0 && b.t >= lineYAt(b.l);
```

The binding corner is the **top-left** (the divider is highest there when the line rises to the
right), and the edge most often missed is the **right** — the plot ends before the frame's content box
does, so an annotation nudged rightward toward the content edge slides out of the tint while every
divider-based test still reports it contained. Measured: an annotation moved to the content box's 524
sat 7px past a plot ending at 516.6, and read as half-on, half-off the shading.

Re-run this after *any* rescale: the tint is a chart child and the annotation usually is not, so the
region slides out from under it.
