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
