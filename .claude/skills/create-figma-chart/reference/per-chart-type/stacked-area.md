# Stacked area charts

> Chart-type conventions from the DI Charts Guidelines. Read **only** the file for the chart in
> hand — see [GUIDELINES.md](../../GUIDELINES.md) → Per chart type for the index.

- Labels inside the areas or in a legend row above the chart; white text over dark fills, ≥12px, strong contrast.
- White-outlined dots for highlighted points; a dot in the chart needs an outline to stand out against the fill.
- **Move grapher's series labels off the right margin and into the bands, white and bold.** The export puts them in a reserved column at the right, each in its series' own color; inside the band they become white bold ~14px and the whole column is reclaimed for plot (see SKILL.md → Step 8 on the x-map). This is the stacked-area equivalent of killing a legend, and it is what the finished pages do.
  - **They will be invisible until you fix the z-order.** grapher orders `text-labels` *before* the fills, which is harmless while they sit in the margin and fatal once they are inside — the areas paint straight over them. Re-append each label to the chart group after moving it (`chart.appendChild(label)`), and check the render rather than the node list, since nothing in the tree looks wrong.
- **Get a band's vertical extent from the `borders` strokes, not by sampling the area polygon.** Each border is the *top* edge of its own series, so band *n* runs from its own border down to the next series' border, and the lowest band's floor is the 0% grid line. Sampling the filled polygon instead looks equivalent and quietly fails: a stacked polygon's bottom edge along the baseline may carry only its two end vertices, so a window around your sample x catches top-edge points only and the band measures ~0 tall. That is how a 75px-tall nuclear band came back as 0.5px.
- **A very tall band takes its label near the top, not centered.** Centering is right for a thin band, but in a band covering half the plot the label drifts into empty space and the middle is exactly where the annotation wants to go. Put it ~14px under the band's top edge and leave the middle free.
- **Fold the value into the in-area label when the chart has only two or three series.** A 100%-stacked chart of two categories needs no axis reading: `Overfished: 36%` as a bold 16px line with a 14px explanatory line under it — white, right-aligned, in an auto-layout block inside the band — says more than a legend plus a y-axis. Round so the parts sum to 100 (64.5 and 35.5 become 64 and 36), and note that this makes the label a claim to verify, not decoration.

**Labeling is decided per band, not per chart — and one chart normally does both.** Thick bands take a white bold label *inside*; a band too thin for one takes its label *outside* in the band's own color, tied back by a thin leader or a bracket. `375:304`, `562:70`, `572:70`, `585:105`, `604:909`, `651:169` — six witnesses, all mixing the two. The outside labels are not a legend row: they are a stacked column at the right or above, one leader each.

- **On a pale band the inside label goes dark, not white** — `1:1656`, `70:439`. Contrast decides.
- **When every band is thin, move all the labels out** — `72:583` stacks six colored labels above the plot with curved leaders; `295:139` puts them down the left margin and **moves the y-axis to the right** to make room. Direct labels outrank the axis for margin space.
- **A sliver band at the very bottom is labeled below the axis** in its own color — `70:439`.
- **Frame the story with a start and an end annotation** — `1:4786` runs one sentence across the plot: "From **2 billion** in 1990…" at the left, "…to almost **700 million** in 2024" at the right.
- Endpoint dots carry their value outside the plot in the series color, white-ringed — `651:169`, `70:438`, `185:76`.

Exemplars: `375:304` (both labeling treatments in one chart), `72:583` (all labels out), `295:139` (labels left, axis right), `613:59` (value folded into the band label), `596:474` (white in-fill annotations).
