# Superseded: the nested-logo template generation

> Kept only so a regression stays recognizable. **Its arithmetic no longer applies** — the live
> geometry is in [TEMPLATES.md](../TEMPLATES.md). Nothing here should be used to lay out a step.
> Moved out of TEMPLATES.md so a run does not load it; the decision to retain it is the original
> author's and is unchanged.

**`logo_px` was per template, including within the 850-wide pair.** All three frames held the same
logo instance (35.18 px), but the wrapper around it did not: Vertical's `Frame 1` (`5332:97`) added
6.08 px of top padding, making its row **41.26**, while Horizontal's (`25398:755`) added 0.08 and came
to **35.26**. So the logo sat 6 px lower on Vertical, and any figure derived from `logo_px` differed
between the two by that much. Note also that the Horizontal slot table above records the logo as `35`
— that is the *instance*, not the row, and in that generation it was the row that set the header's
height.

**`origin_y` and `row_pad_px` never both carry the 16 px.** The two families put that padding in
different places, and the tables above are the only place that shows it: on the 850-wide pair the
header block starts at the frame's own top edge (`y = 0`) and the *title row* holds the 16.22 px,
while on mobile the block starts at `y = 16` and its title row holds none. Count it on both rows and
a two-line title puts the subtitle at 96.44 against the measured 80.22, dropping the whole band by
16.22 px.

**Two things here were counter-intuitive and both cost a round to find.** The first no longer holds:
while the logo capped the title row, a one-line title did *not* shrink the header by a line — below
`logo_px` the **logo** set the row's height, so the header bottomed out at **82.48 on Vertical** and
**76.48 on Horizontal** however short the title got. With the logo now a sibling that cap is gone and
a one-line title does shrink the header (see the note above). The second still holds: the footer's
rows are pinned to the frame's bottom margin, so a Note gaining a line does not push the source row
down — it eats the chart's height instead.

### A one-line title left a gap the templates were never exercised for — and the fix has shipped

That first point had a visible consequence, not just an arithmetic one. Because the title row hugged
the taller of the title and the logo, and both were top-aligned, the logo's surplus height landed
*between the title and the subtitle*: **12.26 px on Vertical, 6.26 on Horizontal**, on top of the 6 px
auto-layout gap. Every finished page in the Charts file shows **6 px** there — they all have two-line
titles, taller than the logo — so a one-line title was the only case that looked wrong, and on
Vertical it looked wrong by a factor of three.

**The templates have since been rebuilt with the logo outside the header, which is that fix.** There
is no longer a title row to take the logo out of, so do **not** go looking for one to set
`layoutPositioning = "ABSOLUTE"` on — the step-side half is all that is left: keep `logo_px` at 0 so
the derived band follows the header up. Kept here because the gap is what a regression would look
like: if a one-line title ever comes back with 12 px above its subtitle, the logo has been moved back
inside the header.

**Check one thing per frame, since the header now sits raised by default.** The subtitle's first line
runs at the logo's height, and the subtitle slot is full-width in every template. So the raised header
is safe only where that line's ink stops short of the logo's left edge — on the Vertical frame it
ended at x=588 against a logo at 770, comfortably clear; on the 540-wide one it reached x=493 against
a logo at 476, which collides. **Keeping the logo in the header's flow is no longer one of the
remedies** — it is a sibling on every family now, so the gap it used to buy is gone. Where the first
line reaches the logo, shorten or wrap the subtitle instead. Measure that line rather than eyeballing
it (the recipe below), and expect the answer to differ between the two frames of the same chart.

Calibrate any implementation against both ends — **but not against the figures below until they are
re-measured.** They are the padded, logo-capped generation's: two-line/two-line reproducing **118.22**
on either 850-wide frame (the current templates measure **118**), and a one-line/one-line clone
**82.48** on Vertical (the live clone of the day measured 82.47) or **76.48** on Horizontal, where the
current structure puts Static Vertical at **70**. Take the two-line/two-line end from
`/create-figma-chart`'s node map, and measure the one-line/one-line end on a live clone before
calibrating against it.
