# Superseded: the nested-logo template generation

> Kept only so a regression stays recognizable. **Its arithmetic no longer applies** — the live
> geometry is in [TEMPLATES.md](../TEMPLATES.md). Nothing here should be used to lay out a step.

**While the logo capped the title row, a one-line title did *not* shrink the header by a line** — below
`logo_px` the **logo** set the row's height, so the header bottomed out at **82.48 on Vertical** and
**76.48 on Horizontal** however short the title got. With the logo now a sibling that cap is gone and
a one-line title does shrink the header (see TEMPLATES.md).

### A one-line title left a gap the templates were never exercised for

That had a visible consequence, not just an arithmetic one. Because the title row hugged
the taller of the title and the logo, and both were top-aligned, the logo's surplus height landed
*between the title and the subtitle*: **12.26 px on Vertical, 6.26 on Horizontal**, on top of the 6 px
auto-layout gap. Every finished page in the Charts file shows **6 px** there — they all have two-line
titles, taller than the logo — so a one-line title was the only case that looked wrong, and on
Vertical it looked wrong by a factor of three.

**The current templates put the logo outside the header, which fixes that gap.** There
is no longer a title row to take the logo out of, so do **not** go looking for one to set
`layoutPositioning = "ABSOLUTE"` on — the step-side half is all that is left: keep `logo_px` at 0 so
the derived band follows the header up. Kept here because the gap is what a regression would look
like: if a one-line title ever comes back with 12 px above its subtitle, the logo has been moved back
inside the header.

**Check one thing per frame, since the header now sits raised by default.** The subtitle's first line
runs at the logo's height, and the subtitle slot is full-width in every template. So the raised header
is safe only where that line's ink stops short of the logo's left edge — on the Vertical frame it
ended at x=588 against a logo at 770, comfortably clear; on the 540-wide one it reached x=493 against
a logo at 476, which collides. **Keeping the logo in the header's flow is not one of the
remedies** — it is a sibling on every family. Where the first
line reaches the logo, shorten or wrap the subtitle instead. Measure that line rather than eyeballing
it (the recipe below), and expect the answer to differ between the two frames of the same chart.

Calibrate any implementation against both ends — **but not against the figures below until they are
re-measured.** They are the padded, logo-capped generation's: two-line/two-line reproducing **118.22**
on either 850-wide frame (the current templates measure **118**), and a one-line/one-line clone
**82.48** on Vertical or **76.48** on Horizontal, where the
current structure puts Static Vertical at **70**. Take the two-line/two-line end from
`/owid-staff:create-figma-chart`'s node map, and measure the one-line/one-line end on a live clone before
calibrating against it.
