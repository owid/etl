# Misc — treemaps, arrow charts, dot-and-interval

> Chart-type conventions from the DI Charts Guidelines. Read **only** the file for the chart in
> hand — see [GUIDELINES.md](../../GUIDELINES.md) → Per chart type for the index.

Three forms with no other home:

- **Treemap** (`229:192`, 540×800). Category legend as a row of **colored bold words, no swatches**. In-box labels white: name bold ~16px, share beneath at ~12px, and large boxes carry an extra absolute line ("210,000 annual deaths") above the share. Boxes too small for a label take an abbreviated hyphen-wrapped one. No axes — and the subtitle must explain the encoding ("The size of each box is proportional to…").
- **Arrow chart on a log axis** (`263:377`). Horizontal arrows from a `1x` baseline to each value, sorted descending, each labeled at the arrowhead in the series color. Log ticks `1x 2x 4x 8x 16x`, no legend, no gridlines.
- **Dot chart with uncertainty bars** (`222:825`). Dots direct-labeled with the entity name, vertical whiskers for the interval, and the subtitle saying so ("The bars show uncertainty in the estimates").

Exemplars: `229:192`, `263:377`, `222:825`.
