# Marimekkos

> Chart-type conventions from the DI Charts Guidelines. Read **only** the file for the chart in
> hand — see [GUIDELINES.md](../../GUIDELINES.md) → Per chart type for the index.

The page is really **ranked many-entity bar charts** (grapher's marimekko / variable-width discrete bar), and all three share one problem: far too many bars to label.

- **Number the top few and call them out in a list** — `1. Tajikistan 48%`, rank and name and value on one line, name and value in the entity's category color, tied to its bar by a hairline leader or a bracket. `603:844` (top 5), `633:1417` (top 4).
- **Keep the category legend only when color encodes a category.** `603:844` and `633:1417` keep the continent legend because 150 bars cannot be direct-labeled. `99:723` drops it: there color marks the extremes (teal top block, red bottom block, everything else gray) and the colored entity labels down the left are the key.
- **Rotate and category-color the x-axis labels, and sample them** — `603:844`, `633:1417`. With ~150 categorical bars there is no horizontal option; this is a deliberate exception to "keep text horizontal".
- **A bracket ties a block of named entities to a range of bars** — `99:723` gathers the five named top countries and the three named bottom ones.
- Keep a reference entity as a full-width rule in its own color — `99:723` draws "World" as a tan bar cutting across the gray stack.

Exemplars: `99:723`, `603:844`, `633:1417`.
