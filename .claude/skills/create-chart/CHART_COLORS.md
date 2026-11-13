# Chart colors: which scheme to pick

Reference for the "Chart colors" rule in `SKILL.md`. It follows OWID's [Choosing colors for visualizations](https://app.notion.com/p/owid/Choosing-colors-for-visualizations-30f7269f4f69449a9628d38872c588b4) decision tree, with the guide's palette names translated into the `baseColorScheme` names Grapher uses (`ColorSchemeName` in owid-grapher's `GrapherTypes.ts`). Map palettes for numeric maps are in [`MAP_PALETTES.md`](MAP_PALETTES.md).

## Decision tree

Go through the questions in order and stop at the first that applies.

1. **Is the data one of these?**
   - Binary (yes / no): `BinaryMapPaletteA` to `BinaryMapPaletteE`.
   - World regions and income groups: `continents` on charts, `OwidCategoricalMap` on maps. Both color each region and the four World Bank income groups by name, so the same group gets the same color in every chart and in every map. Regions get the strong chart colors on charts and the muted map colors on maps; the income groups keep the same four colors on both. Never put a map on `continents`.
   - Energy sources: `OwidEnergy` (`OwidEnergyLines` for line charts).
   - Gender: the guide calls for a gender palette, but Grapher has no scheme for it. Set the colors with `customCategoryColors` or per-series colors, and ask the design team which ones.
2. **Are the values ordered?**
   - Numbers that start at zero and go up, or categories with an order that aren't numbers: a sequential ColorBrewer scheme (see `MAP_PALETTES.md`) or `Viridis`.
   - Numbers with a clear midpoint: a diverging scheme, with `midpoint` set.
3. **Is it an unstacked bar chart or a scatter plot without clear subgroups?** Use one OWID color: `SingleColorDenim`, `SingleColorTeal`, `SingleColorPurple`, `SingleColorDustyCoral` or `SingleColorDarkCopper`. These are the first colors of the five categorical palettes below.
4. **Otherwise, pick a categorical scheme by the number of categories:**

   | Categories | Scheme |
   |---|---|
   | 8 or fewer | Any of `OwidCategoricalA` to `OwidCategoricalE` |
   | 9 | `OwidCategoricalA` or `OwidCategoricalB` |
   | 10 | `OwidCategoricalC` or `OwidCategoricalD` |
   | 11 | `OwidCategoricalE` |
   | 12 | `OwidCategoricalA` |
   | 13 | `OwidCategoricalB` |
   | 14 to 24 | `OwidCategoricalC` |
   | 25 or more | Reconsider the chart: small multiples or fewer, aggregated categories. If it has to stay, `OwidCategoricalC` |

**Maps of categories with no order** use `OwidCategoricalMap`, the muted palette designed for map fills, and also the scheme for maps of regions and income groups. It colors known regions and income groups by name and the other categories in order. When you set colors one by one with `customCategoryColors`, take them from the same palette (`OwidMapColors` in owid-grapher's `CustomSchemes.ts`).

## What the admin shows first

The admin's color dropdown puts the schemes Grapher prefers for the chart type at the top, then every other scheme. A scheme further down still works; the order is a hint, not a limit.

| Chart type | Preferred schemes, in order |
|---|---|
| Map | `BinaryMapPaletteA` to `E`, `OwidCategoricalMap` |
| Line chart | `OwidDistinctLines` |
| Discrete bar | `SingleColorDenim`, `SingleColorDustyCoral`, `SingleColorPurple`, `SingleColorTeal`, `SingleColorDarkCopper` |
| Marimekko | `continents`, the five `SingleColor*`, `OwidCategoricalA` to `E` |
| Scatter plot, slope chart | `continents`, `OwidDistinctLines` |
| Stacked area, stacked bar, stacked discrete bar | `owid-distinct`, `OwidCategoricalA` to `E`, the five `SingleColorGradient*` |
| Swimlane | `OwidCategoricalA` to `E`, the five `SingleColorGradient*` |
| Dumbbell | None: dumbbell charts don't take a color scheme |

For maps, the sequential and diverging schemes aren't in the preferred list, so they appear below it, in the dropdown's default order.

## Schemes the guide doesn't cover

Grapher also has these. Prefer the guide's schemes above; use these only for a reason the guide doesn't address.

- `owid-distinct` and `stackedAreaDefault`: the default for stacked charts and the older default categorical scheme.
- `OwidDistinctLines`: the default for line charts.
- `SingleColorGradientDenim`, `Teal`, `Purple`, `DustyCoral`, `DarkCopper`: shades of one OWID color, for the series of a stacked chart.
- `Magma`, `Inferno`, `Plasma`: perceptual scales like `Viridis`. The guide names only `Viridis`.
- ColorBrewer qualitative schemes: `Accent`, `Dark2`, `Paired`, `Pastel1`, `Pastel2`, `Set1`, `Set2`, `Set3`. The OWID categorical schemes replace them.
- `Greys`: not used.
