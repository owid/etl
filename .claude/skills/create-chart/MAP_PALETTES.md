# Map palettes: which scheme to pick

Reference for the "Map palettes" rules in `SKILL.md`. Values are `map.colorScale.baseColorScheme` names from the grapher schema (`DEFAULT_GRAPHER_SCHEMA` in `etl/config.py`).

## Sequential schemes, by whether more is good or bad

The ColorBrewer schemes go from light for low values to dark for high ones.

| More is | Schemes |
|---|---|
| Good (blue, green) | `Blues`, `Greens`, `BuGn`, `GnBu`, `YlGn`, `YlGnBu`, `PuBu`, `PuBuGn` |
| Bad (red, orange, brown) | `Reds`, `Oranges`, `OrRd`, `YlOrRd`, `YlOrBr`, `PuRd` |
| Either (purple) | `Purples`, `BuPu`, `RdPu` |

We don't use `Greys`.

## Other schemes

- **`SingleColorGradient*`** (`Denim`, `Teal`, `Purple`, `DustyCoral`, `DarkCopper`) are OWID's own shades of one brand color, from dark to light. They're for coloring the series of stacked charts (as the top-level `baseColorScheme`), and are rarely used on maps.
- **Perceptual schemes** (`Viridis`, `Magma`, `Inferno`, `Plasma`) carry no good or bad reading. `Magma` and `Inferno` end in near-white yellow, so their top values can blend into a white background.
- **Diverging schemes** (`RdYlGn`, `RdBu`, `RdYlBu`, `BrBG`, `PiYG`, `PRGn`, `PuOr`, `RdGy`, `Spectral`) are for indicators with a meaningful midpoint, such as a change that can be positive or negative. Set `midpoint` to it. The red, pink or brown end reads as bad. Set `colorSchemeInvert: true` if that end needs to be on the other side.
- **Binary schemes** (`BinaryMapPaletteA` to `BinaryMapPaletteE`) have two colors, for maps with two classes such as a yes/no indicator. They work on numeric maps too: most charts that use them color a 0–1 indicator with a few manual bins (`binningStrategy: manual`).
- **Categorical schemes** (`Set*`, `Pastel*`, `Paired`, `Accent`, `Dark2`, `Owid*`, `continents`) are for categorical maps, not numeric scales.
