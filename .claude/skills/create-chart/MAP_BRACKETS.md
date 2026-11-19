# Map brackets: how to set them

Reference for the "Map brackets" rule in `SKILL.md`. It follows OWID's [How to pick map brackets](https://app.notion.com/p/owid/How-to-pick-map-brackets-fca269b396c3470e8d54dd762390ce2a) and Grapher's binning code ([readme](https://github.com/owid/owid-grapher/blob/master/packages/%40ourworldindata/grapher/src/color/readme.md), `BinningStrategies.ts` and `ColorScale.ts` in `packages/@ourworldindata/grapher/src/color/`). The fields live under `map.colorScale`.

## The rule

- **Brackets follow a regular pattern**, for the same reason axis ticks do: each step in color is the same step in value.
  - Linear: each edge is the previous one plus a fixed amount (0, 10, 20, 30, …).
  - Log: each edge is the previous one times 2 or 10 (1, 10, 100, …).
  - Log-like: 1, 2, 5, 10, 20, 50, … or 1, 3, 10, 30, 100, ….
- **Never move an edge to make particular countries stand out.** That puts an editorial view on top of the data, and the bracket usually empties at the next update.
- **The one exception is a real-world threshold**, such as poverty lines, SDG targets or the World Bank's income cutoffs.
- **Use 5 to 8 brackets**, up to 10 if needed. Four or fewer lose information; more than 10 make the legend hard to read.
- **Don't skip a level** in a linear scale, and **don't mix progressions** (1, 2, 4, 8, then 20, 30).
- **Don't let the lowest or highest bracket hold most countries**, and leave room for the historical values and projections the map's timeline shows.

## Prefer the automated brackets

Grapher computes regular brackets itself. `binningStrategy` defaults to `auto`, which uses equal-size brackets when the values span fewer than three orders of magnitude, and log brackets otherwise.

| Strategy | Brackets |
|---|---|
| `equalSizeBins-normal` | Equal steps of a round size, 5 to 9 brackets |
| `equalSizeBins-few-bins`, `equalSizeBins-many-bins` | Fewer (2 to 5) or more (8 to 12) |
| `equalSizeBins-percent` | 0 to 100 in steps of 10; `auto` picks it for percentages that reach at least 60% |
| `log-1-2-5`, `log-1-3`, `log-10` | The log-like patterns above; `log-auto` picks one |

By default the range runs between low and high percentiles of the values (the 5th and 95th for equal-size brackets, the 15th and 99.5th for log brackets), so outliers fall into open-ended end brackets. When that range gives awkward brackets, set `minValue` and `maxValue`: Grapher then fills that range with regular steps.

For example, the age at which people can receive a full pension runs from 47 to 67, and most countries are between 60 and 67. For the 2024 values, the default range gives two-year brackets from 50 to 68, most of them nearly empty. Setting the range to 59 to 67 gives one-year brackets, with an open-ended "60 and below":

```yaml
map:
  colorScale:
    baseColorScheme: Purples
    binningStrategy: equalSizeBins-normal
    minValue: 59
    maxValue: 67
```

## Open-ended brackets

Grapher opens the first bracket when some values lie below its lower edge, and the last bracket when some lie above its upper edge. It then draws an arrow at that end of the legend and hides the edge's number. So an open end needs no setting of its own: put the outer edge inside the range of the data (`minValue` above the lowest value, or the last manual edge below the highest). The admin tricks in the Notion guide (an upper bound below the lower one for the last bracket, a "Minimum value" for the first) work the same way.

## Manual brackets

Use `binningStrategy: manual` only for real-world thresholds, or when no automated strategy gives a regular scale. `customNumericValues` lists every edge, lowest first, and the edges still follow one of the patterns above:

```yaml
map:
  colorScale:
    baseColorScheme: Greens
    binningStrategy: manual
    customNumericValues: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
```
