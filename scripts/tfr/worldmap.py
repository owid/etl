"""Turn Natural Earth's country outlines into SVG paths, keyed by ISO code.

The projection is Robinson, which is what OWID's own maps use — grapher's world map is
``geoRobinson()`` from d3-geo-projection. The maths below is d3's implementation rather than a
lookup-and-straight-line approximation: the same twenty-entry table, interpolated quadratically
through each point's neighbours, so the outlines match what grapher draws.

Antarctica is dropped, as it is on OWID's maps. Coordinates are rounded to one decimal place, finer
than a pixel at the size this is drawn.
"""

import json
import math
import os

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "map")
SOURCE = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
          "ne_110m_admin_0_countries.geojson")
WIDTH = 1000.0
SKIP = {"ATA"}

# d3-geo-projection's Robinson table. Index 1 is the equator; index 0 exists so the quadratic
# through each point's neighbours works there too.
K = [
    (0.9986, -0.0620), (1.0000, 0.0000), (0.9986, 0.0620), (0.9954, 0.1240), (0.9900, 0.1860),
    (0.9822, 0.2480), (0.9730, 0.3100), (0.9600, 0.3720), (0.9427, 0.4340), (0.9216, 0.4958),
    (0.8962, 0.5571), (0.8679, 0.6176), (0.8350, 0.6769), (0.7986, 0.7346), (0.7597, 0.7903),
    (0.7186, 0.8435), (0.6732, 0.8936), (0.6213, 0.9394), (0.5722, 0.9761), (0.5322, 1.0000),
]


def robinson_raw(lon, lat):
    """d3's robinsonRaw, taking degrees. Returns unscaled projection units, y up."""
    lam, phi = math.radians(lon), math.radians(lat)
    i = min(18.0, abs(phi) * 36 / math.pi)
    ai = int(i)
    di = i - ai
    ax, ay = K[ai]
    bx, by = K[ai + 1]
    cx, cy = K[min(19, ai + 2)]
    x = lam * (bx + di * (cx - ax) / 2 + di * di * (cx - 2 * bx + ax) / 2)
    y = (math.pi / 2 if phi > 0 else -math.pi / 2) * (
        by + di * (cy - ay) / 2 + di * di * (cy - 2 * by + ay) / 2
    )
    return x, y


def _rings(geom):
    if geom["type"] == "Polygon":
        return geom["coordinates"]
    if geom["type"] == "MultiPolygon":
        return [ring for poly in geom["coordinates"] for ring in poly]
    return []


def paths():
    """({iso3: svg path}, viewBox height), fitted to a WIDTH-wide box."""
    src = fetch(SOURCE, os.path.join(DATA, "ne_110m.geojson"))
    features = []
    for f in json.load(open(src))["features"]:
        p = f["properties"]
        iso = p.get("ADM0_A3") or p.get("ISO_A3")
        if not iso or iso == "-99" or iso in SKIP:
            continue
        rings = [[robinson_raw(lon, lat) for lon, lat in ring] for ring in _rings(f["geometry"])]
        if rings:
            features.append((iso, rings))

    pts = [pt for _, rings in features for ring in rings for pt in ring]
    x0, x1 = min(p[0] for p in pts), max(p[0] for p in pts)
    y0, y1 = min(p[1] for p in pts), max(p[1] for p in pts)
    scale = WIDTH / (x1 - x0)
    height = round((y1 - y0) * scale, 1)

    out = {}
    for iso, rings in features:
        parts = []
        for ring in rings:
            pretty, last = [], None
            for x, y in ring:
                # y is flipped because SVG counts downward
                xy = (round((x - x0) * scale, 1), round((y1 - y) * scale, 1))
                if xy != last:
                    pretty.append(xy)
                    last = xy
            if len(pretty) > 2:
                parts.append("M" + "L".join(f"{a},{b}" for a, b in pretty) + "Z")
        if parts:
            out[iso] = out.get(iso, "") + "".join(parts)
    return out, height


if __name__ == "__main__":
    d, h = paths()
    print(len(d), "countries, viewBox 0 0", WIDTH, h,
          "| path bytes", sum(len(v) for v in d.values()))
