"""Keep top100.csv honest.

The file has two kinds of column. `searched` and `note` are written by hand as each country's
source hunt finishes. The rest are derived from the registry every time the page is built, so the
file can never claim a country is plotted when it is not.
"""

import csv
import os

from countries import COUNTRIES

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "top100.csv")
FIELDS = ["rank", "country", "population_2026", "searched", "in_dataset", "recalculated", "tier", "note"]


def refresh():
    """Rewrite the derived columns from COUNTRIES. Returns a one-line summary."""
    # a few countries are plotted under a name that is not the UN's, so key on the WPP name
    plotted = {c["wpp_name"]: c for c in COUNTRIES if c["loader"]}

    rows = list(csv.DictReader(open(PATH)))
    for r in rows:
        c = plotted.get(r["country"])
        r["in_dataset"] = "yes" if c else "no"
        r["recalculated"] = ("yes" if c["recalculated"] else "no") if c else ""
        r["tier"] = c["tier"] if c else ""

    with open(PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    n = len(rows)
    return (f"top100: {sum(r['searched'] == 'full' for r in rows)}/{n} searched in full, "
            f"{sum(r['in_dataset'] == 'yes' for r in rows)} plotted, "
            f"{sum(r['recalculated'] == 'yes' for r in rows)} recalculated by us")


if __name__ == "__main__":
    print(refresh())
