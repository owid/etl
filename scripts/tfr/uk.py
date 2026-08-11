"""United Kingdom: births from all three registration offices over ONS's UK population estimates.

The UN publishes fertility for the United Kingdom and for nothing smaller, so comparing England and
Wales against it was never like for like: it set about 89% of UK births against a rate covering all of
them, and coloured Scotland and Northern Ireland on the map with a figure that excluded both. There is
no England or England-and-Wales on the UN's side to compare against instead, so the only honest fix is
to build the United Kingdom.

No office publishes a UK-wide fertility rate. ONS's UK reference tables carry births, deaths and
marriages for the UK and its four countries but no fertility rate, and they stop at 2021. So the rate
here is assembled:

* England and Wales births by the mother's age, from ONS's own births workbook, table 10.
* Scotland births by the mother's age, from National Records of Scotland's vital events reference
  tables, table 3.01b.
* Northern Ireland births by the mother's age, from NISRA's Registrar General tables, table 3.3, which
  gives single years of age and is summed into the same groups as the other two.
* Women by single year of age for the United Kingdom, from ONS's mid-year population estimates for the
  UK and its constituent countries, sheet MYEB4.

Two conventions, both ONS's own. Births under 20 include every birth below that age, and births at 40
and over are divided by the women aged 40 to 44 — which is how ONS's own long-running rate is built,
and what the England and Wales series here used before. It slightly overstates the oldest group's rate,
by the handful of births above 45, and understates nothing.

The window is 2011 to 2024. It starts where the population file starts and ends at the last year all
three offices have published; England and Wales alone reaches 2025, but a UK figure cannot.

The three offices' own published rates are the check. Running this arithmetic on one country at a time
reproduces each of them to within a hundredth, which is what `python uk.py` prints.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data")
SCOTLAND = ("https://www.nrscotland.gov.uk/media/wp2eyitb/"
            "vital-events-reference-tables-chapter-3.xlsx")
NORTHERN_IRELAND = ("https://www.nisra.gov.uk/system/files/statistics/2026-02/"
                    "Section%203%20-%20Births_Tables_2024-Final.xlsx")
MYEB = ("https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/"
        "populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland/"
        "mid2011tomid2024/myebtablesuk20112024.xlsx")

# the groups ONS's births workbook uses. The last one holds births at 40 and over against women 40-44.
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44)]
EW_BANDS = {"Under 20": (15, 19), "20 to 24": (20, 24), "25 to 29": (25, 29),
            "30 to 34": (30, 34), "35 to 39": (35, 39), "40 and over": (40, 44)}
SCOTLAND_BANDS = {"Number under 20": (15, 19), "Number 20-24": (20, 24), "Number 25-29": (25, 29),
                  "Number 30-34": (30, 34), "Number 35-39": (35, 39),
                  "Number 40-44": (40, 44), "Number 45 and over": (40, 44)}
FIRST, LAST = 2011, 2024


def _england_wales_births():
    """{year: {band: births}} from ONS table 10, mothers, England and Wales."""
    d = pd.read_excel(os.path.join(DATA, "uk_births.xlsx"), sheet_name="Table_10", header=5)
    d = d[(d.iloc[:, 2] == "Mother") & (d.iloc[:, 1] == "England, Wales and Elsewhere")]
    out = {}
    for _, r in d.iterrows():
        year = pd.to_numeric(r.iloc[0], errors="coerce")
        band = EW_BANDS.get(str(r.iloc[3]).strip())
        births = pd.to_numeric(r.iloc[4], errors="coerce")
        if pd.isna(year) or band is None or pd.isna(births):
            continue
        out.setdefault(int(year), {})
        out[int(year)][band] = out[int(year)].get(band, 0.0) + float(births)
    return out


def _scotland_births():
    """{year: {band: births}} from table 3.01b, the row covering all parental marital statuses."""
    path = fetch(SCOTLAND, os.path.join(DATA, "uk", "scotland_births.xlsx"))
    d = pd.read_excel(path, sheet_name="Table_301b", header=3)
    d = d[d.iloc[:, 1].astype(str).str.strip() == "All"]
    out = {}
    for _, r in d.iterrows():
        year = pd.to_numeric(r.iloc[0], errors="coerce")
        if pd.isna(year):
            continue
        year = int(year)
        for label, band in SCOTLAND_BANDS.items():
            v = pd.to_numeric(r.get(label), errors="coerce")
            if pd.notna(v):
                out.setdefault(year, {})
                out[year][band] = out[year].get(band, 0.0) + float(v)
        # the table carries its own all-ages total and a count whose age was not stated, so the groups
        # can be checked against it. Reading one column short would otherwise pass silently.
        total = pd.to_numeric(r.get("Number All ages"), errors="coerce")
        unstated = pd.to_numeric(r.get("Number age not stated"), errors="coerce") or 0.0
        if pd.notna(total) and year in out:
            counted = sum(out[year].values())
            assert abs(counted - (float(total) - float(unstated))) < 1, (
                f"Scotland {year}: groups sum to {counted:,.0f}, table says {total:,.0f} "
                f"less {unstated:,.0f} of unstated age")
    return out


def _northern_ireland_births():
    """{year: {band: births}} from NISRA table 3.3, single years of age summed into groups.

    Under 20 takes every age below 20, and the last group every age from 40 up, matching how the other
    two offices report theirs.
    """
    path = fetch(NORTHERN_IRELAND, os.path.join(DATA, "uk", "ni_births.xlsx"))
    d = pd.read_excel(path, sheet_name="Table 3.3", header=4)
    ages = {}
    for j, label in enumerate(d.columns):
        a = pd.to_numeric(str(label).replace(" and over", "").replace("+", ""), errors="coerce")
        if pd.notna(a) and 5 <= a <= 60:
            ages[j] = int(a)
    out = {}
    for _, r in d.iterrows():
        year = pd.to_numeric(r.iloc[0], errors="coerce")
        if pd.isna(year):
            continue
        year = int(year)
        for j, age in ages.items():
            v = pd.to_numeric(r.iloc[j], errors="coerce")
            if pd.isna(v):
                continue
            band = (15, 19) if age < 20 else next((b for b in BANDS if b[0] <= age <= b[1]), None)
            if age >= 40:
                band = (40, 44)
            if band:
                out.setdefault(year, {})
                out[year][band] = out[year].get(band, 0.0) + float(v)
        total = pd.to_numeric(r.iloc[1], errors="coerce")
        if pd.notna(total) and year in out:
            counted = sum(out[year].values())
            assert abs(counted - float(total)) < 1, (
                f"Northern Ireland {year}: single ages sum to {counted:,.0f}, table says {total:,.0f}")
    return out


def _women(name="UNITED KINGDOM"):
    """{year: {band: women}} from ONS's mid-year estimates, sheet MYEB4."""
    path = fetch(MYEB, os.path.join(DATA, "uk", "myeb.xlsx"))
    d = pd.read_excel(path, sheet_name="MYEB4", header=1)
    d = d[(d.Name == name) & (d.sex == "f")]
    out = {}
    for col in [c for c in d.columns if str(c).startswith("population_")]:
        year = int(str(col).split("_")[1])
        for _, r in d.iterrows():
            age = pd.to_numeric(r.age, errors="coerce")
            v = pd.to_numeric(r[col], errors="coerce")
            if pd.isna(age) or pd.isna(v):
                continue
            band = next((b for b in BANDS if b[0] <= int(age) <= b[1]), None)
            if band:
                out.setdefault(year, {})
                out[year][band] = out[year].get(band, 0.0) + float(v)
    return out


def _rate(births, women):
    """The seven-group total: each group's births over its women, summed, times the group width."""
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if not all(b.get(x) and w.get(x) for x in BANDS):
            continue
        if not FIRST <= year <= LAST:
            continue
        rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def _uk_births():
    ew, sc, ni = _england_wales_births(), _scotland_births(), _northern_ireland_births()
    out = {}
    for year in sorted(set(ew) & set(sc) & set(ni)):
        merged = {}
        for part in (ew[year], sc[year], ni[year]):
            for band, v in part.items():
                merged[band] = merged.get(band, 0.0) + v
        # a missing group would silently shrink the rate rather than fail, so require all six
        assert set(merged) == set(BANDS), f"UK {year}: groups {sorted(merged)} not {BANDS}"
        # Scotland and Northern Ireland are each far smaller than England and Wales; if a parse
        # silently returned the wrong column the shares would move a long way from these
        share = sum(sc[year].values()) / sum(merged.values())
        assert 0.05 < share < 0.09, f"UK {year}: Scotland is {share:.1%} of UK births, expected 5-9%"
        share = sum(ni[year].values()) / sum(merged.values())
        assert 0.02 < share < 0.04, f"UK {year}: Northern Ireland is {share:.1%}, expected 2-4%"
        out[year] = merged
    return out


def uk_tfr():
    return _rate(_uk_births(), _women())


def uk_detail(year):
    births, women = _uk_births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if births.get(b) and women.get(b)}


def _published():
    """{country: {year: its own rate}}, each read from that office's own file.

    Every figure here comes out of a spreadsheet rather than from anybody's memory. The first version
    of this check compared against rates typed in by hand, and two of the six were wrong -- Northern
    Ireland's was out by 0.16, which made a correct parse look like a 12% error.
    """
    ons = pd.read_excel(os.path.join(DATA, "uk_births.xlsx"), sheet_name="Table_10", header=5)
    ons = ons[(ons.iloc[:, 2] == "Mother") & (ons.iloc[:, 1] == "England, Wales and Elsewhere")]
    ons = ons[ons.iloc[:, 3].astype(str).str.strip().isin(EW_BANDS)]
    ons["year"] = pd.to_numeric(ons.iloc[:, 0], errors="coerce")
    ew = (pd.to_numeric(ons.iloc[:, 5], errors="coerce").groupby(ons.year).sum() * 5 / 1000).to_dict()

    sc = pd.read_excel(fetch(SCOTLAND, os.path.join(DATA, "uk", "scotland_births.xlsx")),
                       sheet_name="Table_304", header=4)
    sc = {int(y): float(v) for y, v in zip(pd.to_numeric(sc.iloc[:, 0], errors="coerce"),
                                          pd.to_numeric(sc.iloc[:, 5], errors="coerce"))
          if pd.notna(y) and pd.notna(v)}

    d = pd.read_excel(fetch(NORTHERN_IRELAND, os.path.join(DATA, "uk", "ni_births.xlsx")),
                      sheet_name="Table 3.13", header=3)
    ni = {}
    for _, r in d.iterrows():
        y = pd.to_numeric(r.iloc[0], errors="coerce")
        rates = [pd.to_numeric(v, errors="coerce") for v in r.iloc[2:9]]
        if pd.notna(y) and not any(pd.isna(v) for v in rates):
            ni[int(y)] = float(sum(rates)) * 5 / 1000
    return {"ENGLAND AND WALES": {int(k): v for k, v in ew.items() if pd.notna(k)},
            "SCOTLAND": sc, "NORTHERN IRELAND": ni}


if __name__ == "__main__":
    t = uk_tfr()
    print(t.to_string(index=False), f"({len(t)} years)")
    print("\nthe same arithmetic one country at a time, against each office's own rate from its own file:")
    published = _published()
    for name, births in [("ENGLAND AND WALES", _england_wales_births()),
                         ("SCOTLAND", _scotland_births()),
                         ("NORTHERN IRELAND", _northern_ireland_births())]:
        ours = {int(r.year): r.value for r in _rate(births, _women(name)).itertuples()}
        for year in (2015, 2020, 2023, 2024):
            theirs = published[name].get(year)
            if year in ours and theirs:
                print(f"  {name} {year}: ours {ours[year]:.3f}, its own {theirs:.3f}, "
                      f"{ours[year] - theirs:+.3f}")
