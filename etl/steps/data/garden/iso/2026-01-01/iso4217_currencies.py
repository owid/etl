"""ISO 4217 currency codes per country, harmonized to OWID country names.

Processes the raw SIX Group XML (converted to JSON in the snapshot) to produce a clean
table of one currency per country, with OWID-harmonized country names.

Processing steps:
1. Drop entries with IsFund=true (special financial instruments, not everyday currencies).
2. Harmonize country names to OWID standard names.
3. Drop countries that still have more than one currency after step 1 — we can't be sure
   which currency PIP/WDI data uses for those countries, so it's safer to leave them blank.

Known caveats:
- Kosovo: not in ISO 4217 (uses EUR unofficially); will have a missing currency code in output.
- Countries dropped due to dual currencies after fund filtering include: Bhutan (BTN + INR),
  El Salvador (SVC + USD), Haiti (HTG + USD), Lesotho (LSL + ZAR), Namibia (NAD + ZAR),
  Panama (PAB + USD), Uruguay (UYU + UYW).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("iso4217_currencies.json")
    tb = snap.read_json()

    # Drop fund entries (special financial instruments, not everyday currencies).
    # These are flagged with is_fund=True in the snapshot (parsed from IsFund="true" XML attribute).
    tb = tb[~tb["is_fund"].fillna(False)]

    # Drop rows with no currency code (territories with no universal currency, e.g. Antarctica).
    tb = tb.dropna(subset=["currency_code"])

    # Normalise country names from ALL CAPS to title case before harmonization.
    # Also normalize Unicode right single quotation mark (U+2019) to ASCII apostrophe for consistent matching.
    tb["country"] = tb["country_name"].str.title().str.replace("\u2019", "'", regex=False)

    # Harmonize to OWID country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Drop countries that still have multiple currencies — we can't determine which one
    # PIP/WDI data uses for those countries, so it's safer to leave them blank.
    # Countries dropped include: Bhutan (BTN + INR), El Salvador (SVC + USD), Haiti (HTG + USD),
    # Lesotho (LSL + ZAR), Namibia (NAD + ZAR), Panama (PAB + USD), Uruguay (UYU + UYW).
    counts = tb.groupby("country")["currency_code"].transform("count")
    tb = tb[counts == 1]

    tb = tb[["country", "currency_code", "currency_name"]].set_index("country")
    tb.metadata.short_name = paths.short_name

    ds = paths.create_dataset(tables=[tb], check_variables_metadata=False)
    ds.save()
