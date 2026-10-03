"""Garden step for SDG 11.2.1 - Access to Public Transport.

Creates a table with country-level data: Population-weighted country averages across all cities
"""

import unicodedata

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Manual name mapping for cities that don't match automatically
CITY_NAME_MAPPING = {
    # Format: (country, sdg_city_name): ghsl_city_name
    # Albania
    ("Albania", "Elbasan (Elbasani)"): "Elbasan",
    # Azerbaijan
    ("Azerbaijan", "Baku"): "Bakı (Baku)",
    # Bangladesh
    ("Bangladesh", "Chittagong"): "Chattogram",
    ("Bangladesh", "Jessore"): "Jashore",
    ("Bangladesh", "Bogra"): "Bogura",
    ("Bangladesh", "Comilla"): "Cumilla",
    # Belarus
    ("Belarus", "Babrujsk (Babruysk)"): "Babruysk",
    ("Belarus", "Gomel"): "Homel",
    ("Belarus", "Grodno"): "Hrodna",
    ("Belarus", "Vitebsk"): "Vitsebsk",
    # Belgium
    ("Belgium", "Brugge"): "Bruges",
    ("Belgium", "Gent"): "Ghent",
    # China
    ("China", "Hong Kong, Hong Kong"): "Hong Kong",
    # Cyprus
    ("Cyprus", "Lemesos"): "Limassol",
    # Czechia
    ("Czechia", "Plzen"): "Plzeň",
    # Egypt
    ("Egypt", "Bur Sa'id"): "Port Said",
    # Iran
    ("Iran", "Tehran"): "Tehrān",
    # Iraq
    ("Iraq", "Az-Zubayr (Zubayr)"): "Az Zubayr",
    # Israel
    ("Israel", "Be'er Sheva"): "Beersheba",
    # Italy
    ("Italy", "Venezia"): "Venice",
    ("Italy", "Padova"): "Padua",
    # Kuwait
    ("Kuwait", "Al-Kuwayt (Kuwait City)"): "Kuwait City",
    # Laos
    ("Laos", "Viengchan"): "Vientiane",
    ("Laos", "Vientiane"): "Vientiane",
    # Luxembourg
    ("Luxembourg", "Luxembourg (Lëtzebuerg)"): "Luxembourg-Ville",
    # Madagascar
    ("Madagascar", "Mahajanga (Majunga)"): "Mahajanga",
    ("Madagascar", "Taolanaro (Tola'aro, Tôlanaro, Fort-Dauphin)"): "Tolanaro",
    ("Madagascar", "Toliara (Tuléar)"): "Toliara",
    # Malawi
    ("Malawi", "Blantyre-Limbe"): "Blantyre",
    # Malaysia
    ("Malaysia", "W.P.Putrajaya"): "Putrajaya",
    # Maldives
    ("Maldives", "Male'"): "Malé",
    # Mexico
    ("Mexico", "Oaxaca"): "Oaxaca de Juárez",
    ("Mexico", "Veracruz de Ignacio de la Llave"): "Veracruz",
    # Monaco
    ("Monaco", "Monaco-Ville"): "Monaco",
    # Montenegro
    ("Montenegro", "Podgorica (Titograd)"): "Podgorica",
    # Morocco
    ("Morocco", "Dar-el-Beida (Casablanca)"): "Casablanca",
    ("Morocco", "Marrakech"): "Marrakesh",
    ("Morocco", "Tanger"): "Tangier",
    ("Morocco", "Fès"): "Fez",
    # Mozambique
    ("Mozambique", "Nacala Porto"): "Nacala",
    # Myanmar
    ("Myanmar", "Bago (Pegu)"): "Pegu",
    ("Myanmar", "Kale (Kalemyo, Kalay)"): "Kalay",
    ("Myanmar", "Mawlamyine (Moulmein)"): "Mawlamyine",
    ("Myanmar", "Myeik (Mergui)"): "Myeik",
    # New Zealand
    ("New Zealand", "Palmerston"): "Palmerston North",
    # Norway
    ("Norway", "Fredrikstad/Sarpsborg"): "Fredrikstad",
    # Oman
    ("Oman", "Al-Buraymī"): "Al-Buraimi",
    ("Oman", "Salahah (Salalah)"): "Salalah",
    ("Oman", "Ṣuḥār (Sohar)"): "Sohar",
    # Poland
    ("Poland", "Łódź"): "Lodz",
    ("Poland", "Kraków (Cracow)"): "Krakow",
    # Portugal
    ("Portugal", "Coimbra"): "Coimbra",
    # Qatar
    ("Qatar", "Al-Khawr (Al Khor)"): "Al Khor",
    ("Qatar", "Musaī'īd (Umm Sa'īd, Mesaieed)"): "Mesaieed",
    # Republic of Moldova
    ("Republic of Moldova", "Bălți (Bel'cy)"): "Bălți",
    ("Republic of Moldova", "Tiraspol (Tiraspol')"): "Tiraspol",
    # Russia
    ("Russia", "Perm"): "Perm'",
    # Russian Federation
    ("Russian Federation", "Al'metjevsk (Almetyevsk)"): "Almetyevsk",
    ("Russian Federation", "Ačinsk (Achinsk)"): "Achinsk",
    ("Russian Federation", "Batajsk (Bataysk)"): "Bataysk",
    ("Russian Federation", "Bijsk (Biysk)"): "Biysk",
    ("Russian Federation", "Blagoveščensk (Blagoveshchensk)"): "Blagoveshchensk",
    ("Russian Federation", "Dzeržinsk (Dzerzhinsk)"): "Dzerzhinsk",
    ("Russian Federation", "Sankt Peterburg (Saint Petersburg)"): "Saint Petersburg",
    # Rwanda
    ("Rwanda", "Gitarama (Muhanga)"): "Muhanga",
    ("Rwanda", "Nyanza (Nyabisindu)"): "Nyanza",
    # Saudi Arabia
    ("Saudi Arabia", "Ad-Dammam"): "Dammam",
    ("Saudi Arabia", "Al-Madinah (Medina)"): "Medina",
    ("Saudi Arabia", "Ar-Riyadh (Riyadh)"): "Ar-Riyāḑ (Riyadh)",
    ("Saudi Arabia", "Jiddah"): "Jeddah",
    ("Saudi Arabia", "Makkah (Mecca)"): "Mecca",
    ("Saudi Arabia", "Rafḥā'"): "Rafha",
    # Senegal
    ("Senegal", "Mbour (M'Bour)"): "M'bour",
    ("Senegal", "Touba"): "Touba Toul",
    # Somalia
    ("Somalia", "Hargeysa"): "Hargeisa",
    # South Africa
    ("South Africa", "Durban (Ethekwini)"): "Durban",
    ("South Africa", "Uitenhage (Kwanobuhle - Despatch)"): "KwaNobuhle",
    # Spain
    ("Spain", "A Coruña (La Coruña)"): "A Coruña",
    ("Spain", "Donostia (San Sebastián)"): "Donostia / San Sebastián",
    ("Spain", "Ferrol (El Ferrol)"): "Ferrol",
    ("Spain", "Irun (Irún)"): "Irun",
    ("Spain", "Oviedo"): "Oviedo / Uviéu",
    # Sweden
    ("Sweden", "Göteborg"): "Gothenburg",
    # Syria
    ("Syria", "Hamah"): "Hama",
    ("Syria", "Lattakia"): "Latakia",
    # United Arab Emirates
    ("United Arab Emirates", "Rā's al-Khaymah (Ras al-Khaimah)"): "Ras al Khaimah",
    # Vietnam
    ("Vietnam", "Hà Noi"): "Hanoi",
    ("Vietnam", "Da Nang"): "Danang",
    # Yemen
    ("Yemen", "Sana'a'"): "Sanaa",
    ("Yemen", "Adan (Aden)"): "Aden",
}

# Administrative entries to exclude (not real cities)
ADMIN_KEYWORDS = [
    "national",
    "oblast",
    "province",
    "state",
    "region",
    "district",
    "territory",
    "prefecture",
    "county",
    "total",
    "average",
    "baden-wurtemberg",
    "bavaria",
    "hesse",
    "lower saxony",
    "north rhine-westphalia",
    "rhineland palatinate",
    "saarland",
    "saxony",
    "schleswig-holstein",
    "thuringia",
    "mecklenburg western pomerania",
    "saxony-anhalt",
    "balochistan-",
    "khyber pakhtunkhwa-",
    "punjab-",
    "sindh-",
    "baja california",
    "chiapas",
    "coahuila",
    "guerrero",
    "jalisco",
    "michoacan",
    "nayarit",
    "quintana roo",
    "sinaloa",
    "sonora",
    "tabasco",
    "tamaulipas",
    "yucatan",
]


def normalize_city_name(name: str) -> str:
    """Normalize city names for better matching.

    Removes diacritics, converts to lowercase, removes extra spaces,
    and handles parenthetical alternative names.
    """
    if pd.isna(name):
        return ""

    # Remove diacritics/accents
    name = "".join(c for c in unicodedata.normalize("NFD", str(name)) if unicodedata.category(c) != "Mn")

    # Convert to lowercase
    name = name.lower()

    # Remove extra spaces
    name = " ".join(name.split())

    # Extract content from parentheses if present (e.g., "Elbasan (Elbasani)" -> "elbasan|elbasani")
    if "(" in name and ")" in name:
        main_name = name.split("(")[0].strip()
        alt_name = name.split("(")[1].split(")")[0].strip()
        # Return both for matching
        return f"{main_name}|{alt_name}"

    return name


def match_cities_with_ghsl(tb_sdg: Table, tb_ghsl: pd.DataFrame) -> Table:
    """Match SDG cities with GHSL population data using improved matching.

    Uses multiple strategies:
    1. Exact match on normalized names
    2. Alternative name matching (from parentheses)
    3. Substring matching (SDG name in GHSL name, GHSL name in SDG name)
    4. Compound name matching (try first part before comma, hyphen, or slash)
    5. Prefix matching (first 5 characters)
    """
    tb_sdg = tb_sdg.copy()
    tb_ghsl = tb_ghsl.copy()

    # Normalize names
    tb_sdg["city_normalized"] = tb_sdg["city"].apply(normalize_city_name)
    tb_ghsl["city_normalized"] = tb_ghsl["ucname"].apply(normalize_city_name)

    # Strategy 1: Direct exact match on normalized names
    tb_ghsl_unique = tb_ghsl.drop_duplicates(subset=["city_normalized", "year"], keep="first")

    merged = pr.merge(
        tb_sdg,
        tb_ghsl_unique[["city_normalized", "year", "pop"]],
        on=["city_normalized", "year"],
        how="left",
        suffixes=("", "_ghsl"),
    )

    # Strategy 2: For unmatched cities with alternative names (containing "|"),
    # try matching on both parts
    unmatched_mask = merged["pop"].isna()
    unmatched_with_alt = merged[unmatched_mask & merged["city_normalized"].str.contains("|", regex=False)]

    for idx, row in unmatched_with_alt.iterrows():
        if "|" in row["city_normalized"]:
            parts = row["city_normalized"].split("|")
            for part in parts:
                ghsl_match = tb_ghsl[(tb_ghsl["city_normalized"] == part.strip()) & (tb_ghsl["year"] == row["year"])]
                if len(ghsl_match) > 0:
                    merged.loc[idx, "pop"] = ghsl_match.iloc[0]["pop"]
                    break

    # Strategy 3-5: Advanced matching for still-unmatched cities
    still_unmatched = merged[merged["pop"].isna()]

    for idx, row in still_unmatched.iterrows():
        sdg_norm = row["city_normalized"]
        country = row["country"]
        year = row["year"]

        # Get GHSL cities for this country and year
        ghsl_country = tb_ghsl[(tb_ghsl["country"] == country) & (tb_ghsl["year"] == year)]

        if len(ghsl_country) == 0:
            continue

        # Try different matching strategies
        matched = False

        for _, ghsl_row in ghsl_country.iterrows():
            ghsl_norm = ghsl_row["city_normalized"]

            # Skip if already matched or missing
            if pd.isna(ghsl_norm) or ghsl_norm == "":
                continue

            # Strategy 3a: Substring matching (SDG name in GHSL name)
            # E.g., "kabul" in "kabul (kabul)"
            if sdg_norm in ghsl_norm and len(sdg_norm) >= 4:
                merged.loc[idx, "pop"] = ghsl_row["pop"]
                matched = True
                break

            # Strategy 3b: Reverse substring (GHSL name in SDG name)
            # E.g., "guangzhou" in "guangzhou, guangdong"
            if ghsl_norm in sdg_norm and len(ghsl_norm) >= 4:
                merged.loc[idx, "pop"] = ghsl_row["pop"]
                matched = True
                break

            # Strategy 4: Compound name - try first part before delimiter
            # E.g., "guangzhou, guangdong" → "guangzhou"
            for delimiter in [",", "-", "/", "_"]:
                if delimiter in sdg_norm:
                    first_part = sdg_norm.split(delimiter)[0].strip()
                    if first_part == ghsl_norm and len(first_part) >= 4:
                        merged.loc[idx, "pop"] = ghsl_row["pop"]
                        matched = True
                        break
            if matched:
                break

            # Strategy 5: Prefix matching (first 5 characters)
            # E.g., "lashkargah" and "lashkar gah"
            if len(sdg_norm) >= 5 and len(ghsl_norm) >= 5:
                if sdg_norm[:5] == ghsl_norm[:5]:
                    merged.loc[idx, "pop"] = ghsl_row["pop"]
                    matched = True
                    break

    # Drop temporary column
    merged = merged.drop(columns=["city_normalized"])

    return merged


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets
    ds_meadow = paths.load_dataset("sdg_11_2_1")
    ds_ghsl = paths.load_dataset("ghsl_urban_centers", namespace="urbanization", version="2025-12-10")

    # Read tables
    tb = ds_meadow["sdg_11_2_1"].reset_index()
    tb_ghsl = ds_ghsl["ghsl_urban_centers_raw"].reset_index()

    # Rename GHSL columns to match our needs
    tb_ghsl = tb_ghsl.rename(
        columns={
            "urban_center_name": "ucname",
            "urban_pop": "pop",
        }
    )

    #
    # Process data.
    #
    # Harmonize country names
    tb = paths.regions.harmonize_names(tb)

    # Filter out administrative regions (not real cities)
    def is_admin_region(city_name):
        """Check if city name is actually an administrative region."""
        if pd.isna(city_name):
            return True
        city_lower = str(city_name).lower()
        return any(keyword in city_lower for keyword in ADMIN_KEYWORDS)

    tb_original_len = len(tb)
    tb = tb[~tb["city"].apply(is_admin_region)].copy()
    excluded_count = tb_original_len - len(tb)
    if excluded_count > 0:
        paths.log.info(f"Excluded {excluded_count} administrative regions from SDG data")

    # Apply manual name mapping before automatic matching
    for (country, sdg_name), ghsl_name in CITY_NAME_MAPPING.items():
        # Find matching rows in tb_ghsl
        ghsl_match = tb_ghsl[(tb_ghsl["country"] == country) & (tb_ghsl["ucname"] == ghsl_name)]
        if len(ghsl_match) > 0:
            # Add temporary mapping row to tb_ghsl for matching
            for _, ghsl_row in ghsl_match.iterrows():
                # Create a copy with the SDG name
                new_row = ghsl_row.copy()
                new_row["ucname"] = sdg_name
                tb_ghsl = pd.concat([tb_ghsl, pd.DataFrame([new_row])], ignore_index=True)

    # Match SDG cities with GHSL population data
    tb = match_cities_with_ghsl(tb, tb_ghsl)

    #
    # Create country-level table with population-weighted averages
    #
    # For cities with population data, calculate population-weighted averages
    tb_with_pop = tb[tb["pop"].notna()].copy()

    if len(tb_with_pop) > 0:
        # Calculate weighted average by country-year
        tb_weighted = (
            tb_with_pop.groupby(["country", "year"], observed=True)
            .apply(
                lambda x: pd.Series(
                    {
                        "public_transport_access": (x["public_transport_access"] * x["pop"]).sum() / x["pop"].sum()
                        if x["pop"].sum() > 0
                        else None,
                    }
                ),
                include_groups=False,
            )
            .reset_index()
        )
    else:
        # Fallback to simple average if no population data
        tb_weighted = (
            tb.groupby(["country", "year"], observed=True).agg({"public_transport_access": "mean"}).reset_index()
        )

    # Format the country-level table
    tb_countries = tb_weighted.format(["country", "year"], short_name="sdg_11_2_1_country")

    #
    # Save outputs.
    #
    # Create a new garden dataset with only the country-level table
    ds_garden = paths.create_dataset(tables=[tb_countries])

    # Save changes in the new garden dataset.
    ds_garden.save()
