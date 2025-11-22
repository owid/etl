import json

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

ds_garden = paths.load_dataset("gbd_treemap")
ds_garden_child = paths.load_dataset("gbd_child_treemap")

tb = ds_garden["gbd_treemap"].reset_index()
tb_child = ds_garden_child["gbd_child_treemap"].reset_index()
tb = pr.concat([tb, tb_child], ignore_index=True)

# Filter data for Number metric only (keep all sexes)
tb_filtered = tb[tb["metric"] == "Number"].copy()

tb_filtered.set_index(["country", "year", "age", "sex", "broad_cause", "cause", "metric"], verify_integrity=True)
# Get unique values for mappings
countries = sorted(tb_filtered["country"].unique())
causes = sorted(tb_filtered["cause"].unique())
categories = sorted(tb_filtered["broad_cause"].unique())
sexes = sorted(tb_filtered["sex"].unique())

# Create age group mapping with proper display names
age_group_map = {
    "All ages": {"id": 1, "name": "All ages"},
    "<5 years": {"id": 2, "name": "Children under 5"},
    "5-14 years": {"id": 3, "name": "Children aged 5 to 14"},
    "15-49 years": {"id": 4, "name": "Adults aged 15 to 49"},
    "50-69 years": {"id": 5, "name": "Adults aged 50 to 69"},
    "70+ years": {"id": 6, "name": "Adults aged 70+"},
}

# Create entities list
entities = [{"id": i + 1, "name": country} for i, country in enumerate(countries)]

# Create age groups list
age_groups = [{"id": v["id"], "name": v["name"]} for v in sorted(age_group_map.values(), key=lambda x: x["id"])]

# Create sex mapping
sex_map = {
    "Both": {"id": 1, "name": "Both sexes"},
    "Female": {"id": 2, "name": "Female"},
    "Male": {"id": 3, "name": "Male"},
}

# Create sex list
sex_list = [{"id": v["id"], "name": v["name"]} for v in sorted(sex_map.values(), key=lambda x: x["id"])]

# Create categories list
categories_list = [{"id": i + 1, "name": cat} for i, cat in enumerate(categories)]
category_name_to_id = {cat: i + 1 for i, cat in enumerate(categories)}

# Optional descriptions for specific causes (add more as needed)
cause_descriptions = {
    "Heart diseases": "Heart attacks, strokes, and other cardiovascular diseases",
    "Chronic respiratory diseases": "COPD, Asthma, and others",
    "Neurological diseases": "Alzheimer's disease, Parkinson's disease, epilepsy, and others",
    "Digestive diseases": "Cirrhosis and others",
    "Respiratory infections": "Pneumonia, influenza, COVID-19 and others",
    "Neonatal deaths": "Babies who died in the first 28 days of life",
    # Add more descriptions here as needed
}

# Create variables list with age group associations
variables = []
for i, cause in enumerate(causes):
    # Find which age groups this cause appears in
    age_groups_for_cause = tb_filtered[tb_filtered["cause"] == cause]["age"].unique()
    age_group_ids = [age_group_map[age]["id"] for age in age_groups_for_cause if age in age_group_map]

    # Get category
    broad_cause = tb_filtered[tb_filtered["cause"] == cause]["broad_cause"].iloc[0]
    category_id = category_name_to_id[broad_cause]

    # Build variable entry
    variable_entry = {
        "id": i + 1,
        "name": cause,
        "ageGroup": sorted(age_group_ids),
    }

    # Only add description if it exists for this cause
    if cause in cause_descriptions:
        variable_entry["description"] = cause_descriptions[cause]

    variable_entry["category"] = category_id

    variables.append(variable_entry)

# Create metadata JSON
metadata = {
    "timeRange": {"start": int(tb_filtered["year"].min()), "end": int(tb_filtered["year"].max())},
    "source": "IHME, Global Burden of Disease (2025)",
    "categories": categories_list,
    "dimensions": {"entities": entities, "ageGroups": age_groups, "sexes": sex_list, "variables": variables},
}

# Create mappings for data JSON
country_to_id = {country: i + 1 for i, country in enumerate(countries)}
cause_to_id = {cause: i + 1 for i, cause in enumerate(causes)}
age_to_id = {age: age_group_map[age]["id"] for age in age_group_map.keys()}
sex_to_id = {sex: sex_map[sex]["id"] for sex in sex_map.keys()}

# Save metadata file
with open("causes-of-death.metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)

# Create one data file per entity
for country in countries:
    entity_id = country_to_id[country]

    # Filter data for this entity
    tb_entity = tb_filtered[tb_filtered["country"] == country].copy()

    # Sort data for consistent output
    tb_sorted = tb_entity.sort_values(["year", "sex", "age", "cause"]).reset_index(drop=True)

    # Create data JSON for this entity
    data = {
        "values": tb_sorted["value"].tolist(),
        "variables": [cause_to_id[c] for c in tb_sorted["cause"]],
        "years": tb_sorted["year"].tolist(),
        "ageGroups": [age_to_id[a] for a in tb_sorted["age"]],
        "sexes": [sex_to_id[s] for s in tb_sorted["sex"]],
    }

    # Save entity data file
    with open(f"causes-of-death.{entity_id}.json", "w") as f:
        json.dump(data, f, indent=2)

print(
    f"Created causes-of-death.metadata.json with {len(variables)} variables, {len(entities)} entities, {len(age_groups)} age groups, {len(sex_list)} sexes"
)
print(f"Created {len(entities)} data files (causes-of-death.<entityId>.json)")

# ds_garden = paths.create_dataset(tables=[metadata], formats=["json"])
