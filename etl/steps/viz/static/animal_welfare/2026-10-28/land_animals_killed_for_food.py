"""Check the numbers shown in the static chart of land animals killed for food.

The chart is drawn by hand in Figma, as frame `land-animals-killed-for-food` on page "20260916 Land animals killed
for food (Pablo R)" of the Charts (2026) file:
https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=30160-125
Each icon in the chart represents 1 billion animals killed in 2024.

Unlike other static viz steps, this one draws nothing. It fails when the data behind the chart changes, as a reminder
to update the chart in Figma.
"""

from owid.catalog import Table, Variable

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year shown in the chart.
YEAR = 2024

# FAOSTAT item codes of animals slaughtered for meat: all animals, and chickens.
ITEM_CODE_ALL_MEAT = "00001765"
ITEM_CODE_CHICKENS = "00001058"
# FAOSTAT element codes of animals slaughtered (mammals, and poultry and rabbits, respectively).
ELEMENT_CODES_SLAUGHTERED = ["005320", "005321"]

# FAOSTAT items that each icon of the row of other land animals stands for, as the chart's note says. The cow icon
# stands for cattle, turkeys and all the rest.
OTHER_ANIMAL_ICONS = {
    "duck": {"00001069": "ducks", "00001073": "geese"},
    "pig": {"00001035": "pigs"},
    "sheep": {"00000977": "sheep", "00001017": "goats"},
    "rabbit": {"00001141": "rabbits"},
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load our estimates of chickens killed in egg production.
    ds_eggs = paths.load_dataset("chickens_killed_for_eggs")
    tb_eggs = ds_eggs.read("chickens_killed_for_eggs")

    # Load FAOSTAT QCL dataset and read its main (long) table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl", safe_types=False)

    #
    # Process data.
    #
    # Chickens killed in egg production in the world.
    eggs = tb_eggs[(tb_eggs["country"] == "World") & (tb_eggs["year"] == YEAR)]
    assert len(eggs) == 1, f"Expected one row for World in {YEAR}."
    eggs = eggs.iloc[0]

    # Animals slaughtered for meat in the world, by FAOSTAT item.
    slaughtered = select_slaughtered_animals(tb_qcl=tb_qcl)

    # Numbers shown in the chart, in billions.
    numbers = {
        "chickens_killed_in_meat_production": slaughtered[ITEM_CODE_CHICKENS] / 1e9,
        "hens_killed_in_egg_production": eggs["hens_killed"] / 1e9,
        "male_chicks_killed_in_egg_production": eggs["male_chickens_killed"] / 1e9,
        "chickens_killed_in_egg_production": eggs["chickens_killed"] / 1e9,
        "other_land_animals": (slaughtered[ITEM_CODE_ALL_MEAT] - slaughtered[ITEM_CODE_CHICKENS]) / 1e9,
    }
    for icon, items in OTHER_ANIMAL_ICONS.items():
        numbers[f"{icon}_icons"] = sum(slaughtered[item] for item in items) / 1e9
    numbers["cow_icons"] = numbers["other_land_animals"] - sum(numbers[f"{icon}_icons"] for icon in OTHER_ANIMAL_ICONS)
    paths.log.info("Numbers in the chart (billions): " + ", ".join(f"{k} {v:.2f}" for k, v in numbers.items()))

    # Check that the chart shows these numbers, rounded.
    check_numbers_in_chart(numbers=numbers)


def select_slaughtered_animals(tb_qcl: Table) -> Variable:
    """Number of animals slaughtered for meat in the world in YEAR, indexed by FAOSTAT item code."""
    tb = tb_qcl[
        (tb_qcl["country"] == "World")
        & (tb_qcl["year"] == YEAR)
        & (tb_qcl["element_code"].isin(ELEMENT_CODES_SLAUGHTERED))
        & ((tb_qcl["item_code"] == ITEM_CODE_ALL_MEAT) | tb_qcl["fao_item"].astype(str).str.startswith("Meat of "))
    ]
    tb = tb[["item_code", "fao_item", "unit", "value"]].reset_index(drop=True)
    tb["item_code"] = tb["item_code"].astype(str)
    assert set(tb["unit"].astype(str)) == {"animals"}, "Unexpected units of animals slaughtered."
    assert not tb["item_code"].duplicated().any(), "Duplicated items of animals slaughtered."

    # The meat items should add up to the total of all animals.
    items = tb[tb["item_code"] != ITEM_CODE_ALL_MEAT]["value"].sum()
    total = tb[tb["item_code"] == ITEM_CODE_ALL_MEAT]["value"].item()
    assert abs(items / total - 1) < 0.005, f"Animals slaughtered by item add up to {items:.4g}, not {total:.4g}."

    return tb.set_index("item_code")["value"]


def check_numbers_in_chart(numbers: dict) -> None:
    # Rounded numbers (and icons) shown in the chart.
    expected = {
        "chickens_killed_in_meat_production": 79,
        "hens_killed_in_egg_production": 7,
        "male_chicks_killed_in_egg_production": 7,
        "chickens_killed_in_egg_production": 14,
        "other_land_animals": 9,
        "duck_icons": 5,
        "pig_icons": 1,
        "sheep_icons": 1,
        "rabbit_icons": 1,
        "cow_icons": 1,
    }
    changed = {
        name: round(float(numbers[name]), 2) for name, value in expected.items() if round(numbers[name]) != value
    }
    assert not changed, f"Numbers in the chart have changed (update the chart in Figma): {changed}"
