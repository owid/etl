"""Script to create a snapshot of WRI data on GHG emissions by sector.

The data was manually extracted from
https://www.wri.org/data/world-greenhouse-gas-emissions-sector-2021-sunburst-chart

Note that this snapshot is private; WRI does not make this data public, for some reason.
So, for now, we will keep it private and not downloadable, and will contact them to see if we can make it public (and also update it to use more recent data, which seems to be available on the Climate Watch explorer).

"""

import json
from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Share of emissions by sector.
SUBSECTORS = {
    "Electricity and heat": {
        "Residential buildings": 7.5,
        "Commercial buildings": 4.8,
        "Unallocated fuel combustion": 2.8,
        "Chemical and petrochemical": 2.3,
        "Other industry": 2.2,
        "Iron and steel": 1.8,
        "Non-ferrous metals": 1.4,
        "Machinery": 1.4,
        "Agriculture and fishing energy use": 1,
        "Non-metallic minerals": 0.8,
        "Food and tobacco": 0.8,
        "Textile and leather": 0.5,
        "Mining and quarrying": 0.5,
        "Paper, pulp and printing": 0.5,
        "Transport equipment": 0.4,
        "Rail": 0.3,
        "Construction": 0.3,
        "Wood and wood products": 0.1,
        "Road": 0.1,
        "Pipeline": 0,
        "Other transportation": 0,
    },
    "Transportation": {
        "Road": 12.1,
        "Air": 0.7,
        "Ship": 0.3,
        "Pipeline": 0.3,
        "Rail": 0.2,
        "Other transportation": 0.1,
    },
    "Manufacturing and construction": {
        "Iron and steel": 4.3,
        "Other industry": 2.4,
        "Non-metallic minerals": 2.3,
        "Chemical and petrochemical": 1.5,
        "Food and tobacco": 0.4,
        "Non-ferrous metals": 0.4,
        "Construction": 0.3,
        "Mining and quarrying": 0.3,
        "Paper, pulp and printing": 0.3,
        "Machinery": 0.2,
        "Textile and leather": 0.1,
        "Transport equipment": 0.1,
        "Wood and wood products": 0,
    },
    "Buildings": {
        "Residential buildings": 5,
        "Commercial buildings": 1.6,
    },
    "Fugitive emissions": {
        "Vented": 4.4,
        "Flared": 1,
        "Production": 0.7,
        "Transmission and distribution": 0.4,
        "Unallocated fuel combustion": 0.1,
    },
    "Other fuel combustion": {
        "Unallocated fuel combustion": 3.5,
        "Agriculture and fishing energy use": 0.9,
    },
    "International bunker": {
        "Ship": 1.3,
        "Air": 0.7,
    },
    "Agriculture": {
        "Livestock and manure": 5.9,
        "Agriculture soils": 4.1,
        "Rice cultivation": 1.2,
        "Burning": 0.5,
    },
    "Industrial processes": {
        "Cement": 3.4,
        "Chemical and petrochemical (ip)": 2.6,
        "Other industry (ip)": 0.1,
        "Electronics (ip)": 0.1,
        "Electric power systems": 0.1,
        "Non-ferrous metals (ip)": 0.1,
    },
    "Waste": {
        "Landfills": 2,
        "Wastewater": 1.3,
        "Other waste": 0.1,
    },
    "Land-use change and forestry": {
        "Drained organic soils": 1.7,
        "Forest land": 0.6,
        "Forest fires": 0.4,
        "Fires in organic soils": 0,
    },
}


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"wri/{SNAPSHOT_VERSION}/emissions_by_detailed_sector.json")

    # Save dictionary as a json file.
    snap.path.write_text(json.dumps(SUBSECTORS))

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
