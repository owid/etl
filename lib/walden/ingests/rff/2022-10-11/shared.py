import io
import zipfile

import pandas as pd
import requests

# Link to download the zipped repository of the World Carbon Pricing Database.
WCPD_URL = "https://github.com/g-dolphin/WorldCarbonPricingDatabase/archive/refs/heads/master.zip"
# Path (from the root of the repository) to the data at the national level.
WCPD_DATA_DIR = "WorldCarbonPricingDatabase-master/_dataset/data/CO2/national/"
# Path to the sources of the data at the national level.
WCPD_SOURCES_DIR = "WorldCarbonPricingDatabase-master/_dataset/sources/CO2/national/"
# Path (from the root of the repository) to the data at the sub-national level.
WCPD_SUBNATIONAL_DATA_DIR = "WorldCarbonPricingDatabase-master/_dataset/data/CO2/subnational/"


def extract_data_from_remote_zip_folder(zip_url: str, path_to_folder: str) -> pd.DataFrame:
    # Fetch remote zipped folder.
    r = requests.get(zip_url)
    # Create a temporary zipfile object.
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # List data files in a specific path (within the zipped folder).
        file_paths = sorted([path for path in z.namelist() if path.startswith(path_to_folder) if path.endswith(".csv")])
        # Read each file and concatenate them in a single dataframe.
        data = pd.concat([pd.read_csv(z.open(file_name)) for file_name in file_paths], ignore_index=True)

    return data
