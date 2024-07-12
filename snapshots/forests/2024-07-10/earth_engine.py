import ee
import pandas as pd
import rasterio
from rasterio.warp import Resampling, calculate_default_transform, reproject

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
ee.Authenticate()
# Initialize the Earth Engine module.
ee.Initialize(project="ee-fiona-forest")

# Load country boundaries from LSIB - a standard dataset in Earth Engine
countries = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")

# Select a subset of five countries to test the code works as expected
# countries = countries.limit(5)

# Get the loss image.
# This dataset is updated yearly, this is the latest version as of the time of writing.
gfc2023 = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
lossImage = gfc2023.select(["loss"])
lossAreaImage = lossImage.multiply(ee.Image.pixelArea())
lossYear = gfc2023.select(["lossyear"])
treecover = gfc2023.select(["treecover2000"])

# Mask out areas with tree cover less than 30% in the year 2000 - this is the default on global forest watch
treeCoverMask = treecover.gt(30)
maskedLossAreaImage = lossAreaImage.updateMask(treeCoverMask)

# Get the dominant driver data - which I reprojected to 4326 elsewhere
dominant = ee.Image("projects/ee-fiona-forest/assets/reprojected_dominant_driver")
driverCat = dominant.select(["b1"])

# Combine the masked loss area, loss year, and driver categories into one image.
combinedImage = maskedLossAreaImage.addBands([lossYear, driverCat])


# Define a function to calculate the area lost in each category for each year.
def calculateLossByYearAndCategory(feature):
    geometry = feature.geometry()
    countryName = feature.get("country_na")  # Assuming 'country_na' is the country name property

    result = combinedImage.reduceRegion(
        reducer=ee.Reducer.sum()
        .group(
            groupField=1,  # Group by year
            groupName="year",
        )
        .group(
            groupField=2,  # Group by driver category
            groupName="category",
        ),
        geometry=geometry,
        scale=30,
        maxPixels=2.5e10,  # Adjust maxPixels if necessary
    )

    return ee.Feature(None, {"country": countryName, "groups": result.get("groups")})


# Map over each country and compute the results
results = countries.map(calculateLossByYearAndCategory)


# Flatten and format the results
def flattenResults(feature):
    groups = ee.List(feature.get("groups"))
    country = feature.get("country")

    formatted = groups.map(
        lambda categoryGroup: ee.List(ee.Dictionary(categoryGroup).get("groups")).map(
            lambda yearGroup: ee.Feature(
                None,
                {
                    "country": country,
                    "category": ee.Dictionary(categoryGroup).get("category"),
                    "year": ee.Dictionary(yearGroup).get("year"),
                    "area": ee.Dictionary(yearGroup).get("sum"),
                },
            )
        )
    ).flatten()

    return ee.FeatureCollection(formatted)


formattedResults = results.map(flattenResults).flatten()

# Convert the FeatureCollection to a list of dictionaries
data = formattedResults.getInfo()["features"]

# Extract relevant fields and convert to pandas DataFrame
data_dicts = [feature["properties"] for feature in data]
df = pd.DataFrame(data_dicts)

# Display the DataFrame
print(df)


def load_dominant_driver_and_reproject(output_raster_path: str):
    snap = paths.load_snapshot("dominant_driver.tif")
    with rasterio.open(snap.path) as src:
        src_data = src.read(1)
        src_transform = src.transform
        src_crs = src.crs

        # Define the target CRS (EPSG:4326)
        target_crs = "EPSG:4326"

        # Calculate the transform and dimensions for the target CRS
        transform, width, height = calculate_default_transform(src_crs, target_crs, src.width, src.height, *src.bounds)

        # Update the metadata for the target CRS
        kwargs = src.meta.copy()
        kwargs.update({"crs": target_crs, "transform": transform, "width": width, "height": height})

        # Open the destination file and write the reprojected data
        with rasterio.open(output_raster_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest,
                )


def upload_file_to_earth_engine(output_raster_path: str, asset_id: str):
    # Upload the reprojected raster to Earth Engine
    ee.Initialize()
    ee_image = ee.Image(output_raster_path)
    ee_asset = ee.Image(asset_id)
    task = ee.data.startIngestion(
        ee.data.newTaskId()[0],
        ee.data.newTaskId()[1],
        {
            "sources": [{"primaryPath": output_raster_path}],
            "destination": {"collection": asset_id},
        },
    )
    print(task)


# Function to flatten and format the results
# def flattenResults(feature):
#    groups = ee.List(feature.get("groups"))
#    country = feature.get("country")
#
#   formatted = groups.map(
#        lambda categoryGroup: ee.List(ee.Dictionary(categoryGroup).get("groups")).map(
#            lambda yearGroup: ee.Feature(
#                None,
#                {
#                    "country": country,
#                    "category": ee.Dictionary(categoryGroup).get("category"),
#                    "year": ee.Dictionary(yearGroup).get("year"),
#                    "area": ee.Dictionary(yearGroup).get("sum"),
#                },
#            )
#        )
#    ).flatten()

#    return ee.FeatureCollection(formatted)


# Initialize an empty list to store the results
# results_list = []

# Loop through each country and calculate results
# country_list = countries.toList(countries.size()).getInfo()
# for country in country_list:
#    country_feature = ee.Feature(country)
#    result = calculateLossByYearAndCategory(country_feature)
#    flattened_result = flattenResults(result)

# Get the data as a list of dictionaries
#    data = flattened_result.getInfo()["features"]
#    data_dicts = [feature["properties"] for feature in data]
#    results_list.extend(data_dicts)

# Convert the list of dictionaries to a pandas DataFrame
# df = pd.DataFrame(results_list)

# Display the DataFrame
# print(df)
