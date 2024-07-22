# type: ignore
"""
Scripts in this folder must be run in this order:
 1. snapshots/forests/2024-07-10/dominant_driver.py
 2. snapshots/forests/2024-07-10/reproject_raster.py
 3. Manual upload of the reprojected raster to Earth Engine assets
 4. python snapshots/forests/2024-07-10/run_earth_engine.py
 5. Grab the Google Sheet IDs from the output of the run_earth_engine.py script and run the dominant_driver.py script with the IDs.


To run this script you need to set up a Google Earth Engine account and authenticate it - visit here to set up an account: https://code.earthengine.google.com/ and to start a new project.

Much of the required data is available in the Earth Engine Data Catalog, with the exception of the dominant driver data which there is a separate snapshot for, this must be reprojected into EPSG:4326 and added to the projects Earth Engine assets.

The script will calculate the area of tree cover in each driver of loss (according to Curtis et al. (2018)) for each year for a subset of countries and export the results to a CSV file in Google Drive.

Once it has completed, you can run the XXXX step to load the data to Snapshot.
"""
import click
import ee

# Set the tree cover threshold - this is the default on global forest watch
TREE_COVER_THRESHOLD = 30
DEBUG = False


@click.command()
@click.option(
    "--chunk_size",
    type=int,
    default=100,
    show_default=True,
    help="The number of countries included in the chunk",
)
@click.option(
    "--starting_point",
    type=int,
    default=1,
    show_default=True,
    help="The starting point of the chunk",
)
def main(chunk_size: int, starting_point: int):
    # Authenticate to Earth Engine - may require a code to be entered in the terminal
    ee.Authenticate()
    # Initialize the Earth Engine module.
    ee.Initialize(project="ee-fiona-forest")
    # Load country boundaries from LSIB - a standard dataset in Earth Engine
    countries = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")
    # Select a subset of five countries for debugging purposes
    if DEBUG:
        countries = countries.limit(10)  # Select countries 1-10 to debug
    country_list = countries.toList(chunk_size, starting_point)
    countries = ee.FeatureCollection(country_list)
    # Country boundaries
    gfc2023 = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    # Get the loss image.
    # This dataset is updated yearly, this is the latest version as of the time of writing.
    lossImage = gfc2023.select(["loss"])
    # The area of each pixel.
    lossAreaImage = lossImage.multiply(ee.Image.pixelArea())
    lossYear = gfc2023.select(["lossyear"])
    treecover = gfc2023.select(["treecover2000"])

    # Mask out areas with tree cover less than 30% in the year 2000 - this is the default on global forest watch
    treeCoverMask = treecover.gt(TREE_COVER_THRESHOLD)
    maskedLossAreaImage = lossAreaImage.updateMask(treeCoverMask)

    # Get the dominant driver data - which I reprojected to EPSG:4326 in reprojection_raster.py
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
            maxPixels=2.5e10,
            bestEffort=True,
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

    # Export the result to a CSV file in Google Drive - the 'forests' folder must exist in your Google Drive, if not it will be saved in root.
    # This step will take >6 hours to complete
    # Current folder is here: https://drive.google.com/drive/folders/1U5xylX1uqljdQ8OzPDJrsQFfdDHPQCbJ
    export_task = ee.batch.Export.table.toDrive(
        collection=formattedResults,
        description=f"Forest_Loss_By_Year_And_Driver_Per_Hundred_Countries_{starting_point}_{starting_point + chunk_size - 1}",
        folder="forests",
        fileNamePrefix=f"Forest_Loss_By_Year_And_Driver_Per_Hundred_Countries_{starting_point}_{starting_point + chunk_size - 1}",
        fileFormat="CSV",
    )
    export_task.start()

    # Print task status
    print("Export task started. Check the Earth Engine Tasks tab for progress.")


if __name__ == "__main__":
    main()
