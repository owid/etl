import ee

# Set the tree cover threshold - this is the default on global forest watch
TREE_COVER_THRESHOLD = 30
CHUNK_SIZE = 20


def main():
    # Authenticate to Earth Engine - may require a code to be entered in the terminal
    ee.Authenticate()
    # Initialize the Earth Engine module.
    ee.Initialize(project="ee-fiona-forest")

    # Load country boundaries from LSIB - a standard dataset in Earth Engine
    countries = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")
    countries = countries.limit(100)

    # Function to get countries in chunks of specified size
    def get_chunks(fc, chunk_size):
        size = fc.size().getInfo()
        chunks = []
        for i in range(0, size, chunk_size):
            chunk = fc.toList(chunk_size, i)
            chunks.append(ee.FeatureCollection(chunk))
        return chunks

    # Get the countries in chunks of 20
    chunks = get_chunks(countries, CHUNK_SIZE)

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

    # Define a function to calculate the area lost in each category for each year
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

    # Loop through each chunk and process them
    all_results = []
    for index, chunk in enumerate(chunks):
        print(f"Processing chunk {index + 1} with up to {CHUNK_SIZE} countries")
        results = chunk.map(calculateLossByYearAndCategory)
        all_results.append(results)

    # Combine results from all chunks
    combined_results = ee.FeatureCollection(all_results).flatten()

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

    formattedResults = combined_results.map(flattenResults).flatten()

    # Export the result to a CSV file in Google Drive
    export_task = ee.batch.Export.table.toDrive(
        collection=formattedResults,
        description="Forest_Loss_By_Year_And_Driver_Per_Chunk",
        folder="forests",
        fileNamePrefix="Forest_Loss_By_Year_And_Driver_Per_Country_ETL_2024-07-10_Chunks",
        fileFormat="CSV",
    )
    export_task.start()

    # Print task status
    print("Export task started. Check the Earth Engine Tasks tab for progress.")


if __name__ == "__main__":
    main()
