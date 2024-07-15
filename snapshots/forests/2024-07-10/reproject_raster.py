import os

import rasterio
from rasterio.warp import Resampling, calculate_default_transform, reproject

DOMINANT_DRIVER_PATH = "data/snapshots/forests/2024-07-10/dominant_driver.tif"
OUTPUT_DIR = "snapshots/forests/2024-07-10"


def reproject_dominant_driver():
    """
    In the dominant_driver snapshot the raster is downloaded in the Goode-Homosline projection.
    This is incompatible with the Earth Engine projection system, which requires EPSG:4326.
    In this function we reprojection the raster to EPSG:4326 and save it as a local file which then must be manually uploaded to the Earth Engine assets.
    """

    # Open the raster
    with rasterio.open(DOMINANT_DRIVER_PATH) as src:
        # Set the new CRS
        dst_crs = "EPSG:4326"

        # Calculate the transform
        transform, width, height = calculate_default_transform(src.crs, dst_crs, src.width, src.height, *src.bounds)

        # Create the output file
        kwargs = src.meta.copy()
        kwargs.update(
            {
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height,
            }
        )

        # Define output file path
        output_path = os.path.join(OUTPUT_DIR, "reprojected_dominant_driver.tif")

        # Write the output file
        with rasterio.open(output_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest,
                )

        print(f"Reprojected raster saved to {output_path}")


if __name__ == "__main__":
    reproject_dominant_driver()
