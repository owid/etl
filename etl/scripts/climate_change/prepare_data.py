from pathlib import Path
import plotly.express as px
import pandas as pd
import numpy as np

ROOT_PATH = Path("/Users/prosado/Documents/owid/repos/")
TEMPERATURE_DATA_PATH = ROOT_PATH / "importers/climate_change/ready/nasa_global-temperature-anomaly.csv"

df = pd.read_csv(TEMPERATURE_DATA_PATH)

# For now, select data for the World.
df = df[df["location"] == "World"].reset_index(drop=True)

# Create a column for year and month.
df["year"] = df["date"].str.split("-").str[0].astype(int)
df["month"] = df["date"].str.split("-").str[1].astype(int)

# Sort by year and month.
df = df.sort_values(["year", "month"]).reset_index(drop=True)

# Count the number of months per year and assert that there are 12 months per year (except for the current one).
df["month_count"] = df.groupby("year")["month"].transform("count")
assert df[df["year"] != 2023]["month_count"].unique() == 12
# Remove the month count column.
df = df.drop(columns=["month_count"])

# Create a column for each year and plot a curve for each year.
df_plot = df.pivot(index="month", columns="year", values="temperature_anomaly").reset_index()
# px.line(df_plot, x="month", y=df_plot.columns[1:], title="Global Temperature Anomaly")

# Combine data in decades prior to year 2000 and compute the average temperature anomaly per decade and month.
df_decade = df.copy()
df_decade["decade"] = df["year"] // 10 * 10
df_decade = df_decade.groupby(["decade", "month"]).agg({"temperature_anomaly": "mean", "location": "first"}).reset_index().rename(columns={"decade": "year"})

# Combine decadal and yearly data from year 2000 onwards.
DECADE_TO_YEAR = 2010
df_combined = pd.merge(df[df["year"] > DECADE_TO_YEAR], df_decade[df_decade["year"]<DECADE_TO_YEAR], how="outer").sort_values(["year", "month"]).reset_index(drop=True).drop(columns=["date"])

# Create a column for each year and plot a curve for each year.
df_plot = df_combined.pivot(index="month", columns="year", values="temperature_anomaly").reset_index()
px.line(df_plot, x="month", y=df_plot.columns[1:], title="Global Temperature Anomaly")

