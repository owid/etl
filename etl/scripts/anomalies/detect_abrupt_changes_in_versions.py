"""Load two consecutive versions of an ETL grapher dataset, and identify the most significant changes.

"""

from owid.catalog import find

from etl.data_helpers.misc import compare_tables

# Load old and new versions of a dataset.
candidates = find("electricity_mix", channels=["grapher"])
tb_old = candidates[(candidates["version"] == "2024-05-08") & (candidates["channel"] == "grapher")].load().reset_index()
tb_new = candidates[(candidates["version"] == "2024-06-20") & (candidates["channel"] == "grapher")].load().reset_index()


old = tb_old.copy()
new = tb_new.copy()

countries = None
columns = None
# compare_tables(
#     old=old, new=new, countries=countries, columns=columns, only_coincident_rows=False
# )
# compare_tables(
#     old=old, new=new,countries=countries, columns=columns, only_coincident_rows=True
# )
# compare_tables(
#     old=old, new=new,countries=countries, columns=columns, only_coincident_rows=True, bard_eps=1, bard_max=0.2
# )

# # Check specifically the case of Pakistan - primary energy consumption, where there is an abrupt dip in 2016.
# countries = ["Pakistan"]
# columns = ["primary_energy_consumption__twh"]
# compare_tables(
#     old=old, new=new, countries=countries, columns=columns, only_coincident_rows=True
# )
# countries = ["CIS (EI)"]
# columns = ["oil_generation__twh"]
# compare_tables(
#     old=old, new=new, countries=countries, columns=columns, only_coincident_rows=True
# )
# _old = old[old["country"] == "CIS (EI)"][["year", "oil_generation__twh"]].reset_index(drop=True)
# _new = new[new["country"] == "CIS (EI)"][["year", "oil_generation__twh"]].reset_index(drop=True)
# eps = np.percentile(pd.concat([_old, _new]).dropna(), q=0.1)
# bard(_old[column], _new[column], eps=eps)
# compare_tables(
#     old=old, new=new, countries=countries, columns=columns, only_coincident_rows=True
# )

# TODO: We'd need to calculate epsilon for each indicator.
countries = None
columns = None
compare_tables(
    old=old, new=new, countries=countries, columns=columns, only_coincident_rows=True, bard_eps=0.1, bard_max=0.5
)


# We want to raise an alert if:
# * There are years that used to have data in the old table and now they don't.
#   * This should be easy, and requires no parameters.
# * The maximum deviation (BARD) is larger than a certain amount (at any single point).
#   * Two parameters are required: the maximum BARD allowed, and the BARD epsilon.

# How to calculate eps:
# (A) The eps could be calculated as the qth (e.g. 10th) percentile of the data (concatenating old and new series).
# (B.1) Based on charts: If the indicator is used in charts using linear scales, eps could be (max - min) / 10.
#     Here, max and min are the absolute max and min of the data (concatenating old and new series).
#     The reasoning is: if you plotted all countries in a line chart (with linear y-axis), the smallest deviation you would care about is 10% of the range.
#     NOTE: This assumes that we only care about the default view (with all countries put together).
# (B.2) If the indicator is used in charts using log scale, we'd need to import some of the logic used in map bracketer, possibly consider the minimum interval (e.g. based on number of decimal places).
