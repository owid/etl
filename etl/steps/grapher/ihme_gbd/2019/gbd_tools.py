import pandas as pd
from owid.catalog import Dataset, Source, Table, VariableMeta
from owid.catalog.utils import underscore

from etl import grapher_helpers as gh


def create_var_name(df: pd.DataFrame) -> pd.Series:

    age_dict = {
        "Early Neonatal": "0-6 days",
        "Late Neonatal": "7-27 days",
        "Post Neonatal": "28-364 days",
        "1 to 4": "1-4 years",
    }

    df = df.replace({"age": age_dict}, regex=False)
    # For risk factor variables we want to include the risk factor and the cause of death so need a slightly different variable format
    try:
        if "rei" in df.columns:
            df[["measure", "cause", "rei", "sex", "age", "metric"]] = df[
                ["measure", "cause", "rei", "sex", "age", "metric"]
            ].astype(str)
            df["name"] = (
                df["measure"]
                + " - Cause: "
                + df["cause"]
                + " - Risk: "
                + df["rei"]
                + " - Sex: "
                + df["sex"]
                + " - Age: "
                + df["age"]
                + " ("
                + df["metric"]
                + ")"
            )
        else:
            df[["measure", "cause", "sex", "age", "metric"]] = df[["measure", "cause", "sex", "age", "metric"]].astype(
                str
            )
            df["name"] = (
                df["measure"]
                + " - "
                + df["cause"]
                + " - Sex: "
                + df["sex"]
                + " - Age: "
                + df["age"]
                + " ("
                + df["metric"]
                + ")"
            )
    except ValueError:
        pass

    assert "name" in df.columns
    return df


# Use in grapher step
# def calculate_omms(N: Any, df: pd.DataFrame) -> pd.DataFrame:
#    f = str(N.directory) +'/' + N.short_name + ".variables_to_sum.json"
#    with open(f) as file:
#        vars_to_calc = json.load(file)
#
#    for var in vars_to_calc:
#        print(var)
#        id = vars.loc[vars["name"] == var].id
#        assert (vars["name"] == var).any(), "%s not in list of variables, check spelling!" % (var)
#        vars_to_sum = vars[vars.name.isin(vars_to_calc[var])].id.to_list()
#        df_sum = []
#        for file in vars_to_sum:
#            df = pd.read_csv(
#                os.path.join(outpath, "datapoints", "datapoints_%d.csv" % file),
#                index_col=None,
#                header=0,
#            )
#            df["id"] = file
#            df_sum.append(df)
#        df = pd.concat(df_sum, ignore_index=True)
#        df = df.drop_duplicates()
#        df.groupby(["country", "year"])["value"].sum().reset_index().to_csv(
#            os.path.join(outpath, "datapoints", "datapoints_%d.csv" % id)
#        )
def add_metadata_and_prepare_for_grapher(df_gr: pd.DataFrame, var_name: str, walden_ds: Dataset) -> Table:
    source = Source(
        name=df_gr["source"].iloc[0],
        url=walden_ds.metadata["url"],
        source_data_url=walden_ds.metadata["source_data_url"],
        owid_data_url=walden_ds.metadata["owid_data_url"],
        date_accessed=walden_ds.metadata["date_accessed"],
        publication_date=walden_ds.metadata["publication_date"],
        publication_year=walden_ds.metadata["publication_year"],
        published_by=walden_ds.metadata["name"],
        publisher_source=df_gr["source"].iloc[0],
    )

    df_gr["meta"] = VariableMeta(
        title=df_gr["variable_name_meta"].iloc[0],
        description=df_gr["seriesdescription"].iloc[0] + "\n\nFurther information available at: %s" % (source_url),
        sources=[source],
        unit=df_gr["long_unit"].iloc[0],
        short_unit=df_gr["short_unit"].iloc[0],
        additional_info=None,
    )
    # Taking only the first 255 characters of the var name as this is the limit (there is at least one that is too long)
    df_gr["variable"] = underscore(df_gr["variable_name"].iloc[0][0:254])

    df_gr = df_gr[["country", "year", "value", "variable", "meta"]].copy()
    # convert integer values to int but round float to 2 decimal places, string remain as string
    df_gr["entity_id"] = gh.country_to_entity_id(df_gr["country"], create_entities=True)
    df_gr = df_gr.drop(columns=["country"]).set_index(["year", "entity_id"])

    return Table(df_gr)
