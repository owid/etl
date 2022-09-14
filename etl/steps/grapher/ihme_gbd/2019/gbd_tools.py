import pandas as pd


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
            df["variable"] = (
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
            df["variable"] = (
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

    assert "variable" in df.columns
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
