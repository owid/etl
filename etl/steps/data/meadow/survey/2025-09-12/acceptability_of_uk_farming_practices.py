"""Load a snapshot and create a meadow dataset."""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Some sessions were finished in less than 1 minute.
# It's hard to believe someone could complete this survey in such a short period of time, in a meaningful way.
# Minimum number of minutes spent in a survey (shorter sessions will be removed).
MINIMUM_MINUTES = 2

# Define the start of the names of columns to keep from the raw data (the end contains an irrelevant hash).

# Columns on people's acceptability on animal welfare issues.
COLUMNS_ACCEPTABILILTY = {
    "Cutting or grinding the teeth of new-born piglets": "Cutting or grinding the teeth of newborn piglets",
    # NOTE: The following is empty, but another one with a similar name is not empty.
    #'Keeping chickens in cages with 750cm2 space (about the size of an A4 piece of paper)',
    "Keeping chickens in cages with 750 sqcm space (about the size of an A4 piece of paper)": "Keeping chickens in cages with about the space of an A4 sheet of printer paper per bird",
    # Clarification: I can't think of a better phrasing that doesn't significantly change the meaning.
    # I think the ideal phrasing would be something like "Killing newborn male calves who are not profitable for dairy farms" (which implies that not all male are killed).
    # However, that would change the meaning of the survey too much. I think using “as” is not too bad. It’s exposing a practice that happens somewhat frequently, not always, and it happens because newborn male calves cannot produce milk.
    "Killing new-born calves who cannot produce milk": "Killing newborn male calves as they cannot produce milk",
    # Clarification here: It would be more accurate to say "Cutting off part of the beak", but I suppose we should stick to something closer to the original survey.
    "Cutting the beaks of new-born chickens": "Cutting the beaks of newborn chicks",
    # Clarification: It would be more accurate to say "part of", but I suppose we should stick to the original phrasing of the survey.
    "Cutting the tails off new-born piglets": "Cutting the tails off newborn piglets",
    "Killing new-born chicks by use of CO2 gassing or meat-grinders": "Killing newborn chicks using CO₂ gassing or meat-grinders",
    "Removing calves' horn buds using hot iron": "Removing calves' horn buds using hot iron",
    "Keeping pigs in cages which prevent them from turning around for several weeks": "Keeping pigs in cages in which they cannot turn around for several weeks",
    # Clarification: "crushing the testicles" is not inaccurate, but may be misinterpreted.
    # What actually happens is that the spermatic cords are crushed with a clamp.
    "Castrating new-born calves by cutting or crushing the testicles": "Castrating newborn calves by cutting or crushing the testicles",
}
# NOTE: There were other potentially interesting columns, but the survey results for these were less complete.
COLUMNS_AGREEMENT = {
    # Columns on people's agreement on animal welfare issues.
    # 'It is OK to use animals in medical research for the benefit of humans',
    # 'Animals deserve to be respected',
    # 'Animals experience emotions (e.g. fear or joy)',
}

COLUMNS_DEMOGRAPHICS = {
    # Columns on demographics.
    "Please indicate your gender": "gender",
    "In general, how would you describe your political views?": "political_views",
    "Would you say you live in a...": "town_size",
    "What is your *age* ?": "age",
    "What is the highest level of education you have completed?": "education",
    "What is your annual household income before taxes?": "income",
    "Which of the following best describes your diet?": "diet",
}


# Other columns.
COLUMNS_OTHER = {
    # Column describing the meaning of the number scale of answers.
    "answerScale": "answer_scale",
    # Column describing the meaning of the agreement scale.
    "agreementScale": "agreement_scale",
    # Other auxiliary technical columns of the survey itself.
    # Column on the number of minutes spent by the user to complete the survey (to filter out spurious entries).
    "Minutes Spent": "time",
    # Anonymized columns to be able to detect when a user completed the survey in multiple sessions.
    "PROLIFIC_PID": "user_id",
    "SESSION_ID": "session_id",
}

# All columns.
COLUMNS = {**COLUMNS_ACCEPTABILILTY, **COLUMNS_AGREEMENT, **COLUMNS_DEMOGRAPHICS, **COLUMNS_OTHER}


def sanity_check_inputs(tb):
    error = "Found columns with only nans."
    assert tb.columns[tb.isna().all()].empty, error
    error = "Answer scale has changed."
    assert set(tb["answer_scale"].fillna("NAN")) == {
        "NAN",
        '[["Acceptable",1],["Not acceptable",2],["Don\'t know/No opinion",3]]',
    }, error
    error = "Agreement scale has changed."
    assert set(tb["agreement_scale"].fillna("NAN")) == {
        "NAN",
        '[["Strongly agree",5],["Agree",4],["Neither agree nor disagree",3],["Disagree",2],["Strongly disagree",1]]',
    }, error
    # Most users have only one entry, but some users completed the survey in 2 or 3 sessions.
    error = "Unexpected repeated rows."
    assert len(tb) - 1 == len(set(tb["session_id"])) + len(set(tb[tb["user_id"].duplicated()]["user_id"])), error
    counts = tb.groupby("user_id").count()
    assert (
        len([column for column in counts.drop(columns=["time", "session_id"]).columns if counts[column].max() > 1]) == 0
    ), error


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("acceptability_of_uk_farming_practices.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Raw Data")

    #
    # Process data.
    #
    # Fix encoding issues in strings.
    tb.columns = tb.columns.str.replace("â€™", "'", regex=False)

    # Select and rename relevant columns.
    tb = tb[[column for column in tb.columns if column.startswith(tuple(COLUMNS))]]
    tb = tb.rename(
        columns={c: next(new for old, new in COLUMNS.items() if c.startswith(old)) for c in tb.columns}, errors="raise"
    )

    # Fix encoding issues in income.
    tb["income"] = tb["income"].str.replace("Â£", "£", regex=False)

    # Sanity checks.
    sanity_check_inputs(tb=tb)

    # I detected spurious entries, e.g. "2 | 2".
    # Check that they are always the same value twice, and replace them by that value.
    for column in tb.columns:
        _filter = tb[column].astype("string").str.contains("|", regex=False)
        if _filter.any():
            assert all([len(set(case.replace("|", "").split())) == 1 for case in set(tb.loc[_filter, column])])
            tb[column] = tb[column].str.split(" | ").str[0]

    # Map acceptability scales to their meaning.
    for column in COLUMNS_ACCEPTABILILTY.values():
        tb[column] = map_series(
            series=tb[column].astype("Int64"),
            mapping={1: "Acceptable", 2: "Not acceptable", 3: "Don't know/No opinion"},
        )

    # Map agreement scales to their meaning.
    for column in COLUMNS_AGREEMENT.values():
        tb[column] = map_series(
            series=tb[column],
            mapping={
                1: "Strongly disagree",
                2: "Disagree",
                3: "Neither agree nor disagree",
                4: "Agree",
                5: "Strongly agree",
            },
        )

    # Remove rows that have no questions answered.
    tb = tb.dropna(
        subset=[column for column in tb.columns if not column.startswith(tuple(COLUMNS_OTHER.values()))], how="all"
    ).reset_index(drop=True)

    # Check that this removes all cases of users having multiple sessions.
    error = "A user may have completed the survey in different sessions, which is easy to fix."
    assert tb[tb["user_id"].duplicated()].empty, error
    # NOTE: I suppose it's in principle possible that a user completed a survey in multiple sessions. If so:
    # tb = tb.groupby("user_id", as_index=False).first()

    # Remove sessions that were too short.
    tb = tb[tb["time"] > MINIMUM_MINUTES].reset_index(drop=True)

    # All questions about acceptability were answered.
    error = "Unexpected incomplete surveys"
    assert len([column for column in COLUMNS_ACCEPTABILILTY.values() if tb[column].isnull().any()]) == 0, error
    # However, some questions about demographics were left unanswered.
    # [column for column in COLUMNS_DEMOGRAPHICS if tb[column].isnull().any()]
    error = "Unexpectedly high number of surveys without demographic information."
    assert (
        len(sorted(set(sum([list(tb[tb[column].isnull()].index) for column in COLUMNS_DEMOGRAPHICS.values()], []))))
        < 50
    ), error

    # Keep only rows with all questions answered (both on acceptability, and demographics).
    tb = tb.dropna(axis=0, how="any").reset_index(drop=True)

    # Sanity check.
    error = "Unexpectedly short number of survey results."
    assert len(tb) > 950, error

    # Drop unnecessary tables.
    tb = tb.drop(columns=["answer_scale", "agreement_scale", "user_id", "session_id", "time"], errors="raise")

    # Add a column with a random number to identify the survey, and transpose table conveniently.
    tb = (
        tb.reset_index()
        .rename(columns={"index": "user"})
        .melt(id_vars=["user"] + list(COLUMNS_DEMOGRAPHICS.values()), var_name="question", value_name="answer")
    )
    # Improve tables format.
    tb = tb.format(["user", "question"]).astype({"answer": "string"})

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
