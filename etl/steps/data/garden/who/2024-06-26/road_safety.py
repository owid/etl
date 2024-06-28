"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

FATALITIES_COLS = [
    "country",
    "reported_fatalities",
    "year_reported_fatalities",
    "reported_fatalities_gender_distribution__pct_males",
    "reported_fatalities_gender_distribution__pctfemales",
    "reported_fatalities_user_distribution__pct_powered_light_vehicles",
    "reported_fatalities_user_distribution__pct_powered_2__wheelers",
    "reported_fatalities_user_distribution__pct_pedestrian",
    "reported_fatalities_user_distribution__pct_cyclist",
    "reported_fatalities_user_distribution__pct_other",
]
VEHICLES_COLS = [
    "country",
    "total_registered_vehicles",
    "total_registered_vehicles_rate_per_100_000_pop",
    "cars_and_4_wheeled_light_vehicles",
    "powered_2_and_3_wheelers",
    "heavy_trucks",
    "buses",
    "other",
    "year_total_registered_vehicles",
]

WHO_EST_COLS_2023 = [
    "country",
    "who_estimated_road_traffic_fatalities",
    "lower_bound_who_estimated_road_traffic_fatalities",
    "upper_bound_who_estimated_road_traffic_fatalities",
    "year_who_estimated_road_traffic_fatalities",
    "who_estimated_rate_per_100_000_population",
]
WHO_EST_COLS_2010 = [
    "country",
    "_2010_who_estimated_road_traffic_fatalities__update",
    "_2010_lower_bound_who_estimated_road_traffic_fatalities__update",
    "_2010_upper_bound_who_estimated_road_traffic_fatalities__update",
    "_2010_who_estimated_rate_per_100_000_population__update",
]
WHO_EST_COLS_2016 = [
    "country",
    "_2016_who_estimated_road_traffic_fatalities__update",
    "_2016_lower_bound_who_estimated_road_traffic_fatalities__update",
    "_2016_upper_bound_who_estimated_road_traffic_fatalities__update",
    "_2016_who_estimated_rate_per_100_000_population__update",
]
PAVED_KM_COLS = [
    "country",
    "total_paved_kilometres__year",
    "year_total_paved_kilometres",
]

REST_COLS = [
    "country",
    "national_law_on_universal_access_to_emergency_care",
    "national_law_guaranteeing_free_of_charge_access_to_rehabilitative_care_for_all_injured",
    "national_law_guaranteeing_free_of_charge_access_to_psychological_services_to_road_crash_victims_and_their_families",
    "national_good_samaritan_law",
    "national_emergency_care_access_number",
    "legislation_on_periodic_vehicle_technical_inspection",
    "presence_of_high_quality_safety_standards_for_used_vehicle_imports_exports_2",
    "national_law_on_front_and_side_impact_protection",
    "national_law_on_seat_belt_and_seat_belt_anchorages",
    "national_law_on_electronic_stability_control",
    "national_law_on_pedestrian_protection",
    "national_law_on_motorcycle_anti_lock_braking_systems",
    "government_vehicle_procurement_practices_include_safety_prerequisites",
    "presence_of_strategies_to_promote_alternatives_to_individuals_use_of_powered_vehicles",
    "national_road_safety_strategy",
    "funding_to_implement_strategy",
    "national_legislation_mandating_third_party_liability_insurance_for_powered_vehicles",
    "national_law_on_driving_time_and_rest_periods_for_professional_drivers",
    "presence_of_national_lead_agency_to_implement_national_road_safety_action_plan",
    "presence_of_agencies_that_coordinate_pre_hospital_and_emergency_medical_services",
    "legislation_setting_appropriate_urban_speed_limits_for_passenger_cars_and_motorcycles",
    "national_law_setting_a_speed_limit",
    "maximum_urban_speed_limit",
    "maximum_rural_speed_limit",
    "maximum_motorway_speed_limit",
    "legislation_on_drink_driving",
    "national_law_on_drink_driving",
    "bac_limit__general_population",
    "bac_limit__young_or_novice_drivers",
    "legislation_on_drug_driving",
    "legislation_on_distracted_driving__mobile_phones__while_driving",
    "ban_on_mobile_phone_use__hand_held",
    "ban_on_mobile_phone_use__hand_free",
    "legislation_requiring_adult_motorcycle_riders_to_wear_a_helmet_properly_fastened_that_meets_appropriate_standards",
    "national_motorcycle_helmet_law",
    "helmet_wearing_rate__pctdriver",
    "helmet_wearing_rate__pct_passenger",
    "minimum_age_height_children_are_allowed_as_passengers_on_motorcycles_nationally",
    "legislation_on_the_use_of_seat_belts_for_all_motor_vehicle_occupants__unvti_8a",
    "national_seat_belt_law",
    "legislation_applies_to_front_and_rear_seat_occupants",
    "seat_belt_wearing_rate__pct_drivers",
    "seat_belt_wearing_rate__pct_front_seat_occupants",
    "seat_belt_wearing_rate__pct_rear_seat_occupants",
    "legislation_requiring_the_use_of_child_safety_restraint_systems_that_meet_appropriate_standards",
    "national_child_restraints_use_law",
    "children_seated_in_front_seat",
    "age_or_height_specified_for_children_requiring_child_restraint",
    "child_restraint_standard_referred_to_and_or_specified",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("road_safety")

    # Read table from meadow dataset.
    tb = ds_meadow["road_safety"].reset_index()

    # seperate table in different topics
    tb_who_est_2023 = tb[WHO_EST_COLS_2023]
    tb_who_est_2010 = tb[WHO_EST_COLS_2010]
    tb_who_est_2016 = tb[WHO_EST_COLS_2016]

    tb_fatalities = tb[FATALITIES_COLS]
    tb_vehicles = tb[VEHICLES_COLS]
    tb_paved_km = tb[PAVED_KM_COLS]
    tb_rest = tb[REST_COLS]

    # concatenate tables with WHO estimated fatalities
    # add year column
    tb_who_est_2023 = tb_who_est_2023.rename(columns={"year_who_estimated_road_traffic_fatalities": "year"})
    tb_who_est_2016["year"] = 2016
    tb_who_est_2010["year"] = 2010

    # standardize column names
    tb_who_est_2023 = tb_who_est_2023.rename(
        columns={
            "who_estimated_road_traffic_fatalities": "who_est_fatalities",
            "lower_bound_who_estimated_road_traffic_fatalities": "low_bound_who_est_fatalities",
            "upper_bound_who_estimated_road_traffic_fatalities": "up_bound_who_est_fatalities",
            "who_estimated_rate_per_100_000_population": "who_est_rate_per_100k_pop",
        }
    )
    tb_who_est_2016 = tb_who_est_2016.rename(
        columns={
            "_2016_who_estimated_road_traffic_fatalities__update": "who_est_fatalities",
            "_2016_lower_bound_who_estimated_road_traffic_fatalities__update": "low_bound_who_est_fatalities",
            "_2016_upper_bound_who_estimated_road_traffic_fatalities__update": "up_bound_who_est_fatalities",
            "_2016_who_estimated_rate_per_100_000_population__update": "who_est_rate_per_100k_pop",
        }
    )
    tb_who_est_2010 = tb_who_est_2010.rename(
        {
            "_2010_who_estimated_road_traffic_fatalities__update": "who_est_fatalities",
            "_2010_lower_bound_who_estimated_road_traffic_fatalities__update": "low_bound_who_est_fatalities",
            "_2010_upper_bound_who_estimated_road_traffic_fatalities__update": "up_bound_who_est_fatalities",
            "_2010_who_estimated_rate_per_100_000_population__update": "who_est_rate_per_100k_pop",
        }
    )

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
