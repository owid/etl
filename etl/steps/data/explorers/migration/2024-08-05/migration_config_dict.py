P_TOTAL = "Total"
P_CHANGE = "Five-year change"
P_NEW = "Annual/ New"

SM_TOTAL = "Total"
SM_SHARE = "Per capita/ Share of population"

A_TOTAL = "Total"
A_UNDER_18 = "Under 18"
A_UNDER_15 = "Under 15"

PR_RAW = "Raw data"
PR_AVG = "Five-year average"


METRICS = {
    "immigrants": "International immigrants",
    "emigrants": "International emigrants",
    "net_migration": "Net migration",
    "asylum_seekers_dest": "Asylum Seekers by destination",
    "asylum_seekers_origin": "Asylum Seekers by origin",
    "refugees_dest": "Refugees by destination",
    "refugees_origin": "Refugees by origin",
    "resettlement_dest": "Resettled refugees by destination",
    "resettlement_origin": "Resettled refugees by origin",
    "returned_origin": "Returned refugees by origin",
    "returned_dest": "Returned refugees by destination",
    "internal_displ_total": "Internally displaced persons",
    "internal_displ_conflict": "Internally displaced persons (conflict)",
    "internal_displ_disaster": "Internally displaced persons (disaster)",
    "remittance_gdp": "Remittances as share of GDP",
    "remittance_cost_ib": "Cost of receiving remittances",
    "remittance_cost_ob": "Cost of sending remittances",
}
SORTER = list(METRICS.values())

ADDITIONAL_DESCRIPTIONS = {
    "net_migration": {
        "title": "Net migration",
        "description": "The total number of [immigrants](#dod:immigrant) (people moving into a given country) minus the number of [emigrants](#dod:emigrant) (people moving out of the country).",
    },
    "net_migration_rate": {
        "title": "Net migration rate",
        "description": "The total number of [immigrants](#dod:immigrant) (people moving into a given country) minus the number of [emigrants](#dod:emigrant) (people moving out of the country), per 1,000 people in the population.",
    },
    "bx_trf_pwkr_dt_gd_zs": {
        "title": "Remittances as share of GDP",
        "description": "Share of GDP that is made up of the sum of all personal [remittances](#dod:remittances) sent by migrants to their home countries. Remittances are in-kind or cash transfers made from individuals in a given country to households outside of the host country.",
    },
    "si_rmt_cost_ib_zs": {
        "title": "Average cost for sending remittances to country",
        "description": "The average [transaction cost](#dod:remittancecost) as a percentage of total [remittance](#dod:remittances) sent from abroad to this country. Remittances are in-kind or cash transfers made from individuals in a given country to households outside of the host country. The cost is based on a single transaction of USD 200. ",
    },
    "si_rmt_cost_ob_zs": {
        "title": "Average cost for sending remittances from country",
        "description": "The average [transaction cost](#dod:remittancecost) as a percentage of total [remittance](#dod:remittances) received from abroad. Remittances are in-kind or cash transfers made from individuals in a given country to households outside of the host country. The cost is based on a single transaction of USD 200. ",
    },
}


CONFIG_DICT = {
    # UNICEF values
    "international_migrants_under_18_dest": {
        "metric": METRICS["immigrants"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "migrants_under_18_dest_per_1000": {
        "metric": METRICS["immigrants"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "refugees_under_18_asylum": {
        "metric": METRICS["refugees_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "refugees_under_18_origin": {
        "metric": METRICS["refugees_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "refugees_under_18_asylum_per_1000": {
        "metric": METRICS["refugees_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "refugees_under_18_origin_per_1000": {
        "metric": METRICS["refugees_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "idps_under_18_conflict_violence": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "idps_under_18_disaster": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "idps_under_18_total": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "new_idps_under_18_conflict_violence": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "new_idps_under_18_disaster": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "new_idps_under_18_total": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "idps_under_18_conflict_violence_per_1000": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "idps_under_18_disaster_per_1000": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "idps_under_18_total_per_1000": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "new_idps_under_18_conflict_violence_per_1000": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "new_idps_under_18_disaster_per_1000": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    "new_idps_under_18_total_per_1000": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
        "processing_radio": PR_RAW,
    },
    # UNHCR values
    "refugees_under_unhcrs_mandate_origin": {
        "metric": METRICS["refugees_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "asylum_seekers_origin": {
        "metric": METRICS["asylum_seekers_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "refugees_under_unhcrs_mandate_asylum": {
        "metric": METRICS["refugees_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "asylum_seekers_asylum": {
        "metric": METRICS["asylum_seekers_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "returned_refugees_origin": {
        "metric": METRICS["returned_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "resettlement_arrivals_origin": {
        "metric": METRICS["resettlement_origin"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "returned_refugees_dest": {
        "metric": METRICS["returned_dest"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "resettlement_arrivals_dest": {
        "metric": METRICS["resettlement_dest"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "refugees_per_1000_pop_origin": {
        "metric": METRICS["refugees_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "refugees_per_1000_pop_asylum": {
        "metric": METRICS["refugees_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "asylum_seekers_per_100k_pop_origin": {
        "metric": METRICS["asylum_seekers_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "asylum_seekers_per_100k_pop_asylum": {
        "metric": METRICS["asylum_seekers_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "resettlement_per_100k_origin": {
        "metric": METRICS["resettlement_origin"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "resettlement_per_100k_dest": {
        "metric": METRICS["resettlement_dest"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    # UNDESA values (TODO: 5 year change??)
    "immigrants_all": {
        "metric": METRICS["immigrants"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "immigrants_change_5_years": {
        "metric": METRICS["immigrants"],
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "emigrants_all": {
        "metric": METRICS["emigrants"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "emigrants_change_5_years": {
        "metric": METRICS["emigrants"],
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "immigrant_share_of_dest_population_all": {
        "metric": METRICS["immigrants"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "immigrants_change_5_years_per_1000": {
        "metric": METRICS["immigrants"],
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "emigrants_share_of_total_population": {
        "metric": METRICS["emigrants"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "emigrants_change_5_years_per_1000": {
        "metric": METRICS["emigrants"],
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    # UN WPP values
    "net_migration": {
        "metric": METRICS["net_migration"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "net_migration_rate": {
        "metric": METRICS["net_migration"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    # World Bank
    # average remittance cost sending to country
    "si_rmt_cost_ib_zs": {
        "metric": METRICS["remittance_cost_ib"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    # average remittance cost sending from country
    "si_rmt_cost_ob_zs": {
        "metric": METRICS["remittance_cost_ob"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    # remittance as share of GDP
    "bx_trf_pwkr_dt_gd_zs": {
        "metric": METRICS["remittance_gdp"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    # IDMC internal displacement values
    "conflict_internal_displacements": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "disaster_internal_displacements": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "total_internal_displacements": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "conflict_stock_displacement": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "disaster_stock_displacement": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "total_stock_displacement": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "share_of_internally_displaced_pop": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "share_of_conflict_displaced_pop": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "share_of_disaster_displaced_pop": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "displacements_per_100_people": {
        "metric": METRICS["internal_displ_total"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "conflict_displacements_per_100_people": {
        "metric": METRICS["internal_displ_conflict"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
    "disaster_displacements_per_100_people": {
        "metric": METRICS["internal_displ_disaster"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_RAW,
    },
}


FIVE_YEAR_AVG_METRICS = {
    "refugees_origin_5y_avg": {
        "metric": METRICS["refugees_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "refugees_asylum_5y_avg": {
        "metric": METRICS["refugees_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "asylum_seekers_origin_5y_avg": {
        "metric": METRICS["asylum_seekers_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "asylum_seekers_asylum_5y_avg": {
        "metric": METRICS["asylum_seekers_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "refugees_origin_5y_avg_per_1000_pop": {
        "metric": METRICS["refugees_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "refugees_asylum_5y_avg_per_1000_pop": {
        "metric": METRICS["refugees_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "asylum_seekers_origin_5y_avg_per_100k_pop": {
        "metric": METRICS["asylum_seekers_origin"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "asylum_seekers_asylum_5y_avg_per_100k_pop": {
        "metric": METRICS["asylum_seekers_dest"],
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "resettlement_origin_5y_avg": {
        "metric": METRICS["resettlement_origin"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "resettlement_dest_5y_avg": {
        "metric": METRICS["resettlement_dest"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "resettlement_origin_5y_avg_per_100k_pop": {
        "metric": METRICS["resettlement_origin"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
    "resettlement_dest_5y_avg_per_100k_pop": {
        "metric": METRICS["resettlement_dest"],
        "period_radio": P_NEW,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
        "processing_radio": PR_AVG,
    },
}


MAP_BRACKETS = {
    "idps_under_18_conflict_violence": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 100000.0, 3000000.0, 0],
    },
    "idps_under_18_disaster": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 0],
    },
    "idps_under_18_total": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 0],
    },
    "international_migrants_under_18_dest": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [5000.0, 10000.0, 20000.0, 50000.0, 100000.0, 200000.0, 500000.0, 1000000.0, 0],
    },
    "new_idps_under_18_conflict_violence": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 0],
    },
    "new_idps_under_18_disaster": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 0],
    },
    "new_idps_under_18_total": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 0],
    },
    "refugees_under_18_asylum": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 100000.0, 0],
    },
    "refugees_under_18_origin": {
        "colorScaleScheme": "YlOrRd",
        "colorScaleNumericBins": [10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 0],
    },
    "refugees_under_18_asylum_per_1000": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20, 0],
    },
    "refugees_under_18_origin_per_1000": {
        "colorScaleScheme": "YlOrRd",
        "colorScaleNumericBins": [0.1, 0.3, 1.0, 3.0, 10.0, 30, 100, 0],
    },
    "migrants_under_18_dest_per_1000": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [1.0, 2.0, 5.0, 10.0, 20.0, 0],
    },
    "idps_under_18_total_per_1000": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 0],
    },
    "new_idps_under_18_total_per_1000": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 0],
    },
    "idps_under_18_conflict_violence_per_1000": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 0],
    },
    "idps_under_18_disaster_per_1000": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0],
    },
    "new_idps_under_18_conflict_violence_per_1000": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 0],
    },
    "new_idps_under_18_disaster_per_1000": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0],
    },
    "refugees_under_unhcrs_mandate_origin": {
        "colorScaleScheme": "YlOrRd",
        "colorScaleNumericBins": [300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 100000.0, 0],
    },
    "asylum_seekers_origin": {
        "colorScaleScheme": "YlOrRd",
        "colorScaleNumericBins": [300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 0],
    },
    "refugees_under_unhcrs_mandate_asylum": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 0],
    },
    "asylum_seekers_asylum": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 0],
    },
    "returned_refugees_origin": {
        "colorScaleScheme": "BuGn",
        "colorScaleNumericBins": [10.0, 100.0, 1000.0, 10000.0, 100000.0, 0],
    },
    "resettlement_arrivals_origin": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [10.0, 30.0, 100.0, 300.0, 1000.0, 3000.0, 10000.0, 0],
    },
    "returned_refugees_dest": {
        "colorScaleScheme": "GnBu",
        "colorScaleNumericBins": [100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 0],
    },
    "resettlement_arrivals_dest": {
        "colorScaleScheme": "BuGn",
        "colorScaleNumericBins": [30.0, 100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 0],
    },
    "refugees_per_1000_pop_origin": {
        "colorScaleScheme": "YlOrRd",
        "colorScaleNumericBins": [0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0, 0],
    },
    "refugees_per_1000_pop_asylum": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 0],
    },
    "asylum_seekers_per_100k_pop_origin": {
        "colorScaleScheme": "YlOrRd",
        "colorScaleNumericBins": [10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 0.0],
    },
    "asylum_seekers_per_100k_pop_asylum": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 0.0],
    },
    "resettlement_per_100k_origin": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [0.3, 1.0, 3.0, 10.0, 30.0, 100.0, 0],
    },
    "resettlement_per_100k_dest": {
        "colorScaleScheme": "BuGn",
        "colorScaleNumericBins": [0.0, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0, 0],
    },
    "immigrants_all": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [50000.0, 100000.0, 200000.0, 500000.0, 1000000.0, 2000000.0, 5000000.0, 0],
    },
    "immigrant_share_of_dest_population_all": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [1.0, 2.0, 5.0, 10.0, 10.0, 20.0, 0],
    },
    "emigrants_all": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [100000.0, 200000.0, 500000.0, 1000000.0, 2000000.0, 5000000.0, 10000000, 0],
    },
    "immigrants_change_5_years": {
        "colorScaleScheme": "RdYlBu",
        "colorScaleNumericBins": [-100000, -30000.0, -10000.0, -0.0, 10000.0, 30000.0, 100000.0, 300000, 1000000.0, 0],
    },
    "immigrants_change_5_years_per_1000": {
        "colorScaleScheme": "RdYlBu",
        "colorScaleNumericBins": [-10.0, -5.0, -2.0, -1.0, -0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 0],
    },
    "emigrants_change_5_years": {
        "colorScaleScheme": "RdYlBu",
        "colorScaleNumericBins": [
            -50000.0,
            -20000.0,
            -10000.0,
            -0.0,
            10000.0,
            20000.0,
            50000.0,
            100000.0,
            200000.0,
            500000.0,
            1000000.0,
            0,
        ],
    },
    "emigrants_change_5_years_per_1000": {
        "colorScaleScheme": "RdYlBu",
        "colorScaleNumericBins": [-10.0, -5.0, -2.0, -1.0, -0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 0],
    },
    "emigrants_share_of_total_population": {
        "colorScaleScheme": "YlGnBu",
        "colorScaleNumericBins": [2.0, 5.0, 10.0, 15.0, 0],
    },
    "net_migration": {
        "colorScaleScheme": "RdYlBu",
        "colorScaleNumericBins": [
            -300000.0,
            -100000.0,
            -30000,
            -10000.0,
            -0.0,
            10000.0,
            30000.0,
            100000.0,
            300000.0,
            0,
        ],
    },
    "net_migration_rate": {
        "colorScaleScheme": "RdYlBu",
        "colorScaleNumericBins": [-5, -2, -1, -0.5, -0.2, -0.0, 0.2, 0.5, 1, 2, 5, 10, 0],
    },
    "bx_trf_pwkr_dt_gd_zs": {
        "colorScaleScheme": "BuPu",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0],
    },
    "si_rmt_cost_ib_zs": {
        "colorScaleScheme": "Oranges",
        "colorScaleNumericBins": [2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 0.996667],
    },
    "si_rmt_cost_ob_zs": {
        "colorScaleScheme": "Oranges",
        "colorScaleNumericBins": [2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 1.943111],
    },
    "conflict_stock_displacement": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 3000000.0, 1640],
    },
    "conflict_internal_displacements": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 230],
    },
    "disaster_internal_displacements": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 230],
    },
    "disaster_stock_displacement": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 3000000.0, 1640],
    },
    "total_stock_displacement": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 3000000.0, 1640],
    },
    "total_internal_displacements": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [30000.0, 100000.0, 300000.0, 1000000.0, 0],
    },
    "share_of_internally_displaced_pop": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0.0],
    },
    "share_of_conflict_displaced_pop": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0.0],
    },
    "share_of_disaster_displaced_pop": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0.0],
    },
    "displacements_per_100_people": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0],
    },
    "conflict_displacements_per_100_people": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0],
    },
    "disaster_displacements_per_100_people": {
        "colorScaleScheme": "OrRd",
        "colorScaleNumericBins": [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 0],
    },
}
