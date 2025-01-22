#%%
import pandas as pd

#%%

#%% calculate pop weights and pop-weighted values


def add_weights_and_calc_aggregates(df,ref_years,proportional_change_threshold):

    averages = {}
    unweighted_avg = {}
    weighted_avg = {}
    total_pop = {}
    abs_thresholds =[1,2]

    for y in ref_years:
        # Group by 'series_code' and calculate total population for each group
        total_pop[y] = df.groupby('series_code')[f'population_{y}'].sum().rename(f'population_{y}_total').reset_index()

        # Merge the total populations back to the original DataFrame
        df = df.merge(total_pop[y], on='series_code', suffixes=('', '_total'))
    
        # Calculate the weight and weighted-value for each country within its 'series_code' group for both years
        df[f'weight_{y}'] = df[f'population_{y}'] / df[f'population_{y}_total']
        df[f'weightedvalue_{y}'] = df[f'weight_{y}'] * df[f'value_{y}']

        df = df.drop(columns=f'population_{y}_total')


        # Calculate aggregations by ref year
        
        unweighted_avg[y] = df.groupby('series_code')[f'value_{y}'].mean().reset_index()
        unweighted_avg[y].rename(columns={f'value_{y}': f'unweighted_avg_{y}'}, inplace=True)

        weighted_avg[y] = df.groupby('series_code')[f'weightedvalue_{y}'].sum().reset_index()
        weighted_avg[y].rename(columns={f'weightedvalue_{y}': f'weighted_avg_{y}'}, inplace=True)

    # Count the number of countries with falling, stable or rising inequality
    df['abs_change'] = df[f'value_{ref_years[1]}'] - df[f'value_{ref_years[0]}']
    df['proportional_change'] = df['abs_change']/df[f'value_{ref_years[0]}']

    # Dummies to indicate falling, stable or rising inequality
    df['is_falling_count_prop'] = (df['proportional_change'] < -proportional_change_threshold).astype(int)
    df['is_stable_count_prop'] = ((df['proportional_change'] >= -proportional_change_threshold) & (df['proportional_change'] <= proportional_change_threshold)).astype(int)
    df['is_rising_count_prop'] = (df['proportional_change'] > proportional_change_threshold).astype(int)

    # Cross the dummy with the pop weight for the latter year
    df['is_falling_popshare_prop'] = df['is_falling_count_prop'] * df[f'weight_{ref_years[0]}']
    df['is_stable_popshare_prop'] = df['is_stable_count_prop'] * df[f'weight_{ref_years[0]}']
    df['is_rising_popshare_prop'] = df['is_rising_count_prop'] * df[f'weight_{ref_years[0]}']


    for abs_threshold in abs_thresholds:
        df[f'is_falling_count_abs_{abs_threshold}'] = (df['abs_change'] < -abs_threshold).astype(int)
        df[f'is_stable_count_abs_{abs_threshold}'] = ((df['abs_change'] >= -abs_threshold) & (df['abs_change'] <= abs_threshold)).astype(int)
        df[f'is_rising_count_abs_{abs_threshold}'] = (df['abs_change'] > abs_threshold).astype(int)

        df[f'is_falling_popshare_abs_{abs_threshold}'] = df[f'is_falling_count_abs_{abs_threshold}'] * df[f'weight_{ref_years[0]}']
        df[f'is_stable_popshare_abs_{abs_threshold}'] = df[f'is_stable_count_abs_{abs_threshold}'] * df[f'weight_{ref_years[0]}']
        df[f'is_rising_popshare_abs_{abs_threshold}'] = df[f'is_rising_count_abs_{abs_threshold}'] * df[f'weight_{ref_years[0]}']



    change_type = ['is_falling_', 'is_stable_', 'is_rising_']
    count_type = ['count_abs_', 'popshare_abs_']
    abs_var_list = [change + count + str(thresh) for thresh in abs_thresholds for count in count_type for change in change_type]

    prop_var_list = [
        'is_falling_count_prop',
        'is_stable_count_prop',
        'is_rising_count_prop',
        'is_falling_popshare_prop',
        'is_stable_popshare_prop',
        'is_rising_popshare_prop'  
    ]
    
    var_list = prop_var_list + abs_var_list

    rises_and_falls = df.groupby('series_code')[var_list].sum().reset_index()
  
    # merge average dataframes together    
    averages['unweighted'] = pd.merge(unweighted_avg[ref_years[0]], unweighted_avg[ref_years[1]], how='left')
    averages['weighted'] = pd.merge(weighted_avg[ref_years[0]], weighted_avg[ref_years[1]], how='left')

    total_pops = pd.merge(total_pop[ref_years[0]], total_pop[ref_years[1]], how='left')

    summary = pd.merge(averages['unweighted'], averages['weighted'], how='left')
    summary = pd.merge(summary, rises_and_falls, how='left')
    summary = pd.merge(summary, total_pops, how='left')


    return summary




######################################
####### Run on different ref_years #######

#%%
ref_yrs = [1993, 2018]

########## GLOBAL SUMMARY ON MAIN DATA ##############
#%%
selected_data = pd.read_csv(f'data/selected_observations/{ref_yrs[0]}_{ref_yrs[1]}_by_country_series_code.csv')

# %%
global_summary = add_weights_and_calc_aggregates(
    selected_data,
    ref_yrs,
    0.05
    )

# %%
# Save to csv
global_summary.to_csv(f'data/summary_stats/{ref_yrs[0]}_{ref_yrs[1]}_summary_global.csv', index=False)




########## REGIONAL SUMMARIES ON MAIN DATA ##############

# %%

regions = selected_data['region'].unique().tolist()
region_summary_dict = {}
# %%
for reg in regions:

    #Filter data for region
    region_data = selected_data[selected_data['region'] == reg]

    #Calculate summary stats
    region_summary = add_weights_and_calc_aggregates(
    region_data,
    ref_yrs,
    0.05
    )

    # Move the region column first
    region_summary['region'] = reg
    column_to_move = 'region'
    cols = list(region_summary.columns)
    cols.insert(0, cols.pop(cols.index(column_to_move)))
    region_summary = region_summary[cols]


    #Add to dictionary
    region_summary_dict[reg] = region_summary

# %%
# append region summaries together into a single dataframe
regional_summaries = pd.concat(region_summary_dict.values(), axis=0)

# %%
#Re-sort
regional_summaries = regional_summaries.sort_values(by=['series_code','region'], ascending=[True, True])

# Save to csv
regional_summaries.to_csv(f'data/summary_stats/{ref_yrs[0]}_{ref_yrs[1]}_summary_by_region.csv', index=False)




########## GLOBAL SUMMARY ON ALT COVERAGE DATA ##############
#%%
selected_data = pd.read_csv(f'data/selected_observations/{ref_yrs[0]}_{ref_yrs[1]}_by_country_series_code_added_wid_extrapolations.csv')

# ALT 1 – including WID extroplated data
# %%
global_summary = add_weights_and_calc_aggregates(
    selected_data,
    ref_yrs,
    0.05
    )

# %%
# Save to csv
global_summary.to_csv(f'data/summary_stats/{ref_yrs[0]}_{ref_yrs[1]}_summary_global_added_wid_extrapolations.csv', index=False)


# ALT 2 – excluding WID extroplated data
# (leaving only observations with matching PIP and non-extrap WID data)

# Filter out WID extrapolations 
# %%
selected_data = selected_data[selected_data['key']=='wid_not_extrapolated']
# %%
global_summary = add_weights_and_calc_aggregates(
    selected_data,
    ref_yrs,
    0.05
    )

# %%
# Save to csv
global_summary.to_csv(f'data/summary_stats/{ref_yrs[0]}_{ref_yrs[1]}_summary_global_added_only_original_matching.csv', index=False)





###### Match shorter period and longer period observations and recalculate summary for shorter period
# %%
short_period = pd.read_csv(f'data/selected_observations/1993_2018_by_country_series_code.csv')
long_period = pd.read_csv(f'data/selected_observations/1980_2018_by_country_series_code.csv')


# %%
merged_df = short_period.merge(long_period, on=['country', 'series_code'], how='inner', suffixes=('_short', '_long'))
# %% Drop the '_long' period columns
columns_to_drop = [col for col in merged_df.columns if col.endswith('_long')]
merged_df = merged_df.drop(columns=columns_to_drop)

# %% Remove '_short' from the end of remaining columns
new_columns = {col: col[:-len('_short')] if col.endswith('_short') else col for col in merged_df.columns}
merged_df.rename(columns=new_columns, inplace=True)


# %% Generate summary
global_summary = add_weights_and_calc_aggregates(
    merged_df,
    [1993,2018],
    0.05
    )

# %%
# Save to csv
global_summary.to_csv(f'data/summary_stats/1993_2018_summary_global_countries_from_long_period.csv', index=False)



# %% OLDER CODE THAT IS NOT USED IN THE PIPELINE, BUT MIGHT BE HELPFUL IN THE FUTURE

#%%
# # replace india value
# def set_india_wid_gini_in_2018(df, new_value):

#     condition = (df['country']=="India") & (df['series_code']=="gini_wid_pretaxNational_perAdult")
#     df.loc[condition, 'value_2018'] = new_value

#     return df


# def extract_avgs(summary_stats, series_code):

#     #Filter for the series of interest
#     series_data = summary_stats[summary_stats['series_code'] == series_code]

#     # Grab the two unweighted avg columns
#     avgs = series_data.filter(like='_avg')

    
#     return avgs

# # %%
# threshold = 0.05
# ref_years = [1993,2018]

# india_scenarios = {}
# # %%
# original_wid_value = output_short_main[
#     (output_short_main['series_code']=='gini_wid_pretaxNational_perAdult')  & \
#     (output_short_main['country']=='India')]['value_2018'].values[0]
# # %%
# for india_wid_gini_value in [original_wid_value, 0.6, 0.55, 0.5]:

#     summary_stats =  add_weights_and_calc_aggregates(
#         set_india_wid_gini_in_2018(output_short_main, india_wid_gini_value),
#         ref_years,
#         threshold
#         )

#     india_scenarios[india_wid_gini_value] = extract_avgs(
#         summary_stats,
#         'gini_wid_pretaxNational_perAdult'
# )
# # %%
# india_scenarios = pd.concat(india_scenarios).reset_index()
# india_scenarios = india_scenarios.rename(columns={'level_0': 'Assumed India Gini in 2018'})
# india_scenarios = india_scenarios.drop(columns='level_1')
# india_scenarios['unweighted_change'] = india_scenarios['unweighted_avg_2018'] - india_scenarios['unweighted_avg_1993'] 
# india_scenarios['weighted_change'] = india_scenarios['weighted_avg_2018'] - india_scenarios['weighted_avg_1993'] 

# # Save to csv
# india_scenarios.to_csv("data/summary_stats/india_wid_scenarios.csv", index=False)

# %%

#### Longer period


#%%
# output_long_main = pd.read_csv("data/selected_observations/long_period_all_obs.csv")


# # %%
# global_summary = add_weights_and_calc_aggregates(
#     output_long_main,
#     [1980, 2018],
#     0.05
#     )


# # %%
# # Save to csv
# global_summary.to_csv("data/summary_stats/long_period_all_obs.csv", index=False)


