/*
WID COMMANDS FOR OUR WORLD IN DATA

This program extracts inequality data from LIS for three types of income and one type of wealth:
	- Pretax national income, income before the payment and receipt of taxes and benefits, but after payment of public and private pensions.
	- Post-tax disposable income, income that includes all cash redistribution through the tax and transfer system, but does not include in-kind benefits and therefore does not add up to national income.
	- Post-tax national income, income that includes all cash redistribution through the tax and transfer system and also in-kind transfers (i.e., government consumption expenditures) to individuals.
	- Net national wealth, the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.

The inequality variables extracted from here include Gini coefficients, averages, thresholds and shares per decile, statistics for the top 1, 0.1, 0.01 and 0.001% percentile and share ratios.
When needed, values are converted to PPP (2011 vintage) adjusted to prices of the most recent year available.

HOW TO EXECUTE:

1. Open this do-file in a local installation of Stata (execution time: ~5-10 minutes)
2. It generates one file, wid_indices_992j.csv, which needs to be imported as a snapshot in the ETL, as
	python snapshots/wid/2023-01-27/world_inequality_database.py --path-to-file wid_indices_992j.csv

	(Change the date for future updates)

*/

*Show entire output
set more off

*Get maximum year value to have to correct PPP conversion
qui wid, indicators(xlcusp) clear
qui sum year
global max_year = r(max)

*Get ppp data to convert to USD
wid, indicators(xlcusp) year($max_year) clear
rename value ppp
tempfile ppp
save "`ppp'"

*Get average and threshold income for pre tax and post tax (nat and dis) data
wid, indicators(aptinc tptinc adiinc tdiinc acainc tcainc ahweal thweal) perc(p0p10 p10p20 p20p30 p30p40 p40p50 p50p60 p60p70 p70p80 p80p90 p90p100 p0p100 p99p100 p99.9p100 p99.99p100 p99.999p100) ages(992) pop(j) exclude clear

*Merge with ppp data to transform monetary values to international-$
merge n:1 country using "`ppp'", keep(match)
replace value = value/ppp
drop ppp
drop _merge
tempfile avgthr
save "`avgthr'"

*Gets shares and Gini for pre and post tax income
wid, indicators(sptinc gptinc sdiinc gdiinc scainc gcainc shweal ghweal) perc(p0p10 p10p20 p20p30 p30p40 p40p50 p50p60 p60p70 p70p80 p80p90 p90p100 p0p100 p0p50 p50p90 p99p100 p99.9p100 p99.99p100 p99.999p100) ages(992) pop(j) exclude clear

*Union with average and threshold income
append using "`avgthr'"

*Variable adjustments to create a wide dataset

*Create percentile-variable and country-year variables (used as indices when the table is reshaped)
egen varp = concat(percentile variable), punct(_)
egen couy = concat(country year), punct(+)

*Drop variables to only keep joined indices
drop variable percentile country year

*Replace all occurrences of "." in the newly created `varp` (mainly in p99.9p100 and similar)
*This is because names of variables with "." are not allowed
replace varp = subinstr(varp, ".", "_", .)

*Reshape dataset: couy is the main index and varp are what Stata calls subobservations, in this case metrics associated with percentiles
reshape wide value, j(varp) i(couy) string

*After the reshape, country and years are split into two variables again and the outcome is renamed
split couy, p(+) destring
rename couy1 country
rename couy2 year

*Drop couy, as it is not longer needed
drop couy

*Internal WID codes are replaced for more human-readable variable names

rename value* *
rename *sptinc* *share_pretax
rename *gptinc* *gini_pretax
rename *aptinc* *avg_pretax
rename *tptinc* *thr_pretax
rename *sdiinc* *share_posttax_nat
rename *gdiinc* *gini_posttax_nat
rename *adiinc* *avg_posttax_nat
rename *tdiinc* *thr_posttax_nat
rename *scainc* *share_posttax_dis
rename *gcainc* *gini_posttax_dis
rename *acainc* *avg_posttax_dis
rename *tcainc* *thr_posttax_dis
rename *shweal* *share_wealth
rename *ghweal* *gini_wealth
rename *ahweal* *avg_wealth
rename *thweal* *thr_wealth

*Drop shares and thresholds for the entire distribution, as they do not have relevance for analysis (or they repeat other numbers from the dataset)
drop p0p100_share*
drop p0p100_thr*

*Define each income/wealth variable
local var_names pretax posttax_nat posttax_dis wealth

*Calculate ratios for each variable + create a duplicate variable for median
foreach var in `var_names' {

	gen palma_ratio_`var' = p90p100_share_`var' / (p0p50_share_`var' - p40p50_share_`var')
	gen s90_s10_ratio_`var' = p90p100_share_`var' / p0p10_share_`var'
	gen s80_s20_ratio_`var' = (p80p90_share_`var' + p90p100_share_`var') / (p0p10_share_`var' + p10p20_share_`var')
	gen s90_s50_ratio_`var' = p90p100_share_`var' / p0p50_share_`var'
	gen p90_p10_ratio_`var' = p90p100_thr_`var' / p10p20_thr_`var'
	gen p90_p50_ratio_`var' = p90p100_thr_`var' / p50p60_thr_`var'
	gen p50_p10_ratio_`var' = p50p60_thr_`var' / p10p20_thr_`var'
	
	gen median_`var' = p50p60_thr_`var'

}

*Order variables according to different variable groups
order country year *gini_pretax *gini*dis *gini*nat *gini_wealth *_ratio*pretax *_ratio*dis *_ratio*nat *_ratio*wealth *share_pretax *share*dis *share*nat *share_wealth *avg_pretax *avg*dis *avg*nat *avg_wealth *thr_pretax *thr*dis *thr*nat *thr_wealth median*

*Sort country and year
sort country year

*Export csv
export delimited using "wid_indices_992j.csv", replace

** In case of needing it in a Stata datafile
*save "wid_indices_992j.dta", replace

exit, clear
