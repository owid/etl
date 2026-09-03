/*
WID COMMANDS FOR OUR WORLD IN DATA

This program extracts inequality data from WID.world for three types of income and one type of wealth:
	- Pretax national income, income before the payment and receipt of taxes and benefits, but after payment of public and private pensions.
	- Post-tax disposable income, income that includes all cash redistribution through the tax and transfer system, but does not include in-kind benefits and therefore does not add up to national income.
	- Post-tax national income, income that includes all cash redistribution through the tax and transfer system and also in-kind transfers (i.e., government consumption expenditures) to individuals.
	- Household net wealth, the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.

The inequality variables extracted from here include Gini coefficients, averages, thresholds and shares per decile, statistics for the top 1, 0.1, 0.01 and 0.001% percentile and share ratios.
When needed, monetary values are converted to international dollars (PPP) at the prices of the latest year in WID's price index.

DATA QUALITY (wid v1.0.7, July 2026)

The wid command no longer has an `exclude` option to drop extrapolated observations. Instead every
download carries WID's row-level data_quality score (0-5; 0 = no underlying data, 5 = the best
annual sources). This script therefore makes ONE unfiltered pass and keeps that score, so the
decision about which observations to publish is taken downstream, in the garden step, where it is
versioned and testable. The score is constant within a country-year-welfare concept (the same for
averages, thresholds, shares and Ginis, and across percentiles), so it is stored once per concept:
	- wid_indices_992j.csv: one data_quality_<welfare> column per welfare concept
	- wid_distribution_992j.csv: one data_quality column (rows are already per welfare concept)
	- wid_indices_fiscal_992ijt.csv: one data_quality_fiscal992<pop> column per fiscal series (i, j, t)
	- wid_population_992_999_i.csv: raw passthrough (includes WID's data_quality column)
The script requires wid v1.0.7 or newer and stops in its first call if the column is missing. It also
stops if a download carries conflicting scores within a country-year-concept, writing those rows to
wid_data_quality_conflicts_option<n>.csv, since the garden step relies on one score per concept.

HOW TO EXECUTE:

1. Run this do-file in a local installation of Stata (it takes several hours). Use the full path to the
   binary, since several Stata versions can sit side by side under /Applications and a bare stata-se
   may resolve to one whose licence has expired, e.g. from the folder holding this file:
	/Applications/StataNow/StataSE.app/Contents/MacOS/stata-se -b do wid_indices.do
   The batch log (wid_indices.log) lands in the working directory, as do the CSVs.
2. It generates four CSV files in this directory (snapshots/wid/2026-09-02/): key indicators,
   distribution, fiscal income and population. A single command imports all four as snapshots in the
   ETL (the script world_inequality_database.py maps each generated CSV to its snapshot):
	etls wid/2026-09-02/world_inequality_database
3. Delete the leftover csv files: from the snapshots/wid/2026-09-02/ folder, run
    rm *.csv
   (kept on separate lines so the path and the glob never form a "slash-star" token, which
    Stata would otherwise read as a nested block-comment opener and comment out the whole file)

	(Change the date for future updates)

*/

//////////////////////////////////////////////////////////////////////////////////////
/* SETTINGS

This code will run these two options automatically
1 is the main dataset with key indicators: Gini, thresholds, shares, averages
2 is the distributional dataset, that includes 130 fractiles
*/

global options 1 2

* Select age. The default is individuals over age 20 (992). See the full list: https://wid.world/codes-dictionary/#three-digit-code
global age 992

* Select population unit. The default is equal-split adults (j).See the full list (2.1.5): https://wid.world/codes-dictionary/#one-letter-code
global unit j

*Select the dataset to extract. "all" for the entire WID data, "test" for test data, small (CL GB)
global dataset = "all"

///////////////////////////////////////////////////////////////////////////////////////

* Average and threshold indicators do not vary between key variables and distributional datasets
global indicators_avg_thr aptinc tptinc adiinc tdiinc acainc tcainc ahweal thweal

*Show entire output
set more off

*Get maximum year value to have to correct PPP conversion
qui wid, indicators(xlcusp) clear

* Fail fast on an outdated wid package: before v1.0.7 the download has no data_quality column, and
* this script would otherwise fail hours in, at the first reshape.
capture confirm variable data_quality
if _rc {
	di as error "The wid command returned no data_quality column. This script needs wid v1.0.7 or newer: run  ssc install wid, replace"
	exit 198
}

qui sum year
global max_year = r(max)

dis "Year of PPP data: $max_year"

*Get ppp data to convert to USD
qui wid, indicators(xlcusp) year($max_year) clear
rename value ppp
keep country ppp
tempfile ppp
qui save "`ppp'"

* If condition to select all the data or a part of it
if "$dataset" == "all" {
	*Get distinct values of countries and call it list_of countries
	*I will use this list to extract data per country instead of one big dataset that generates issues
	qui wid, indicators(xlcusp) clear
	qui levelsof country, local(list_of_countries) clean
}

else if "$dataset" == "test" {
	local list_of_countries CL GB
}




foreach option in $options {

	* Define different indicators and percentiles depending on the dataset
	if `option' == 1 {
		global indicators_gini_share sptinc gptinc sdiinc gdiinc scainc gcainc shweal ghweal
		global percentiles p0p10 p10p20 p20p30 p30p40 p40p50 p50p60 p60p70 p70p80 p80p90 p90p100 p0p100 p0p50 p99p100 p99.9p100 p99.99p100 p99.999p100
	}

	else if `option' == 2 {
		global indicators_gini_share sptinc sdiinc scainc shweal
		global percentiles p0p1 p1p2 p2p3 p3p4 p4p5 p5p6 p6p7 p7p8 p8p9 p9p10 p10p11 p11p12 p12p13 p13p14 p14p15 p15p16 p16p17 p17p18 p18p19 p19p20 p20p21 p21p22 p22p23 p23p24 p24p25 p25p26 p26p27 p27p28 p28p29 p29p30 p30p31 p31p32 p32p33 p33p34 p34p35 p35p36 p36p37 p37p38 p38p39 p39p40 p40p41 p41p42 p42p43 p43p44 p44p45 p45p46 p46p47 p47p48 p48p49 p49p50 p50p51 p51p52 p52p53 p53p54 p54p55 p55p56 p56p57 p57p58 p58p59 p59p60 p60p61 p61p62 p62p63 p63p64 p64p65 p65p66 p66p67 p67p68 p68p69 p69p70 p70p71 p71p72 p72p73 p73p74 p74p75 p75p76 p76p77 p77p78 p78p79 p79p80 p80p81 p81p82 p82p83 p83p84 p84p85 p85p86 p86p87 p87p88 p88p89 p89p90 p90p91 p91p92 p92p93 p93p94 p94p95 p95p96 p96p97 p97p98 p98p99 p99p100 p99p99.1 p99.1p99.2 p99.2p99.3 p99.3p99.4 p99.4p99.5 p99.5p99.6 p99.6p99.7 p99.7p99.8 p99.8p99.9 p99.9p100 p99.9p99.91 p99.91p99.92 p99.92p99.93 p99.93p99.94 p99.94p99.95 p99.95p99.96 p99.96p99.97 p99.97p99.98 p99.98p99.99 p99.99p100 p99.99p99.991 p99.991p99.992 p99.992p99.993 p99.993p99.994 p99.994p99.995 p99.995p99.996 p99.996p99.997 p99.997p99.998 p99.998p99.999 p99.999p100
	}

	foreach c in `list_of_countries' {

		di "avg thr for `c'"

		*Get average and threshold income for pre tax and post tax (nat and dis) data
		qui wid, indicators($indicators_avg_thr) perc($percentiles) areas(`c') ages($age) pop($unit) clear

		local c: subinstr local c "-" "_", all

		tempfile avgthr_`c'
		qui save "`avgthr_`c''"
	}

	clear

	foreach c in `list_of_countries' {
		local c: subinstr local c "-" "_", all
		append using "`avgthr_`c''"

	}

	*Merge with ppp data to transform monetary values to international-$
	qui merge n:1 country using "`ppp'", keep(match) nogenerate
	qui replace value = value/ppp
	drop ppp
	tempfile avgthr
	qui save "`avgthr'"

	foreach c in `list_of_countries' {

		di "gini share for `c'"

		*Gets shares and Gini for pre and post tax income
		qui wid, indicators($indicators_gini_share) perc($percentiles) areas(`c') ages($age) pop($unit) clear

		local c: subinstr local c "-" "_", all

		tempfile ginishare_`c'
		qui save "`ginishare_`c''"
	}

	clear

	foreach c in `list_of_countries' {
		local c: subinstr local c "-" "_", all
		append using "`ginishare_`c''"

	}

	*Union with average and threshold income
	qui append using "`avgthr'"

	* The welfare concept is letters 2-6 of WID's variable code (ptinc, diinc, cainc, hweal)
	gen concept = substr(variable, 2, 5)

	* Set WID's data_quality score aside, one row per country-year-concept, so it survives the reshape.
	* The score must be constant across series types and percentiles within that key: the garden step
	* applies one score per concept, so a conflict cannot be resolved downstream. If a download ever
	* breaks that, the extraction stops here and writes the offending rows to a CSV for inspection,
	* rather than picking one of the scores and publishing it.
	preserve
		keep country year concept data_quality
		bysort country year concept: egen _qmin = min(data_quality)
		bysort country year concept: egen _qmax = max(data_quality)
		qui count if _qmin != _qmax
		local n_conflicts = r(N)
		if `n_conflicts' > 0 {
			keep if _qmin != _qmax
			export delimited using "wid_data_quality_conflicts_option`option'.csv", replace
			di as error "option `option': `n_conflicts' rows with conflicting data_quality within country-year-concept, written to wid_data_quality_conflicts_option`option'.csv. The extraction stops here because the garden step relies on one score per country-year-concept."
			exit 9
		}
		* All scores within a key are identical at this point, so any aggregate returns that value.
		collapse (max) data_quality, by(country year concept)
		tempfile quality
		qui save "`quality'"
	restore
	drop data_quality

	if `option' == 1 {

		*Variable adjustments to create a wide dataset

		*Create percentile-variable and country-year variables (used as indices when the table is reshaped)
		egen varp = concat(percentile variable), punct(_)
		egen couy = concat(country year), punct(+)

		*Drop variables to only keep joined indices
		drop variable percentile country year concept age pop

		*Replace all occurrences of "." in the newly created `varp` (mainly in p99.9p100 and similar)
		*This is because names of variables with "." are not allowed
		qui replace varp = subinstr(varp, ".", "_", .)

		*Reshape dataset: couy is the main index and varp are what Stata calls subobservations, in this case metrics associated with percentiles
		qui reshape wide value, j(varp) i(couy) string

		*After the reshape, country and years are split into two variables again and the outcome is renamed
		qui split couy, p(+) destring
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
		*Same for some p0p50 indicators
		drop p0p100_share*
		drop p0p100_thr*
		drop p0p50_avg*
		drop p0p50_thr*

		*Define each income/wealth variable
		local var_names pretax posttax_nat posttax_dis wealth

		*Calculate ratios for each variable + create a duplicate variable for median
		* Also, generate a variable for the share between p90 and p99 and recalculate p50p90_share, because their components are more available.
		foreach var in `var_names' {

			qui gen palma_ratio_`var' = p90p100_share_`var' / (p0p50_share_`var' - p40p50_share_`var')
			qui gen s90_s10_ratio_`var' = p90p100_share_`var' / p0p10_share_`var'
			qui gen s80_s20_ratio_`var' = (p80p90_share_`var' + p90p100_share_`var') / (p0p10_share_`var' + p10p20_share_`var')
			qui gen s90_s50_ratio_`var' = p90p100_share_`var' / p0p50_share_`var'
			qui gen p90_p10_ratio_`var' = p90p100_thr_`var' / p10p20_thr_`var'
			qui gen p90_p50_ratio_`var' = p90p100_thr_`var' / p50p60_thr_`var'
			qui gen p50_p10_ratio_`var' = p50p60_thr_`var' / p10p20_thr_`var'

			qui gen median_`var' = p50p60_thr_`var'

			qui gen p90p99_share_`var' = p90p100_share_`var' - p99p100_share_`var'

			qui gen p50p90_share_`var' = p50p60_share_`var' + p60p70_share_`var' + p70p80_share_`var' + p80p90_share_`var'

		}

		* Bring WID's data_quality back as one column per welfare concept: data_quality_pretax,
		* data_quality_posttax_nat, data_quality_posttax_dis, data_quality_wealth
		preserve
			use "`quality'", clear
			qui replace concept = "pretax" if concept == "ptinc"
			qui replace concept = "posttax_nat" if concept == "diinc"
			qui replace concept = "posttax_dis" if concept == "cainc"
			qui replace concept = "wealth" if concept == "hweal"
			qui reshape wide data_quality, i(country year) j(concept) string
			rename data_quality* data_quality_*
			qui save "`quality'", replace
		restore
		qui merge 1:1 country year using "`quality'", assert(match) nogenerate

		*Order variables according to different variable groups
		order country year *gini_pretax *gini*dis *gini*nat *gini_wealth *_ratio*pretax *_ratio*dis *_ratio*nat *_ratio*wealth *share_pretax *share*dis *share*nat *share_wealth *avg_pretax *avg*dis *avg*nat *avg_wealth *thr_pretax *thr*dis *thr*nat *thr_wealth median* data_quality_*


		*Sort country and year
		sort country year

		*Export csv
		export delimited using "wid_indices_${age}${unit}.csv", replace

	}

	else if `option' == 2 {

		* Extract from variable the indicator (a,t,s) and welfare (ptinc, diinc, cainc, hweal)
		gen indicator = substr(variable, 1, 1)
		rename concept welfare

		* Create an index variable to make the table wide
		egen couypw = concat(country year percentile welfare), punct(+)
		drop country year percentile welfare variable age pop

		* Make the table wide
		qui reshape wide value, j(indicator) i(couypw) string

		* Split the index variable to recover the columns
		qui split couypw, p(+) destring

		* Rename resulting columns and drop what's not needed
		rename couypw1 country
		rename couypw2 year
		rename couypw3 percentile
		rename couypw4 welfare

		drop couypw

		* Rename resulting average, share and threshold columns
		rename valuea avg
		rename values share
		rename valuet thr

		* Bring WID's data_quality back (one score per country-year-welfare; welfare still in WID codes here)
		preserve
			use "`quality'", clear
			rename concept welfare
			qui save "`quality'", replace
		restore
		qui merge m:1 country year welfare using "`quality'", assert(match) nogenerate

		* Replace welfare codes with new text
		qui replace welfare = "pretax" if welfare == "ptinc"
		qui replace welfare = "posttax_nat" if welfare == "diinc"
		qui replace welfare = "posttax_dis" if welfare == "cainc"
		qui replace welfare = "wealth" if welfare == "hweal"

		* Extract percentile from WID's name
		qui split percentile, p(p)
		qui destring percentile2, generate(p)
		qui replace p = p/100
		drop percentile1 percentile2 percentile3

		* Sort, order and save
		sort country year p welfare

		order country year welfare percentile p thr avg share data_quality

		export delimited using "wid_distribution_${age}${unit}.csv", replace

	}

}

* Add fiscal income data (Chartbook of Economic Inequality)
qui wid, indicators(sfiinc) perc(p99p100) ages(992) pop(i j t) clear

*Variable adjustments to create a wide dataset

*Create percentile-variable and country-year variables (used as indices when the table is reshaped)
egen varp = concat(percentile variable), punct(_)
egen couy = concat(country year), punct(+)

*Drop variables to only keep joined indices
drop variable percentile country year pop age

*Reshape dataset: couy is the main index and varp are what Stata calls subobservations, in this case metrics associated with percentiles.
*data_quality is reshaped alongside value, so each fiscal series (i, j, t) keeps its own score.
qui reshape wide value data_quality, j(varp) i(couy) string

*After the reshape, country and years are split into two variables again and the outcome is renamed
qui split couy, p(+) destring
rename couy1 country
rename couy2 year

*Drop couy, as it is not longer needed
drop couy

*Internal WID codes are replaced for more human-readable variable names.
*The quality columns drop the percentile (only p99p100 is extracted) to stay within Stata's
*32-character variable-name limit: data_quality_fiscal992i / 992j / 992t.

rename value* *
rename data_qualityp99p100_sfiinc* data_quality_fiscal*
rename *sfiinc* *share_fiscal*

*Order variables according to different variable groups
order country year *share* data_quality_*

*Sort country and year
sort country year

*Export csv
export delimited using "wid_indices_fiscal_992ijt.csv", replace

*-------------------------------------------------------------------------------
* POPULATION
*-------------------------------------------------------------------------------
* WID's population counts (npopul) for ages 992 (adults, 20+) and 999 (all ages).
* These are the demographic yardstick for the derived poverty_inequality steps:
* WID's income series are per adult while the World Bank's are per capita, so
* converting between the two bases, and weighting countries in global
* decompositions, needs WID's own adult and total counts (another source's
* population would leak cross-source demographic disagreements into the
* comparison).
*
* Unit i (individuals) is deliberate. The population dimension only exists for
* distributed series: WID's codes dictionary says "for aggregate series (or
* prices, exchange rates, and population series), we use the letter 'i' (for
* individuals) by default", and the API confirms it — requesting population(i j)
* for npopul returns "1 population category" and yields only npopul992i/999i,
* while the same request on aptinc returns both 992i and 992j. The j in
* aptinc992j says how income is split among adults, not how many adults there
* are, so aptinc992j / npopul992i is the correct pairing.
*
* This block is a single API call and takes a few minutes, unlike the extractions
* above; it is last so it can be re-run on its own if only population changes.

qui wid, indicators(npopul) areas(_all) ages(992 999) population(i) clear

* Raw passthrough: export the response exactly as the wid command returns it
* (country / variable / percentile / year / value / age / pop / data_quality). The reshape
* to one column per variable and the descriptive column names happen in the meadow step.
export delimited using "wid_population_992_999_i.csv", replace delim(",")

exit, clear
