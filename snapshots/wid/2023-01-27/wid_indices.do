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
2. It generates two files, wid_indices_992j_exclude.csv and wid_indices_992j_include.csv, which exclude and include extrapolations. They need to be imported as snapshots in the ETL, as
	python snapshots/wid/2023-01-27/world_inequality_database.py --path-to-file snapshots/wid/2023-01-27/wid_indices_992j_exclude.csv
	python snapshots/wid/2023-01-27/world_inequality_database_with_extrapolations.py --path-to-file snapshots/wid/2023-01-27/wid_indices_992j_include.csv

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

*Select the dataset to extract. "all" for the entire LIS data, "test" for test data, small (CL GB)
global dataset = "all"

///////////////////////////////////////////////////////////////////////////////////////

* If condition to select all the data or a part of it
if "$dataset" == "all" {
	global areas _all 
}

else if "$dataset" == "test" {
	global areas CL GB
}

*Run this code to include and exclude extrapolations
global exclude_extrapolations 1 0

* Average and threshold indicators do not vary between key variables and distributional datasets
global indicators_avg_thr aptinc tptinc adiinc tdiinc acainc tcainc ahweal thweal

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

foreach option in $options {
	
	* Define different indicators and percentiles depending on the dataset
	if `option' == 1 {
		global indicators_gini_share sptinc gptinc sdiinc gdiinc scainc gcainc shweal ghweal
		global percentiles p0p10 p10p20 p20p30 p30p40 p40p50 p50p60 p60p70 p70p80 p80p90 p90p100 p0p100 p99p100 p99.9p100 p99.99p100 p99.999p100
	}

	else if `option' == 2 {
		global indicators_gini_share sptinc sdiinc scainc shweal
		global percentiles p0p1 p1p2 p2p3 p3p4 p4p5 p5p6 p6p7 p7p8 p8p9 p9p10 p10p11 p11p12 p12p13 p13p14 p14p15 p15p16 p16p17 p17p18 p18p19 p19p20 p20p21 p21p22 p22p23 p23p24 p24p25 p25p26 p26p27 p27p28 p28p29 p29p30 p30p31 p31p32 p32p33 p33p34 p34p35 p35p36 p36p37 p37p38 p38p39 p39p40 p40p41 p41p42 p42p43 p43p44 p44p45 p45p46 p46p47 p47p48 p48p49 p49p50 p50p51 p51p52 p52p53 p53p54 p54p55 p55p56 p56p57 p57p58 p58p59 p59p60 p60p61 p61p62 p62p63 p63p64 p64p65 p65p66 p66p67 p67p68 p68p69 p69p70 p70p71 p71p72 p72p73 p73p74 p74p75 p75p76 p76p77 p77p78 p78p79 p79p80 p80p81 p81p82 p82p83 p83p84 p84p85 p85p86 p86p87 p87p88 p88p89 p89p90 p90p91 p91p92 p92p93 p93p94 p94p95 p95p96 p96p97 p97p98 p98p99 p99p100 p99p99.1 p99.1p99.2 p99.2p99.3 p99.3p99.4 p99.4p99.5 p99.5p99.6 p99.6p99.7 p99.7p99.8 p99.8p99.9 p99.9p100 p99.9p99.91 p99.91p99.92 p99.92p99.93 p99.93p99.94 p99.94p99.95 p99.95p99.96 p99.96p99.97 p99.97p99.98 p99.98p99.99 p99.99p100 p99.99p99.991 p99.991p99.992 p99.992p99.993 p99.993p99.994 p99.994p99.995 p99.995p99.996 p99.996p99.997 p99.997p99.998 p99.998p99.999 p99.999p100
	}
	
	foreach excl_option in $exclude_extrapolations {
		
		* If excl_option is 1 we will add exclude_option to the wid command and add "exclude" as a variable suffix
		if `excl_option' == 1 {
			global exclude_option exclude
			local excl_option_slug = "exclude"
		}

		* If excl_option is 0 no text is included to the wid command and "include" is added as a variable suffix
		else if `excl_option' == 0 {
			global exclude_option
			local excl_option_slug = "include"
		}

		*Get average and threshold income for pre tax and post tax (nat and dis) data
		wid, indicators($indicators_avg_thr) perc($percentiles) areas($areas) ages($age) pop($unit) $exclude_option clear

		*Merge with ppp data to transform monetary values to international-$
		merge n:1 country using "`ppp'", keep(match)
		replace value = value/ppp
		drop ppp
		drop _merge
		tempfile avgthr
		save "`avgthr'"

		*Gets shares and Gini for pre and post tax income
		wid, indicators($indicators_gini_share) perc($percentiles) areas($areas) ages($age) pop($unit) $exclude_option clear

		*Union with average and threshold income
		append using "`avgthr'"
		
		if `option' == 1 {
		
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
			* Also, generate a variable for the share between p90 and p99 and recalculate p50p90_share, because their components are more available.
			foreach var in `var_names' {

				gen p0p50_share_`var' = p0p10_share_`var' + p10p20_share_`var' + p20p30_share_`var' + p30p40_share_`var' + p40p50_share_`var'
				
				gen palma_ratio_`var' = p90p100_share_`var' / (p0p50_share_`var' - p40p50_share_`var')
				gen s90_s10_ratio_`var' = p90p100_share_`var' / p0p10_share_`var'
				gen s80_s20_ratio_`var' = (p80p90_share_`var' + p90p100_share_`var') / (p0p10_share_`var' + p10p20_share_`var')
				gen s90_s50_ratio_`var' = p90p100_share_`var' / p0p50_share_`var'
				gen p90_p10_ratio_`var' = p90p100_thr_`var' / p10p20_thr_`var'
				gen p90_p50_ratio_`var' = p90p100_thr_`var' / p50p60_thr_`var'
				gen p50_p10_ratio_`var' = p50p60_thr_`var' / p10p20_thr_`var'

				gen median_`var' = p50p60_thr_`var'

				gen p90p99_share_`var' = p90p100_share_`var' - p99p100_share_`var'

				gen p50p90_share_`var' = p50p60_share_`var' + p60p70_share_`var' + p70p80_share_`var' + p80p90_share_`var'

			}

			*Order variables according to different variable groups
			order country year *gini_pretax *gini*dis *gini*nat *gini_wealth *_ratio*pretax *_ratio*dis *_ratio*nat *_ratio*wealth *share_pretax *share*dis *share*nat *share_wealth *avg_pretax *avg*dis *avg*nat *avg_wealth *thr_pretax *thr*dis *thr*nat *thr_wealth median*

			*Sort country and year
			sort country year

			*Export csv
			export delimited using "wid_indices_${age}${unit}_`excl_option_slug'.csv", replace
		
		}
		
		else if `option' == 2 {
			
			* Extract from variable the indicator (a,t,s) and welfare (ptinc, diinc, cainc, hweal)
			gen indicator = substr(variable, 1, 1)
			gen welfare = substr(variable, 2, 5)
			
			* Create an index variable to make the table wide
			egen couypw = concat(country year percentile welfare), punct(+)
			drop country year percentile welfare variable age pop

			* Make the table wide
			reshape wide value, j(indicator) i(couypw) string

			* Split the index variable to recover the columns
			split couypw, p(+) destring
			
			* Rename resulting columns and drop what's not needed
			rename couypw1 country
			rename couypw2 year
			rename couypw3 percentile
			rename couypw4 welfare

			drop couyp
			
			* Rename resulting average, share and threshold columns
			rename valuea avg
			rename values share
			rename valuet thr
			
			* Replace welfare codes with new text
			replace welfare = "pretax" if welfare == "ptinc"
			replace welfare = "posttax_nat" if welfare == "diinc"
			replace welfare = "posttax_dis" if welfare == "cainc"
			replace welfare = "wealth" if welfare == "hweal"
			
			* Extract percentile from WID's name
			split percentile, p(p)
			destring percentile2, generate(p)
			replace p = p/100
			drop percentile1 percentile2 percentile3
			
			* Sort, order and save
			sort country year p welfare

			order country year welfare percentile p thr avg share

			export delimited using "wid_distribution_${age}${unit}_`excl_option_slug'.csv", replace
			
		}

	}
	
}

exit, clear
