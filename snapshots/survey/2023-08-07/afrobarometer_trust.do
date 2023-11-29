/*
COMMANDS TO EXTRACT TRUST QUESTIONS FROM AFROBAROMETER
This code collapses microdata on trust from the Afrobarometer and generates a csv file.

NOTE: Only Round 8 (2022) is used, because it is the only round that includes the main trust question.

INSTRUCTIONS

	1. In the Afrobarometer Merged data page (https://www.afrobarometer.org/data/merged-data/), download the Merged Round 8 Data (34 countries) (2022) file.
	2. Copy the file to the same directory as this do file. Though it is a SPSS file, it can be read by Stata.
	3. Run this do-file in Stata. If it fails, check the name of the dta file in the first line of the code.
	4. The code generates a csv file called afrobarometer_trust.csv. Copy this file to the snapshots/ess/{version} directory.
	5. Add snapshot. The command is:
 		python snapshots/survey/{version}/afrobarometer_trust.py --path-to-file snapshots/survey/{version}/afrobarometer_trust.csv
	6. Delete csv file (and sav file)
	7. Run `etl afrobarometer_trust`

*/

**2022**

local year 2022

import spss using "/Users/parriagadap/Downloads/Afrobarometer/afrobarometer_release-dataset_merge-34ctry_r8_en_2023-03-01.sav", clear

rename COUNTRY country
gen year = `year'

* Keep only "most people can be trusted" (1), "Need to be very careful" (0)
keep if Q83 >= 0 & Q83 <= 1

gen trust = 0
replace trust = 1 if Q83 == 1

collapse (mean) trust [w=withinwt_hh], by (country year)

tempfile trust_`year'
save "`trust_`year''"

* Get a list of variables excluding country and year
ds country year, not

* Multiply variables by 100 to get percentages
foreach var of varlist `r(varlist)' {
	replace `var' = `var'*100
}

* Export as csv
export delimited using "afrobarometer_trust.csv", datafmt replace
