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
