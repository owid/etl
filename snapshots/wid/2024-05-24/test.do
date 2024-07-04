qui wid, indicators(xlcusp) clear

*Get distinct values of countries and call it list_of countries
*I will use this list to extract data per country instead of one big dataset that generates issues
qui levelsof country, local(list_of_countries) clean

local list_of_countries CL GB ZA

foreach c in `list_of_countries' {
	
	dis "`c'"
	
	wid, indicators(aptinc tptinc adiinc tdiinc acainc tcainc ahweal thweal) perc(p99p100) areas(`c') ages(992) pop(j) exclude clear
	
	local c: subinstr local c "-" "_", all
	
	tempfile country_`c'
	save "`country_`c''"
	
}

clear

foreach c in `list_of_countries' {
	local c: subinstr local c "-" "_", all
	append using "`country_`c''"

}
