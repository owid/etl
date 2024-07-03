global areas CL GB ZA

foreach a in $areas {
	
	wid, indicators(aptinc tptinc adiinc tdiinc acainc tcainc ahweal thweal) perc(p0p10 p10p20 p20p30 p30p40 p40p50 p50p60 p60p70 p70p80 p80p90 p90p100 p0p100 p0p50 p99p100 p99.9p100 p99.99p100 p99.999p100) areas(`a') ages(992) pop(j) exclude clear
	
	tempfile country_`a'
	save "`country_`a''"
	
}

clear

foreach a in $areas {
	append using "`country_`a''"

}
