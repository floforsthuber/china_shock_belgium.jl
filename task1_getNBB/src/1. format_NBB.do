
	clear all
	set varabbrev off // to use variable names like log_
	set more off // for the code to run through without breaks

	global dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

*-------------------------------------------------------------------------------
* 1. Import NBB dataset
*-------------------------------------------------------------------------------

	use "$dir/task1_getNBB/input/NBB_85_14.dta", clear

*-------------------------------------------------------------------------------
* 2. Data Cleaning
*-------------------------------------------------------------------------------

	* Generate employment variable
	gen employment = avgemp_before1996 if year <= 1995
	replace employment = avgfte_after1996 if year >= 1996
	
	* Keep variables of interest
	// VAT, year, NACE (2digits & 5digits)
	// Operating revenue, value added, renumeration, employment
	// Cloture (end of bookyear) and number of months of book year
	// materials, material_inputs, services_inputs 
	rename renumeration wagebill
	rename cloture closure
	rename month_nb number_months
	
	keep vat year nace_rev2_5d_code nace_rev2_2d_code turnover operating_revenue ///
		valueadded wagebill hours_effective closure number_months materials ///
		material_inputs services_inputs


	* Obtain NACE codes
	merge m:m nace_rev2_2d_code using "$dir/task1_getNBB/input/NACE_rev2.dta"
	keep if _merge == 3

	* Generate NACE code for four digits level
	rename nace_rev2_section nace_1d
	rename nace_rev2_5d_code nace_5d
	rename nace_rev2_2d_code nace_2d
	gen nace_4d = trunc(nace_5d / 10)
	
	
*-------------------------------------------------------------------------------
* 3. Adjusting broken bookyears: flow variables
*	- simply adjust via averaging across the book year (i.e. number_months)
*-------------------------------------------------------------------------------

	foreach series in valueadded wagebill turnover operating_revenue ///
						hours_effective materials material_inputs services_inputs {

		gen `series'_raw = `series'

	}

	
	* Obtain month number in which the bookyear closes
	gen tmp_d_closure = dofC(closure)
	gen end_month = month(tmp_d_closure)

	* Drop some observations. Adding the additional code for bookyears spanning ///
	* more than 3 calender years is not worth the effort.
	drop if number_months > 36

	* Calculate how many duplications we need.
	* E.g., if the bookyear spans two years, then we duplicate once.
	gen number_duplication = end_month - number_months
	replace number_duplication = floor(number_duplication / 12)
	expand abs(number_duplication)+1 if number_duplication < 0

	* Give a unique id to each duplicated row
	gsort vat year
	by vat year: gen counter = _n - 1

	* Calculate which row is the 'last duplicated' one.
	* We use this below in the if conditions.
	by vat year: egen max_counter = max(counter)

	* Calculate the monthly observations.
	foreach series in valueadded wagebill turnover operating_revenue ///
						hours_effective materials material_inputs services_inputs {

		gen monthly_`series' = `series' / number_months

	}

	
	* Split a row in two rows if the bookyear spans two years
	replace number_months = number_months - end_month if counter == 1
	replace end_month = 12 if counter == 1
	replace number_months = end_month if counter == 0
	replace year = year - 1 if counter == 1
	gsort vat year counter

	* Split a row in three rows if the bookyear spans three years
	replace year = year - 2 if counter == 2
	gsort year counter
	replace number_months = 12 if counter > 0 & counter < max_counter
	replace number_months = number_months - 12 - end_month if counter == 2 & max_counter == 2
	replace end_month = 12 if counter == 2
	
	* Multiply the monthly value added by the corresponding number of months
	foreach series in valueadded wagebill turnover operating_revenue ///
						hours_effective materials material_inputs services_inputs {
		
		drop `series'
		gen `series' = monthly_`series' * number_months

	}

	* Collapse to one observation per vat-year combination
	gsort vat year
	by vat year: egen tot_months = sum(number_months)
	collapse (max) end_month ///
			 (sum) number_months valueadded wagebill turnover operating_revenue ///
						hours_effective materials material_inputs services_inputs ///
			 (first) nace_1d nace_2d nace_4d nace_5d, by(vat year)
	
	
	* We are not able to distinguish between 1) firms exiting in 2014 (conditional
	* on having reported a bookyear in 2014) and 2) firms which are going to report
	* a broken bookyear in 2015. Therefore, we extrapolate all variables proportionally
	* in the year 2014. This introduces an error as we give too much weigh to firms
	* which exit in 2014 and never intended to report a broken bookyear in 2015.
	* Not extrapolating would give too little weigh to the surviving firms which still
	* have to report. This error will be bigger.
	foreach series in valueadded wagebill turnover operating_revenue ///
						hours_effective materials material_inputs services_inputs {
		
		replace `series' = `series' * (12 / number_months) ///
				if number_months != 12 & year == 2014 & end_month != number_months

	}	
	
	* Formatting
	gsort vat year
	
	keep vat year nace_1d nace_2d nace_4d nace_5d /// 
			valueadded wagebill turnover operating_revenue ///
			hours_effective materials material_inputs services_inputs
	
	save "$dir/task1_getNBB/temp/NBB_flow_variables.dta", replace
	
	
*-------------------------------------------------------------------------------
* 4. Stock variables
*	- merge with broken book year adjusted dataset
*-------------------------------------------------------------------------------

	use "$dir/task1_getNBB/input/NBB_85_14.dta", clear

	* Generate employment variable
	gen employment = avgemp_before1996 if year <= 1995
	replace employment = avgfte_after1996 if year >= 1996
	
	keep vat year employment depreciation totalassets totalfixedassets ///
			tangiblefixedassets ownequity debtless1y tradedebt
			
	save "$dir/task1_getNBB/temp/NBB_stock_variables.dta", replace
	
	
*-------------------------------------------------------------------------------
* 5. Final dataset
*	- merge on (vat, year): NBB_flow_variables.dta and NBB_stock_variables.dta
*-------------------------------------------------------------------------------

	use "$dir/task1_getNBB/temp/NBB_flow_variables.dta", clear
	
	merge m:m vat year using "$dir/task1_getNBB/temp/NBB_stock_variables.dta"
	keep if _merge == 3
	
	* check for any duplicates
	sort vat year
    quietly by vat year:  gen dup = cond(_N==1,0,_n)
	tab dup
	drop if dup > 0
	
	save "$dir/task1_getNBB/temp/NBB_bookyear_adj", replace

	
*-------------------------------------------------------------------------------
