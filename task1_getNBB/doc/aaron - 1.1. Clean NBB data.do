*************************************************************************************
**** The Role of Intangibles in the Productivity Contribution of Superstar Firms ****
*************************************************************************************

	clear
	set more off
	
*************** DESCRIPTION *******************************************
* NBB Data cleaning
* 
* Version 1.0
* Last edit: 6/17/2022 
***********************************************************************	
	
	
	
* Loading the National Bank of Belgium Balcance sheet data (1985-2014).	
	*cd "X:\VIVES\1-Personal\Aaron\Paper 3. Superstar firms\data"
	use ".\data\rawdata\NBB_85_14.dta", clear 
	
	
	********************
	* 1. Data Cleaning *
	********************

	* Generate employment variable
	gen employment = avgemp_before1996 if year <= 1995
	replace employment = avgfte_after1996 if year >= 1996

	* Keep variables of interest
	// VAT, year, NACE (2digits & 5digits)
	// Operating revenue, value added, renumeration, employment
	// Cloture (end of bookyear) and number of months of book year
	rename renumeration remuneration
	keep vat year nace_rev2_5d_code nace_rev2_2d_code turnover operating_revenue valueadded remuneration hours_effective ///
		avgfte_after1996 avgemp_before1996 employment cloture month_nb

	** Keep renumeration > 0 and value added > 0
	** Drop rows if one of these variables are missing
	inspect remuneration
	drop if remuneration <0 
	drop if remuneration == 0
	drop if mi(remuneration)
	inspect remuneration

	inspect valueadded
	drop if valueadded == 0
	drop if mi(valueadded)
	inspect valueadded
	
	inspect hours_effective
	drop if hours_effective == 0
	drop if mi(hours_effective)
	inspect hours_effective

	** Show missing values for each variable
	mdesc

	** Obtain NACE codes
	merge m:m nace_rev2_2d_code using ".\data\rawdata\NACE_rev2.dta"
	drop if _merge == 1
	drop if _merge == 2

	** Generate NACE code for four digits level
	rename nace_rev2_section nace_1d
	rename nace_rev2_5d_code nace_5d
	rename nace_rev2_2d_code nace_2d
	gen nace_4d = trunc(nace_5d /10)

	* Save final dataset
	keep vat year nace_1d nace_2d nace_4d remuneration valueadded employment turnover operating_revenue cloture month_nb hours_effective
	order vat year nace_1d nace_2d nace_4d remuneration valueadded employment turnover operating_revenue cloture month_nb hours_effective
	gsort vat year
	save ".\data\temp\NBB_85_14_cleaned.dta", replace
	

	
	*********************************
	* 2. Adjusting broken bookyears *
	*********************************

	clear
	use ".\data\temp\NBB_85_14_cleaned.dta"

	gen valueadded_raw = valueadded
	gen remuneration_raw = remuneration
	gen employment_raw = employment
	gen turnover_raw = turnover
	gen operating_revenue_raw = operating_revenue
	gen hours_effective_raw = hours_effective

	* Obtain month number in which the bookyear closes
	gen tmp_D_CLOTURE = dofC(cloture)
	gen end_month = month(tmp_D_CLOTURE)

	* Rename variable
	rename month_nb number_months

	* Drop some observations. Adding the additional code for bookyears which spans ///
	* more than 3 calender years is not worth the effort.
	drop if number_months > 36

	* Keep only variables of interest
	*keep vat year end_month number_months valueadded

	* Check that all key observations are present
	mdesc

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
	gen monthly_va = valueadded / number_months
	gen monthly_rem = remuneration / number_months
	gen monthly_emp = employment / number_months
	gen monthly_turnov = turnover / number_months
	gen monthly_operating_revenue = operating_revenue / number_months
	gen monthly_hours_effective = hours_effective / number_months

	

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

	* Multiply the monthly value added by the corresponding numer of months
	gen va = monthly_va * number_months
	gen rem = monthly_rem * number_months
	gen emp = monthly_emp * number_months
	gen turnov = monthly_turnov * number_months
	gen op_revenue = monthly_operating_revenue * number_months
	gen hours = monthly_hours_effective * number_months

	* Collapse to one observation per vat-year combination
	gsort vat year
	by vat year: egen tot_months = sum(number_months)
	collapse (max) end_month (sum) number_months va rem emp turnov op_revenue hours (first) nace_1d ///
		nace_2d nace_4d, by(vat year)
		
	* We are not able to distinguish between 1) firms exiting in 2014 (conditional
	* on having reported a bookyear in 2014) and 2) firms which are going to report
	* a broken bookyear in 2015. Therefore, we extrapolate all variables proportionally
	* in the year 2014. This introduces an error as we give too much weigh to firms
	* which exit in 2014 and never intended to report a broken bookyear in 2015.
	* Not extrapolating would give too little weigh to the surviving firms which still
	* have to report. This error will be bigger.
	replace va = va * (12 / number_months) if number_months != 12 & year == 2014 & end_month != number_months
	replace rem = rem * (12 / number_months) if number_months != 12 & year == 2014 & end_month != number_months
	replace emp = emp * (12 / number_months) if number_months != 12 & year == 2014 & end_month != number_months
	replace turnov = turnov * (12 / number_months) if number_months != 12 & year == 2014 & end_month != number_months
	replace op_revenue = op_revenue * (12 / number_months) if number_months != 12 & year == 2014 & end_month != number_months
	replace hours = hours * (12 / number_months) if number_months != 12 & year == 2014 & end_month != number_months
	


	gsort vat year

	drop end_month number_months

	rename va valueadded
	rename rem remuneration
	rename emp employment
	rename turnov turnover
	rename op_revenue operating_revenue
	rename hours hours_effective
	order vat year nace_1d nace_2d nace_4d remuneration valueadded employment turnover operating_revenue hours_effective
	

	
	
	save ".\data\temp\NBB_85_14_cleaned_BB.dta", replace

	* Keep only positive value added observations
	drop if valueadded < 0
	save ".\data\temp\NBB_85_14_cleaned_BB_NonNegVA.dta", replace

		
		
