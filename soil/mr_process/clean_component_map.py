#!/usr/bin/env python

# note: data provided is very clean (no unexpected values) except there might be null values. thus cleaning process here simply involves
# 1) excluding lines with null values in required_cols (these are critical fields like primary key, foreign key, and weighting factors)
# 2) outputing only cols necessary for the analytic (these are required_cols + features)

import sys

output_cols = {
	0: 'cokey',
	2: 'mukey',
	3: 'comppct_r',
	5: 'slope_r',
	11: 'tfact',
	15: 'elev_r'
	} 

required_cols = {
	0: 'cokey',
	2: 'mukey',
	3: 'comppct_r'
	}

for line in sys.stdin:
	l = line.split("|")

	# if row is missing a required field, filter out the row  
	if '' in [l[i] for i in required_cols.keys()]:
		pass 

	# else output the cols that we want to keep 
	else:
		output_vals = [l[i] for i in output_cols.keys()]
		print("|".join(output_vals))