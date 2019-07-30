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

	# strip stdin and split into a list of column fields 
	l = line.strip().split("|")

	# check that all required fields are present
	if '' not in [l[i] for i in required_cols.keys()]:
		# if so, print output fields to std_out  
		output_vals = [l[i] for i in output_cols.keys()]
		print("|".join(output_vals))