#!/usr/bin/env python

import sys

output_cols = {
	0: 'chkey', 
	1: 'cokey',
	2: 'hzdept_r',
	3: 'hzdepb_r',
	12: 'sandtotal_r',
	18: 'silttotal_r',
	21: 'claytotal_r',
	23: 'om_r',
	24: 'awc_r',
	35: 'cec7_r',
	38: 'ph1to1h2o_r'} 

required_cols = {
	0: 'chkey', 
	1: 'cokey',
	2: 'hzdept_r',
	3: 'hzdepb_r'
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