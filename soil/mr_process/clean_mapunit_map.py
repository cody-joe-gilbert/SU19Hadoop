#!/usr/bin/env python

import sys

output_cols = {
	0: 'mukey',
	3: 'muacres',
	7: 'lkey'
	} 

required_cols = {
	0: 'mukey',
	3: 'muacres',
	7: 'lkey'
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