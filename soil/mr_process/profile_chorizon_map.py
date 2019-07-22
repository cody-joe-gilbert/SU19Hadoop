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

for line in sys.stdin:
	l = line.strip().split("|")

	for i in output_cols.keys():
		if l[i] == '':
			val = 'NULL'
		else:
			val = l[i]
		print("%s\t%s" % (output_cols[i], val))