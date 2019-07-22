#!/usr/bin/env python

import sys

output_cols = {
	0: 'cokey',
	2: 'mukey',
	3: 'comppct_r',
	5: 'slope_r',
	11: 'tfact',
	15: 'elev_r'} 

for line in sys.stdin:
	l = line.strip().split("|")

	for i in output_cols.keys():
		if l[i] == '':
			val = 'NULL'
		else:
			val = l[i]
		print("%s\t%s" % (output_cols[i], val))