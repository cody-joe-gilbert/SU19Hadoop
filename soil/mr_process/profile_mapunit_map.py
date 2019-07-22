#!/usr/bin/env python

import sys

output_cols = {
	0: 'mukey',
	3: 'muacres',
	7: 'lkey'
	} 

for line in sys.stdin:
	val = line.strip()

	for i in output_cols.keys():
		print("%s\t%s" % (output_cols[i], val[i]))
