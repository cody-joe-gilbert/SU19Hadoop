#!/usr/bin/env python

import sys

output_cols = {
	0: 'mukey',
	3: 'muacres',
	7: 'lkey'
	} 

for line in sys.stdin:
	l = line.strip().split("|")

	for i in output_cols.keys():
		if l[i] == '':
			val = 'NULL'
		else:
			val = l[i]
		print("%s\t%s" % (output_cols[i], val))
