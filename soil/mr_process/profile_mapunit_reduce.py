#!/usr/bin/env python

import sys

# initialize variables 
cur_field, cur_min, cur_max, cur_null = None, 99999, -99999, 0

# process each key/value pair from map output 
for line in sys.stdin: 
	l = line.strip().split("\t")
	field, val = l[0], l[1]

	# if incoming field is the current field, update summary statistics 
	if field == cur_field: 
		if val == '':
			cur_null += 1 
		else: 
			cur_min = min(cur_min, float(val))
			cur_max = max(cur_max, float(val))

	# else it must be a new field, and we must have finished the previous field 
	else: 
		# assuming current field is not just initial None, print results of previous field 
		if cur_field is not None: 
			print("%s\t%s\t%s\t%s" % (cur_field, cur_min, cur_max, cur_null))
		# swap new field as current and update its summary statistics  
		cur_field = field 
		if val == '':
			cur_null = 1 
		else: 
			cur_null = 0
			cur_min, cur_max = float(val), float(val) 

# handle the last key 
print("%s\t%s\t%s\t%s" % (cur_field, cur_min, cur_max, cur_null))