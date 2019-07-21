#!/usr/bin/env python

import sys

col_order = [
    'decade',
    'stationid',
    'month',
    'max_temp',
    'min_temp',
    'avg_temp',
    'max_humid',
    'min_humid',
    'avg_humid',
    'max_ws',
    'min_ws',
    'avg_ws',
    'max_p1',
    'min_p1',
    'avg_p1',
    'max_p6',
    'min_p6',
    'avg_p6'
]

for l in sys.stdin:
    a = l.strip().split(",")
    cur_fields = dict(zip(col_order, a))
    to_print = "%s,%s\t" % (cur_fields['decade'], cur_fields['stationid'])
    for cur_col in col_order[2:]:
        to_print += cur_fields[cur_col] + ","
    print(to_print[:-1])


