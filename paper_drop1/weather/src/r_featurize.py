#!/usr/bin/env python

import sys

col_order = [
    'decade',
    'stationid',
    'month',
    'max_temp',
    'min_temp',
    'avg_temp',
    'qq_range_temp',
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
    'avg_p6',
]

out_order = [
    'avg_temp',
    'qq_range_temp',
    'avg_humid',
    'avg_ws',
    'avg_p1',
    'avg_p6',
]

month_str_order = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']

# init
last_k = None
res_dict = {}


def do_print(key, results_dict):
    try:
        to_print = key + "\t"
        for m in month_str_order:
            for ofield in out_order:
                to_print += str(results_dict[m][ofield]) + ","
        if '\\N' not in to_print and 'None' not in to_print:
            print(to_print[:-1])
    except:
        pass


for l in sys.stdin:
    k, v = l.strip().split("\t")
    a = v.split(",")

    try:
        # load fields
        cur_fields = dict(zip(col_order[2:], a))

        # new k branche
        if last_k and last_k != k:
            do_print(last_k, res_dict)

            # resetting, making sure the full dict is initialized
            res_dict = {}
            for cur_m in month_str_order:
                res_dict[cur_m] = {}
                for cur_ofield in out_order:
                    res_dict[cur_m][cur_ofield] = None

        # handling a new value
        res_dict[cur_fields['month']] = {}
        for cur_ofield in out_order:
            res_dict[cur_fields['month']][cur_ofield] = cur_fields[cur_ofield]
    except:
        pass

    last_k = k

if last_k:
    do_print(last_k, res_dict)
