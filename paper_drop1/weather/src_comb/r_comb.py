#!/usr/bin/env python

import sys
import numpy as np


def np_arr_to_str(np_arr):
    rt_str = ''
    for cur_v in np_arr:
        rt_str += str(cur_v) + ","
    return rt_str[:-1]


def do_print(cur_key, cur_col_arr, cur_val_arr):
    to_print = "%s\t" % cur_key
    idx = np.argsort(cur_col_arr)
    sorted_val_arr = cur_val_arr[idx]
    to_print += np_arr_to_str(sorted_val_arr)
    print(to_print)


last_k = None
col_arr = np.array([])
val_arr = np.array([])

for l in sys.stdin:
    # k = orig key identifier
    k, v = l.strip().split("\t")
    cur_col, cur_val = v.split(",")

    if last_k and last_k != k:
        # deal with the full key
        do_print(k, col_arr, val_arr)

        # resetting
        col_arr = np.array([])
        val_arr = np.array([])

    # handling a new value
    col_arr = np.append(col_arr, int(cur_col))
    val_arr = np.append(val_arr, float(cur_val))

    last_k = k

if last_k:
    do_print(k, col_arr, val_arr)
