#!/usr/bin/env python

import sys
import numpy as np


# helpers
def np_arr_to_str(np_arr):
    rt_str = ''
    for cur_v in np_arr:
        rt_str += str(cur_v) + ","
    return rt_str[:-1]


def do_print(cur_col, orig_key, norm_arr):
    for cur_val in norm_arr:
        print("%s, %s, %s" % (orig_key, cur_col, cur_val))


last_k = None
all_current_vals = np.array([])
orig_key = None
norm_arr = None

for l in sys.stdin:
    # k = column number
    k, v = l.strip().split("\t")
    val, orig_key = v.split(",")

    if last_k and last_k != k:
        # deal with the full key
        norm_arr = (all_current_vals - np.mean(all_current_vals)) / np.std(all_current_vals)
        do_print(k, orig_key, norm_arr)

        # resetting
        all_current_vals = np.array([])

    # handling a new value
    all_current_vals = np.append(all_current_vals, val)

    last_k = k

if last_k:
    # print last key
    norm_arr = (all_current_vals - np.mean(all_current_vals)) / np.std(all_current_vals)
    do_print(last_k, orig_key, norm_arr)
