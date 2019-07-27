#!/usr/bin/env python

import sys

for l in sys.stdin:
    orig_k, vector = l.strip().split("\t")
    vec_arr = vector.split(",")
    for i, cur_vec_val in enumerate(vec_arr):
        print("%s\t%s\t%s" % (i, cur_vec_val, orig_k))
