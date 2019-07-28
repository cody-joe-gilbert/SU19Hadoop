#!/usr/bin/env python

import sys

for l in sys.stdin:
    orig_key, cur_col, cur_val = l.strip().split("\t")
    print("%s\t%s,%s" % (orig_key, cur_col, cur_val))


