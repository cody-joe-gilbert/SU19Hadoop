#!/usr/bin/python
#!/usr/bin/env python

import sys

for l in sys.stdin:
    input = l.strip().split("\t")
    orig_k = input[0]
    vec_arr = input[1:]
    for i, cur_vec_val in enumerate(vec_arr):
        print("%s\t%s|%s" % (i, cur_vec_val, orig_k))
