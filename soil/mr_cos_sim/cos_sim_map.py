#!/usr/bin/env python
## Compute cosine similarity of soil regions against Napa (modified from Rong's code for weather)

import sys
import numpy as np

# hardcode napa's soil features 
napa = "-0.03333754710534963,0.2542202653122082,-0.7523754331513007,-0.02452543102259914,0.3346216312202514,0.3453574861418536,-0.39268400595791675,2.3860042328498072,-1.7348325300240985,-0.16109099107405658"

# helpers 
def str_to_np(in_str):
    a = in_str.strip().split(",")
    py_list = []
    for i in a:
        py_list.append(float(i))
    return np.array(py_list)

def cos_sim(v1, v2):
    return v1.dot(v2) / np.linalg.norm(v1) / np.linalg.norm(v2)

v_napa = str_to_np(napa)

for l in sys.stdin:
    vals = l.strip().split(',')
    k = vals[0] 
    v = ",".join(vals[1:]) # convert back to string to conform to Rong's data format 
    if 'None' not in v and '\\N' not in v:
        np_v = str_to_np(v)
        sim = cos_sim(v_napa, np_v)

        print("%s|%s" % (k, sim))