#!/usr/bin/python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Second pass normalizer mapper for Hadoop MapReduce execution.
Takes in a row of data with an associated dictionary of {column: [mean, stdev]}
values used to standardixe the values of each column. Columns not in the
dictionary are skipped and values unchanged.

~Assumptions~
This mapper assumes data is in a form of a row-by-column comma-delimited
schema that has constant form; i.e. all rows contain the same number of fields/
columns. Columns not skipped may not contain non-numeric or NULL values.

~Output~
Same schema as input, with values not skipped standardized.

@author: Cody Gilbert
"""

import sys
import pickle
with open('runHadoopPickle.pkl', 'rb') as pf:
    pickDict = pickle.load(pf)

stats = pickDict['stats']
delim = pickDict['delim']

for line in sys.stdin:
    out = ""
    sline = line.strip().split(delim)
    for i, elem in enumerate(sline):
        if i not in stats:
            out += elem + "\t"
            continue
        value = float(elem)  # If not numeric, will raise exception here
        stdval = (value - stats[i][0])/stats[i][1]
        out += str(stdval) + "\t"
    print(out)


