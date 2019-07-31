#!/usr/bin/python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

First pass normalizer mapper for Hadoop MapReduce execution.
Takes in a row of data, determines the extrema of numeric data, and  This
info sets up the second pass mapper that will determine the distribution of the
numeric data.

~Assumptions~
This mapper assumes data is in a form of a row-by-column comma-delimited
schema that has constant form; i.e. all rows contain the same number of fields/
columns. Columns not skipped may not contain non-numeric or NULL values.

~Output~
"key  Value" string where
key: Column number
Value: numeric value

@author: Cody Gilbert
"""

import sys
import pickle
with open('runHadoopPickle.pkl', 'rb') as pf:
    pickDict = pickle.load(pf)

skipCols = pickDict['skipCols']
delim = pickDict['delim']

for line in sys.stdin:
    sline = line.strip().split(delim)
    for i, elem in enumerate(sline):
        if i in skipCols:
            continue
        value = float(elem)  # If not numeric, will raise exception here
        print("{key}\t{value}".format(key=i, value=elem))


