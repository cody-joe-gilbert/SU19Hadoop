#!/usr/bin/env python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Second pass profiler reducer for Hadoop MapReduce execution.
For each column key input, determines count of values within each histogram
interval and sums each variance increment.

~Assumptions~
Assumes a small number of unique string values such that the distinct set
fits within a dictionary in local memory. This reducer is not suited to
free-text entry applications.

~Input~
"key [type] Value" string where
key: Column number
type: if element is variance element "VE", if a histogram interval then "HI"
Value: variance element or histogram interval set with interval the point is in

~Output~
"key 'Variance' var" for each unique string where
key: Column number
Count: Total numeric variance

"key 'HI' HI int count" for each column
key: Column number
HI: The histogram interval set (5, 10, 30, etc.)
int: Interval within the HI set
count: the number of occurences within the HI's interval

@author: Cody Gilbert
"""

import sys
hist = {}
lastCol = ""
curVar = 0

for line in sys.stdin:
    sline = line.strip().split("\t")
    col = sline[0]
    ty = sline[1]
    if lastCol == "" or lastCol == col:
        lastCol = col
        if ty == "VI":
            curVar += float(sline[2])
        else:  # Must be a histogram count
            HI = int(sline[2])
            inter = int(sline[3])
            if HI not in hist:
                hist.update({HI: [0]*HI})
            hist[HI][inter] += 1
    else:  # lastCol != col; new column to process
        # Output last column values
        if len(hist) != 0:  # Don't output values with 0 distribution
            out = ("{col}\t".format(col=lastCol) +
                   "Var\t{v}\t".format(v=curVar))
            for HI in sorted(hist):
                out += "{hi}\t".format(hi=HI)
                for count in hist[HI]:
                    out += "{c}\t".format(c=count)
        print(out)
        # Reset Column Values
        hist = {}
        lastCol = ""
        curVar = 0
        # Read in new Column Values
        lastCol = col
        if ty == "VI":
            curVar += float(sline[2])
        else:  # Must be a histogram count
            HI = int(sline[2])
            inter = int(sline[3])
            if HI not in hist:
                hist.update({HI: [0]*HI})
            hist[HI][inter] += 1
if len(hist) != 0:  # Don't output values with 0 distribution
    out = ("{col}\t".format(col=lastCol) +
           "Var\t{v}\t".format(v=curVar))
    for HI in sorted(hist):
        out += "{hi}\t".format(hi=HI)
        for count in hist[HI]:
            out += "{c}\t".format(c=count)
    print(out)
