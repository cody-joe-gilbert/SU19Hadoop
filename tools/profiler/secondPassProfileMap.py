#!/usr/bin/env python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Second pass profiler mapper for Hadoop MapReduce execution.
Requires results of first-pass to be included as a pickled list of dictionaries
with data describing the first-pass results as 'fpPickle.pkl'. See
profileData.py for the description of this data sctructure.

Takes in a row of data, screens out non-numeric values, and finds the standard
deviation and histogram interval counts

~Assumptions~
This mapper assumes data is in a form of a row-by-column comma-delimited
schema that has constant form; i.e. all rows contain the same number of fields/
columns. Columns may contain mixed numeric and non-numeric data.
Columns containing no data (",," form) will be recorded as the string "NULL"

~Output~
"key [type] Value" string where
key: Column number
type: if element is variance increment "VI", if a histogram interval then "HI"
Value: variance element or histogram interval set with interval the point is in

@author: Cody Gilbert
"""

import sys
import pickle

with open('fpPickle.pkl', 'rb') as pf:
    fpResults = pickle.load(pf)

for line in sys.stdin:
    sline = line.strip().split(",")
    for i, elem in enumerate(sline):
        try:
            value = float(elem)  # If not numeric, will raise exception here
        except ValueError:  # Value is a string: skip
            continue
        # Numerical data: find VI
        mean = fpResults[i]['Mean']
        N = fpResults[i]['Count'] - 1
        if N > 0:  # Catch for < 1 data points in column
            VI = ((value - fpResults[i]['Mean'])**2)/N
            print("{key}\tVI\t{value}".format(key=i, value=VI))
        # Numerical data: find histogram points
        # Print the lowest point the data divides interval
        if 'histIntervals' in fpResults[i]:
            for HI in fpResults[i]['histIntervals']:
                interval = (fpResults[i]['Max'] - fpResults[i]['Min'])/HI
                score = int((value - fpResults[i]['Min'])//interval)
                score = min(score, HI - 1)
                print("{key}\tHI\t{HI}\t{sc}".format(key=i, HI=HI, sc=score))
