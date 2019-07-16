#!/usr/bin/env python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

First pass profiler mapper for Hadoop MapReduce execution.
Takes in a row of data, and produces string counts
of each non-numeric value, and determines the extrema of numeric data. This
info sets up the second pass mapper that will determine the distribution of the
numeric data.

~Assumptions~
This mapper assumes data is in a form of a row-by-column comma-delimited
schema that has constant form; i.e. all rows contain the same number of fields/
columns. Columns may contain mixed numeric and non-numeric data.
Columns containing no data (",," form) will be recorded as the string "NULL"

~Output~
"key [type] Value" string where
key: Column number
type: if element is a string "S", if numeric "N", not included if NULL
Value: numeric value if numeric, string value if string, "NULL" if null.

@author: Cody Gilbert
"""

import sys
for line in sys.stdin:
    sline = line.strip().split(",")
    for i, elem in enumerate(sline):
        # Check if it's a missing value
        if elem == "":
            print("{key}\t{value}".format(key=i, value="NULL"))
            continue
        # Check if it's numeric
        try:
            value = float(elem)  # If not numeric, will raise exception here
            print("{key}\tN\t{value}".format(key=i, value=elem))
        except ValueError:  # Value is a string
            print("{key}\tS\t{value}".format(key=i, value=elem))







