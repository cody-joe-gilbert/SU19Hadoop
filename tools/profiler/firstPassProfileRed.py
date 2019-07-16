#!/usr/bin/env python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

First pass profiler reducer for Hadoop MapReduce execution.
For each column key input, determines the extrema of numeric values and the
the count of non-numeric strings.

~Assumptions~
Assumes a small number of unique string values such that the distinct set
fits within a dictionary in local memory. This reducer is not suited to
free-text entry applications.

~Input~
"key [type] Value" firstPassProfileMap mapper output string where
key: Column number
type: if element is a string "S", if numeric "N", not included if NULL
Value: numeric value if numeric, string value if string, "NULL" if null

~Output~
"key 'String' Count" for each unique string where
key: Column number
Count: Number of string occurances

"key 'Min' minValue 'Max' maxValue" for each column
key: Column number
minValue: Minimum numeric value
maxValue: Maximum numeric value

"key 'NULL' Count" for each NULL
key: Column number
Count: Number of null occurances


@author: Cody Gilbert
"""

import sys
stringCount = {}
nullCount = 0
lastCol = ""
curMin = None
curMax = None
CMSum = 0
NCount = 0
for line in sys.stdin:
    sline = line.strip().split("\t")
    col = sline[0]
    ty = sline[1]
    if lastCol == "":
        # First element found
        lastCol = col
        if ty == "NULL":
            nullCount += 1
        elif ty == "N":
            n = float(sline[2])
            curMin = n  # Numeric Minimum
            curMax = n  # Numeric Maximum
            CMSum += n  # Cumulative Sum
            NCount += 1  # Number of entries
        else:  # Must be a string
            stringCount.update({sline[2]: 1})
    elif lastCol == col:
        # Still working on the same column
        if ty == "NULL":
            nullCount += 1
        elif ty == "N":
            n = float(sline[2])
            if n > curMax:
                curMax = n
            if n < curMin:
                curMin = n
            CMSum += n
            NCount += 1
        else:  # Must be a string
            s = sline[2]
            if s in stringCount:
                stringCount.update({s: stringCount[s] + 1})
            else:
                stringCount.update({s: 1})
    else:  # lastCol != col; new column to process
        # Output last column values
        if curMin is not None:
            # At least one numeric value was found
            print("{col}\t".format(col=lastCol) +
                  "Min\t{mini}\t".format(mini=curMin) +
                  "Max\t{maxi}\t".format(maxi=curMax) +
                  "CMSum\t{cs}\t".format(cs=CMSum) +
                  "Count\t{n}\t".format(n=NCount))
        if len(stringCount) != 0:
            for s in stringCount:
                print("{col}\t".format(col=lastCol) +
                      "{st}\t".format(st=s) +
                      "{N}".format(N=stringCount[s]))
        if nullCount > 0:
            print("{col}\t".format(col=lastCol) +
                  "NULL\t{N}".format(N=nullCount))
        # Reset Column Values
        curMin = None
        CurMax = None
        stringCount = {}
        nullCount = 0
        CMSum = 0
        NCount = 0
        # Read in new Column Values
        # First element found
        lastCol = col
        if ty == "NULL":
            nullCount += 1
        elif ty == "N":
            n = float(sline[2])
            curMin = n  # Numeric Minimum
            curMax = n  # Numeric Maximum
            CMSum += n  # Cumulative Sum
            NCount += 1  # Number of entries
        else:  # Must be a string
            stringCount.update({sline[2]: 1})
# Print out the last column values
if curMin is not None:
    # At least one numeric value was found
    print("{col}\t".format(col=lastCol) +
          "Min\t{mini}\t".format(mini=curMin) +
          "Max\t{maxi}\t".format(maxi=curMax) +
          "CMSum\t{cs}\t".format(cs=CMSum) +
          "Count\t{n}\t".format(n=NCount))
if len(stringCount) != 0:
    for s in stringCount:
        print("{col}\t".format(col=lastCol) +
              "{st}\t".format(st=s) +
              "{N}".format(N=stringCount[s]))
if nullCount > 0:
    print("{col}\t".format(col=lastCol) +
          "NULL\t{N}".format(N=nullCount))
