#!/usr/bin/python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

First pass normalizer reducer for Hadoop MapReduce execution.
For each column key input, creates the normalization weights

~Input~
"key Value" firstPassMapNormalizer mapper output string where
key: Column number
Value: numeric value

~Output~
"key mean stdev" for each column
key: Column number
mean: mean value of the column
stdev: sample standard deviation of the column (1 dof)

@author: Cody Gilbert
"""

import sys
import math
lastCol = ""
CMSum = 0
CMSum2 = 0
NCount = 0
for line in sys.stdin:
    sline = line.strip().split("\t")
    col = sline[0]
    if lastCol == "":
        # First element found
        lastCol = col
        n = float(sline[1])
        CMSum += n  # Cumulative Sum
        CMSum2 += n**2  # Cumulative Sum Squared
        NCount += 1  # Number of entries
    elif lastCol == col:
        # Still working on the same column
        n = float(sline[1])
        CMSum += n
        CMSum2 += n**2
        NCount += 1
    else:  # lastCol != col; new column to process
        # Output last column values
        mean = CMSum/NCount
        stdev = math.sqrt((CMSum2 - 2*CMSum*mean + NCount*mean**2)/(NCount - 1))
        print("{col}\t".format(col=lastCol) +
              "{mean}\t".format(mean=mean) +
              "{stdev}".format(stdev=stdev))
        # Reset Column Values
        CMSum = 0
        CMSum2 = 0
        NCount = 0
        # Read in new Column Values
        lastCol = col
        n = float(sline[1])
        CMSum += n  # Cumulative Sum
        CMSum2 += n**2  # Cumulative Sum Squared
        NCount += 1  # Number of entries
# Print out the last column values
if NCount > 0:
    mean = CMSum/NCount
    stdev = (CMSum2 - 2*CMSum*mean + NCount*mean**2)/(NCount - 1)
    print("{col}\t".format(col=lastCol) +
          "{mean}\t".format(mean=mean) +
          "{stdev}".format(stdev=stdev))
