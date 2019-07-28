#!/usr/bin/python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Map script for findEntry.py. Given a column and value, returns the unchanged
row that contains the value and the column. If column is None, returns the
row if any entry contains the value.

~Input~
(data file row format)

~Output~
(data file row format): only for selected row

@author: Cody Gilbert
"""
import sys
import pickle
with open('runHadoopPickle.pkl', 'rb') as pf:
    pickDict = pickle.load(pf)

col = pickDict['col']
value = pickDict['value']
delim = pickDict['delim']

for line in sys.stdin:
    sline = line.strip().split(delim)
    if col is not None:
        if sline[col] == value:
            print(line)
    else:
        for i, elem in enumerate(sline):
            if elem == value:
                print(line)
