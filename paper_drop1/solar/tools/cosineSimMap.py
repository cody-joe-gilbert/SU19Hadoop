#!/usr/bin/python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Map script for cosineSim.py. Given a record in the dataset, applies cosine
similarity to every record in the dataset.

~Input~
(data file row format)

~Output~
"key CosineSim"
key: Orignal record key
CosineSim: cosine similarity of the record to the given base record

@author: Cody Gilbert
"""
import sys
import math
import pickle
with open('runHadoopPickle.pkl', 'rb') as pf:
    pickDict = pickle.load(pf)

inputRecord = pickDict['inputRecord']
skipCols = pickDict['skipCols']
delim = pickDict['delim']
sInpRecord = inputRecord.strip().split(delim)
for line in sys.stdin:
    dotCM = 0
    lineMag = 0
    rMag = 0
    sline = line.strip().split(delim)
    for i, elem in enumerate(sline):
        if i in skipCols:
            continue
        dotCM += float(elem)*float(sInpRecord[i])
        lineMag += float(elem)**2
        rMag += float(sInpRecord[i])**2
    if rMag == 0 or lineMag == 0:
        sim = 0
    else:
        sim = dotCM / (math.sqrt(rMag) * math.sqrt(lineMag))
    print("{key}\t{cs}".format(key=sline[0], cs=sim))
