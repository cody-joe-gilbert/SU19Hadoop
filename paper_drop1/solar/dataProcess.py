# -*- coding: utf-8 -*-
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Submits MapReduce jobs to pool solar data by months and return the max GHI and
monthly average GHI for each month as columns and by location in rows

e.g.        Jan_maxGHI Jan_avgGHI  Feb_maxGHI Feb_avgGHI ....
lat-lon_1
Lat-Lon_2
...

This data will be used to calculate difference measures between a given point
and the rest of the data. 

After featurizing and standardizing the data, this script
will then find the Napa Valley record and create a cosine similarity
measure between Napa and every other record, producing a single
set of values for merging together for a single regional index.


@author: Cody Gilbert
"""

import HadoopTools as ht
import subprocess
from findEntry import findEntry
from cosineSim import cosineSim

schema = {1: "Year",
          2: "Month",
          3: "Day",
          4: "Hour",
          5: "Minute",
          6: "GHI",
          7: "DHI",
          8: "DNI",
          9: "Wind Speed",
          10: "Temperature",
          11: "Solar Zenith Angle",
          12: "Source",
          13: "Location ID",
          14: "City",
          15: "State",
          16: "Country",
          17: "Latitude",
          18: "Longitude",
          19: "Time Zone",
          20: "Elevation",
          21: "Local Time Zone",
          22: "Clearsky DHI Units",
          23: "Clearsky DNI Units",
          24: "Clearsky GHI Units",
          25: "Dew Point Units",
          26: "DHI Units",
          27: "DNI Units",
          28: "GHI Units",
          29: "Solar Zenith Angle Units",
          30: "Temperature Units",
          31: "Pressure Units",
          32: "Relative Humidity Units",
          33: "Precipitable Water Units",
          34: "Wind Direction Units",
          35: "Cloud Type -15",
          36: "Cloud Type 0",
          37: "Cloud Type 1",
          38: "Cloud Type 2",
          39: "Cloud Type 3",
          40: "Cloud Type 4",
          41: "Cloud Type 5",
          42: "Cloud Type 6",
          43: "Cloud Type 7",
          44: "Cloud Type 8",
          45: "Cloud Type 9",
          46: "Cloud Type 10",
          47: "Cloud Type 11",
          48: "Cloud Type 12",
          49: "Fill Flag 0",
          50: "Fill Flag 1",
          51: "Fill Flag 2",
          52: "Fill Flag 3",
          53: "Fill Flag 4",
          54: "Fill Flag 5",
          55: "Surface Albedo Units",
          56: "Version"}

months = {1: "JAN",
          2: "FEB",
          3: "MAR",
          4: "APR",
          5: "MAY",
          6: "JUN",
          7: "JUL",
          8: "AUG",
          9: "SEP",
          10: "OCT",
          11: "NOV",
          12: "DEC"}

# 1. Submit job to reformat into wide form as shown above
runner = ht.runHadoop()
runner.verbose = False

# Run first-pass Widen MapReduce execution
inputFile = '/user/cjg507/solarData.csv'
widenMapScript = './widenDataMap.py'
widenRedScript = './widenDataRed.py'
runner = ht.runHadoop()
runner.outputLogFile = './widendata.log'
runner.include('schema', schema)
runner.include('months', months)
runner.MapReduce(inpFile=inputFile,
                mapper=widenMapScript,
                reducer=widenRedScript,
                outFile='widenedSolarData',
                NumReducers=32)
runner.tail()

# Standardize the data
# Run first-pass MapReduce standardizer execution
# Broken from standard form to suppose shell script passing
inputFile = 'widenedSolarData'
MapScript = 'src_norm/m_norm.sh'
RedScript = 'src_norm/r_norm.sh'
folderFile = 'src_norm/'
outFile='standardSolarData'
outputLogFile = './standardData.log'
hLibPath = '/opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib'
mrStreaming = hLibPath + '/hadoop-mapreduce/hadoop-streaming.jar'
args = ['hadoop jar ' + mrStreaming +
                ' -Dmapreduce.job.reduces=' + "32" +
                ' -files ' + folderFile +
                ' -input ' + inputFile +
                ' -output ' + outFile +
                ' -mapper ' + MapScript +
                ' -reducer ' + RedScript]
subprocess.check_call(args, shell=True)

# # Run second-pass MapReduce execution to combine standard data
inputFile = 'widenedSolarData'
MapScript = 'src_norm/m_norm.sh'
RedScript = 'src_norm/r_norm.sh'
folderFile = 'src_norm/'
outFile='standardSolarData'
outputLogFile = './standardData.log'
hLibPath = '/opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib'
mrStreaming = hLibPath + '/hadoop-mapreduce/hadoop-streaming.jar'
args = ['hadoop jar ' + mrStreaming +
                ' -Dmapreduce.job.reduces=' + "32" +
                ' -files ' + folderFile +
                ' -input ' + inputFile +
                ' -output ' + outFile +
                ' -mapper ' + MapScript +
                ' -reducer ' + RedScript]
subprocess.check_call(args, shell=True)

# Find the Napa Valley Region
findEntry('38.490:-122.340', 'standardCombSolarData', col=0, delim='\t')

napaRecord = ""

# Run cosine sim measure
cosineSim(napaRecord, 'standardCombSolarData')

# Final code is in the /user/cjg507/cosineSim folder




