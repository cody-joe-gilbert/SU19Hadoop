#!/usr/bin/env python
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Performs exploratory data analysis on a given HDFS data file using Hadoop
MapReduce. Generates statistics, unique string counts, and histogram plots.

~Instructions~
1. Ensure requirements below are met.
2. Set input parameters as specified below
3. Execute with '$ python profileData.py'
4. On successful execution, examine the result files specified below.

~Requirements~
This mapper assumes data is in a form of a row-by-column comma-delimited
schema that has constant form; i.e. all rows contain the same number of fields/
columns. Columns may contain mixed numeric and non-numeric data.
Columns containing no data (",," form) will be recorded as the string "NULL"

The following MapReduce scripts are available:
HadoopTools.py: Module containing Hadoop wrapper functions
firstPassProfileMap.py: first-pass profiler mapper script for Hadoop MapReduce
firstPassProfileRed.py: first-pass profiler reducer script for Hadoop MapReduce
secondPassProfileMap.py: second-pass profiler mapper script for Hadoop MapReduce
secondPassProfileRed.py: second-pass profiler reducer script for Hadoop MapReduce

Python Version 2.7 or higher must be used

~Inputs~
Set the following parameters below prior to execution:

resultsFolder: path to folder containing results
inputFile: HDFS path to input file
fpMapScript: file path to first-pass profiler mapper firstPassProfileMap.py
fpRedScript: file path to first-pass profiler reducer firstPassProfileRed.py
spMapScript: file path to second-pass profiler mapper secondPassProfileMap.py
spRedScript: file path to second-pass profiler reducer secondPassProfileRed.py
colNames: List of data column names, or None to use 'Column X' notation
histIntervals: number of bins between the maximum and minimum numeric values

~Outputs~
The resultsFolder will contain the following summary files:

profileResults.txt: text summary of data set
profilePlotNum.pdf: plots including histograms of the numerical data fields
profilePlotStrings.pdf: plots of unique string counts per column

@author: Cody Gilbert
"""

import pickle
import math
import sys
import matplotlib
import HadoopTools as ht
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt


# Set the filename in HDFS
resultsFolder = '/scratch/cjg507/profiling/'
inputFile = 'solarData.csv'
fpMapScript = './firstPassProfileMap.py'
fpRedScript = './firstPassProfileRed.py'
spMapScript = './secondPassProfileMap.py'
spRedScript = './secondPassProfileRed.py'
colNames = ["Year",
            "Month",
            "Day",
            "Hour",
            "Minute",
            "GHI",
            "DHI",
            "DNI",
            "Wind Speed",
            "Temperature",
            "Solar Zenith Angle",
            "Source",
            "Location ID",
            "City",
            "State",
            "Country",
            "Latitude",
            "Longitude",
            "Time Zone",
            "Elevation",
            "Local Time Zone",
            "Clearsky DHI Units",
            "Clearsky DNI Units",
            "Clearsky GHI Units",
            "Dew Point Units",
            "DHI Units",
            "DNI Units",
            "GHI Units",
            "Solar Zenith Angle Units",
            "Temperature Units",
            "Pressure Units",
            "Relative Humidity Units",
            "Precipitable Water Units",
            "Wind Direction Units",
            "Cloud Type -15",
            "Cloud Type 0",
            "Cloud Type 1",
            "Cloud Type 2",
            "Cloud Type 3",
            "Cloud Type 4",
            "Cloud Type 5",
            "Cloud Type 6",
            "Cloud Type 7",
            "Cloud Type 8",
            "Cloud Type 9",
            "Cloud Type 10",
            "Cloud Type 11",
            "Cloud Type 12",
            "Fill Flag 0",
            "Fill Flag 1",
            "Fill Flag 2",
            "Fill Flag 3",
            "Fill Flag 4",
            "Fill Flag 5",
            "Surface Albedo Units",
            "Version"]
histIntervals = [5, 10, 20, 30]


# Some operations need Python 2.7 to run without errors
if float(str(sys.version_info[0]) + "." + str(sys.version_info[1])) < 2.7:
    message = ("Must be using Python 2.7 or above. \n" +
               "For Dumbo users, execute \"module load python/gnu/2.7.11\"")
    raise Exception(message)

# Run first-pass MapReduce execution
runner = ht.runHadoop()
runner.outputLogFile = './firstpass.log'
runner.MapReduce(inpFile=inputFile,
                 mapper=fpMapScript,
                 reducer=fpRedScript,
                 outFile='firstpassprofile',
                 NumReducers=32)
runner.poolResults(resultsFolder + 'fpresults.txt')

# Read in results of first pass
resData = []  # List contains a dict of summary data for each columns
with open(resultsFolder + 'fpresults.txt', 'r') as fprFile:
    for line in fprFile:
        sline = line.strip().split('\t')
        col = int(sline[0])
        while col > len(resData) - 1:
            resData.append({})
        if sline[1] == 'Min':
            # Numerical results input line
            cMin = float(sline[2])
            cMax = float(sline[4])
            cCMSum = float(sline[6])
            cCount = float(sline[8])
            resData[col].update({'Min': cMin,
                                 'Max': cMax,
                                 'CMSum': cCMSum,
                                 'Count': cCount,
                                 'Mean': cCMSum / cCount})
        elif sline[1] == 'NULL':
            resData[col].update({'NULL': int(sline[2])})
        else:  # A string count is given
            if 'Strings' not in resData[col]:
                resData[col].update({'Strings': {}})
            resData[col]['Strings'].update({sline[1]: int(sline[2])})

# Add settings to the resData struct
for i, col in enumerate(resData):
    # Setup the histogram intervals in results with numerical quantities
    if ('Min' in col) and (col['Min'] != col['Max']):
        resData[i].update({'histIntervals': histIntervals})
    else:  # No distribution; set to 0 and forget
        resData[i].update({'Var': 0.0, 'stdev': 0.0})
    if colNames is not None:
        resData[i].update({'Name': colNames[i]})
    else:
        resData[i].update({'Name': 'Column {i}'.format(i=i)})
# Pickle the resData struct for use in mappers
with open('fpPickle.pkl', 'wb') as pf:
    pickle.dump(resData, pf)

# Run second-pass MapReduce execution
runner = ht.runHadoop()
runner.outputLogFile = './secondpass.log'
runner.files += './fpPickle.pkl,'
runner.MapReduce(inpFile=inputFile,
                 mapper=spMapScript,
                 reducer=spRedScript,
                 outFile='secondpassprofile',
                 NumReducers=32)
runner.poolResults(resultsFolder + 'spresults.txt')

# Read in results of second pass
with open(resultsFolder + 'spresults.txt', 'r') as sprFile:
    for line in sprFile:
        sline = line.strip().split('\t')
        col = int(sline[0])
        Var = float(sline[2])
        stdev = math.sqrt(Var)
        resData[col].update({'Var': Var, 'stdev': stdev})
        HI = int(sline[3])
        if 'histData' not in resData[col]:
            resData[col].update({'histData': {}})
        resData[col]['histData'].update({HI: [0]*HI})
        i, end = 4, 4 + HI - 1
        while i < len(sline):
            if i > end:
                HI = int(sline[i])
                resData[col]['histData'].update({HI: [0]*HI})
                end = i + HI
            else:
                ir = i - (end - HI) - 1
                resData[col]['histData'][HI][ir] = int(sline[i])
            i += 1

# Print results
with open(resultsFolder + 'profileResults.txt', 'w') as res:
    for i, col in enumerate(resData):
        res.write('*************************\n')
        res.write('*************************\n')
        mes = '~~Results for {name}~~ \n'.format(name=col['Name'])
        res.write(mes)

        # For each column, print applicable data:
        if 'Min' in col:
            res.write('\nNumerical results:\n')
            res.write('Min: {v}\n'.format(v=col['Min']))
            res.write('Max: {v}\n'.format(v=col['Max']))
            res.write('Mean: {v}\n'.format(v=col['Mean']))
            res.write('Variance: {v}\n'.format(v=round(col['Var'], 3)))
            res.write('Standard Dev: {v}\n'.format(v=round(col['stdev'], 3)))
        else:
            res.write('No numeric values.\n')
        if 'Strings' in col:
            res.write('\nSting Counts: \n')
            for s in col['Strings']:
                c = col['Strings'][s]
                res.write('{s} \t\t {c}\n'.format(s=s, c=c))
        else:
            res.write('\n No strings found \n')
        if 'NULL' in col:
            res.write('\nNo. of Nulls: {c}\n'.format(c=col['NULL']))
        else:
            res.write('\nNo. of Nulls: 0\n')

# Plot figures as appropriate
plt.ioff()
with PdfPages(resultsFolder + 'profilePlotNum.pdf') as pdf:
    for i, col in enumerate(resData):
        if col['Var'] != 0.0:
            for HI in sorted(col['histData']):
                dx = (col['Max'] - col['Min'])/HI
                x = [col['Min'] + dx/2]
                for i in range(1, HI):
                    x.append(x[-1] + dx)
                fig, ax = plt.subplots()
                bars = ax.bar(x, col['histData'][HI], dx, align='center')
                ax.set_ylabel('Number')
                ax.set_title('{name} by {hi} Bins'.format(name=col['Name'],
                                                          hi=HI))
                ax.set_xlim(min(x) - 2, max(x) + 2)
                ax.set_ylim(None, max(col['histData'][HI]) + 2)
                plt.grid(True)
                plt.savefig(pdf, format='pdf')
                plt.close(fig)

with PdfPages(resultsFolder + 'profilePlotStrings.pdf') as pdf:
    for i, col in enumerate(resData):
        if 'Strings' in col:
            y = []
            label = []
            for s, c in sorted(col['Strings'].items(),
                               key=lambda kv: (kv[1], kv[0])):
                y.append(c)
                label.append(s)
            dx = 0.8
            x = [x + 0.5 for x in range(len(col['Strings']))]
            fig, ax = plt.subplots()
            bars = ax.bar(x, y, dx, align='center')
            ax.set_ylabel('Number')
            ax.set_title('{name} Strings'.format(name=col['Name']))
            plt.xticks(rotation=45)
            ax.set_xticks(x)
            ax.set_xticklabels(label)
            ax.set_xlim(min(x) - 2, max(x) + 2)
            ax.set_ylim(None, max(y) + 2)
            plt.grid(True)
            fig.tight_layout()
            plt.savefig(pdf, format='pdf')
            plt.close(fig)





