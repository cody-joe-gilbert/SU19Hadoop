# Data Profiling Scripts

This folder contains a set of Python 2.7 scripts for automatic data profiling of big data contained on an HDFS cluster using Hadoop MapReduce. The primary script is *profileData.py* which uses a two-pass MapReduce scheme to find the summary statistics, histograms, and unique string counts of the input data.

## profileData.py

### Instructions
1. Ensure requirements below are met.
2. Set input parameters as specified below
3. Execute with '$ python profileData.py'
4. On successful execution, examine the result files specified below.

### Requirements

**Python Version 2.7 or higher must be used**

This mapper assumes data is in a form of a row-by-column comma-delimited
schema that has constant form; i.e. all rows contain the same number of fields/
columns. Columns may contain mixed numeric and non-numeric data.
Columns containing no data (",," form) will be recorded as the string "NULL"

Ensure the following MapReduce scripts are available:

* HadoopTools.py: Module containing Hadoop wrapper functions
* firstPassProfileMap.py: first-pass profiler mapper script for Hadoop MapReduce
* firstPassProfileRed.py: first-pass profiler reducer script for Hadoop MapReduce
* secondPassProfileMap.py: second-pass profiler mapper script for Hadoop MapReduce
* secondPassProfileRed.py: second-pass profiler reducer script for Hadoop MapReduce

### Inputs
Set the following parameters below prior to execution:

* resultsFolder: path to folder containing results
* inputFile: HDFS path to input file
* fpMapScript: file path to first-pass profiler mapper firstPassProfileMap.py
* fpRedScript: file path to first-pass profiler reducer firstPassProfileRed.py
* spMapScript: file path to second-pass profiler mapper secondPassProfileMap.py
* spRedScript: file path to second-pass profiler reducer secondPassProfileRed.py
* colNames: List of data column names, or None to use 'Column X' notation
* histIntervals: number of bins between the maximum and minimum numeric values

### Outputs
The resultsFolder will contain the following summary files:

* profileResults.txt: text summary of data set
* profilePlotNum.pdf: plots including histograms of the numerical data fields
* profilePlotStrings.pdf: plots of unique string counts per column

@author: Cody Gilbert