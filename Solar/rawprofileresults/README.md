# Profiler Results of Raw Solar Data
This folder contains the results of running the `profileData.py` on the raw solar data set `solarData.csv`. 

## Included files

### Scripts
* `profileData.py` Primary Data profiling script. Runs with the following:
	* `HadoopTools.py` Module containing Hadoop Python wrapper functions
	* `firstPassProfileMap.py` first-pass profiler mapper script for Hadoop MapReduce
	* `firstPassProfileRed.py` first-pass profiler reducer script for Hadoop MapReduce
	* `secondPassProfileMap.py` second-pass profiler mapper script for Hadoop MapReduce
	* `secondPassProfileRed.py` second-pass profiler reducer script for Hadoop MapReduce

### Data Files
* `profileResults.txt` contains the text-based results of the profiler
* `profilePlotNum.pdf` pdf containing histogram plots
* `profilePlotStrings.pdf` pdf containing unique string frequency plots

## Process Description

### Data Cleaning and Profiling
The majority of cleaning and screening occurred in the data acquisition step by calling only the necessary data attributes and performing initial wrangling on the small data files. 

Data profiling was performed using the team-distributed `profileData.py` data tool. This tool takes in a HDFS file location and string of column names, and automatically generates numerical range and statistics data for numerical elements, and unique string counts for string elements. The tool also generates pdfs with variable-binned histograms of numerical data and unique string frequency bar plots of string data. 

The results of running `profileData.py` on the solar data file `soil_regions.csv` are shown above in the Data Files section. 