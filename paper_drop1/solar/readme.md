# Solar Irradiance Processing Codes

This folder contains the codes used to process the already acquired, cleaned, and profiled NSRDB solar irradiance data that is stored in hdfs folder `/user/cjg507/solarData.csv`. 

## HDFS Data
* Full Solar Data Set: `/user/cjg507/solarData.csv` 
* Cosine Similarity Output: `/user/cjg507/cosineSim` 

## Folders
* `tools` contains the scripts used by the `dataProcess.py` script. The driver scripts assume that these tools are located in the same folder, but I've tucked them in this for ease of reading.

## Scripts
* `dataProcess.py` is the primary driver script for the NSRDB solar irradiance data. This script take in the cleaned solar data, featurizes the data as described in the design implementation, standardizes and computes the the cosine similarity of each region to the baseline Napa Valley region. The output can then be passed to the index weighted code in the root folder.