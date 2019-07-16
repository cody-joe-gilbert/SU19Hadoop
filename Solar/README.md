# Solar Irradiance Data
This folder contains the processing tools and select results of the NREL solar irradiance data used in the *Finding Napa* project.

***
## Folders

* **dataacquisition** contains the scripts for fetching NREL NSRDB data and output
* **rawprofileresults** contains the results of running the profiler scripts on the raw (unmodified) NREL solar irradiance data.

***
## Data Files
* **solarRegions.csv** lists the solar regions and year for which data was obtained. Provides a translation between the soil regions (SoilLat, SoilLon) and the solar regions (SolarLat, SolarLon).
* **solarScheme.csv** contains the column names to each of the raw solar data columns within *solarData.csv*

***
## HDFS Data
* Full Solar Data Set: /user/cjg507/solarData.csv
* Sample of Solar Data Set: /user/cjg507/solarDataSample.csv