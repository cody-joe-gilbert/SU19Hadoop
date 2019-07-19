# NREL NSRDB Data Acquisition 

This folder contains the scripts, fetched data, and metadata for solar data retrieved from the [NREL NSRDB]( https://nsrdb.nrel.gov/ ).


## Included files

### Scripts

* `APIKeys.py` Contains the API keys used to access the NSRDB API. **CONDFIDENTIAL: DO NOT DISTRIBUTE**
* `fetchSolarData.py` Script to fetch, reformat, and save NREL data.

### Data Files

* `completed.log` log of successfully fetched data, including the original soil lat-longs, the output solar lat-longs, and the data year.

## Process Description

### Data Acquisition 
All solar data was taken from the [NREL NSRDB]( https://nsrdb.nrel.gov/ ) via API calls. The NSRDB accepts API calls for a single latitude-longitude point for a given year, and outputs solar data for the *closest* database latitude-longitude point for the entire year in 60-minute intervals. The Python script `fetchSolarData.py` automates the API calls, processes the data, and appends new data to the accumulated data file.

The maximum number of API calls to the NSRDB is limited to 300 calls/day. The number of regions defined by the soil dataset is in excess of 3000. Therefore, this project was limited to taking only a single solar geographic point for a single year. The `fetchSolarData.py` iterates over the soil regions in `soil_regions.csv` and submits an API call for each midpoint latitude-longitude on 2017. Some entries in the `soil_regions.csv` are outside of the NRSDB's US Data range, and were subsequently dropped. `completed.log` contains the input soil lat-long, output solar lat-long, and year of each successful data pull and can be used to join the soil regions to the solar regions.

Each API call returns a mid-sized CSV file containing a header with point data followed by rows of temporal data. Because the individual files are relatively small in size, the header data was appended as columns of constant values to each row of temporal data.  

The final data file containing all comma-delimited solar data `solarData.csv` was zipped and moved to the Dumbo cluster via SCP. The copied file was unzipped and moved to HDFS with the command

```
$ hdfs dfs -put /scratch/cjg507/solarData.csv solarData.csv
```
where it can be accessed at `/user/cjg507/solarData.csv`



