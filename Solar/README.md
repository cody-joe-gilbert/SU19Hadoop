# Solar Irradiance Data
This folder contains the processing tools and select results of the NREL solar irradiance data used in the *Finding Napa* project.


## Folders
* `dataacquisition` contains the scripts for fetching NREL NSRDB data and output
* `rawprofileresults` contains the results of running the profiler scripts on the raw (unmodified) NREL solar irradiance data.

## Data Files
* `solarRegions.csv` lists the solar regions and year for which data was obtained. Provides a translation between the soil regions (SoilLat, SoilLon) and the solar regions (SolarLat, SolarLon).
* `solarScheme.csv` contains the column names to each of the raw solar data columns within `solarData.csv`

## HDFS Data
* Full Solar Data Set: `/user/cjg507/solarData.csv` 
This data set *includes* the following schema:

| Column Name  | Data Type | Description | Range |
| ----------- | ----------- | ----------- | ----------- |
| Year      | Int       |  Year Represented | 2017 |
| Month      | Int       |  Month Represented | 1-12 |
| Day      | Int       |  Day Represented | 1-31 |
| Hour      | Int       |  Hour Represented | 0-23  |
| Minute      | Int       |  Minute Represented | 30 (hour midpoint) |
| GHI      | Int       |  Total amount of direct and diffuse solar radiation (Whr/m^2)  received on a horizontal surface during the 60-minute period ending at the timestamp | Min: 0, Max: 1137 |
| DHI      | Int       |  Amount of solar radiation (Whr/m^2) received from the sky (excluding the solar disk) on a horizontal surface during the 60-minute period ending at the timestamp | Min: 0, Max: 682 |
| DNI      | Int       |  Amount of solar radiation (Whr/m^2) received in a collimated beam on a surface normal to the sun during the 60-minute period ending at the timestamp | Min: 0, Max: 1121 |
| Latitude     | Float       |  Latitude of modeled data point | Min: -14.27, Max: 59.77 |
| Longitude     | Float       |  Longitude of modeled data point | Min: -170.38, Max: -64.82 |
**Note:** This schema does not reflect all columns within `solarData.csv`, but rather a subset that are considered useful for analysis. A full collection of column names is included in `Solar/solarSchema.csv`. 



* Sample of Solar Data Set: `/user/cjg507/solarDataSample.csv`