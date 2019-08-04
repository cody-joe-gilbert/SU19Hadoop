# Composite Score (Final Analytic)
This folder contains Hive queries used to combine soil, weather, and solar radiance cosine similarities into a composite score describing each soil area's similarity to Napa. Currently the composite score is a simple average of the three cosine similarities, with possible values ranging from -1 to +1. 

Note that one intermediate step is to wrangle weather similarities from long-format (each row is a station-decade/Napa-decade comparison) to wide-format (each row is a station), where we defined each weather region's similarity with Napa-2010 in each decade. This added step is necessary because unlike soil and solar, weather data had a time dimension. 

The resulting `weather_sim` contains three columns of weather similarities: `weather_sim_2000` describes how similar each station's 2000 weather is to today's Napa (2010), `weather_sim_2010` describes how similar each station's 2010 weather is to today's Napa, and `weather_sim_future` estimates how similar each station's future weather will be to today's Napa (assuming that the former goes up by 5 degrees by 2100).   

Another intermediate step here is to derive a mapping of region IDs across the three datasets: `lkey` for soil, `solar_region_key` for solar radiance, and `station_id` for weather. Mapping between `lkey` and `solar_region_key` is trivial, since we queried the solar radiance API by soil's lat/longs, essentially obtaining each `solar_region_key` from a known `lkey`. 

Mapping between `lkey` and weather `station_id` is more involved: we took a cross product between the soil region (`legend.txt`) and weather region (`weather_stations.txt`) meta tables, computed the haversine distance between each pair's latitudes and longitudes, then for each `lkey` select the `station_id` with the smallest distance (subject to a few other constraints) as its mapped `station_id`. While this involves a `CROSS JOIN` in Hive, the solution is highly tractable even with no additional optimization (query ran within a couple minutes) since there are only a few thousands of soil areas and weather areas.  

## Hive Query Files 
* `wrangle_weather_sim.sql` wrangles weather similarities from long- to wide-format 
* `map_regions` maps region IDs across the soil, weather, and solar datasets
* `compute_final_analytic` computes composite score by taking simple average of the three similarities 

## HDFS Input Data

Lat/longs and other meta data for each soil, weather, and solar region can be found in: 

| Table Content | HDFS File Path | Hive Table | 
| ----------- | ----------- | ----------- | 
| Soil Areas | /user/yjn214/rbda-proj/legend.txt | yjn214.db/legend | 
| Solar Regions | /user/yjn214/rbda-proj/solarRegionsV2.csv | yjn214.db/solar_regions | 
| Weather Stations | /user/yjn214/rbda-proj/weather_stations.txt | yjn214.db/weather_stations | 

Cosine similarities for soil, weather, and solar regions as computed from previous pipelines can be found in: 

| Table Content | HDFS File Path | Hive Table | 
| ----------- | ----------- | ----------- | 
| Soil Similarities | /user/yjn214/rbda-proj/soil_cos_sim | yjn214.db/soil_sim | 
| Solar Similarities | /user/cjg507/cosineSim | yjn214.db/solar_sim | 
| Weather Similarities | /user/rf1316/cos_sim | yjn214.db/weather_sim_1 | 
| Weather Similarities (with offset) | /user/rf1316/cos_sim2 | yjn214.db/weather_sim_2 | 

## HDFS Output Data

Below are the relevant output tables in HDFS and Hive: 

| Table Content | HDFS File Path | Hive Table | 
| ----------- | ----------- | ----------- | 
| Region Mapping | hdfs://dumbo/user/hive/warehouse/yjn214.db/region_mapping_local | yjn214.db/region_mapping | 
| Composite Score | hdfs://dumbo/user/hive/warehouse/yjn214.db/composite_sim_local | yjn214.db/composite_sim | 
