### Compute composite similarity by taking weighted average of weather/solar/soil cosine similarities ### 

## Note that unlike for soil and solar, weather similarity between Napa and each region is computed for each of 4 decades: 
## 1990s, 2000s, 2010s, and future projected. To compute composite similarities for each decade, we take the weighted average 
## between the relevant decade's weather similarity and the constant soil and solar radiation similarities. 

# login to hive 
beeline 
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
use yjn214; 

# copy files (weather sim processed separately in wrangle_weather_sim.sql)
hdfs dfs -cp /user/cjg507/cosineSim /user/yjn214/rbda-proj/solarSim
hdfs dfs -cp /user/yjn214/rbda-proj/soil_cos_sim /user/yjn214/rbda-proj/soilSim 

# create hive table schemas 
CREATE EXTERNAL TABLE solar_sim (solar_region_key STRING, solar_sim FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
CREATE EXTERNAL TABLE soil_sim (lkey STRING, soil_sim FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

# load data into hive 
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/solarSim' OVERWRITE INTO TABLE solar_sim; 
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/soilSim' OVERWRITE INTO TABLE soil_sim; 

# join tables and compute final analytic 
CREATE TABLE composite_sim AS 
	SELECT m.areaname, m.lkey, m.station_id, m.solar_region_key, m.weather_station AS station_name, a.latitude, a.longitude,
		soil_sim, solar_sim, weather_sim_1990, weather_sim_2000, weather_sim_2010, weather_sim_future,
		1/3*soil_sim + 1/3*solar_sim + 1/3*weather_sim_1990 AS comp_sim_1990,
		1/3*soil_sim + 1/3*solar_sim + 1/3*weather_sim_2000 AS comp_sim_2000, 
		1/3*soil_sim + 1/3*solar_sim + 1/3*weather_sim_2010 AS comp_sim_2010,
		1/3*soil_sim + 1/3*solar_sim + 1/3*weather_sim_future AS comp_sim_future
	FROM region_mapping m 
	LEFT JOIN soil_areas a ON a.lkey = m.lkey 
	LEFT JOIN soil_sim sl ON sl.lkey = m.lkey 
	LEFT JOIN solar_sim sa ON sa.solar_region_key = m.solar_region_key
	LEFT JOIN weather_sim w ON w.station_id = m.station_id 
	ORDER BY comp_sim_2010 DESC;  

# replicate schema, populate single row with column headings 
CREATE TABLE composite_sim_local  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED as textfile
AS 
SELECT 'areaname' AS areaname, 'lkey' AS lkey, 'station_id' AS station_id, 'solar_region_key' AS solar_region_key, 
'station_name' AS station_name, 'latitude' AS latitude, 'longitude' AS longitude, 'soil_sim' AS soil_sim, 'solar_sim' AS solar_sim, 
'weather_sim_1990' AS weather_sim_1990, 'weather_sim_2000' AS weather_sim_2000, 'weather_sim_2010' AS weather_sim_2010, 
'weather_sim_future' AS weather_sim_future, 'comp_sim_1990' AS comp_sim_1990, 'comp_sim_2000' AS comp_sim_2000, 
'comp_sim_2010' AS comp_sim_2010, 'comp_sim_future' AS comp_sim_future;

# fill in rest of table with data from Hive table 
INSERT INTO composite_sim_local
SELECT areaname, lkey, station_id, solar_region_key, station_name, latitude, longitude,
soil_sim, solar_sim, weather_sim_1990, weather_sim_2000, weather_sim_2010, weather_sim_future,
comp_sim_1990, comp_sim_2000, comp_sim_2010, comp_sim_future
FROM composite_sim;

# move file from Hive/HDFS > Dumbo > local computer 
hadoop fs -cat hdfs://dumbo/user/hive/warehouse/yjn214.db/composite_sim_local/* > $HOME/rbda-proj/composite_sim_local
scp yjn214@dumbo.hpc.nyu.edu:rbda-proj/composite_sim_local composite_sim_v2.csv