# load soil regions 
tail -n+2 legend.txt > legend_no_header.txt
hdfs dfs -put $HOME/rbda-proj/legend_no_header.txt /user/yjn214/rbda-proj
CREATE TABLE legend (areasymbol STRING, lkey BIGINT, areaname STRING, mlraoffice STRING, areaacres INT, mbrminx FLOAT, mbrminy FLOAT, mbrmaxx FLOAT, mbrmaxy FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/legend_no_header.txt' OVERWRITE INTO TABLE legend; 

CREATE TABLE soil_areas AS 
	SELECT lkey, areasymbol, areaname, mlraoffice, (mbrminx + mbrmaxx) / 2.0 AS longitude, (mbrminy + mbrmaxy) / 2.0 AS latitude 
	FROM legend; 

# load weather regions (note weather similarity data was separately processed in wrangle_weather_sim.sql)
tail -n+2 weather_stations.txt > weather_stations_no_header.txt
hdfs dfs -put weather_stations_no_header.txt /user/yjn214/rbda-proj
CREATE TABLE weather_stations (usaf STRING, wban STRING, station_name STRING, ctry STRING, st STRING, call STRING, latitude FLOAT, longitude FLOAT, elev FLOAT, begin_dt STRING, end_dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/weather_stations_no_header.txt' OVERWRITE INTO TABLE weather_stations; 

# load solar regions 
tail -n+2 solarRegionsV2.csv > solar_regions_no_header.csv
hdfs dfs -put solar_regions_no_header.csv /user/yjn214/rbda-proj
CREATE TABLE solar_regions (soil_lat FLOAT, soil_lon FLOAT, solar_lat FLOAT, solar_long FLOAT, year INT, soil_region_key STRING, solar_region_key STRING, lkey BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/solar_regions_no_header.csv' OVERWRITE INTO TABLE solar_regions; 

# map soil <> weather <> solar 

CREATE TEMPORARY TABLE soil AS 
	SELECT lkey, areaname, radians(latitude) AS lat_s, radians(longitude) AS long_s 
	FROM soil_areas;  

CREATE TEMPORARY TABLE solar AS 
	SELECT solar_region_key, lkey
	FROM solar_regions;  

-- CREATE TABLE weather AS 
-- 	SELECT CONCAT(usaf, '-', wban) AS station_id, station_name, radians(latitude) AS lat_w, radians(longitude) AS long_w 
-- 	FROM weather_stations 
-- 	WHERE ctry='US' AND latitude IS NOT NULL AND longitude IS NOT NULL; 

CREATE TABLE weather AS 
	SELECT t1.station_id, station_name, lat_w, long_w  
	FROM 
		(SELECT CONCAT(usaf, '-', wban) AS station_id, station_name, radians(latitude) AS lat_w, radians(longitude) AS long_w 
		FROM weather_stations w
		WHERE ctry='US' AND latitude IS NOT NULL AND longitude IS NOT NULL) t1
	INNER JOIN 
		(SELECT station_id FROM weather_sim WHERE weather_sim_2010 IS NOT NULL) t2 
	ON t1.station_id = t2.station_id; 

CREATE TABLE soil_weather AS 
	SELECT lkey, station_id, dist 
	FROM 
		(SELECT *, ROW_NUMBER() OVER (PARTITION BY lkey ORDER BY dist ASC) AS rank 
		FROM 
			(SELECT lkey, station_id, 
				2*3956.27*asin(sqrt(pow(sin((lat_w - lat_s) / 2), 2) + cos(lat_s)*cos(lat_w)*pow(sin((long_w - long_s) / 2), 2))) AS dist 
			FROM soil CROSS JOIN weather) t1) t2
	WHERE rank = 1 AND dist < 50;

CREATE TABLE region_mapping AS 
	SELECT sw.lkey, sw.station_id, s2.solar_region_key, s1.areaname, w.station_name AS weather_station
	FROM soil_weather sw 
	INNER JOIN soil s1 ON s1.lkey = sw.lkey 
	INNER JOIN solar s2 ON s2.lkey = sw.lkey 
	INNER JOIN weather w ON w.station_id = sw.station_id; 

# output mapping to local 

CREATE TABLE region_mapping_local 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED as textfile
AS
SELECT 'lkey' AS lkey, 'station_id' AS station_id, 'solar_region_key' AS solar_region_key, 
'areaname' AS areaname, 'weather_station' AS weather_station;

INSERT INTO region_mapping_local
SELECT lkey, station_id, solar_region_key, areaname, weather_station
FROM region_mapping;

hadoop fs -cat hdfs://dumbo/user/hive/warehouse/yjn214.db/region_mapping_local/* > $HOME/rbda-proj/region_mapping_v2.txt
scp yjn214@dumbo.hpc.nyu.edu:rbda-proj/region_mapping_v2.txt .