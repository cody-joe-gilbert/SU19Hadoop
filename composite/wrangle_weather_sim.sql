### Wrange weather similarity data from long- to wide- format, with similarities for each decade as separate columns ### 

# login to hive 
beeline 
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
use yjn214; 

# copy files 
hdfs dfs -cp /user/rf1316/cos_sim /user/yjn214/rbda-proj/weatherSim1_v2
hdfs dfs -cp /user/rf1316/cos_sim2 /user/yjn214/rbda-proj/weatherSim2_v2 

# create hive table schemas 
CREATE EXTERNAL TABLE weather_sim_1 (station_decade STRING, station_id STRING, napa_decade STRING, weather_sim FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
CREATE EXTERNAL TABLE weather_sim_2 (station_decade STRING, station_id STRING, napa_decade STRING, weather_sim FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

# load data into hive 
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/weatherSim1_v2' OVERWRITE INTO TABLE weather_sim_1;
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/weatherSim2_v2' OVERWRITE INTO TABLE weather_sim_2;

# wrangle weather sim data from long- to wide-format, with similarities for each decade as separate columns  
# note that we are comparing each station's similarity with Napa-2010

CREATE TABLE weather_sim AS 
	SELECT t1.station_id, weather_sim_2000, weather_sim_2010, weather_sim_future
	FROM (
		SELECT station_id, 
			AVG(CASE WHEN station_decade = '200' THEN weather_sim ELSE NULL END) AS weather_sim_2000, 
			AVG(CASE WHEN station_decade = '201' THEN weather_sim ELSE NULL END) AS weather_sim_2010
		FROM weather_sim1 
		WHERE napa_decade = '2010'
		GROUP BY station_id) t1 
	LEFT JOIN (
		SELECT station_id, 
			AVG(CASE WHEN station_decade = '201' THEN weather_sim ELSE NULL END) AS weather_sim_future 	
		FROM weather_sim2
		WHERE napa_decade = '2010' 
		GROUP BY station_id) t2 
	ON t1.station_id = t2.station_id
	WHERE weather_sim_2000 IS NOT NULL AND weather_sim_2010 IS NOT NULL AND weather_sim_future IS NOT NULL;  