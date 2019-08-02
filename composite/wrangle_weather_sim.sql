### Wrange weather similarity data from long- to wide- format, with similarities for each decade as separate columns ### 

# login to hive 
beeline 
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
use yjn214; 

# copy files 
hdfs dfs -cp /user/rf1316/cos_sim /user/yjn214/rbda-proj/weatherSim1
hdfs dfs -cp /user/rf1316/cos_sim2 /user/yjn214/rbda-proj/weatherSim2

# create hive table schemas 
CREATE EXTERNAL TABLE weather_sim_1 (idx STRING, station_id STRING, decade STRING, weather_sim FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
CREATE EXTERNAL TABLE weather_sim_2 (idx STRING, station_id STRING, decade STRING, weather_sim FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

# load data into hive 
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/weatherSim1' OVERWRITE INTO TABLE weather_sim_1;
LOAD DATA INPATH 'hdfs:/user/yjn214/rbda-proj/weatherSim2' OVERWRITE INTO TABLE weather_sim_2;

# wrangle weather sim data from long- to wide-format, with similarities for each decade as separate columns  
CREATE TABLE weather_sim AS 
	SELECT station_id, 
		AVG(CASE WHEN decade = '1990' THEN weather_sim ELSE NULL END) AS weather_sim_1990, 
		AVG(CASE WHEN decade = '2000' THEN weather_sim ELSE NULL END) AS weather_sim_2000,
		AVG(CASE WHEN decade = '2010' THEN weather_sim ELSE NULL END) AS weather_sim_2010,
		AVG(CASE WHEN decade = 'future' THEN weather_sim ELSE NULL END) AS weather_sim_future
	FROM
		(SELECT station_id, decade, weather_sim 
		FROM weather_sim_1 WHERE decade IN ('1990', '2000', '2010')
		UNION ALL
		SELECT station_id, CASE WHEN decade = '2010' THEN 'future' ELSE NULL END AS decade, weather_sim 
		FROM weather_sim_2 WHERE decade = '2010') t
	GROUP BY station_id;