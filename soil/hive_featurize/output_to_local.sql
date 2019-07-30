### Output soil features from Hive to local text file (with column names) ### 

# replicate schema, populate single row with column headings 
CREATE TABLE soil_features_local 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED as textfile
AS 
SELECT 'lkey' AS lkey, 'ph1to1h2o' AS ph1to1h2o, 'cec7' AS cec7, 'awc' AS awc, 'om' AS om, 'claytotal' AS claytotal, 
'silttotal' AS silttotal, 'sandtotal' AS sandtotal, 'slope' AS slope, 'tfact' AS tfact, 'elev' AS elev;

# fill in rest of table with data from Hive table 
INSERT INTO soil_features_local
SELECT lkey, ph1to1h2o, cec7, awc, om, claytotal, silttotal, sandtotal, slope, tfact, elev
FROM soil_features;

# move file from Hive/HDFS > Dumbo > local computer 
hadoop fs -cat hdfs://dumbo/user/hive/warehouse/yjn214.db/soil_features_local/* > $HOME/rbda-proj/soil_features_local
scp yjn214@dumbo.hpc.nyu.edu:rbda-proj/soil_features_local .