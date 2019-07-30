### Load cleaned soil tables from HDFS into Hive ### 

# login to Hive 
beeline 
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
use yjn214; 

# create Hive table schema 
CREATE TABLE legend (areasymbol STRING, lkey BIGINT, areaname STRING, mlraoffice STRING, areaacres INT, mbrminx FLOAT, mbrminy FLOAT, mbrmaxx FLOAT, mbrmaxy FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
CREATE TABLE mapunit (mukey STRING, muacres STRING, lkey STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
CREATE TABLE component (cokey BIGINT, mukey BIGINT, comppct_r INT, slope_r FLOAT, tfact INT, elev_r FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
CREATE TABLE horizon (chkey BIGINT, cokey BIGINT, hzdept_r INT, hzdepb_r INT, sandtotal_r FLOAT, silttotal_r FLOAT, claytotal_r FLOAT, om_r FLOAT, awc_r FLOAT, cec7_r FLOAT, ph1to1h2o_r FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

# load cleaned data 
load data inpath 'hdfs:/user/yjn214/rbda-proj/legend_no_header.txt' overwrite into table legend; 
load data inpath 'hdfs://dumbo/user/yjn214/rbda-proj/mapunit_no_header_cleaned' overwrite into table mapunit;
load data inpath 'hdfs://dumbo/user/yjn214/rbda-proj/component_no_header_cleaned' overwrite into table component;
load data inpath 'hdfs://dumbo/user/yjn214/rbda-proj/chorizon_no_header_cleaned' overwrite into table horizon; 