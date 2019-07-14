# login 
beeline 
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
use yjn214; 

# load data onto DUMBO 
scp legend.txt yjn214@dumbo.hpc.nyu.edu:rbda-proj/
scp mapunit.txt yjn214@dumbo.hpc.nyu.edu:rbda-proj/
scp component.txt yjn214@dumbo.hpc.nyu.edu:rbda-proj/
scp chorizon.txt yjn214@dumbo.hpc.nyu.edu:rbda-proj/

# put in HDFS 
hdfs dfs -mkdir rbda-proj 
hdfs dfs -put $HOME/rbda-proj/legend.txt /user/yjn214/rbda-proj
hdfs dfs -put $HOME/rbda-proj/mapunit.txt /user/yjn214/rbda-proj
hdfs dfs -put $HOME/rbda-proj/component.txt /user/yjn214/rbda-proj
hdfs dfs -put $HOME/rbda-proj/chorizon.txt /user/yjn214/rbda-proj
hadoop fs -chmod -R 777 /user/yjn214/rbda-proj

# create table schema 
create table legend (areasymbol STRING, lkey BIGINT, areaname STRING, mlraoffice STRING, areaacres INT, mbrminx FLOAT, mbrminy FLOAT, mbrmaxx FLOAT, mbrmaxy FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
create table mapunit (mukey BIGINT, muname STRING, mukind STRING, muacres INT, farmlndcl INT, museq INT, nationalmusym STRING, lkey BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
create table component (cokey BIGINT, compname STRING, mukey BIGINT, comppct_r INT, localphase STRING, slope_r FLOAT, compkind STRING, majcompflag STRING, drainagecl STRING, taxpartsize STRING, runoff STRING, tfact INT, wei STRING, erocl STRING, hydricrating STRING, elev_r FLOAT, aspectrep INT, nirrcapcl STRING, irrcapcl STRING, frostact STRING, hydgrp STRING, taxceactcl STRING, taxreaction STRING, taxtempcl STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;
create table chorizon (chkey BIGINT, cokey BIGINT, hzdept_r INT, hzdepb_r INT, hzname STRING, hzthk_r FLOAT, fraggt10_r FLOAT, frag3to10_r FLOAT, sieveno4_r FLOAT, sieveno10_r FLOAT, sieveno40_r FLOAT, sieveno200_r FLOAT, sandtotal_r FLOAT, sandvc_r FLOAT, sandco_r FLOAT, sandmed_r FLOAT, sandfine_r FLOAT, sandvf_r FLOAT, silttotal_r FLOAT, siltco_r FLOAT, siltfine_r FLOAT, claytotal_r FLOAT, claysizedcarb_r FLOAT, om_r FLOAT, awc_r FLOAT, wtenthbar_r FLOAT, wthirdbar_r FLOAT, wfifteenbar_r FLOAT, wsatiated_r FLOAT, kwfact STRING, kffact STRING, caco3_r FLOAT, gypsum_r FLOAT, sar_r FLOAT, ec_r FLOAT, cec7_r FLOAT, ecec_r FLOAT, sumbases_r FLOAT, ph1to1h2o_r FLOAT, ph01mcacl2_r FLOAT, freeiron_r FLOAT, feoxalate_r FLOAT, ptotal_r FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

# load tables 
load data inpath 'hdfs:/user/yjn214/rbda-proj/legend.txt' overwrite into table legend; 
load data inpath 'hdfs:/user/yjn214/rbda-proj/mapunit.txt' overwrite into table mapunit;
load data inpath 'hdfs:/user/yjn214/rbda-proj/component.txt' overwrite into table component;
load data inpath 'hdfs:/user/yjn214/rbda-proj/chorizon.txt' overwrite into table chorizon; 
