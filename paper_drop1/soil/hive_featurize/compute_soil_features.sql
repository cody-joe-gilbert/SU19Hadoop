# weightings to aggregate map units into areas(legends)
CREATE TABLE mapunit_weights AS 
SELECT lkey, mukey, muacres, SUM(muacres) OVER (PARTITION BY lkey) AS muacres_total,
  CAST(muacres AS FLOAT) / SUM(muacres) OVER (PARTITION BY lkey) AS mu_wt
FROM mapunit WHERE lkey IS NOT NULL
ORDER BY lkey, muacres DESC;

# weightings to aggregate components into map units 
CREATE TABLE component_weights AS 
SELECT mukey, cokey, comppct_r, SUM(comppct_r) OVER (PARTITION BY mukey) AS comppct_total,
  CAST(comppct_r AS FLOAT) / SUM(comppct_r) OVER (PARTITION BY mukey) AS co_wt
FROM component 
ORDER BY mukey, comppct_r DESC;

# weightings to aggregate horizons/layers into components 
CREATE TABLE chorizon_weights AS 
SELECT cokey, chkey, hzdept_r, hzdepb_r, hzthk_calc, 
  SUM(hzthk_calc) OVER (PARTITION BY cokey) AS hzthk_total, 
  CAST(hzthk_calc AS FLOAT) / SUM(hzthk_calc) OVER (PARTITION BY cokey) AS ch_wt,
  ROW_NUMBER() OVER (PARTITION BY cokey ORDER BY hzdept_r) AS ch_order 
FROM (
  SELECT cokey, chkey, hzdept_r, hzdepb_r, hzdepb_r - hzdept_r AS hzthk_calc 
  FROM chorizon WHERE cokey IS NOT NULL) t1
ORDER BY cokey, hzthk_calc DESC; 

# aggregate horizon-level features into component-level 
CREATE TABLE chorizon_agg AS 
SELECT ch.cokey, 
  SUM(COALESCE(chw.ch_wt * ch.ph1to1h2o_r, 0)) / SUM(CASE WHEN ch.ph1to1h2o_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_ph1to1h2o, 
  SUM(COALESCE(chw.ch_wt * ch.cec7_r, 0)) / SUM(CASE WHEN ch.cec7_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_cec7, 
  SUM(COALESCE(chw.ch_wt * ch.awc_r, 0)) / SUM(CASE WHEN ch.awc_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_awc,
  SUM(COALESCE(chw.ch_wt * ch.om_r, 0)) / SUM(CASE WHEN ch.om_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_om,
  SUM(COALESCE(chw.ch_wt * ch.claytotal_r, 0)) / SUM(CASE WHEN ch.claytotal_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_claytotal,
  SUM(COALESCE(chw.ch_wt * ch.silttotal_r, 0)) / SUM(CASE WHEN ch.silttotal_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_silttotal,
  SUM(COALESCE(chw.ch_wt * ch.sandtotal_r, 0)) / SUM(CASE WHEN ch.sandtotal_r IS NULL THEN 0 ELSE chw.ch_wt END) AS co_sandtotal
FROM chorizon ch
INNER JOIN chorizon_weights chw ON chw.chkey = ch.chkey
GROUP BY ch.cokey;  

# aggregate component-level features into mapunit-level 
CREATE TABLE component_agg AS 
SELECT co.mukey, 
  SUM(COALESCE(cow.co_wt * cha.co_ph1to1h2o, 0)) / SUM(CASE WHEN cha.co_ph1to1h2o IS NULL THEN 0 ELSE cow.co_wt END) AS mu_ph1to1h2o, 
  SUM(COALESCE(cow.co_wt * cha.co_cec7, 0)) / SUM(CASE WHEN cha.co_cec7 IS NULL THEN 0 ELSE cow.co_wt END) AS mu_cec7,
  SUM(COALESCE(cow.co_wt * cha.co_awc, 0)) / SUM(CASE WHEN cha.co_awc IS NULL THEN 0 ELSE cow.co_wt END) AS mu_awc,
  SUM(COALESCE(cow.co_wt * cha.co_om, 0)) / SUM(CASE WHEN cha.co_om IS NULL THEN 0 ELSE cow.co_wt END) AS mu_om,
  SUM(COALESCE(cow.co_wt * cha.co_claytotal, 0)) / SUM(CASE WHEN cha.co_claytotal IS NULL THEN 0 ELSE cow.co_wt END) AS mu_claytotal,
  SUM(COALESCE(cow.co_wt * cha.co_silttotal, 0)) / SUM(CASE WHEN cha.co_silttotal IS NULL THEN 0 ELSE cow.co_wt END) AS mu_silttotal,
  SUM(COALESCE(cow.co_wt * cha.co_sandtotal, 0)) / SUM(CASE WHEN cha.co_sandtotal IS NULL THEN 0 ELSE cow.co_wt END) AS mu_sandtotal,
  SUM(COALESCE(cow.co_wt * co.slope_r, 0)) / SUM(CASE WHEN co.slope_r IS NULL THEN 0 ELSE cow.co_wt END) AS mu_slope,
  SUM(COALESCE(cow.co_wt * co.tfact, 0)) / SUM(CASE WHEN co.tfact IS NULL THEN 0 ELSE cow.co_wt END) AS mu_tfact,
  SUM(COALESCE(cow.co_wt * co.elev_r, 0)) / SUM(CASE WHEN co.elev_r IS NULL THEN 0 ELSE cow.co_wt END) AS mu_elev
FROM component co 
INNER JOIN component_weights cow ON co.cokey = cow.cokey 
LEFT JOIN chorizon_agg cha ON co.cokey = cha.cokey 
GROUP BY co.mukey;

# aggregate mapunit-level features into area/legend-level 
CREATE TABLE mapunit_agg AS 
SELECT mu.lkey, 
  SUM(COALESCE(muw.mu_wt * coa.mu_ph1to1h2o, 0)) / SUM(CASE WHEN coa.mu_ph1to1h2o IS NULL THEN 0 ELSE muw.mu_wt END) AS ph1to1h2o, 
  SUM(COALESCE(muw.mu_wt * coa.mu_cec7, 0)) / SUM(CASE WHEN coa.mu_cec7 IS NULL THEN 0 ELSE muw.mu_wt END) AS cec7, 
  SUM(COALESCE(muw.mu_wt * coa.mu_awc, 0)) / SUM(CASE WHEN coa.mu_awc IS NULL THEN 0 ELSE muw.mu_wt END) AS awc, 
  SUM(COALESCE(muw.mu_wt * coa.mu_om, 0)) / SUM(CASE WHEN coa.mu_om IS NULL THEN 0 ELSE muw.mu_wt END) AS om, 
  SUM(COALESCE(muw.mu_wt * coa.mu_claytotal, 0)) / SUM(CASE WHEN coa.mu_claytotal IS NULL THEN 0 ELSE muw.mu_wt END) AS claytotal, 
  SUM(COALESCE(muw.mu_wt * coa.mu_silttotal, 0)) / SUM(CASE WHEN coa.mu_silttotal IS NULL THEN 0 ELSE muw.mu_wt END) AS silttotal, 
  SUM(COALESCE(muw.mu_wt * coa.mu_sandtotal, 0)) / SUM(CASE WHEN coa.mu_sandtotal IS NULL THEN 0 ELSE muw.mu_wt END) AS sandtotal, 
  SUM(COALESCE(muw.mu_wt * coa.mu_slope, 0)) / SUM(CASE WHEN coa.mu_slope IS NULL THEN 0 ELSE muw.mu_wt END) AS slope,
  SUM(COALESCE(muw.mu_wt * coa.mu_tfact, 0)) / SUM(CASE WHEN coa.mu_tfact IS NULL THEN 0 ELSE muw.mu_wt END) AS tfact,
  SUM(COALESCE(muw.mu_wt * coa.mu_elev, 0)) / SUM(CASE WHEN coa.mu_elev IS NULL THEN 0 ELSE muw.mu_wt END) AS elev
FROM mapunit mu
INNER JOIN mapunit_weights muw ON mu.mukey = muw.mukey
LEFT JOIN component_agg coa ON mu.mukey = coa.mukey
GROUP BY mu.lkey;

# standardize features and drop areas with null features 
CREATE TABLE soil_features AS 
SELECT lkey, 
  (ph1to1h2o - AVG(ph1to1h2o) OVER ()) / (STDDEV_POP(ph1to1h2o) OVER ()) AS ph1to1h2o, 
  (cec7 - AVG(cec7) OVER ()) / (STDDEV_POP(cec7) OVER ()) AS cec7,
  (awc - AVG(awc) OVER ()) / (STDDEV_POP(awc) OVER ()) AS awc,
  (om - AVG(om) OVER ()) / (STDDEV_POP(om) OVER ()) AS om,
  (claytotal - AVG(claytotal) OVER ()) / (STDDEV_POP(claytotal) OVER ()) AS claytotal,
  (silttotal - AVG(silttotal) OVER ()) / (STDDEV_POP(silttotal) OVER ()) AS silttotal,
  (sandtotal - AVG(sandtotal) OVER ()) / (STDDEV_POP(sandtotal) OVER ()) AS sandtotal,
  (slope - AVG(slope) OVER ()) / (STDDEV_POP(slope) OVER ()) AS slope,
  (tfact - AVG(tfact) OVER ()) / (STDDEV_POP(tfact) OVER ()) AS tfact,
  (elev - AVG(elev) OVER ()) / (STDDEV_POP(elev) OVER ()) AS elev
FROM mapunit_agg
WHERE ph1to1h2o IS NOT NULL AND cec7 IS NOT NULL AND awc IS NOT NULL AND om IS NOT NULL 
AND claytotal IS NOT NULL AND silttotal IS NOT NULL AND sandtotal IS NOT NULL 
AND slope IS NOT NULL AND tfact IS NOT NULL AND elev IS NOT NULL; 

# output to local 
CREATE TABLE soil_features_local 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED as textfile
AS 
SELECT 'lkey' AS lkey, 'ph1to1h2o' AS ph1to1h2o, 'cec7' AS cec7, 'awc' AS awc, 'om' AS om, 'claytotal' AS claytotal, 
'silttotal' AS silttotal, 'sandtotal' AS sandtotal, 'slope' AS slope, 'tfact' AS tfact, 'elev' AS elev;

INSERT INTO soil_features_local
SELECT lkey, ph1to1h2o, cec7, awc, om, claytotal, silttotal, sandtotal, slope, tfact, elev
FROM soil_features;

hadoop fs -cat hdfs://dumbo/user/hive/warehouse/yjn214.db/soil_features_local/* > $HOME/rbda-proj/soil_features_local
scp yjn214@dumbo.hpc.nyu.edu:rbda-proj/soil_features_local .