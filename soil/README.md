# Soil Data
This folder contains the code used to process soil attribute data obtained from USDA NRCS (https://sdmdataaccess.sc.egov.usda.gov/). The goal is to extract and aggregate features for each soil region, and compute their cosine similarity against Napa. The resulting soil similarity is one of three components in the composite soil similarity used in the final analytic for the *Finding Napa* project. 

## Folders
* `ingest_data` contains SQL queries used to extract input data from USDA database, and scripts to load them into HDFS
* `mr_process` contains MapReduce code used to profile and clean input data 
* `hive_featurize` contains Hive queries used to standardize and aggregate soil horizon/component-level attributes into soil area-level features 
* `mr_cos_sim` contains MapReduce code used to compute cosine similarities between each soil area and Napa 

## HDFS Input Data

Soil data is provided at four hierarchical levels: 
1. a soil area is composed of several map units 
2. a map unit is composed of several components 
3. a component is composed of several horizons/layers 
4. a horizon is the most granular unit 

For instance, our reference soil region Napa County, California (lkey=14083) comprises 146 map units, 500 components, and 714 horizons. 

Our unit of analysis is at the soil area level, but since most of the soil attributes (e.g. pH, clay percent) are provided at the horizon and component level, we need to aggregate these attributes up the soil hierarchy, from: 
* horizon/layer -> component (weighting by thickness of horizon/layer)
* component -> map unit (weighting by component percent, provided directly by NCRS)
* map unit -> area/legend (weighting by number of acres)

| Soil Hierarchy Level | HDFS File Path | Primary Key | Number of Rows | 
| ----------- | ----------- | ----------- | ----------- |
| Soil Area | /user/yjn214/rbda-proj/legend.txt | lkey | 3266 | 
| Map Unit | /user/yjn214/rbda-proj/mapunit.txt |  mukey | 320288 | 
| Component | /user/yjn214/rbda-proj/component.txt | cokey | 1188424 | 
| Horizon | /user/yjn214/rbda-proj/chorizon.txt | chkey | 3748010 | 

Data schemas may be found [here](ingest_data/Table%20schemas%20for%20soil%20database.pdf). 

## HDFS Output Data

Below are the output tables in HDFS and Hive that are of interest: 

| Table Content | HDFS File Path | Hive Table | 
| ----------- | ----------- | ----------- | 
| Soil Area Features | hdfs://dumbo/user/yjn214/rbda-proj/soil_features_local | yjn214.db/soil_features | 
| Soil Area Cosine Similarities | hdfs://dumbo/user/yjn214/rbda-proj/soil_cos_sim | yjn214.db/soil_sim |  
