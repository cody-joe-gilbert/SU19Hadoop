## Weather Data and Processing

The main dataset is hosted by NOAA at https://www.ncdc.noaa.gov/isd and can be download via FTP. The total disk space after decompression is approximately 270GB.

## Folders

- src: map reduce program for featurizing Hive data that was generated from raw data
- src_norm: map reduce program for normalizing features, 1st pass puts everything int K-V pairs
- src_comb: map reduce program for combining K-V pairs back into original table
- src_cos: map reduce program for computing cosine similarity of normalized data

## Processing Overview

1. PIG program to convert raw data into a readable Hive format:
- Reading fix position format files is hard in Hive, so we used some pig scripts to transform the raw file into a comma delimited one. The script can be fund at the top of beeline.txt

2. Hive loading data:
- Section 2 of beeline.txt, loads the above PIG output into a Hive table

3. Hive aggregation data extraction:
- Section 3 of beeline.txt, using SQL, we extract our raw features by decade, region, month

4. Featurising extracted features:
- Combines the raw features that is 1-row-per-month into a wider table that has each month as a feature instead of adding a new row for it. Section 4 of beeline.txt.
- The code used is in the src/ folder

5. Normalizing features:
- Each feature is then normalized by subtracting mean and dividing by standard dev. This is accomplished by a 2-step Map Reduce program. 
- The first step explodes the data into KV pairs with the raw numerical data normalized, the code used is in the src_norm/ folder
- The 2nd step combines the KV pairs into the original matrix, with all numerical data points now normalized, the code used is in the src_comb/ folder

6. Computing Cosine Similarity:
- Napa's feature vector was hardcoded into the .py file and all other regions are computed against Napa, resulting in a similarity score for each region, with Napa's score being 1.0, code is in the src_cos/ folder.
