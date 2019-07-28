## Processing the Weather data

The main flow of the scripting is described in beeline.txt

The steps are summarized below:
1. Use pig to process the raw weather files so that it can be loaded into Hive
2. Load the data into Hive
3. Use SQL in hive to extract the features
4. First MapReduce job to featurize the data
5. Second MapReduce job to normalize the data
6. Third MapReduce to collect the normalized data into tabular form
7. Forth MapReduce to compute cosine similarity
