# HDFS Record Normalizer
This folder contains the tools used normalize (or standardize) a Hadoop HDFS file of numerical records by performing a two-pass Hadoop MapReduce to first flatten values by column and then standardizing by subtracting the column mean and dividing by column standard deviation. 

## Included Files

### Scripts

* `normalizer.py`: Python function to drive normalization.
* `firstPassMapNormalizer.py`: Hadoop MapReduce mapper used by `normalizer.py` to flatten entries by column.
* `firstPassRedNormalizer.py`: Hadoop MapReduce reducer used by `normalizer.py` to flatten entries by column.
* `secondPassMapNormalizer.py`: Hadoop MapReduce mapper used by `normalizer.py` calculate the standardized values.
* `nullMapReduce.py`: Hadoop MapReduce mapper/reducer that passes through data without alteration

