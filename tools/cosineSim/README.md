# Cosine Similarity Generator
This folder contains the tools used to calculate the cosine similarity of a column-based data file within Hadoop HDFS. 

## Included Files

### Scripts

* `cosineSim.py`: Python function to drive the cosine similarity calculation.
* `cosineSimMap.py`: Hadoop MapReduce mapper used by `cosineSim.py`
* `nullMapReduce.py`: Hadoop MapReduce mapper/reducer that passes through data without alteration

