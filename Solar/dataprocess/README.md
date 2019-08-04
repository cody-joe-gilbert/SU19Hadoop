# Main Solar Data Processing
This folder contains the tools used to featurize, standardize, and produce the
output cosine similarities used for the combination codes.

## Data Files

* `dataProcess.py1` Python script that drives the data processing.
* `widenDataMap.py` Hadoop MapReduce mapper used by `dataProcess.py` to featurize the solar input data.
* `widenDataRed.py` Hadoop MapReduce reducer used by `dataProcess.py` to featurize the solar input data.
