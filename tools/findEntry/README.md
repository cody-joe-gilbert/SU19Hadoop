# HDFS File Record Finder
This folder contains the tools used to find a record or records that contain a given string from a file within Hadoop HDFS.

## Included Files

### Scripts

* `findEntry.py`: Python function to drive the record finding. See script be details of use.
* `findEntryMap.py`: Hadoop MapReduce mapper used by `findEntry.py`
* `nullMapReduce.py`: Hadoop MapReduce mapper/reducer that passes through data without alteration

