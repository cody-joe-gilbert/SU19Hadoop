# Common Project Tools
This folder contains the common project team scripts for various Hadoop and NYU Dumbo cluster applications.


## Folders
* `profiler` contains tools for profiling big data in HDFS using Hadoop MapReduce
* `cosineSim` contains tools for calculating cosine similarity 
* `findEntry` tools for finding records in a Hadoop HDFS file
* `profiler` automated Hadoop HDFS file profiling tools
* `normalizer` normalizes (standardizes) a Hadoop HDFS file of numerical data
* `voronoiPlotting` creates a Voronoi diagram for the US using Geopandas


### HadoopTools.py
Contains classes that form Python wrappers around common Hadoop tools. The *runHadoop* class automates the dispatch of MapReduce jobs and provides various methods of file management.

