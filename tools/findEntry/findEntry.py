"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Uses MapReduce to find an entry or entries of an HDFS file that match a given
 value. Returns all rows that contain the entry in any column or in the given
 column.

@author: Cody Gilbert
"""

import HadoopTools as ht
def findEntry(value, inputFileName, outputFileName='findEntryResults.txt',
              col=None, delim='\t'):
    '''
    Function for searching an HDFS file and returning all records that
    match a given string at either a given column or any column.
    ~Inputs~
    value: String to be searched
    inputFileName: HDFS file path to be searched
    outputFileName: local path for pooled results. Note: Assumes results are
        suffiently small to fit on local harddrive space.
    col: Column in which to search. If None, will search all columns.
        **NOTE:** 0-indexed; first column at index 0
    delim: file delimiter character
    ~Outputs~
    Creates an HDFS file 'findOutput' with resulting records, and a local file
    outputFileName containing all the merged found records
    '''
    inputFile = inputFileName
    findEntryMapScript = './findEntryMap.py'
    findEntryRedScript = './nullMapReduce.py'
    runner = ht.runHadoop()
    runner.outputLogFile = './findEntry.log'
    runner.verbose = False
    runner.include('value', value)
    runner.include('col', col)
    runner.include('delim', delim)
    runner.MapReduce(inpFile=inputFile,
                     mapper=findEntryMapScript,
                     reducer=findEntryRedScript,
                     outFile='findOutput',
                     NumReducers=32)
    runner.poolResults(outputFileName)
