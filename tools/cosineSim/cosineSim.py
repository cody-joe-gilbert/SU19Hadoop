"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Performs cosine similarity summaries for each line in the input dataset,
given an input column


@author: Cody Gilbert
"""

import HadoopTools as ht


def cosineSim(inputRecord, inputFileName,
              outputFileName='cosineSimResults.txt',
              skipCols=[0], delim='\t'):
    '''
    Function for calculating a cosine similarity measure on all records in
    and HDFS data file, given a comparison record of the same format as that
    file.
    ~Inputs~
    inputRecord: Record as comparison string for calculation
    inputFileName: HDFS file path of input file
    outputFileName: local path for pooled results. Note: Assumes results are
        suffiently small to fit on local harddrive space.
    col: List of columns indicies to skip in calculation. Default skips the
        first key value and uses all other columns
        **NOTE:** 0-indexed; first column at index 0
    delim: file delimiter character
    ~Outputs~
    Creates an HDFS file 'cosineOutput' with resulting records, and a local
    file outputFileName containing all the merged found records
    '''
    inputFile = inputFileName
    cosineSimMapScript = './cosineSimMap.py'
    cosineSimRedScript = './nullMapReduce.py'
    runner = ht.runHadoop()
    runner.outputLogFile = './consineSim.log'
    runner.verbose = False
    runner.include('inputRecord', inputRecord)
    runner.include('skipCols', skipCols)
    runner.include('delim', delim)
    runner.MapReduce(inpFile=inputFile,
                     mapper=cosineSimMapScript,
                     reducer=cosineSimRedScript,
                     outFile='cosineSim',
                     NumReducers=32)
    runner.poolResults(outputFileName)
