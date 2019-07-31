"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Uses MapReduce to normalize an HDFS file of numeric values.

@author: Cody Gilbert
"""

import HadoopTools as ht
def normalize(inputFileName, outputFileName='normalizedData.txt',
              skipCols=[0], delim='\t'):
    '''
    Function for searching an HDFS file and returning all records that
    match a given string at either a given column or any column.
    ~Inputs~
    inputFileName: HDFS file path to be searched
    outputFileName: local path for pooled results. Note: Assumes results are
        suffiently small to fit on local harddrive space.
    skipCols: Columns to skip in normalization
    delim: file delimiter character
    ~Outputs~
    Creates an HDFS file outputFileName with resulting records
    '''
    # first-pass creates the normalization weights
    inputFile = inputFileName
    MapScript = './firstPassMapNormalizer.py'
    RedScript = './firstPassRedNormalizer.py'
    runner = ht.runHadoop()
    runner.outputLogFile = './normalization.log'
    runner.verbose = False
    runner.include('skipCols', skipCols)
    runner.include('delim', delim)
    runner.MapReduce(inpFile=inputFile,
                     mapper=MapScript,
                     reducer=RedScript,
                     outFile='normWeights',
                     NumReducers=32)
    runner.poolResults('normWeights.txt')
    stats = {}
    with open('normWeights.txt', 'r') as wf:
        for line in wf:
            sline = line.strip().split("\t")
            col = int(sline[0])
            mean = float(sline[1])
            stdev = float(sline[2])
            stats.update({col: [mean, stdev]})
    # second pass divides entries by their normalization weights and saves
    inputFile = inputFileName
    MapScript = './secondPassMapNormalizer.py'
    RedScript = './nullMapReduce.py'
    runner = ht.runHadoop()
    runner.outputLogFile = './standardization.log'
    runner.verbose = False
    runner.include('delim', delim)
    runner.include('stats', stats)
    runner.MapReduce(inpFile=inputFile,
                     mapper=MapScript,
                     reducer=RedScript,
                     outFile=outputFileName,
                     NumReducers=32)
