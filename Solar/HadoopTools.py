# -*- coding: utf-8 -*-
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Contains Python wrappers for various Hadoop tools

class runHadoop: contains execution and testing methods for MapReduce

"""
import subprocess
import sys
import os
from shutil import copyfile


# Some operations need Python 2.7 to run without errors
if float(str(sys.version_info[0]) + "." + str(sys.version_info[1])) < 2.7:
    message = ("Must be using Python 2.7 or above. \n" +
               "For Dumbo users, execute \"module load python/gnu/2.7.11\"")
    raise Exception(message)


class runHadoop():
    '''
    Wrapper for running a standard Hadoop MapReduce job
    ~Methods~
    testCodes(inpFile, mapper, reducer, local): Tests the input, mapper,
        reduce python scripts using shell pipes instead of Hadoop. If the local
        flag is True then a local input file is used; if False, a tail
        (last kilobyte of data) is used from an HDFS input file.
    '''
    def __init__(self):
        self.verbose = True
        self.hLibPath = '/opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib'
        self.mrStreaming = (self.hLibPath +
                            '/hadoop-mapreduce/hadoop-streaming.jar')
        self.outputLogFile = './out.log'
        self.NumReducers = 1  # Number of reducer jobs
        self.outFile = None
        self.inpFile = None
        self.mapper = None
        self.reducer = None
        self.files = ''
        self.DEVNULL = open(os.devnull, 'w')  # stdout trash location

    def testCodes(self, inpFile, mapper, reducer, local=False):
        '''
        Performs a non-Hadoop test of the mapper and reducer scripts
        Copies the tail of the input file to a local file and uses local piping
        to create the output
        ~Inputs~
        inpFile: pathname of the index file within HDFS or a local file
        mapper: pathname of the mapper Python script
        reducer: pathname of the reducer Python script
        local: if False, gets a tail from HDFS; if True, uses local file
        ~Outputs~
        testInput.txt: File containing the input data (tail if on HDFS)
        testoutput.txt: File containing the output data
        '''
        self.inpFile = inpFile
        self.mapper = mapper
        self.reducer = reducer

        # Copy over test input
        if not local:
            if self.verbose:
                print("Pulling tail of %s from HDFS..." % self.inpFile)
            with open('testInput.txt', 'w') as out:
                subprocess.call(['hdfs dfs -tail ' + self.inpFile],
                                stdout=out, stderr=out, shell=True)
        else:
            copyfile(self.inpFile, 'testInput.txt')
        # Create shell command
        cmd = ('cat testInput.txt | python ' + self.mapper + ' | sort | ' +
               ' | python ' + self.reducer + ' &> testOutput.txt')
        if self.verbose:
            print('Running the following test code:')
            print(cmd)
        # Execute shell command
        os.system(cmd)
        if self.verbose:
            print('Output:')
            with open('testOutput.txt', 'r') as f:
                f.read()

    def MapReduce(self, inpFile, mapper, reducer,
                  outFile='output', NumReducers=None):
        '''
        Performs a standard MapReduce execution as practiced in class and
        homeworks.
        ~Inputs~
        inpFile: pathname of the index file within HDFS
        mapper: pathname of the mapper Python script
        reducer: pathname of the reducer Python script
        outFile: name of the output directory. Default is 'output'
        NumReducers: Number of reducer tasks; default is defined in __init__
        ~Outputs~
        outputLogFile (Default 'out.log'): contains Hadoop execution details
        outFile: execution results in HDFS
        '''
        self.outFile = outFile
        self.inpFile = inpFile
        self.mapper = mapper
        self.reducer = reducer
        self.files = self.files + mapper + ',' + reducer
        if NumReducers is not None:
            self.NumReducers = NumReducers

        # Check if output already exists; remove if it does
        if subprocess.call(['hdfs', 'dfs', '-find', self.outFile],
                           stdout=self.DEVNULL,
                           stderr=subprocess.STDOUT) == 0:
            if self.verbose:
                print('Output file %s already exists. Removing...' %
                      self.outFile)
            subprocess.call(['hdfs', 'dfs', '-rm', '-r', self.outFile],
                            stdout=self.DEVNULL,
                            stderr=subprocess.STDOUT)
            if self.verbose:
                print('Output file removed!')

        # Setup the MapReduce task command line arguments
        args = ['hadoop jar ' + self.mrStreaming +
                ' -Dmapreduce.job.reduces=' + str(self.NumReducers) +
                ' -files ' + self.files +
                ' -input ' + self.inpFile +
                ' -output ' + self.outFile +
                ' -mapper ' + self.mapper +
                ' -reducer ' + self.reducer]
        with open(self.outputLogFile, 'w') as oLog:
            try:
                if self.verbose:
                    print("Running MapReduce Task")
                    print("Input File: " + self.inpFile)
                    print("Output File: " + self.outFile)
                    print("Mapper: " + self.mapper)
                    print("reducer: " + self.reducer)
                    print("File String: " + self.files)
                    print("# Reducer Tasks: " + str(self.NumReducers))
                    print("Processing. See output log %s for details" %
                          self.outputLogFile)
                subprocess.check_call(args, stdout=oLog, stderr=oLog,
                                      shell=True)
            except subprocess.CalledProcessError:
                # Output the error log if Hadoop fails
                if self.verbose:
                    print("MapReduce Error Occurred:")
                    oLog.close()
                    with open(self.outputLogFile, 'r') as oLog:
                        print(oLog.read())
                raise Exception("MapReduce Job failure")
            if self.verbose:
                print("MapReduce Job Completed!")

    def tail(self):
        '''
        Print the tail of the last MapReduce execution to the log file for
        execution evaluation
        ~Inputs~
        self.outFile: HDFS output folder of the last MapReduce execution
        self.outputLogFile: local log file of the last MapReduce execution
        ~Output~
        self.outputLogFile: Appends the tail of each 'part-####' file from
            the MapReduce output to the log file
        '''
        if self.verbose:
            print("Adding tail contents to output log %s" % self.outputLogFile)
        if self.outFile is None:
            print("No previous MapReduce operation")
            return
        # Since the output directory can have a bunch of part-### files,
        # perform tail on each one
        proc = subprocess.Popen(['hdfs', 'dfs', '-ls', self.outFile],
                                stdout=subprocess.PIPE)
        files = proc.stdout.read()
        proc.kill()
        files = files.split('\n')[1:-1]
        files = [x.split()[-1] for x in files]
        with open(self.outputLogFile, 'a') as oLog:
            for f in files:
                oLog.write(f + ' Tail Contents: \n')
                oLog.flush()  # No idea why this is needed, but it is
                subprocess.call(['hdfs dfs -tail ' + f],
                                stdout=oLog, stderr=oLog, shell=True)

    def __exit__(self):
        # Ensure the devnull "file" gets closed
        self.DEVNULL.close()


