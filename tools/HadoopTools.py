# -*- coding: utf-8 -*-
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Contains Python wrappers for various Hadoop tools

function impRunClass: Unpickles and returns an existing runHadoop class
class runHadoop: contains execution and testing methods for MapReduce

"""
import subprocess
import sys
import os
import shutil
import time
import pickle
from datetime import datetime
from shutil import copyfile


# Some operations need Python 2.7 to run without errors
if float(str(sys.version_info[0]) + "." + str(sys.version_info[1])) < 2.7:
    message = ("Must be using Python 2.7 or above. \n" +
               "For Dumbo users, execute \"module load python/gnu/2.7.11\"")
    raise Exception(message)


def impRunClass(filename='./runHadoopPickle.pkl'):
    '''
    Unpickles the HadoopTools runHadoop class instance from a pickle file
    given by filename. Allows the runHadoop class and associated objects
    to be included in the distributed MapReduce operations
    ~Input~
    filename: pathname of the pickle object containing the runHadoop class
    ~Output~
    RHC: the unpickled runHadoop class
    '''
    with open(filename, 'rb') as pf:
        RHC = pickle.load(pf)
    return(RHC)


class runHadoop():
    '''
    Wrapper for running a standard Hadoop MapReduce job
    ~Methods~
    testCodes(inpFile, mapper, reducer, local): Tests the input, mapper,
        reduce python scripts using shell pipes instead of Hadoop. If the local
        flag is True then a local input file is used; if False, a tail
        (last kilobyte of data) is used from an HDFS input file.
    MapReduce: Runs a MapReduce execution
    tail: Sample the MapReduce execution by grabbing the tail of each reducer
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
        self.proc = None
        self.pickleName = './runHadoopPickle.pkl'
        self.subTime = None
        self.included = None
        self.checkScriptFiles = True
        self.DEVNULL = open(os.devnull, 'w')  # stdout trash location
        self.files = ''


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

        # Include this class instance in the MR job
        if self.included is not None:
            with open(self.pickleName, 'wb') as pf:
                pickle.dump(self.included, pf)
            self.files += self.pickleName + ","

        # Copy over test input
        if not local:
            if self.verbose:
                print("Pulling tail of %s from HDFS..." % self.inpFile)
            with open('testInput.txt', 'w') as out:
                subprocess.call(['hdfs dfs -tail ' + self.inpFile],
                                stdout=out, stderr=out, shell=True)
        else:
            copyfile(self.inpFile, 'testInput.txt')
        # Create map-only shell command
        cmd = ('cat testInput.txt | python ' + self.mapper +
               ' &> testMapOutput.txt')
        if self.verbose:
            print('Running the following test code:')
            print(cmd)
        # Execute shell command
        os.system(cmd)
        cmd = ('cat testInput.txt | python ' + self.mapper + ' | sort' +
               ' | python ' + self.reducer + ' &> testRedOutput.txt')
        if self.verbose:
            print('Running the following test code:')
            print(cmd)
        # Execute shell command
        os.system(cmd)

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

        # Include this class instance in the MR job
        if self.included is not None:
            with open(self.pickleName, 'wb') as pf:
                pickle.dump(self.included, pf)
            self.files += self.pickleName + ","

        # Check MR run scripts for proper headers
        if self.checkScriptFiles:
            self.checkScripts()

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
                ' -mapper ' + self.mapper.split('/')[-1] +
                ' -reducer ' + self.reducer.split('/')[-1]]
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
                self.subTime = datetime.now()
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
        p = subprocess.Popen(['hdfs', 'dfs', '-ls', self.outFile],
                             stdout=subprocess.PIPE)
        files = p.stdout.read()
        p.kill()
        files = files.split('\n')[1:-1]
        files = [x.split()[-1] for x in files]
        with open(self.outputLogFile, 'a') as oLog:
            for f in files:
                oLog.write(f + ' Tail Contents: \n')
                oLog.flush()  # No idea why this is needed, but it is
                subprocess.call(['hdfs dfs -tail ' + f],
                                stdout=oLog, stderr=oLog, shell=True)

    def poolResults(self, outputFile):
        '''
        Merges the HDFS output of the MR run to a single local file
        CAUTION: Assumes data is small enough to allow for local stoarge
        ~Inputs~
        outputFile: the pathname of a single file to which output will be
        merged. If it exists, it will be overwritten.
        '''
        if os.path.exists(outputFile):
            if self.verbose:
                print('Output file %s ' % outputFile +
                      'already exists. Removing...')
            os.remove(outputFile)
        if self.verbose:
            print('Merging %s output to %s...' % (self.outFile, outputFile))
        subprocess.call(['hdfs', 'dfs', '-getmerge', self.outFile, outputFile],
                        stdout=self.DEVNULL,
                        stderr=subprocess.STDOUT)

    def include(self, name, obj):
        '''
        Includes a Python object in MapReduce execution by passing it along
        with the class pickle
        ~Input~
        name: name of the object within the self.include dictionary.
        obj: Any pickle-able (serializable) object
        ~Output~
        self.included: updates the existing runHadoop to include the object
            in the output pickle
        '''
        if self.included is None:
            self.included = {name: obj}
        else:
            self.included.update({name: obj})

    def checkScripts(self, scr=None):
        '''
        Hadoop requires a specific header to be added to the mapper and
        reducer scripts and must have \n Unix EOLs instead of \r\n Windows
        EOLs. This function screens will add the header and convert the EOLs
        prior to Hadoop submittal.
        ~Input~
        scr: Input pathname to script for check. If not provided, checks
        self.mapper and self.reducer.
        '''
        if scr is None:
            if self.verbose:
                print('Checking mapper...')
            self.checkScripts(self.mapper)
            if self.verbose:
                print('Checking reducer...')
            self.checkScripts(self.reducer)
        else:
            if self.verbose:
                print('Checking input script: %s ...' % scr)
            if not os.path.exists(scr):
                raise ValueError("Input script not found!")
            with open(scr, 'r+') as rf, open('tempFile.txt', 'w') as tf:
                tf.write('#!/usr/bin/python\n')
                for line in rf:
                    if line.strip() == '#!/usr/bin/python':
                        continue
                    tf.write(line.rstrip() + '\n')
            # Swap the new corrected file with the old one
            os.remove(scr)
            shutil.move('tempFile.txt', scr)
            os.system('chmod 755 %s' % scr)

    def __exit__(self):
        # Ensure the devnull "file" gets closed
        self.DEVNULL.close()
        # if class holds a process, kill it too
        if self.proc is not None:
            self.proc.kill()


class runHadoopBR(runHadoop):
    '''
    Wrapper for running a backgroud Hadoop MapReduce job. These classes
    allow for multiple executions at the same time.
    Subclass of the runHadoop class.
    ~Methods~
    testCodes(inpFile, mapper, reducer, local): Tests the input, mapper,
        reduce python scripts using shell pipes instead of Hadoop. If the local
        flag is True then a local input file is used; if False, a tail
        (last kilobyte of data) is used from an HDFS input file.
    MapReduce: Runs a MapReduce execution
    tail: Sample the MapReduce execution by grabbing the tail of each reducer
    '''
    def MapReduce(self, inpFile, mapper, reducer, files=None,
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
        files: additional files to include in the execution; comma separated
            w/o spaces string
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
        if files is not None:
            self.files += files

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
            self.subTime = datetime.now()
            self.proc = subprocess.Popen(args, stdout=oLog, stderr=oLog,
                                         shell=True)


class hadoopSched():
    '''
    Scheduler for batches of Hadoop MapReduce jobs
    CAUTION: NOT TESTED
    '''
    def __init__(self):
        self.doneQueue = []
        self.failedQueue = []
        self.activeQueue = []
        self.activeObjs = []
        self.waitQueue = []
        self.maxJobs = 5
        self.sleepTime = 10  # Sleep time interval of 10 seconds
        self.jobCounter = 0
        raise Exception("Feature not available/tested!")

    def add(self, inpFile, mapper, reducer, outFile, files=None,
            NumReducers=None):
        '''
        Adds a MapReduce task to the list of executions to be performed. Takes
        all the inputs used for a runHadoopBR.MapReduce execution. Must
        take a defined output file in outFile.
        '''
        self.waitQueue.append({"inpFile": inpFile,
                               "mapper": mapper,
                               "reducer": reducer,
                               "outFile": outFile,
                               "files": files,
                               "NumReducers": NumReducers,
                               "job": self.jobCounter})
        self.jobCounter += 1
    def execute(self):
        '''
        Begins batch execution of the given series of MapReduce tasks
        '''
        while len(self.waitQueue) != 0 or len(self.activeQueue) < self.maxJobs:
            currentJob = self.waitQueue.pop()
            self.activeQueue.append(currentJob)
            currentObj = runHadoopBR()
            currentObj.verbose = False
            currentObj.MapReduce(inpFile=currentJob['inpFile'],
                                 mapper=currentJob['mapper'],
                                 reducer=currentJob['reducer'],
                                 files=currentJob['files'],
                                 outFile=currentJob['outFile'],
                                 NumReducers=currentJob['NumReducers'])
            self.activeObjs.append(currentObj)
        while len(self.waitQueue) != 0 or len(self.activeQueue) != 0:
            time.sleep(self.sleepTime)
            os.system('clear')  # Clear screen for a nice UI
            mes = 'Job: \t Status: \t Time (min): \n'
            print(mes)
            for i, obj in enumerate(self.activeObjs):
                status = obj.proc.poll()
                runTime = (datetime.now() - obj.subTime).days() * 24 * 60
                if status is None:  # Job still running
                    mes = (str(self.activeQueue[i]["job"]) +
                           "\tRunning\t" +
                           str(runTime))
                    print(mes)
                elif status == 0:  # Job finished successfully
                    mes = (str(self.activeQueue[i]["job"]) +
                           "\tFinished\t" +
                           str(runTime))
                    print(mes)
                    self.doneQueue.append(self.activeQueue.pop(i))
                    self.activeObjs.remove(i)
                    # Add another waiting job
                    if len(self.waitQueue) != 0:
                        currentJob = self.waitQueue.pop()
                        self.activeQueue.append(currentJob)
                        currentObj = runHadoopBR()
                        currentObj.verbose = False
                        currentObj.MapReduce(inpFile=currentJob['inpFile'],
                                             mapper=currentJob['mapper'],
                                             reducer=currentJob['reducer'],
                                             files=currentJob['files'],
                                             outFile=currentJob['outFile'],
                                             NumReducers=currentJob['NumReducers'])
                        self.activeObjs.append(currentObj)
                else:  # Job failed
                    mes = (str(self.activeQueue[i]["job"]) +
                           "\tFailed\t" +
                           str(runTime))
                    print(mes)
                    self.failedQueue.append(self.activeQueue.pop(i))
                    self.activeObjs.remove(i)
                    # Add another waiting job
                    if len(self.waitQueue) != 0:
                        currentJob = self.waitQueue.pop()
                        self.activeQueue.append(currentJob)
                        currentObj = runHadoopBR()
                        currentObj.verbose = False
                        currentObj.MapReduce(inpFile=currentJob['inpFile'],
                                             mapper=currentJob['mapper'],
                                             reducer=currentJob['reducer'],
                                             files=currentJob['files'],
                                             outFile=currentJob['outFile'],
                                             NumReducers=currentJob['NumReducers'])
                        self.activeObjs.append(currentObj)
                print("Jobs Queued: %d" % len(self.waitQueue))
        # All tasks finished
        print("Batch processing completed!")
        print("Job Completed: %d" % len(self.doneQueue))
        print("Tasks Failed: %d" % len(self.failedQueue))
        if len(self.failedQueue) != 0:
            print("Failed Jobs:")
            for job in self.failedQueue:
                print(job)















