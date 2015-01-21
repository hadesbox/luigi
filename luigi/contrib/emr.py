'''
A common module for running AWS EMR Jobs
'''

import abc
import logging
import luigi
import time

import boto.emr

from boto.emr.step import StreamingStep
from luigi.s3 import S3FlagTarget
from boto.emr.step import HiveStep
from boto.emr.step import InstallHiveStep
from boto.emr.step import PigStep
from boto.emr.step import InstallPigStep

logger = logging.getLogger('luigi-interface')

def waitForCompletion(conn, jobid, s3output):
    while 1:

        status = conn.describe_jobflow(jobid)
        logger.info(time.strftime("%Y/%m/%d-%H:%M:%S") + " " + jobid +" " + status.state)

        logger.info("PARAMS ARE " + str(jobid) +  " " + str(s3output))

        if str(status.state) == "COMPLETED" or str(status.state) == "SHUTTING_DOWN" :
            createS3Flag(s3output)
            break

        time.sleep(5)

    return

def createS3Flag(output):

    temp = output[6:]
    _b = temp[:temp.index("/")]
    _k = temp[temp.index("/")+1:]+'_SUCCESS'
    logger.info("bucket is " + _b)
    logger.info("key is " + _k)

    conn = boto.connect_s3()
    bucket = conn.get_bucket(_b)

    key = bucket.new_key(_k)
    key.set_contents_from_string('')

class RunStreamingStep(luigi.Task):
    """
    A luigi task to run a StreamingStep on AWS EMR

    Usage:
    Subclass and override the required `s3input`, `s3output`, `s3output`, 
    `mapper`, `reducer`, `loguri` attributes.
    """

    def s3input(self):
        return None

    def s3output(self):
        return None

    def mapper(self):
        return None

    def reducer(self):
        return None

    def name(self):
        return None

    def loguri(self):
        return None

    def jobid(self):
        return None  

    def run(self):

        step = StreamingStep(name=self.name,
            mapper=self.mapper,
            reducer=self.reducer,
            input=self.s3input,
            output=self.s3output)

        conn = boto.emr.connect_to_region('us-east-1')

        self.jobid = conn.run_jobflow(name=self.name+'-StreamingJobFlow', log_uri=self.loguri, steps=[step])

        print "JobId is " + self.jobid 

        waitForCompletion(conn, self.jobid, self.s3output)

    def output(self):
        result = S3FlagTarget(self.s3output)
        return result



class RunPigStep(luigi.Task):
    """
    A luigi task to run a PigScript on AWS EMR

    Usage:
    Subclass and override the required `s3output`, `s3output`, 
    `pig_file`, `pig_versions`, `pig_args` attributes.
    """

    def s3output(self):
        return None

    def pig_file(self):
        return None

    def pig_versions(self):
        return None

    def pig_args(self):
        return None

    def name(self):
        return None

    def loguri(self):
        return None

    def jobid(self):
        return None  

    def ami_version(self):
        return '2.0'

    def run(self):

        installStep = InstallPigStep(pig_versions='latest')

        step = PigStep(name=self.name,
            pig_file=self.pig_file,
            pig_versions=self.pig_versions,
            pig_args=self.pig_args)

        conn = boto.emr.connect_to_region('us-east-1')

        print "AMI VERSION IS "+ str(self.ami_version)

        self.jobid = conn.run_jobflow(ami_version=self.ami_version, name=self.name+'-Stream-JobFlow', enable_debugging=False, log_uri=self.loguri, steps=[installStep, step])

        print "JobId is " + self.jobid 

        waitForCompletion(conn, self.jobid, self.s3output)

    def output(self):
        result = S3FlagTarget(self.s3output)
        return result


class RunHiveStep(luigi.Task):
    """
    A luigi task to run a HiveScript on AWS EMR

    Usage:
    Subclass and override the required `s3output`, `s3output`, 
    `mapper`, `reducer`, `loguri` attributes.
    """

    def s3output(self):
        return None

    def name(self):
        return None

    def loguri(self):
        return None

    def jobid(self):
        return None  

    def ami_version(self):
        return None

    def hive_versions(self):
        return None

    def hive_file(self):
        return None

    def hive_args(self):
        return None

    def run(self):

        installStep = InstallHiveStep(hive_versions=self.hive_versions)

        queryStep = HiveStep(name=self.name, hive_file=self.hive_file, hive_versions=self.hive_versions, hive_args=self.hive_args)

        conn = boto.emr.connect_to_region('us-east-1')

        self.jobid = conn.run_jobflow(master_instance_type='m1.large', ami_version='3.3', name=self.name+'-HIVE-jobflow', enable_debugging=False, log_uri=self.loguri, steps=[installStep, queryStep])

        print "JobId is " + self.jobid 

        waitForCompletion(conn, self.jobid, self.s3output)

    def output(self):
        result = S3FlagTarget(self.s3output)
        return result