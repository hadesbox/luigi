import luigi
import time
import os
import shutil

from luigi.contrib.emr import RunStreamingStep
from luigi.contrib.emr import RunPigStep
from luigi.contrib.emr import RunHiveStep

class RunHiveScript(RunHiveStep):

    timepref = "444" #time.strftime("%Y%m%d-%H%M%S")

    hive_versions = 'latest'
    hive_file = 's3://us-east-1.elasticmapreduce.samples/cloudfront/code/Hive_CloudFront.q'
    hive_args = ['-d','INPUT=s3://us-east-1.elasticmapreduce.samples', '-d','OUTPUT=s3n://luigi-emr-test/output/hive_output-%s' % timepref]

    s3output = 's3n://luigi-emr-test/output/hive_output-%s' % timepref+'/'
    name = 'Luigi-HiveScript-example-' + timepref
    loguri = 's3://luigi-emr-test/jobflow_logs/hivescript_logs-'+timepref
    ami_version = '3.3'
  
    def requires(self):
        yield RunExampleStreaming()

class RunExamplePigFather(RunPigStep):

    timepref = "333" #time.strftime("%Y%m%d-%H%M%S")

    pig_file = "s3://elasticmapreduce/samples/pig-apache/do-reports2.pig"
    pig_versions = "latest"
    pig_args = ['-p', 'INPUT=s3://elasticmapreduce/samples/pig-apache/input', '-p', 'OUTPUT=s3n://luigi-emr-test/output/pig_output-%s' % timepref]
    s3output = 's3n://luigi-emr-test/output/pig_output-%s' % timepref+'/'
    name = 'Luigi-PigScript-example-' + timepref
    loguri = 's3://luigi-emr-test/jobflow_logs/pigscript_logs-'+timepref
    ami_version = '2.0'
  
    def requires(self):
        yield RunExampleStreaming()

class RunExampleStreaming(RunStreamingStep):

    timepref = "111" #time.strftime("%Y%m%d-%H%M%S")

    s3input = "s3n://elasticmapreduce/samples/wordcount/input/"
    s3output = "s3n://luigi-emr-test/output/wordcount_output_"+timepref+"/"
    mapper='s3n://elasticmapreduce/samples/wordcount/wordSplitter.py'
    reducer='aggregate'
    name = 'Luigi-wordcount-example-' + timepref
    loguri = 's3://luigi-emr-test/jobflow_logs/wordcount_output-'+timepref

    def requires(self):
        for i in xrange(5):
            yield Bar(i)

class Bar(luigi.Task):

    num = luigi.IntParameter()

    def run(self):
        time.sleep(1)
        self.output().open('w').close()

    def output(self):
        time.sleep(1)
        return luigi.LocalTarget('/tmp/bar/%d' % self.num)


if __name__ == "__main__":
    if os.path.exists('/tmp/bar'):
        shutil.rmtree('/tmp/bar')

    luigi.run(['--task', 'RunHiveScript', '--workers', '2'], use_optparse=True)
