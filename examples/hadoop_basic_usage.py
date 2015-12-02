import os

from execo_g5k import get_oar_job_nodes
from hadoop_g5k import HadoopCluster, HadoopJarJob, HadoopV2Cluster

# Parameters
hosts = get_oar_job_nodes(int(os.environ["OAR_JOB_ID"]), None)
hadoop_tar_file = "/home/mliroz/public/sw/hadoop/hadoop-2.6.0.tar.gz"
jar_path = "/home/mliroz/public/sw/hadoop/hadoop-mapreduce-examples-2.6.0.jar"

# Create and configure Hadoop cluster
hc = HadoopV2Cluster(hosts)
hc.bootstrap(hadoop_tar_file)
hc.initialize()
hc.start_and_wait()

# Execute job
job_output_path = "/output"
job = HadoopJarJob(jar_path, ["randomtextwriter", job_output_path])
(out, err) = hc.execute_job(job, verbose=False)

if job.success:
    print "The job finished successfully (with id %s)" % job.job_id
else:
    print "There was a problem during job execution"
