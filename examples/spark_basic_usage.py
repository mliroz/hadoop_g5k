import os

from execo_g5k import get_oar_job_nodes
from hadoop_g5k import HadoopV2Cluster
from hadoop_g5k.ecosystem.spark import YARN_MODE, SparkCluster, \
    JavaOrScalaSparkJob

# Parameters
hosts = get_oar_job_nodes(int(os.environ["OAR_JOB_ID"]), None)

hadoop_tar_file = "/home/mliroz/public/sw/hadoop/hadoop-2.6.0.tar.gz"
spark_tar_file = "/home/mliroz/public/sw/spark/spark-1.5.1-bin-hadoop2.6.tgz"

jar_path = "/home/mliroz/public/sw/spark/spark-examples-1.5.1-hadoop2.6.0.jar"

# Create and configure Hadoop cluster
hc = HadoopV2Cluster(hosts)
hc.bootstrap(hadoop_tar_file)
hc.initialize()
hc.start_and_wait()

# Create and configure Spark cluster
sc = SparkCluster(YARN_MODE, hadoop_cluster=hc)
sc.bootstrap(spark_tar_file)
sc.initialize()
sc.start()

# Execute job
main_class = "org.apache.spark.examples.SparkPi"
params = []

job = JavaOrScalaSparkJob(job_path=jar_path,
                          app_params=params,
                          main_class=main_class)

(out, err) = sc.execute_job(job, verbose=False)

if job.success:
    print "The job finished successfully"
else:
    print "There was a problem during job execution"
