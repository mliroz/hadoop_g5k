import getpass
import os

from execo import Get, Remote
from execo.process import SshProcess
from execo_engine import logger

from hadoop_g5k.cluster import HadoopCluster
from hadoop_g5k.util.conf import replace_in_xml_file

# Configuration files
CORE_CONF_FILE = "core-site.xml"
HDFS_CONF_FILE = "hdfs-site.xml"
MR_CONF_FILE = "mapred-site.xml"
YARN_CONF_FILE = "yarn-site.xml"

# Default parameters
DEFAULT_HADOOP_BASE_DIR = "/tmp/hadoop"
DEFAULT_HADOOP_CONF_DIR = DEFAULT_HADOOP_BASE_DIR + "/etc/hadoop"
DEFAULT_HADOOP_LOGS_DIR = DEFAULT_HADOOP_BASE_DIR + "/logs"
DEFAULT_HADOOP_TEMP_DIR = DEFAULT_HADOOP_BASE_DIR + "/tmp"

DEFAULT_HADOOP_HDFS_PORT = 54310
DEFAULT_HADOOP_MR_PORT = 54311

DEFAULT_HADOOP_LOCAL_CONF_DIR = "conf"


class HadoopV2Cluster(HadoopCluster):
    """This class manages the whole life-cycle of a Hadoop cluster with version
    2 or higher.

    It adds some functionality over HadoopCluster: the YARN server can be
    managed independently. It also warns when trying to use functionality no
    longer supported by this version of Hadoop.
    """

    # Cluster state
    running_yarn = False    
    
    # Default properties
    defaults = {
        "hadoop_base_dir": DEFAULT_HADOOP_BASE_DIR,
        "hadoop_conf_dir": DEFAULT_HADOOP_CONF_DIR,
        "hadoop_logs_dir": DEFAULT_HADOOP_LOGS_DIR,
        "hadoop_temp_dir": DEFAULT_HADOOP_TEMP_DIR,
        "hdfs_port": str(DEFAULT_HADOOP_HDFS_PORT),
        "mapred_port": str(DEFAULT_HADOOP_MR_PORT),

        "local_base_conf_dir": DEFAULT_HADOOP_LOCAL_CONF_DIR
    }    
    
    def __init__(self, hosts, topo_list=None, config_file=None):
        """Create a new Hadoop cluster with the given hosts and topology.
        
        Args:
          hosts (list of Host):
            The hosts to be assigned a topology.
          topo_list (list of str, optional):
            The racks to be assigned to each host. len(hosts) should be equal to
            len(topo_list).
          config_file (str, optional):
            The path of the config file to be used.
        """
        
        super(HadoopV2Cluster, self).__init__(hosts, topo_list, config_file)

        self.conf_mandatory_files = [CORE_CONF_FILE,
                                     HDFS_CONF_FILE,
                                     MR_CONF_FILE,
                                     YARN_CONF_FILE]
        
        self.sbin_dir = self.base_dir + "/sbin"

    def _check_version_compliance(self):
        if self.get_major_version() != 2:
            logger.error("Version of HadoopCluster is not compliant with the "
                        "distribution provided in the bootstrap option. Use "
                        "the appropiate parameter for --version when creating "
                        "the cluster or use another distribution.")
            return False
        else:
            return True

    def _initialize_conf(self):
        """Merge locally-specified configuration files with default files
        from the distribution"""

        action = Remote("cp " + os.path.join(self.conf_dir,
                                             MR_CONF_FILE + ".template ") +
                        os.path.join(self.conf_dir, MR_CONF_FILE),
                        self.hosts)
        action.run()

        super(HadoopV2Cluster, self)._initialize_conf()

    def _get_cluster_params(self, conf_dir, default_tuning=False):
        """Define host-dependant parameters.

           Args:
             conf_dir (str):
               The path of the directory with the configuration files.
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        params = {}

        # Calculate container maximums (for NMs and scheduler)
        # Maximum scheduled container is maximum allowed container in the
        # whole cluster
        sch_max_mem = 0
        sch_max_cores = 0
        for cluster in self.hw.get_clusters():

            num_cores = cluster.get_num_cores()
            available_mem = cluster.get_memory()
            total_conts_mem = min(available_mem - 2 * 1024,
                                  int(0.75 * available_mem))
            max_cont_mem = total_conts_mem
            sch_max_mem = max(sch_max_mem, max_cont_mem)
            sch_max_cores = max(sch_max_cores, num_cores)

            cluster_params = {
                "max_cont_cores": num_cores,
                "max_cont_mem": max_cont_mem,
            }

            params[cluster.get_name()] = cluster_params

        # Global parameters
        params["global"] = {
            "sch_max_mem": sch_max_mem,
            "sch_max_cores": sch_max_cores
        }

        if default_tuning:

            # Calculate min container memory for the whole cluster
            min_cluster_mem = self.hw.get_max_cores_cluster().get_memory()
            for cluster in self.hw.get_clusters():
                min_cluster_mem = min(min_cluster_mem, cluster.get_memory())

            if min_cluster_mem < 8:
                min_cont_mem = 512
            elif min_cluster_mem < 24:
                min_cont_mem = 1024
            else:
                min_cont_mem = 2048

            # Calculate params for map and reduce tasks
            min_map_mem = sch_max_mem
            for cluster in self.hw.get_clusters():
                num_conts = min(int(1.5 * cluster.get_num_cores()),
                                total_conts_mem / min_cont_mem)
                map_mem = max(min_cont_mem, total_conts_mem / num_conts)

                min_map_mem = min(min_map_mem, map_mem)

            map_mem = min_map_mem
            red_mem = 2 * map_mem
            map_java_heap = int(map_mem * 0.8)  # 20% JVM non-heap overhead
            red_java_heap = int(red_mem * 0.8)
            io_sort_mb = max(100, map_java_heap / 2)
            io_sort_factor = max(10, io_sort_mb / 10)

            params["global"].update({
                "min_cont_mem": min_cont_mem,
                "map_mem": map_mem,
                "red_mem": red_mem,
                "map_java_heap": map_java_heap,
                "red_java_heap": red_java_heap,
                "io_sort_mb": io_sort_mb,
                "io_sort_factor": io_sort_factor
            })

        return params

    def _set_common_params(self, params, conf_dir, default_tuning=False):
        """Replace common parameters. Some user-specified values are
        overwritten.

           Args:
             params (str):
               Already defined parameters over all the clusters.
             conf_dir (str):
               The path of the directory with the configuration files.
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        core_file = os.path.join(conf_dir, CORE_CONF_FILE)
        yarn_file = os.path.join(conf_dir, YARN_CONF_FILE)
        mr_file = os.path.join(conf_dir, MR_CONF_FILE)

        global_params = params["global"]
        sch_max_mem = global_params["sch_max_mem"]
        sch_max_cores = global_params["sch_max_cores"]

        # General and HDFS
        replace_in_xml_file(core_file,
                            "fs.defaultFS",
                            "hdfs://%s:%d/" % (self.master.address,
                                               self.hdfs_port),
                            create_if_absent=True,
                            replace_if_present=True)
        replace_in_xml_file(core_file,
                            "hadoop.tmp.dir",
                            self.hadoop_temp_dir,
                            create_if_absent=True,
                            replace_if_present=True)
        replace_in_xml_file(core_file,
                            "topology.script.file.name",
                            self.conf_dir + "/topo.sh",
                            create_if_absent=True,
                            replace_if_present=True)

        # YARN
        replace_in_xml_file(yarn_file,
                            "yarn.resourcemanager.hostname",
                            self.master.address,
                            create_if_absent=True)

        replace_in_xml_file(mr_file,
                            "mapreduce.framework.name", "yarn",
                            create_if_absent=True,
                            replace_if_present=True)

        replace_in_xml_file(yarn_file,
                            "yarn.nodemanager.aux-services",
                            "mapreduce_shuffle",
                            create_if_absent=True,
                            replace_if_present=True)

        replace_in_xml_file(yarn_file,
                            "yarn.scheduler.maximum-allocation-mb",
                            str(sch_max_mem),
                            create_if_absent=True,
                            replace_if_present=default_tuning)
        replace_in_xml_file(yarn_file,
                            "yarn.scheduler.maximum-allocation-vcores",
                            str(sch_max_cores),
                            create_if_absent=True,
                            replace_if_present=default_tuning)

        if default_tuning:

            # YARN
            min_cont_mem = global_params["min_cont_mem"]
            replace_in_xml_file(yarn_file,
                                "yarn.scheduler.minimum-allocation-mb",
                                str(min_cont_mem),
                                create_if_absent=True,
                                replace_if_present=True)

            # MR memory settings
            map_mem = global_params["map_mem"]
            red_mem = global_params["red_mem"]
            map_java_heap = global_params["map_java_heap"]
            red_java_heap = global_params["red_java_heap"]

            replace_in_xml_file(mr_file,
                                "mapreduce.map.memory.mb",
                                str(map_mem),
                                create_if_absent=True,
                                replace_if_present=True)
            replace_in_xml_file(mr_file,
                                "mapreduce.map.java.opts",
                                "-Xmx%dm" % map_java_heap,
                                create_if_absent=True,
                                replace_if_present=True)

            replace_in_xml_file(mr_file,
                                "mapreduce.reduce.memory.mb",
                                str(red_mem),
                                create_if_absent=True,
                                replace_if_present=True)
            replace_in_xml_file(mr_file,
                                "mapreduce.reduce.java.opts",
                                "-Xmx%dm" % red_java_heap,
                                create_if_absent=True,
                                replace_if_present=True)

            # MR core settings
            replace_in_xml_file(mr_file,
                                "mapreduce.map.cpu.vcores", "1",
                                create_if_absent=True,
                                replace_if_present=True)
            replace_in_xml_file(mr_file,
                                "mapreduce.map.reduce.vcores", "1",
                                create_if_absent=True,
                                replace_if_present=True)

            # MR shuffle
            io_sort_mb = global_params["io_sort_mb"]
            io_sort_factor = global_params["io_sort_factor"]

            replace_in_xml_file(mr_file,
                                "mapreduce.map.output.compress", "true",
                                create_if_absent=True,
                                replace_if_present=True)
            replace_in_xml_file(mr_file,
                                "mapreduce.task.io.sort.mb",
                                str(io_sort_mb),
                                create_if_absent=True,
                                replace_if_present=True)
            replace_in_xml_file(mr_file,
                                "mapreduce.task.io.sort.factor",
                                str(io_sort_factor),
                                create_if_absent=True,
                                replace_if_present=True)

    def _set_cluster_params(self, cluster, params,
                            conf_dir, default_tuning=False):
        """Replace cluster-dependent parameters

           Args:
             cluster (PhysicalCluster):
               The PhysicalCluster object to take into account in the
               configuration.
             params (str):
               Already defined parameters over all the clusters.
             conf_dir (str):
               The path of the directory with the configuration files.
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        yarn_file = os.path.join(conf_dir, YARN_CONF_FILE)

        cname = cluster.get_name()
        max_mem = params[cname]["max_cont_mem"]
        max_cores = params[cname]["max_cont_cores"]

        replace_in_xml_file(yarn_file,
                            "yarn.nodemanager.resource.memory-mb",
                            str(max_mem),
                            create_if_absent=True,
                            replace_if_present=default_tuning)
        replace_in_xml_file(yarn_file,
                            "yarn.nodemanager.resource.cpu-vcores",
                            str(max_cores),
                            create_if_absent=True,
                            replace_if_present=default_tuning)

    def start(self):
        """Start the NameNode and DataNodes and then the YARN ResourceManager
        and NodeManagers."""

        self._check_initialization()

        self.start_dfs()
        self.start_yarn()

        self.running = True

    def start_and_wait(self):
        """Start the Namenode and DataNodes and then the YARN ResourceManager
        and NodeManagers. Wait for exiting safemode before continuing."""

        self._check_initialization()

        self.start_dfs_and_wait()
        self.start_yarn()

        self.running = True

    def start_dfs_and_wait(self):
        super(HadoopV2Cluster, self).start_dfs_and_wait()
        if self.running_yarn:
            self.running = True

    def start_yarn(self):
        """Start the YARN ResourceManager and NodeManagers."""

        logger.info("Starting YARN")
        
        self._check_initialization()
        
        proc = SshProcess(self.sbin_dir + "/start-yarn.sh", self.master)
        proc.run()        
        
        if not proc.finished_ok:
            logger.warn("Error while starting YARN")
        else:
            #TODO: get success or not from super.
            self.running_yarn = True
            if self.running_dfs:
                self.running = True

    def start_map_reduce(self):
        """Do nothing. MapReduce has no specific service in Hadoop 2.*"""

        logger.warn("MapReduce does not use any specific service in this "
                    "version of Hadoop.")

    def start_map_reduce_and_wait(self):
        """Do nothing. MapReduce has no specific service in Hadoop 2.*"""

        logger.warn("MapReduce does not use any specific service in this "
                    "version of Hadoop.")
        
    def stop(self):
        """Stop the JobTracker and TaskTrackers and then the NameNode and
        DataNodes."""

        self._check_initialization()

        self.stop_yarn()
        self.stop_dfs()

        self.running = False        
        
    def stop_yarn(self):
        """Stop the YARN ResourceManager and NodeManagers."""
        
        self._check_initialization()

        logger.info("Stopping YARN")

        proc = SshProcess(self.sbin_dir + "/stop-yarn.sh", self.master)
        proc.run()
        
        if not proc.finished_ok:
            logger.warn("Error while stopping YARN")
        else:
            self.running_yarn = False

    def stop_map_reduce(self):
        """Do nothing. MapReduce has no specific service in Hadoop 2.*"""

        logger.warn("MapReduce does not use any specific service in this "
                    "version of Hadoop.")

    def copy_history(self, dest, job_ids=None):
        """Copy history logs from dfs.

        Args:
          dest (str):
            The path of the local dir where the logs will be copied.
          job_ids (list of str, optional):
            A list with the ids of the jobs for which the history should be
            copied. If nothing is passed, the history of all jobs is copied.
        """

        if not os.path.exists(dest):
            logger.warning("Destination directory " + dest +
                           " does not exist. It will be created")
            os.makedirs(dest)

        # Dirs used
        user_login = getpass.getuser()
        hist_dfs_dir = "/tmp/hadoop-yarn/staging/history/done_intermediate/" + \
                       user_login
        hist_tmp_dir = "/tmp/hadoop_hist"

        # Remove file in tmp dir if exists
        proc = SshProcess("rm -rf " + hist_tmp_dir, self.master)
        proc.run()

        # Get files in master
        if job_ids:
            proc = SshProcess("mkdir " + hist_tmp_dir, self.master)
            proc.run()
            for jid in job_ids:
                self.execute("fs -get " + hist_dfs_dir + "/" + jid + "* " +
                             hist_tmp_dir, verbose=False)
        else:
            self.execute("fs -get " + hist_dfs_dir + " " + hist_tmp_dir,
                         verbose=False)

        # Copy files from master
        action = Get([self.master], [hist_tmp_dir], dest)
        action.run()

    def clean_history(self):
        """Remove history."""

        logger.info("Cleaning history")

        restop = False
        if not self.running:
            logger.warn("The cluster needs to be running before cleaning.")
            self.start()
            restop = True

        user_login = getpass.getuser()
        hist_dfs_dir = "/tmp/hadoop-yarn/staging/history/done_intermediate/" +\
                       user_login
        self.execute("fs -rm -R " + hist_dfs_dir, verbose=False)

        if restop:
            self.stop()
