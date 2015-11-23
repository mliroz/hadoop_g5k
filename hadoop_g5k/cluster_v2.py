import getpass
import os
import shutil
import tempfile

from execo import Get, Remote
from execo.process import SshProcess
from execo_engine import logger
from execo_g5k import get_host_attributes

from hadoop_g5k.cluster import HadoopCluster
from hadoop_g5k.util import replace_in_xml_file

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
        
        self.sbin_dir = self.base_dir + "/sbin"

    def _copy_base_conf(self):
        """Copy base configuration files to tmp dir."""

        self.temp_conf_dir = tempfile.mkdtemp("", "hadoop-", "/tmp")
        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.temp_conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        mandatory_files = [CORE_CONF_FILE, HDFS_CONF_FILE, MR_CONF_FILE,
                           YARN_CONF_FILE]

        missing_conf_files = mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        logger.info("Copying missing conf files from master: " + str(
            missing_conf_files))

        remote_missing_files = [os.path.join(self.conf_dir, f)
                                for f in missing_conf_files]

        action = Get([self.master], remote_missing_files, self.temp_conf_dir)
        action.run()

    def _configure_servers(self, hosts=None):
        """Configure servers and host-dependant parameters.

           Args:
             hosts (list of Host, optional):
               The list of hosts to take into account in the configuration. If
               not specified, all the hosts of the Hadoop cluster are used. The
               first host of this list is always used as the reference.
        """

        if not hosts:
            hosts = self.hosts

        # Node variables
        host_attrs = get_host_attributes(hosts[0])
        num_cores = host_attrs[u'architecture'][u'smt_size']
        available_memory = (int(host_attrs[u'main_memory'][u'ram_size']) /
                            (1024 * 1024))

        # General and HDFS
        replace_in_xml_file(os.path.join(self.temp_conf_dir, CORE_CONF_FILE),
                            "fs.defaultFS",
                            "hdfs://" + self.master.address + ":" +
                                        str(self.hdfs_port) + "/",
                            True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, CORE_CONF_FILE),
                            "hadoop.tmp.dir",
                            self.hadoop_temp_dir, True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, CORE_CONF_FILE),
                            "topology.script.file.name",
                            self.conf_dir + "/topo.sh", True)

        # YARN
        total_containers_mem_mb = min(available_memory - 2 * 1024,
                                      int(0.75 * available_memory))
        max_container_mem_mb = total_containers_mem_mb

        replace_in_xml_file(os.path.join(self.temp_conf_dir, YARN_CONF_FILE),
                            "yarn.resourcemanager.hostname",
                            self.master.address, True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, YARN_CONF_FILE),
                            "yarn.nodemanager.resource.memory-mb",
                            str(total_containers_mem_mb), True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, YARN_CONF_FILE),
                            "yarn.nodemanager.resource.cpu-vcores",
                            str(num_cores - 1), True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, YARN_CONF_FILE),
                            "yarn.scheduler.maximum-allocation-mb",
                            str(max_container_mem_mb), True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, YARN_CONF_FILE),
                            "yarn.nodemanager.aux-services",
                            "mapreduce_shuffle", True)

        # MapReduce
        mem_per_task_mb = total_containers_mem_mb / (num_cores - 1)

        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.framework.name", "yarn", True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.map.memory.mb",
                            str(mem_per_task_mb), True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.map.java.opts",
                            "-Xmx" + str(mem_per_task_mb) + "m", True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.map.cpu.vcores", "1", True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.reduce.memory.mb",
                            str(mem_per_task_mb), True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.reduce.cpu.vcores", "1", True)
        replace_in_xml_file(os.path.join(self.temp_conf_dir, MR_CONF_FILE),
                            "mapreduce.reduce.java.opts",
                            "-Xmx" + str(mem_per_task_mb) + "m", True)

    def bootstrap(self, tar_file):
        """Install Hadoop in all cluster nodes from the specified tar.gz file.

        Args:
          hadoop_tar_file (str):
            The file containing Hadoop binaries.
        """

        if super(HadoopV2Cluster, self).bootstrap(tar_file):
            action = Remote("cp " + os.path.join(self.conf_dir,
                                                 MR_CONF_FILE + ".template ") +
                            os.path.join(self.conf_dir, MR_CONF_FILE),
                            self.hosts)
            action.run()

    def _check_version_compliance(self):
        version = self.get_version()
        if not version.startswith("Hadoop 2."):
            logger.error("Version of HadoopCluster is not compliant with the "
                        "distribution provided in the bootstrap option. Use "
                        "the appropiate parameter for --version when creating "
                        "the cluster or use another distribution.")
            return False
        else:
            return True

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

    def start_yarn(self):
        """Start the YARN ResourceManager and NodeManagers."""

        logger.info("Starting YARN")
        
        self._check_initialization()
        
        proc = SshProcess(self.sbin_dir + "/start-yarn.sh", self.master)
        proc.run()        
        
        if not proc.finished_ok:
            logger.warn("Error while starting YARN")
        else:
            self.running_yarn = True

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
