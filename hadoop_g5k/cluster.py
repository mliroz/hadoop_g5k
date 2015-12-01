import getpass
import os
import re
import shutil
import sys
import tempfile

from ConfigParser import ConfigParser

from execo.log import style
from execo.action import Put, TaktukPut, Get, Remote, TaktukRemote, \
    SequentialActions
from execo.process import SshProcess
from execo_engine import logger
from execo_g5k.api_utils import get_host_cluster
from hadoop_g5k.hardware import G5kDeploymentHardware

from hadoop_g5k.objects import HadoopJarJob, HadoopTopology, HadoopException
from hadoop_g5k.util import ColorDecorator, replace_in_xml_file, \
    read_in_xml_file, read_param_in_xml_file, check_java_version, get_java_home

# Configuration files
CORE_CONF_FILE = "core-site.xml"
HDFS_CONF_FILE = "hdfs-site.xml"
MR_CONF_FILE = "mapred-site.xml"

# Default parameters
DEFAULT_HADOOP_BASE_DIR = "/tmp/hadoop"
DEFAULT_HADOOP_CONF_DIR = DEFAULT_HADOOP_BASE_DIR + "/conf"
DEFAULT_HADOOP_LOGS_DIR = DEFAULT_HADOOP_BASE_DIR + "/logs"
DEFAULT_HADOOP_TEMP_DIR = DEFAULT_HADOOP_BASE_DIR + "/tmp"

DEFAULT_HADOOP_HDFS_PORT = 54310
DEFAULT_HADOOP_MR_PORT = 54311

DEFAULT_HADOOP_LOCAL_CONF_DIR = "conf"


class HadoopNotInitializedException(HadoopException):
    pass


class HadoopCluster(object):
    """This class manages the whole life-cycle of a Hadoop cluster.

    HadoopCluster defines the default behavior of a Hadoop Cluster and is
    designed to work with Hadoop 0.* and Hadoop 1.*. For Hadoop 2.* the subclass
    HadoopV2Cluster should be used instead.
    
    Attributes:
      master (Host):
        The host selected as the master. It runs the NameNode and
        JobTracker.
      hosts (list of Hosts):
        List of hosts composing the cluster. All run DataNode and TaskTracker
        processes.
      topology (HadoopTopology):
        The topology of the cluster hosts.
      initialized (bool):
        True if the cluster has been initialized, False otherwise.
      running (bool):
        True if both the NameNode and JobTracker are running, False otherwise.
      running_dfs (bool):
        True if the NameNode is running, False otherwise.
      running_map_reduce (bool):
        True if the JobTracker is running, False otherwise.
    """

    @staticmethod
    def get_cluster_type():
        return "hadoop"

    # Cluster state
    initialized = False
    running = False
    running_dfs = False
    running_map_reduce = False

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
          configFile (str, optional):
            The path of the config file to be used.
        """

        # Load properties
        config = ConfigParser(self.defaults)
        config.add_section("cluster")
        config.add_section("local")

        if config_file:
            config.readfp(open(config_file))

        # Deployment properties
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")
        self.init_conf_dir = tempfile.mkdtemp("", "hadoop-init-", "/tmp")
        self.conf_mandatory_files = [CORE_CONF_FILE,
                                     HDFS_CONF_FILE,
                                     MR_CONF_FILE]

        # Node properties
        self.base_dir = config.get("cluster", "hadoop_base_dir")
        self.conf_dir = config.get("cluster", "hadoop_conf_dir")
        self.logs_dir = config.get("cluster", "hadoop_logs_dir")
        self.hadoop_temp_dir = config.get("cluster", "hadoop_temp_dir")
        self.hdfs_port = config.getint("cluster", "hdfs_port")
        self.mapred_port = config.getint("cluster", "mapred_port")

        self.bin_dir = self.base_dir + "/bin"
        self.sbin_dir = self.base_dir + "/bin"

        self.java_home = None

        # Configure master and slaves
        self.hosts = list(hosts)
        self.master = self.hosts[0]

        # Create topology
        self.topology = HadoopTopology(hosts, topo_list)

        # Store cluster information
        self.hw = G5kDeploymentHardware()
        self.hw.add_hosts(self.hosts)
        self.master_cluster = self.hw.get_cluster(get_host_cluster(self.master))

        # Create a string to display the topology
        t = {v: [] for v in self.topology.topology.values()}
        for key, value in self.topology.topology.iteritems():
            t[value].append(key.address)
        log_topo = ', '.join([style.user2(k) + ': ' +
                              ' '.join(map(lambda x: style.host(x.split('.')[0]), v))
                              for k, v in t.iteritems()])
        
        logger.info("Hadoop cluster created with master %s, hosts %s and topology %s",
                    style.host(self.master.address), 
                    ' '.join([style.host(h.address.split('.')[0]) for h in self.hosts]),
                    log_topo)

    def bootstrap(self, tar_file):
        """Install Hadoop in all cluster nodes from the specified tar.gz file.
        
        Args:
          tar_file (str):
            The file containing Hadoop binaries.
        """

        # 0. Check requirements
        java_major_version = 7
        if not check_java_version(java_major_version, self.hosts):
            msg = "Java 1.%d+ required" % java_major_version
            logger.error(msg)
            raise HadoopException(msg)

        self.java_home = get_java_home(self.master)

        # 1. Copy hadoop tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_dirs = TaktukRemote("rm -rf " + self.base_dir +
                               " " + self.conf_dir +
                               " " + self.logs_dir +
                               " " + self.hadoop_temp_dir,
                               self.hosts)
        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        tar_xf = TaktukRemote(
            "tar xf /tmp/" + os.path.basename(tar_file) + " -C /tmp",
            self.hosts)
        rm_tar = TaktukRemote(
            "rm /tmp/" + os.path.basename(tar_file),
            self.hosts)
        SequentialActions([rm_dirs, put_tar, tar_xf, rm_tar]).run()

        # 2. Move installation to base dir and create other dirs
        logger.info("Create installation directories")
        mv_base_dir = TaktukRemote(
            "mv /tmp/" +
            os.path.basename(tar_file).replace(".tar.gz", "") + " " +
            self.base_dir,
            self.hosts)
        mkdirs = TaktukRemote("mkdir -p " + self.conf_dir +
                              " && mkdir -p " + self.logs_dir +
                              " && mkdir -p " + self.hadoop_temp_dir,
                              self.hosts)
        chmods = TaktukRemote("chmod g+w " + self.base_dir +
                              " && chmod g+w " + self.conf_dir +
                              " && chmod g+w " + self.logs_dir +
                              " && chmod g+w " + self.hadoop_temp_dir,
                              self.hosts)
        SequentialActions([mv_base_dir, mkdirs, chmods]).run()

        # 4. Specify environment variables
        command = "cat >> " + self.conf_dir + "/hadoop-env.sh << EOF\n"
        command += "export JAVA_HOME=" + self.java_home + "\n"
        command += "export HADOOP_LOG_DIR=" + self.logs_dir + "\n"
        command += "HADOOP_HOME_WARN_SUPPRESS=\"TRUE\"\n"
        command += "EOF"
        action = Remote(command, self.hosts)
        action.run()

        # 5. Check version (cannot do it before)
        if not self._check_version_compliance():
            return False

        # 6. Generate initial configuration
        self._initialize_conf()

        return True

    def _initialize_conf(self):
        """Merge locally-specified configuration files with default files
        from the distribution."""

        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.init_conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        missing_conf_files = self.conf_mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        logger.info("Copying missing conf files from master: " + str(
            missing_conf_files))

        remote_missing_files = [os.path.join(self.conf_dir, f)
                                for f in missing_conf_files]

        action = Get([self.master], remote_missing_files, self.init_conf_dir)
        action.run()

    def _check_version_compliance(self):
        if self.get_major_version() >= 2:
            logger.error("Version of HadoopCluster is not compliant with the "
                         "distribution provided in the bootstrap option. Use "
                         "the appropriate parameter for --version when "
                         "creating the cluster or use another distribution.")
            return False
        else:
            return True

    def _check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.

        Raises:
          HadoopNotInitializedException:
            If self.initialized = False
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise HadoopNotInitializedException(
                "The cluster should be initialized")

    def initialize(self, default_tuning=False):
        """Initialize the cluster: copy base configuration and format DFS."""

        self._pre_initialize()

        logger.info("Initializing hadoop")

        # Set basic configuration
        temp_conf_base_dir = tempfile.mkdtemp("", "hadoop-", "/tmp")
        temp_conf_dir = os.path.join(temp_conf_base_dir, "conf")
        shutil.copytree(self.init_conf_dir, temp_conf_dir)

        self._create_master_and_slave_conf(temp_conf_dir)
        self.topology.create_files(temp_conf_dir)
        self._configure_servers(temp_conf_dir, default_tuning)

        shutil.rmtree(temp_conf_base_dir)

        # Format HDFS
        self.format_dfs()

        self.initialized = True

    def _pre_initialize(self):
        """Clean previous configurations"""

        if self.initialized:
            if self.running:
                self.stop()
            self.clean()
        else:
            self.__force_clean()

        self.initialized = False

    def _create_master_and_slave_conf(self, conf_dir):
        """Create master and slaves configuration files."""

        with open(conf_dir + "/masters", "w") as master_file:
            master_file.write(self.master.address + "\n")

        with open(conf_dir + "/slaves", "w") as slaves_file:
            for s in self.hosts:
                slaves_file.write(s.address + "\n")

    def _configure_servers(self, conf_dir, default_tuning=False):
        """Configure servers and host-dependant parameters.

           Args:
             conf_dir (str):
               The path of the directory with the configuration files.
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        if default_tuning:
            logger.info("Default tuning. Beware that this configuration is not"
                        "guaranteed to be optimal for all scenarios.")

        # Get cluster-dependent params
        params = self._get_cluster_params(conf_dir, default_tuning)
        logger.info("Params = " + str(params))

        # Set common configuration
        self._set_common_params(params, conf_dir, default_tuning)

        # Set cluster-dependent configuration and copy back to hosts
        for cluster in self.hw.get_clusters():

            # Create a new dir
            cl_temp_conf_base_dir = tempfile.mkdtemp("", "hadoop-cl-", "/tmp")
            cl_temp_conf_dir = os.path.join(cl_temp_conf_base_dir, "conf")
            shutil.copytree(conf_dir, cl_temp_conf_dir)

            # Replace params in conf files
            self._set_cluster_params(cluster, params, cl_temp_conf_dir,
                                     default_tuning)

            # Copy to hosts and remove temp dir
            hosts = cluster.get_hosts()
            self._copy_conf(cl_temp_conf_dir, hosts)
            shutil.rmtree(cl_temp_conf_base_dir)

    def _get_cluster_params(self, conf_dir, default_tuning=False):
        """Define host-dependant parameters.

           Args:
             conf_dir (str):
               The path of the directory with the configuration files.
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        min_slot_mem = 200

        params = {}
        for cluster in self.hw.get_clusters():

            num_cores = cluster.get_num_cores()
            available_mem = cluster.get_memory()
            mr_memory_mb = available_mem - 2 * 1024
            mem_per_slot_mb = max(min_slot_mem,
                                  mr_memory_mb / (num_cores - 1))
            map_slots = num_cores - 1
            red_slots = num_cores - 1

            cluster_params = {
                "mem_per_slot_mb": mem_per_slot_mb,
                "map_slots": map_slots,
                "red_slots": red_slots
            }

            if default_tuning:
                io_sort_mb = max(100, mem_per_slot_mb / 2)
                io_sort_factor = max(10, io_sort_mb / 10)

                cluster_params.update({
                    "io_sort_mb": io_sort_mb,
                    "io_sort_factor": io_sort_factor
                })

            params[cluster.get_name()] = cluster_params

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
        mr_file = os.path.join(conf_dir, MR_CONF_FILE)

        replace_in_xml_file(core_file,
                            "fs.default.name",
                            "hdfs://%s:%d/" % (self.master.address,
                                               self.hdfs_port),
                            create_if_absent=True)
        replace_in_xml_file(core_file,
                            "hadoop.tmp.dir",
                            self.hadoop_temp_dir,
                            create_if_absent=True)
        replace_in_xml_file(core_file,
                            "topology.script.file.name",
                            self.conf_dir + "/topo.sh",
                            create_if_absent=True)

        replace_in_xml_file(mr_file,
                            "mapred.job.tracker",
                            "%s:%d" % (self.master.address, self.mapred_port),
                            create_if_absent=True)

    def _set_cluster_params(self, cluster, params,
                            conf_dir, default_tuning=False):
        """Replace cluster-dependent parameters.

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

        cname = cluster.get_name()

        mem_per_slot_mb = params[cname]["mem_per_slot_mb"]
        map_slots = params[cname]["map_slots"]
        red_slots = params[cname]["red_slots"]

        if default_tuning:
            mr_file = os.path.join(conf_dir, MR_CONF_FILE)

            replace_in_xml_file(mr_file,
                                "mapred.tasktracker.map.tasks.maximum",
                                str(map_slots),
                                create_if_absent=True,
                                replace_if_present=False)
            replace_in_xml_file(mr_file,
                                "mapred.tasktracker.reduce.tasks.maximum",
                                str(red_slots),
                                create_if_absent=True,
                                replace_if_present=False)
            replace_in_xml_file(mr_file,
                                "mapred.child.java.opts",
                                "-Xmx%dm" % mem_per_slot_mb,
                                create_if_absent=True,
                                replace_if_present=False)

    def _copy_conf(self, conf_dir, hosts=None):
        """Copy configuration files from given dir to remote dir in cluster
        hosts.
        
        Args:
          conf_dir (str):
            The remote configuration dir.
          hosts (list of Host, optional):
            The list of hosts where the configuration is going to be copied. If
            not specified, all the hosts of the Hadoop cluster are used.
        """

        if not hosts:
            hosts = self.hosts

        conf_files = [os.path.join(conf_dir, f) for f in os.listdir(conf_dir)]

        action = TaktukPut(hosts, conf_files, self.conf_dir)
        action.run()

        if not action.finished_ok:
            logger.warn("Error while copying configuration")
            if not action.ended:
                action.kill()

    def change_conf(self, params, conf_file=None, default_file=MR_CONF_FILE):
        """Modify Hadoop configuration. This method copies the configuration
        files from the first host of each g5k cluster conf dir into a local
        temporary dir, do all the changes in place and broadcast the new
        configuration files to all hosts.
        
        Args:
          params (dict of str:str):
            The parameters to be changed in the form key:value.
          conf_file (str, optional):
            The file where parameters should be set. If not specified, all
            files are checked for the parameter name and the parameter is set
            in the file where the property is found. If not found, the
            parameter is set in the default file.
          default_file (str, optional): The default conf file where to set the
            parameter if not found. Only applies when conf_file is not set.
        """

        for cluster in self.hw.get_clusters():
            hosts = cluster.get_hosts()

            # Copy conf files from first host in the cluster
            action = Remote("ls " + self.conf_dir + "/*.xml", [hosts[0]])
            action.run()
            output = action.processes[0].stdout

            remote_conf_files = []
            for f in output.split():
                remote_conf_files.append(os.path.join(self.conf_dir, f))

            tmp_dir = "/tmp/mliroz_temp_hadoop/"
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)

            action = Get([hosts[0]], remote_conf_files, tmp_dir)
            action.run()

            # Do replacements in temp file
            if conf_file:
                f = os.path.join(tmp_dir, conf_file)
                for name, value in params.iteritems():
                    replace_in_xml_file(f, name, value, True)
            else:
                temp_conf_files = [os.path.join(tmp_dir, f) for f in
                                   os.listdir(tmp_dir)]

                for name, value in params.iteritems():
                    for f in temp_conf_files:
                        if replace_in_xml_file(f, name, value):
                            break
                    else:
                        # Property not found - add it in MR_CONF_FILE
                        logger.info("Parameter with name " + name + " has not "
                                    "been found in any conf file. Setting it "
                                    "in " + default_file)
                        f = os.path.join(tmp_dir, default_file)
                        replace_in_xml_file(f, name, value, True)

            # Copy back the files to all hosts
            self._copy_conf(tmp_dir, hosts)

    def _get_conf_files(self, host):

        action = Remote("ls " + self.conf_dir + "/*.xml", [host])
        action.run()
        output = action.processes[0].stdout

        remote_conf_files = []
        for f in output.split():
            remote_conf_files.append(os.path.join(self.conf_dir, f))

        tmp_dir = "/tmp/mliroz_temp_hadoop/"
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        action = Get([host], remote_conf_files, tmp_dir)
        action.run()

        temp_conf_files = [os.path.join(tmp_dir, f) for f in
                           os.listdir(tmp_dir)]

        return temp_conf_files

    def get_conf_param(self, param_name, default=None, node=None):

        # Copy conf files from first host in the cluster
        if node is None:
            node = self.hosts[0]
        temp_conf_files = self._get_conf_files(node)

        # Look in conf files
        for f in temp_conf_files:
            value = read_param_in_xml_file(f, param_name)
            if value:
                return value

        return default

    def get_conf(self, param_names, node=None):

        params = {}
        remaining_param_names = set(param_names)

        # Copy conf files from first host in the cluster
        if node is None:
            node = self.hosts[0]
        temp_conf_files = self._get_conf_files(node)

        # Look in conf files
        for f in temp_conf_files:
            fparams = read_in_xml_file(f, remaining_param_names)
            for p in fparams:
                if fparams[p]:
                    params[p] = fparams[p]
                    remaining_param_names.discard(p)

        return params

    def format_dfs(self):
        """Format the distributed filesystem."""

        logger.info("Formatting HDFS")

        proc = SshProcess(self.bin_dir + "/hadoop namenode -format",
                          self.master)
        proc.run()

        if proc.finished_ok:
            logger.info("HDFS formatted successfully")
        else:
            logger.warn("Error while formatting HDFS")

    def start(self):
        """Start the NameNode and DataNodes and then the JobTracker and
        TaskTrackers."""

        self._check_initialization()

        self.start_dfs()
        self.start_map_reduce()

        self.running = True

    def start_and_wait(self):
        """Start the NameNode and DataNodes and then the JobTracker and
        TaskTrackers. Wait for them to exit safemode before continuing."""

        self._check_initialization()

        self.start_dfs_and_wait()
        self.start_map_reduce_and_wait()

        self.running = True

    def start_dfs(self):
        """Start the NameNode and DataNodes."""

        self._check_initialization()

        logger.info("Starting HDFS")

        if self.running_dfs:
            logger.warn("Dfs was already started")
            return

        proc = SshProcess(self.sbin_dir + "/start-dfs.sh", self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting HDFS")
        else:
            self.running_dfs = True

    def start_dfs_and_wait(self):
        """Start the NameNode and DataNodes and wait for exiting safemode."""

        self._check_initialization()

        self.start_dfs()

        logger.info("Waiting for safe mode to be off")
        proc = SshProcess(self.bin_dir + "/hadoop dfsadmin -safemode wait",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting HDFS")
        else:
            self.running_dfs = True
            if self.running_map_reduce:
                self.running = True

    def start_map_reduce(self):
        """Start the JobTracker and TaskTrackers."""

        self._check_initialization()

        logger.info("Starting MapReduce")

        if self.running_map_reduce:
            logger.warn("Error while starting MapReduce")
            return

        proc = SshProcess(self.sbin_dir + "/start-mapred.sh", self.master)
        proc.run()

        if not proc.finished_ok:
            logger.info("MapReduce started successfully")
        else:
            self.running_map_reduce = True
            if self.running_dfs:
                self.running = True

    def start_map_reduce_and_wait(self):
        """Start the JobTracker and TaskTrackers and wait for exiting safemode.
        """

        self._check_initialization()

        self.start_map_reduce()

        # logger.info("Waiting for safe mode to be off")
        # proc = SshProcess(self.hadoop_base_dir +
        #                   "/bin/hadoop mradmin -safemode wait",
        # self.master)
        # proc.run()

        # TODO - does the jobtracker enter safemode?

    def stop(self):
        """Stop the Jobtracker and TaskTracekrs and then the NameNode and
        DataNodes."""

        self._check_initialization()

        self.stop_map_reduce()
        self.stop_dfs()

        self.running = False

    def stop_dfs(self):
        """Stop the NameNode and DataNodes."""

        self._check_initialization()

        logger.info("Stopping HDFS")

        proc = SshProcess(self.sbin_dir + "/stop-dfs.sh", self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while stopping HDFS")
        else:
            self.running_dfs = False

    def stop_map_reduce(self):
        """Stop the JobTracker and TaskTrackers."""

        self._check_initialization()

        logger.info("Stopping MapReduce")

        proc = SshProcess(self.sbin_dir + "/stop-mapred.sh", self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while stopping MapReduce")
        else:
            self.running_map_reduce = False

    def execute(self, command, node=None, should_be_running=True,
                verbose=True):
        """Execute the given Hadoop command in the given node.

        Args:
          command (str):
            The command to be executed.
          node (Host, optional):
            The host were the command should be executed. If not provided,
            self.master is chosen.
          should_be_running (bool, optional):
            True if the cluster needs to be running in order to execute the
            command. If so, and it is not running, it is automatically started.
          verbose: (bool, optional):
            If True stdout and stderr of remote process is displayed.

        Returns (tuple of str):
          A tuple with the standard and error outputs of the process executing
          the command.
        """

        self._check_initialization()

        if should_be_running and not self.running:
            logger.warn("The cluster was stopped. Starting it automatically")
            self.start()

        if not node:
            node = self.master

        if verbose:
            logger.info("Executing {" + self.bin_dir + "/hadoop " +
                        command + "} in " + str(node))

        proc = SshProcess(self.bin_dir + "/hadoop " + command, node)

        if verbose:
            red_color = '\033[01;31m'

            proc.stdout_handlers.append(sys.stdout)
            proc.stderr_handlers.append(
                ColorDecorator(sys.stderr, red_color))

        proc.start()
        proc.wait()

        return proc.stdout, proc.stderr

    def execute_job(self, job, node=None, verbose=True):
        """Execute the given MapReduce job in the specified node.
        
        Args:
          job (HadoopJarJob):
            The job object.
          node (Host, optional):
            The host were the command should be executed. If not provided,
            self.master is chosen.
          verbose (bool, optional):
            If True stdout and stderr of remote process is displayed.

        Returns (tuple of str):
          A tuple with the standard and error outputs of the process executing
          the job.
        """

        self._check_initialization()

        if not self.running:
            logger.warn("The cluster was stopped. Starting it automatically")
            self.start()

        if not node:
            node = self.master

        exec_dir = "/tmp"

        # Copy necessary files to cluster
        files_to_copy = job.get_files_to_copy()
        action = Put([node], files_to_copy, exec_dir)
        action.run()

        # Get command
        command = job.get_command(exec_dir)

        # Execute
        logger.info("Executing jar job. Command = {" + self.bin_dir +
                    "/hadoop " + command + "} in " + str(node))

        proc = SshProcess(self.bin_dir + "/hadoop " + command, node)

        if verbose:
            red_color = '\033[01;31m'

            proc.stdout_handlers.append(sys.stdout)
            proc.stderr_handlers.append(
                ColorDecorator(sys.stderr, red_color))

        proc.start()
        proc.wait()

        # Get job info
        job.stdout = proc.stdout
        job.stderr = proc.stderr
        job.success = (proc.exit_code == 0)

        for line in job.stdout.splitlines():
            if "Running job" in line:
                if "mapred.JobClient" in line or "mapreduce.Job" in line:
                    # TODO: more possible formats?
                    try:
                        match = re.match('.*Running job: (.*)', line)
                        job.job_id = match.group(1)
                        break
                    except:
                        pass

        return proc.stdout, proc.stderr

    def copy_history(self, dest, job_ids=None):
        """Copy history logs from master.
        
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

        history_dir = os.path.join(self.logs_dir, "history")
        if job_ids:
            pattern = " -o ".join("-name " + jid + "*" for jid in job_ids)
            list_dirs = SshProcess("find " + history_dir + " " + pattern,
                                   self.master)
            list_dirs.run()
        else:
            list_dirs = SshProcess("find " + history_dir + " -name job_*",
                                   self.master)
            list_dirs.run()

        remote_files = []
        for line in list_dirs.stdout.splitlines():
            remote_files.append(line)

        action = Get([self.master], remote_files, dest)
        action.run()

    def clean_history(self):
        """Remove history."""

        logger.info("Cleaning history")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.logs_dir + "/history",
                        [self.master])
        action.run()

        if restart:
            self.start()

    def clean_conf(self):
        """Clean configuration files used by this cluster."""
        pass
        #shutil.rmtree(self.temp_conf_dir)

    def clean_logs(self):
        """Remove all Hadoop logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.logs_dir + "/*", self.hosts)
        action.run()

        if restart:
            self.start()

    def clean_data(self):
        """Remove all data created by Hadoop (including filesystem)."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        logger.info("Cleaning hadoop data")

        restart = False
        if self.running:
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.hadoop_temp_dir + " /tmp/hadoop-" +
                        getpass.getuser() + "-*", self.hosts)
        action.run()

        if restart:
            self.start()

    def clean(self):
        """Remove all files created by Hadoop (logs, filesystem,
        temporary files)."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_conf()
        self.clean_logs()
        self.clean_data()

        self.initialized = False

    def __force_clean(self):
        """Stop previous Hadoop processes (if any) and remove all remote files
        created by it."""

        hadoop_processes = [
            "DataNode",
            "SecondaryNameNode",
            "JobTracker",
            "TaskTracker",
            "NameNode"
        ]

        force_kill = False
        for h in self.hosts:
            proc = SshProcess("jps", self.master)
            proc.run()

            ids_to_kill = []
            for line in proc.stdout.splitlines():
                field = line.split()
                if field[1] in hadoop_processes:
                    ids_to_kill.append(field[0])

            if ids_to_kill:
                force_kill = True
                ids_to_kill_str = ""
                for pid in ids_to_kill:
                    ids_to_kill_str += " " + pid

                proc = SshProcess("kill -9" + ids_to_kill_str, h)
                proc.run()

        if force_kill:
            logger.info(
                "Processes from previous hadoop deployments had to be killed")

        self.clean_logs()
        self.clean_data()

    def get_version(self):
        """Return the Hadoop version.
        
        Returns (str):
          The version used by the Hadoop cluster.
        """

        proc = SshProcess("export JAVA_HOME=" + self.java_home + ";" +
                          self.bin_dir + "/hadoop version",
                          self.master)
        proc.run()
        version = proc.stdout.splitlines()[0]
        return version

    def get_major_version(self):
        str_version = self.get_version()

        match = re.match('Hadoop ([0-9]).*', str_version)
        return int(match.group(1))

