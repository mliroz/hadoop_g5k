import os
import shutil
import sys
import tempfile
import time

from ConfigParser import ConfigParser

from execo.action import Put, TaktukPut, Get, Remote
from execo.process import SshProcess
from execo_engine import logger

from hadoop_g5k.util import ColorDecorator

# Default parameters
DEFAULT_SPARK_BASE_DIR = "/tmp/spark"
DEFAULT_SPARK_CONF_DIR = DEFAULT_SPARK_BASE_DIR + "/conf"
DEFAULT_SPARK_PORT = 7077

DEFAULT_SPARK_LOCAL_CONF_DIR = "spark-conf"

# Modes
STANDALONE_MODE = 0
YARN_MODE = 1

# Other constants
JAVA_HOME = "/usr/lib/jvm/java-7-openjdk-amd64"


class SparkException(Exception):
    pass


class SparkCluster(object):
    """This class manages the whole life-cycle of a Spark cluster.

    Attributes:
      master (Host):
        The host selected as the master.
      hosts (list of Hosts):
        List of hosts composing the cluster.
      initialized (bool):
        True if the cluster has been initialized, False otherwise.
      running_spark (bool):
        True if spark is running, False otherwise.
      mode (int):
        The cluster manager that is used (STANDALONE_MODE or YARN_MODE).
      hc (HadoopCluster):
        A reference to the Hadoop cluster if spark is deployed on top of one.
    """

    # Cluster state
    initialized = False
    running_spark = False

    # Default properties
    defaults = {
        "spark_base_dir": DEFAULT_SPARK_BASE_DIR,
        "spark_conf_dir": DEFAULT_SPARK_CONF_DIR,
        "spark_port": str(DEFAULT_SPARK_PORT),

        "local_base_conf_dir": DEFAULT_SPARK_LOCAL_CONF_DIR
    }

    def __init__(self, mode, config_file=None, hosts=None,
                 hadoop_cluster=None):
        """Create a new Spark cluster. It can be created as a standalone
        cluster or on top of YARN.

        Args:
          mode (int):
            The cluster manager that is used (STANDALONE_MODE or YARN_MODE).
          configFile (str, optional):
            The path of the config file to be used.
          hosts (list of Host, optional):
            The hosts of the cluster (standalone operation).
          hadoop_cluster (HadoopCluster, optional):
            The Hadoop cluster to link.
        """

        # Load cluster properties
        config = ConfigParser(self.defaults)
        config.add_section("cluster")
        config.add_section("local")

        if config_file:
            config.readfp(open(config_file))

        self.spark_base_dir = config.get("cluster", "spark_base_dir")
        self.spark_conf_dir = config.get("cluster", "spark_conf_dir")
        self.spark_port = config.getint("cluster", "spark_port")
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")

        self.spark_bin_dir = self.spark_base_dir + "/bin"
        self.spark_sbin_dir = self.spark_base_dir + "/sbin"

        self.mode = mode

        # Initialize hosts
        if hosts:
            self.hosts = hosts
            self.master = hosts[0]
        elif hadoop_cluster:
            self.hosts = hadoop_cluster.hosts
            self.master = hadoop_cluster.master
        else:
            logger.error("Hosts in the cluster must be specified either"
                         "directly or indirectly through a Hadoop cluster.")
            raise SparkException("Hosts in the cluster must be specified either"
                                 " directly or indirectly through a Hadoop "
                                 "cluster.")

        # Store reference to Hadoop cluster and check if mandatory
        if hadoop_cluster:
            self.hc = hadoop_cluster
        else:
            if mode == YARN_MODE:
                logger.error("When using a YARN_MODE mode, a reference to the "
                             "Hadoop cluster should be provided.")
                raise SparkException("When using a YARN_MODE mode, a reference "
                                     "to the Hadoop cluster should be provided")

        if self.mode == STANDALONE_MODE:
            mode_text = " in standalone mode"
        else:
            mode_text = " on top of YARN "

        logger.info("Spark cluster created " + mode_text + " in hosts " +
                    str(self.hosts) + "." +
                    " It is linked to a Hadoop cluster." if self.hc else "")

    def bootstrap(self, spark_tar_file):

        # 1. Remove used dirs if existing
        action = Remote("rm -rf " + self.spark_base_dir, self.hosts)
        action.run()
        action = Remote("rm -rf " + self.spark_conf_dir, self.hosts)
        action.run()

        # 1. Copy Spark tar file and uncompress
        logger.info("Copy " + spark_tar_file + " to hosts and uncompress")
        action = Put(self.hosts, [spark_tar_file], "/tmp")
        action.run()
        action = Remote(
            "tar xf /tmp/" + os.path.basename(spark_tar_file) + " -C /tmp",
            self.hosts)
        action.run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        action = Remote(
            "mv /tmp/" +
            os.path.basename(spark_tar_file).replace(".tgz", "") + " " +
            self.spark_base_dir,
            self.hosts)
        action.run()

        # 3. Create other dirs
        action = Remote("mkdir -p " + self.spark_conf_dir, self.hosts)
        action.run()

        # 4. Specify environment variables
        command = "cat >> " + self.spark_conf_dir + "/spark-env.sh << EOF\n"
        command += "JAVA_HOME=" + JAVA_HOME + "\n"
        command += "SPARK_MASTER_PORT=" + str(self.spark_port) + "\n"
        if self.hc:
            command += "HADOOP_CONF_DIR=" + self.hc.hadoop_conf_dir + "\n"
        command += "EOF\n"
        command += "chmod +x " + self.spark_conf_dir + "/spark-env.sh"
        action = Remote(command, self.hosts)
        action.run()

    def initialize(self):
        """Initialize the cluster: copy base configuration and format DFS."""

        self._pre_initialize()

        logger.info("Initializing Spark")

        # Set basic configuration
        self._copy_base_conf()
        self._create_slaves_conf()
        self._copy_conf(self.conf_dir, self.hosts)

        self._configure_servers(self.hosts)

        self.initialized = True

    def _pre_initialize(self):
        """Clean previous configurations"""

        if self.initialized:
            if self.running_spark:
                self.stop()
            self.clean()
        else:
            self.__force_clean()

        self.initialized = False

    def _copy_base_conf(self):
        """Copy base configuration files to tmp dir."""

        self.conf_dir = tempfile.mkdtemp("", "spark-", "/tmp")
        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        mandatory_files = []

        missing_conf_files = mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        logger.info("Copying missing conf files from master: " + str(
            missing_conf_files))

        remote_missing_files = [os.path.join(self.spark_conf_dir, f)
                                for f in missing_conf_files]

        action = Get([self.master], remote_missing_files, self.conf_dir)
        action.run()

    def _create_slaves_conf(self):
        """Create slaves configuration files."""

        slaves_file = open(self.conf_dir + "/slaves", "w")
        for s in self.hosts:
            slaves_file.write(s.address + "\n")
        slaves_file.close()

    def _copy_conf(self, conf_dir, hosts=None):
        """Copy configuration files from given dir to remote dir in cluster
        hosts.

        Args:
          conf_dir (str):
            The remote configuration dir.
          hosts (list of Host, optional):
            The list of hosts where the configuration is going to be copied. If
            not specified, all the hosts of the Spark cluster are used.
        """

        if not hosts:
            hosts = self.hosts

        conf_files = [os.path.join(conf_dir, f) for f in os.listdir(conf_dir)]

        action = TaktukPut(hosts, conf_files, self.spark_conf_dir)
        action.run()

        if not action.finished_ok:
            logger.warn("Error while copying configuration")
            if not action.ended:
                action.kill()

    def _configure_servers(self, hosts=None):
        """Configure servers and host-dependant parameters.

           Args:
             hosts (list of Host, optional):
               The list of hosts to take into account in the configuration. If
               not specified, all the hosts of the Spark cluster are used. The
               first host of this list is always used as the reference.
        """

        if not hosts:
            hosts = self.hosts

        # TODO

    def start(self):
        """Start spark processes."""
        self.start_spark()

    def start_spark(self):
        """Start spark processes.
        In STANDALONE mode it starts the master and slaves. In YARN mode it just
        checks that Hadoop is running, and starts it if not.
        """

        logger.info("Starting Spark")

        if self.running_spark:
            logger.warn("Spark was already started")
            return

        if self.mode == STANDALONE_MODE:
            proc = SshProcess(self.spark_sbin_dir + "/start-master.sh;" +
                              self.spark_sbin_dir + "/start-slaves.sh;",
                              self.master)
            proc.run()
            if not proc.finished_ok:
                logger.warn("Error while starting Spark")
                return
        elif self.mode == YARN_MODE:
            if not self.hc.running:
                logger.warn("YARN services must be started first")
                self.hc.start_and_wait()

        self.running_spark = True

    def stop(self):
        """Stop Spark processes."""

        self.stop_spark()

    def stop_spark(self):
        """Stop Spark processes."""

        logger.info("Stopping Spark")

        if self.mode == STANDALONE_MODE:
            proc = SshProcess(self.spark_sbin_dir + "/stop-slaves.sh;" +
                              self.spark_sbin_dir + "/stop-master.sh;",
                              self.master)
            proc.run()
            if not proc.finished_ok:
                logger.warn("Error while stopping Spark")
                return

        self.running_spark = False

    def start_shell(self, language="python"):
        """Open a Spark shell."""

        if self.mode == YARN_MODE:
            options = " --master yarn-client "
        else:
            options = ""

        if language == "python":
            proc = SshProcess(self.spark_bin_dir + "/pyspark" + options,
                              self.master)
        elif language == "scala":
            proc = SshProcess(self.spark_bin_dir + "/spark-shell" + options,
                              self.master)
        else:
            logger.error("Unknown language " + language)
            return

        red_color = '\033[01;31m'
        proc.stdout_handlers.append(sys.stdout)
        proc.stderr_handlers.append(ColorDecorator(sys.stderr, red_color))
        proc.close_stdin = False
        proc.start()
        time.sleep(1)
        line = sys.stdin.readline()
        proc.process.stdin.write(line)
        while line.strip() != "exit()":
            line = sys.stdin.readline()
            proc.process.stdin.write(line)

    def is_on_top_of_yarn(self):
        return self.mode == YARN_MODE

    def is_standalone(self):
        return self.mode == STANDALONE_MODE

    def clean(self):
        """Remove all files created by Spark."""

        if self.running_spark:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        # TODO

        self.initialized = False

    def __force_clean(self):
        """Stop previous Spark processes (if any) and remove all remote files
        created by it."""

        # TODO