import getpass
import os
import shutil
import tempfile

from ConfigParser import ConfigParser
from subprocess import call

from execo.action import TaktukPut, Get, Remote, TaktukRemote, \
    SequentialActions
from execo.log import style
from execo.process import SshProcess
from execo_engine import logger

from hadoop_g5k.util.conf import replace_in_xml_file, create_xml_file

# Default parameters
DEFAULT_HIVE_BASE_DIR = "/tmp/hive"
DEFAULT_HIVE_CONF_DIR = DEFAULT_HIVE_BASE_DIR + "/conf"
DEFAULT_HIVE_LOGS_DIR = DEFAULT_HIVE_BASE_DIR + "/logs"
DEFAULT_HIVE_WAREHOUSE_DIR = DEFAULT_HIVE_BASE_DIR + "/warehouse"
DEFAULT_HIVE_PORT = 7077

__user_login = getpass.getuser()
DEFAULT_HIVE_METASTORE_DIR = "/tmp/hive_" + __user_login + "_metastore/"

DEFAULT_HIVE_LOCAL_CONF_DIR = "hive-conf"


class HiveException(Exception):
    pass


class HiveJobException(HiveException):
    pass


class HiveCluster(object):
    """This class manages the whole life-cycle of a Hive cluster.

    Attributes:
      master (Host):
        The host selected as the master.
      hosts (list of Hosts):
        List of hosts composing the cluster.
      initialized (bool):
        True if the cluster has been initialized, False otherwise.
      running (bool):
        True if Hive is running, False otherwise.
      hc (HadoopCluster):
        A reference to the Hadoop cluster if spark is deployed on top of one.
    """

    @staticmethod
    def get_cluster_type():
        return "hive"

    # Cluster state
    initialized = False
    running = False

    # Default properties
    defaults = {
        "hive_base_dir": DEFAULT_HIVE_BASE_DIR,
        "hive_conf_dir": DEFAULT_HIVE_CONF_DIR,
        "hive_logs_dir": DEFAULT_HIVE_LOGS_DIR,
        "hive_warehouse_dir": DEFAULT_HIVE_WAREHOUSE_DIR,
        "hive_metastore_dir": DEFAULT_HIVE_METASTORE_DIR,

        "local_base_conf_dir": DEFAULT_HIVE_LOCAL_CONF_DIR
    }

    def __init__(self, hadoop_cluster, config_file=None):
        """Create a new Hive cluster. It can be created as a standalone
        cluster or on top of YARN.

        Args:
          hadoop_cluster (HadoopCluster, optional):
            The Hadoop cluster to link.
          configFile (str, optional):
            The path of the config file to be used.
        """

        # Load cluster properties
        config = ConfigParser(self.defaults)
        config.add_section("cluster")
        config.add_section("local")

        if config_file:
            config.readfp(open(config_file))

        self.base_dir = config.get("cluster", "hive_base_dir")
        self.conf_dir = config.get("cluster", "hive_conf_dir")
        self.logs_dir = config.get("cluster", "hive_logs_dir")
        self.warehouse_dir = config.get("cluster", "hive_warehouse_dir")
        self.metastore_dir = config.get("cluster", "hive_metastore_dir")
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")

        self.bin_dir = self.base_dir + "/bin"

        # Initialize hosts
        self.hosts = hadoop_cluster.hosts
        self.master = hadoop_cluster.master

        # Store reference to Hadoop cluster and check if mandatory
        self.hc = hadoop_cluster

        logger.info("Hive cluster created in hosts %s."
                    " It is linked to a Hadoop cluster." if self.hc else "",
                    ' '.join([style.host(h.address.split('.')[0])
                              for h in self.hosts]))

    def bootstrap(self, tar_file):

        # 0. Check that required packages are present
        required_packages = "openjdk-7-jre openjdk-7-jdk"
        check_packages = TaktukRemote("dpkg -s " + required_packages,
                                      self.hosts)
        for p in check_packages.processes:
            p.nolog_exit_code = p.nolog_error = True
        check_packages.run()
        if not check_packages.ok:
            logger.info("Packages not installed, trying to install")
            install_packages = TaktukRemote(
                "export DEBIAN_MASTER=noninteractive ; " +
                "apt-get update && apt-get install -y --force-yes " +
                required_packages, self.hosts).run()
            if not install_packages.ok:
                logger.error("Unable to install the packages")

        get_java_home = SshProcess('echo $(readlink -f /usr/bin/javac | '
                                   'sed "s:/bin/javac::")', self.master)
        get_java_home.run()
        self.java_home = get_java_home.stdout.strip()

        logger.info("All required packages are present")

        # 1. Copy Hive tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_dirs = TaktukRemote("rm -rf " + self.base_dir +
                               " " + self.conf_dir +
                               " " + self.warehouse_dir +
                               " " + self.logs_dir,
                               self.hosts)
        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        tar_xf = TaktukRemote("tar xf /tmp/" + os.path.basename(tar_file) +
                              " -C /tmp", self.hosts)
        SequentialActions([rm_dirs, put_tar, tar_xf]).run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        mv_base_dir = TaktukRemote(
            "mv /tmp/" +
            os.path.basename(tar_file).replace(".tar.gz", "") + " " +
            self.base_dir,
            self.hosts)
        mkdirs = TaktukRemote("mkdir -p " + self.conf_dir +
                              " && mkdir -p " + self.warehouse_dir,
                              self.hosts)
        chmods = TaktukRemote("chmod g+w " + self.base_dir +
                              " && chmod g+w " + self.conf_dir +
                              " && chmod g+w " + self.warehouse_dir,
                              self.hosts)
        SequentialActions([mv_base_dir, mkdirs, chmods]).run()

        # 3. Specify environment variables
        command = "cat >> " + self.conf_dir + "/hive-env.sh << EOF\n"
        command += "JAVA_HOME=" + self.java_home + "\n"
        command += "HIVE_HOME=" + self.base_dir + "\n"
        command += "HIVE_CONF_DIR=" + self.conf_dir + "\n"
        command += "HADOOP_HOME=" + self.hc.base_dir + "\n"
        command += "EOF\n"
        command += "chmod +x " + self.conf_dir + "/hive-env.sh"
        action = Remote(command, self.hosts)
        action.run()

    def initialize(self):
        """Initialize the cluster: copy base configuration and format DFS."""

        self._pre_initialize()

        logger.info("Initializing Hive")

        # Set basic configuration
        self._copy_base_conf()
        self._create_master_and_slave_conf()
        self._configure_servers(self.hosts)

        self._copy_conf(self.temp_conf_dir, self.hosts)

        # Create warehouse dirs in HDFS
        self._create_warehouse()
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

    def _copy_base_conf(self):
        """Copy base configuration files to tmp dir."""

        self.temp_conf_dir = tempfile.mkdtemp("", "hive-", "/tmp")
        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.temp_conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        mandatory_files = ["hive-site.xml"]

        missing_conf_files = mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        # Copy or create mandatory files
        action = SshProcess("ls -1 " + self.conf_dir, self.master)
        action.run()
        files_in_conf_dir = action.stdout

        remote_missing_files = []
        for f in missing_conf_files:
            if f in files_in_conf_dir:
                remote_missing_files.append(os.path.join(self.conf_dir, f))
            else:
                create_xml_file(os.path.join(self.temp_conf_dir, f))

        if remote_missing_files:
            logger.info("Copying missing conf files from master: " + str(
                remote_missing_files))

            action = Get([self.master], remote_missing_files,
                         self.temp_conf_dir)
            action.run()

    def _create_master_and_slave_conf(self):
        """Configure master and create slaves configuration files."""

        pass

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

        action = TaktukPut(hosts, conf_files, self.conf_dir)
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

        conf_file = os.path.join(self.temp_conf_dir, "hive-site.xml")

        replace_in_xml_file(conf_file, "fs.default.name",
                            "hdfs://" + self.hc.master.address + ":" +
                                        str(self.hc.hdfs_port) + "/",
                            True)

        replace_in_xml_file(conf_file, "mapred.job.tracker",
                            self.hc.master.address + ":" +
                            str(self.hc.mapred_port), True)

        replace_in_xml_file(conf_file, "hive.metastore.warehouse.dir",
                            self.warehouse_dir, True)

        replace_in_xml_file(conf_file, "javax.jdo.option.ConnectionURL",
                            "jdbc:derby:;"
                            "databaseName=" + self.metastore_dir + ";"
                            "create=true",
                            True)

    def _create_warehouse(self):
        """ """

        if not self.hc.running:
            logger.warn("Hadoop must be started first")
            self.hc.start_and_wait()

        logger.info("Creating warehouse dirs in HDFS")
        self.hc.execute("fs -mkdir -p /tmp", verbose=False)
        self.hc.execute("fs -mkdir -p /user/hive/warehouse", verbose=False)
        self.hc.execute("fs -chmod g+w /tmp", verbose=False)
        self.hc.execute("fs -chmod g+w /user/hive/warehouse", verbose=False)

    def start(self):
        """Start Hive processes."""

        logger.info("Starting Hive")

        if self.running:
            logger.warn("Hive was already started")
            return

        if not self.hc.running:
            logger.warn("Hadoop must be started first")
            self.hc.start_and_wait()

        # Do nothing
        self.running = True

    def stop(self):
        """Stop Hive processes."""

        self.running = False

    def start_shell(self, node=None, exec_params=None):
        """Open a Hive shell.

        Args:
          node (Host, optional):
            The host were the shell is to be started. If not provided,
            self.master is chosen.
          exec_params (str, optional):
            The list of parameters used in job execution.
        """

        if not node:
            node = self.master

        # Configure execution options
        if not exec_params:
            exec_params = []

        params_str = " " + " ".join(exec_params)

        # Execute shell
        call("ssh -t " + node.address + " '" +
             "export HADOOP_USER_CLASSPATH_FIRST=true;" +
             self.bin_dir + "/hive" + params_str + "'",
             shell=True)
        # Note: export needed to load correct version of jline

    def clean_conf(self):
        """Clean configuration files used by this cluster."""

        if self.temp_conf_dir and os.path.exists(self.temp_conf_dir):
            shutil.rmtree(self.temp_conf_dir)

    def clean_logs(self):
        """Remove all Hive logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.logs_dir + "/* ",
                        self.hosts)
        action.run()

        if restart:
            self.start()

    def clean_data(self):
        """Remove all data used by Hive"""

        logger.info("Cleaning data")

        # Warehouse
        self.hc.execute("fs -rm -r /user/hive/warehouse", verbose=False)

        # Metastore
        # TODO
        shutil.rmtree(self.metastore_dir)

    def clean(self):
        """Remove all files created by Hive."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_conf()
        self.clean_logs()
        self.clean_data()

        self.initialized = False

    def __force_clean(self):
        """Stop previous Hive processes (if any) and remove all remote files
        created by it."""

        hive_processes = []

        force_kill = False
        for h in self.hosts:
            proc = SshProcess("jps", self.master)
            proc.run()

            ids_to_kill = []
            for line in proc.stdout.splitlines():
                field = line.split()
                if field[1] in hive_processes:
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