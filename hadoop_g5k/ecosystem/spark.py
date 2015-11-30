import os
import shutil
import sys
import tempfile

from abc import abstractmethod

from ConfigParser import ConfigParser
from subprocess import call

from execo.action import Put, TaktukPut, Get, Remote, TaktukRemote, \
    SequentialActions
from execo.log import style
from execo.process import SshProcess
from execo_engine import logger
from execo_g5k import get_host_cluster
from hadoop_g5k.hardware import G5kDeploymentHardware

from hadoop_g5k.util import ColorDecorator, read_in_props_file, \
    write_in_props_file, read_param_in_props_file

# Configuration files
SPARK_CONF_FILE = "spark-defaults.conf"

# Default parameters
DEFAULT_SPARK_BASE_DIR = "/tmp/spark"
DEFAULT_SPARK_CONF_DIR = DEFAULT_SPARK_BASE_DIR + "/conf"
DEFAULT_SPARK_LOGS_DIR = DEFAULT_SPARK_BASE_DIR + "/logs"
DEFAULT_SPARK_EVENTS_DIR = ""
DEFAULT_SPARK_WORK_DIR = DEFAULT_SPARK_BASE_DIR + "/work"
DEFAULT_SPARK_PORT = 7077

DEFAULT_SPARK_LOCAL_CONF_DIR = "spark-conf"

# Modes
STANDALONE_MODE = 0
YARN_MODE = 1


class SparkException(Exception):
    pass


class SparkJobException(SparkException):
    pass


class SparkConfException(SparkException):
    pass


class SparkJob(object):
    """This class represents a Spark job.

    Attributes:
      job_path (str):
        The local path of the file containing the job binaries.
      app_params (list of str):
        The list of parameters of the job.
      lib_paths (list of str):
        The list of local paths to the libraries used by the job.
      state (int):
        State of the job.
      success (bool):
        Indicates whether the job have finished successfully or not. Before
        executing its value is None.
    """

    state = -1
    success = None

    def __init__(self, job_path, exec_params=None, app_params=None,
                 lib_paths=None):
        """Create a new Spark job with the given parameters.

        Args:
          job_path (str):
            The local path of the file containing the job binaries.
          exec_params (list of str, optional):
            The list of parameters used in job execution (e.g., driver-memory).
          app_params (list of str, optional):
            The list of parameters of the application.
          lib_paths (list of str, optional):
            The list of local paths to the libraries used by the job.
        """

        if exec_params is None:
            exec_params = []
        if app_params is None:
            app_params = []
        if lib_paths is None:
            lib_paths = []

        # Check if the jar file exists
        if not os.path.exists(job_path):
            logger.error("Job binaries file " + job_path + " does not exist")
            raise SparkJobException("Job binaries file " + job_path +
                                    " does not exist")

        # Check if the libraries exist
        for lp in lib_paths:
            if not os.path.exists(lp):
                logger.warn("Lib file " + lp + " does not exist")
                return  # TODO - exception

        self.job_path = job_path
        self.exec_params = exec_params
        self.app_params = app_params
        self.lib_paths = lib_paths

    def get_files_to_copy(self):
        """Return the set of files that are used by the job and need to be
        copied to the cluster. This includes among others the job binaries and
        the used libraries."""

        # Copy jar and lib files to cluster
        files_to_copy = [self.job_path]
        for lp in self.lib_paths:
            files_to_copy.append(lp)

        return files_to_copy

    @abstractmethod
    def get_command(self, exec_dir="."):
        pass

    def _get_exec_params_str(self):
        if isinstance(self.exec_params, basestring):
            params_str = self.exec_params
        else:
            params_str = " ".join(self.exec_params)
        return params_str + " "

    def _get_app_params_str(self):
        if isinstance(self.app_params, basestring):
            params_str = self.app_params
        else:
            params_str = " ".join(self.app_params)
        return " " + params_str


class PythonSparkJob(SparkJob):

    def get_command(self, exec_dir="."):

        # Get parameters
        job_file = os.path.join(exec_dir, os.path.basename(self.job_path))
        if self.lib_paths:
            libs_param = "--py_files " + \
                         ",".join(os.path.join(exec_dir, os.path.basename(lp))
                                  for lp in self.lib_paths) + \
                         " "
        else:
            libs_param = ""

        exec_params_str = self._get_exec_params_str()
        app_params_str = self._get_app_params_str()

        return exec_params_str + libs_param + job_file + app_params_str


class JavaOrScalaSparkJob(SparkJob):

    def __init__(self, job_path, exec_params=None, app_params=None,
                 lib_paths=None, main_class=None):

        super(JavaOrScalaSparkJob, self).__init__(job_path, exec_params,
                                                  app_params, lib_paths)

        if not main_class:
            call("/usr/bin/jar xf " +
                 os.path.abspath(job_path) + " META-INF/MANIFEST.MF",
                 cwd="/tmp", shell=True)
            if os.path.exists("/tmp/META-INF/MANIFEST.MF"):
                with open("/tmp/META-INF/MANIFEST.MF") as mf:
                    for line in mf:
                        if line.startswith("Main-Class:"):
                            main_class = line.strip().split(" ")[1]
                            break
                    else:
                        raise SparkJobException("A main class should be " +
                                                "provided or specified in the" +
                                                " jar manifest")
            else:
                raise SparkJobException("A main class should be provided or " +
                                        "specified in the jar manifest")

        self.main_class = main_class

    def get_command(self, exec_dir="."):

        # Get parameters
        job_file = os.path.join(exec_dir, os.path.basename(self.job_path))
        if self.lib_paths:
            libs_param = "--jars " + \
                         ",".join(os.path.join(exec_dir, os.path.basename(lp))
                                  for lp in self.lib_paths) + \
                         " "
        else:
            libs_param = ""

        exec_params_str = self._get_exec_params_str()
        app_params_str = self._get_app_params_str()
        main_class = "--class " + self.main_class + " "

        return exec_params_str + main_class + libs_param + job_file + app_params_str


class SparkCluster(object):
    """This class manages the whole life-cycle of a Spark cluster.

    Attributes:
      master (Host):
        The host selected as the master.
      hosts (list of Hosts):
        List of hosts composing the cluster.
      initialized (bool):
        True if the cluster has been initialized, False otherwise.
      running (bool):
        True if spark is running, False otherwise.
      mode (int):
        The cluster manager that is used (STANDALONE_MODE or YARN_MODE).
      hc (HadoopCluster):
        A reference to the Hadoop cluster if spark is deployed on top of one.
    """

    @staticmethod
    def get_cluster_type():
        return "spark"

    # Cluster state
    initialized = False
    running = False

    # Default properties
    defaults = {
        "spark_base_dir": DEFAULT_SPARK_BASE_DIR,
        "spark_conf_dir": DEFAULT_SPARK_CONF_DIR,
        "spark_logs_dir": DEFAULT_SPARK_LOGS_DIR,
        "spark_events_dir": DEFAULT_SPARK_EVENTS_DIR,
        "spark_work_dir": DEFAULT_SPARK_WORK_DIR,
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

        # Deployment properties
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")
        self.init_conf_dir = tempfile.mkdtemp("", "spark-init-", "/tmp")
        self.conf_mandatory_files = [SPARK_CONF_FILE]

        self.base_dir = config.get("cluster", "spark_base_dir")
        self.conf_dir = config.get("cluster", "spark_conf_dir")
        self.logs_dir = config.get("cluster", "spark_logs_dir")
        self.evs_log_dir = config.get("cluster", "spark_events_dir")
        self.work_dir = config.get("cluster", "spark_work_dir")
        self.port = config.getint("cluster", "spark_port")
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")

        self.bin_dir = self.base_dir + "/bin"
        self.sbin_dir = self.base_dir + "/sbin"

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
            raise SparkException("Hosts in the cluster must be specified "
                                 "either directly or indirectly through a "
                                 "Hadoop cluster.")

        # Store cluster information
        self.hw = G5kDeploymentHardware()
        self.hw.add_hosts(self.hosts)
        self.master_cluster = self.hw.get_cluster(get_host_cluster(self.master))

        # Store reference to Hadoop cluster and check if mandatory
        self.hc = hadoop_cluster
        if not self.hc and self.mode == YARN_MODE:
            logger.error("When using a YARN_MODE mode, a reference to the "
                         "Hadoop cluster should be provided.")
            raise SparkException("When using a YARN_MODE mode, a reference "
                                 "to the Hadoop cluster should be provided")

        if self.mode == STANDALONE_MODE:
            mode_text = "in standalone mode"
        else:
            mode_text = "on top of YARN"
        logger.info("Spark cluster created %s in hosts %s." +
                    (" It is linked to a Hadoop cluster." if self.hc else ""),
                    mode_text,
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

        # 1. Copy hadoop tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_dirs = TaktukRemote("rm -rf " + self.base_dir +
                               " " + self.conf_dir,
                               self.hosts)
        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        tar_xf = TaktukRemote(
            "tar xf /tmp/" + os.path.basename(tar_file) + " -C /tmp",
            self.hosts)
        SequentialActions([rm_dirs, put_tar, tar_xf]).run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        mv_base_dir = TaktukRemote(
            "mv /tmp/" + os.path.basename(tar_file).replace(".tgz", "") + " " +
            self.base_dir,
            self.hosts)
        mkdirs = TaktukRemote("mkdir -p " + self.conf_dir +
                              " && mkdir -p " + self.logs_dir,
                              self.hosts)
        chmods = TaktukRemote("chmod g+w " + self.base_dir +
                              " && chmod g+w " + self.conf_dir +
                              " && chmod g+w " + self.logs_dir,
                              self.hosts)
        SequentialActions([mv_base_dir, mkdirs, chmods]).run()

        # 2.1. Create spark-events dir
        if self.evs_log_dir:
            if self.evs_log_dir.startswith("file://") or \
                            "://" not in self.evs_log_dir:
                mk_evs_dir = TaktukRemote("mkdir -p " + self.evs_log_dir +
                                          " && chmod g+w " + self.evs_log_dir,
                                          self.hosts)
                mk_evs_dir.run()
            elif self.evs_log_dir.startswith("hdfs://"):
                self.hc.execute("fs -mkdir -p " + self.evs_log_dir)

        # 3. Specify environment variables
        command = "cat >> " + self.conf_dir + "/spark-env.sh << EOF\n"
        command += "JAVA_HOME=" + self.java_home + "\n"
        command += "SPARK_LOG_DIR=" + self.logs_dir + "\n"
        if self.hc:
            command += "HADOOP_CONF_DIR=" + self.hc.conf_dir + "\n"
        if self.mode == YARN_MODE:
            command += "YARN_CONF_DIR=" + self.hc.conf_dir + "\n"
        command += "EOF\n"
        command += "chmod +x " + self.conf_dir + "/spark-env.sh"
        action = Remote(command, self.hosts)
        action.run()

        # 4. Generate initial configuration
        self._initialize_conf()

    def _initialize_conf(self):
        """Merge locally-specified configuration files with default files
        from the distribution."""

        action = Remote("cp " + os.path.join(self.conf_dir,
                                             SPARK_CONF_FILE + ".template ") +
                        os.path.join(self.conf_dir, SPARK_CONF_FILE),
                        self.hosts)
        action.run()

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

    def initialize(self, default_tuning=False):
        """Initialize the cluster: copy base configuration and format DFS.

           Args
             default_tuning (bool, optional):
               Whether to use automatic tuning based on some best practices or
               leave the default parameters.
        """

        self._pre_initialize()

        logger.info("Initializing Spark")

        # Set basic configuration
        temp_conf_base_dir = tempfile.mkdtemp("", "spark-", "/tmp")
        temp_conf_dir = os.path.join(temp_conf_base_dir, "conf")
        shutil.copytree(self.init_conf_dir, temp_conf_dir)

        self._create_master_and_slave_conf(temp_conf_dir)
        self._configure_servers(temp_conf_dir, default_tuning)

        shutil.rmtree(temp_conf_base_dir)

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
        """Configure master and create slaves configuration files."""

        defs_file = conf_dir + "/spark-defaults.conf"

        if self.mode == STANDALONE_MODE:
            write_in_props_file(defs_file,
                                "spark.master",
                                "spark://%s:%d" % (self.master.address, self.port),
                                create_if_absent=True,
                                override=True)

        elif self.mode == YARN_MODE:
            write_in_props_file(defs_file,
                                "spark.master", "yarn-client",
                                create_if_absent=True,
                                override=False)

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
            cl_temp_conf_base_dir = tempfile.mkdtemp("", "spark-cl-", "/tmp")
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

        defs_file = conf_dir + "/spark-defaults.conf"

        params = {}

        min_exec_mem = self.hw.get_max_memory_cluster().get_memory()
        total_execs = 0

        for cluster in self.hw.get_clusters():

            num_cores = cluster.get_num_cores()
            spark_cores = min(1, num_cores - 2)

            available_mem = cluster.get_memory()
            if self.mode == STANDALONE_MODE:
                spark_mem = available_mem
            elif self.mode == YARN_MODE:
                total_conts_mem = int(self.hc.get_conf_param(
                    "yarn.nodemanager.resource.memory-mb",
                    8192,  # Default value in YARN
                    node=cluster.get_hosts()[0]
                ))

                spark_mem = total_conts_mem

            # Ideally: 5 threads / executor
            execs_per_node = max(1, spark_cores / 5)

            # Split memory and consider overhead
            exec_mem = (spark_mem / execs_per_node)
            mem_overhead = int(read_param_in_props_file(
                defs_file,
                "spark.yarn.executor.memoryOverhead",
                max(385, 0.1 * exec_mem)))

            if exec_mem < mem_overhead:
                error_msg = "Not enough memory to use the specified memory " \
                            "overhead: overhead = %d, executor memory = %d" \
                            % (mem_overhead, exec_mem)
                logger.error(error_msg)
                raise SparkConfException(error_msg)
            exec_mem -= mem_overhead
            min_exec_mem = min(min_exec_mem, exec_mem)

            # Accumulate executors
            total_execs += len(cluster.get_hosts()) * execs_per_node

            params[cluster.get_name()] = {}

        # Global parameters
        params["global"] = {
            "exec_mem": min_exec_mem,
            "exec_cores": 5,
            "total_execs": total_execs
        }

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

        defs_file = conf_dir + "/spark-defaults.conf"

        # spark-env.sh
        command = "cat >> " + self.conf_dir + "/spark-env.sh << EOF\n"
        command += "SPARK_MASTER_PORT=" + str(self.port) + "\n"
        command += "EOF\n"
        action = Remote(command, self.hosts)
        action.run()

        # Get already set parameters
        global_params = params["global"]
        exec_mem = global_params["exec_mem"]
        exec_cores = global_params["exec_cores"]
        total_execs = global_params["total_execs"]

        # Log parameters
        if self.evs_log_dir:
            write_in_props_file(defs_file,
                                "spark.eventLog.enabled", "true",
                                create_if_absent=True,
                                override=True)

            write_in_props_file(defs_file,
                                "spark.eventLog.dir", self.evs_log_dir,
                                create_if_absent=True,
                                override=True)

        write_in_props_file(defs_file,
                            "spark.logConf", "true",
                            create_if_absent=True,
                            override=False)

        if default_tuning:

            write_in_props_file(defs_file,
                                "spark.executor.memory", "%dm" % exec_mem,
                                create_if_absent=True,
                                override=False)
            write_in_props_file(defs_file,
                                "spark.executor.cores", exec_cores,
                                create_if_absent=True,
                                override=False)
            write_in_props_file(defs_file,
                                "spark.executor.instances", total_execs,
                                create_if_absent=True,
                                override=False)

            # if self.mode == YARN_MODE:
            #     write_in_props_file(defs_file,
            #                         "spark.dynamicAllocation.enabled", "true",
            #                         create_if_absent=True,
            #                         override=True)
            #     self.hc.change_conf({
            #         "spark.shuffle": "yarn.nodemanager.aux-services",
            #         "yarn.nodemanager.aux-services.spark_shuffle.class":
            #             "org.apache.spark.network.yarn.YarnShuffleService"
            #     })

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

    def start(self):
        """Start spark processes."""
        self.start_spark()

    def start_spark(self):
        """Start spark processes.
        In STANDALONE mode it starts the master and slaves. In YARN mode it just
        checks that Hadoop is running, and starts it if not.
        """

        logger.info("Starting Spark")

        if self.running:
            logger.warn("Spark was already started")
            return

        if self.mode == STANDALONE_MODE:
            proc = SshProcess(self.sbin_dir + "/start-master.sh;" +
                              self.sbin_dir + "/start-slaves.sh;",
                              self.master)
            proc.run()
            if not proc.finished_ok:
                logger.warn("Error while starting Spark")
                return
        elif self.mode == YARN_MODE:
            if not self.hc.running:
                logger.warn("YARN services must be started first")
                self.hc.start_and_wait()

        self.running = True

    def stop(self):
        """Stop Spark processes."""

        self.stop_spark()

    def stop_spark(self):
        """Stop Spark processes."""

        logger.info("Stopping Spark")

        if self.mode == STANDALONE_MODE:
            proc = SshProcess(self.sbin_dir + "/stop-slaves.sh;" +
                              self.sbin_dir + "/stop-master.sh;",
                              self.master)
            proc.run()
            if not proc.finished_ok:
                logger.warn("Error while stopping Spark")
                return

        self.running = False

    def start_shell(self, language="IPYTHON", node=None, exec_params=None):
        """Open a Spark shell.

        Args:
          language (str, optional):
            The language to be used in the shell.
          node (Host, optional):
            The host were the shell is to be started. If not provided,
            self.master is chosen.
          exec_params (str, optional):
            The list of parameters used in job execution (e.g., driver-memory).
        """

        if not node:
            node = self.master

        # Configure execution options
        if exec_params is None:
            exec_params = []

        if self.mode == YARN_MODE:
            exec_params.append("--master yarn-client")

        params_str = " " + " ".join(exec_params)

        # Execute shell
        if language.upper() == "IPYTHON":
            call("ssh -t " + node.address + " " +
                 "IPYTHON=1 " + self.bin_dir + "/pyspark" + params_str,
                 shell=True)
        elif language.upper() == "PYTHON":
            call("ssh -t " + node.address + " " +
                 self.bin_dir + "/pyspark" + params_str,
                 shell=True)
        elif language.upper() == "SCALA":
            call("ssh -t " + node.address + " " +
                 self.bin_dir + "/spark-shell" + params_str,
                 shell=True)
        else:
            logger.error("Unknown language " + language)
            return

    def is_on_top_of_yarn(self):
        return self.mode == YARN_MODE

    def is_standalone(self):
        return self.mode == STANDALONE_MODE

    def execute_job(self, job, node=None, verbose=True):
        """Execute the given Spark job in the specified node.

        Args:
          job (SparkJob):
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

        if not self.running:
            logger.warn("The cluster was stopped. Starting it automatically")
            self.start()

        if node is None:
            node = self.master

        exec_dir = "/tmp"

        # Copy necessary files to cluster
        files_to_copy = job.get_files_to_copy()
        action = Put([node], files_to_copy, exec_dir)
        action.run()

        # Get command
        command = job.get_command(exec_dir)

        # Execute
        logger.info("Executing spark job. Command = {" + self.bin_dir +
                    "/spark-submit " + command + "} in " + str(node))

        proc = SshProcess(self.bin_dir + "/spark-submit " + command, node)

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

        return proc.stdout, proc.stderr

    def clean_conf(self):
        """Clean configuration files used by this cluster."""

        #if self.temp_conf_dir and os.path.exists(self.temp_conf_dir):
        #    shutil.rmtree(self.temp_conf_dir)
        pass

    def clean_logs(self):
        """Remove all Spark logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.logs_dir + "/* " +
                                    self.work_dir + "/*",
                        self.hosts)
        action.run()

        if restart:
            self.start()

    def clean(self):
        """Remove all files created by Spark."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_conf()
        self.clean_logs()

        self.initialized = False

    def __force_clean(self):
        """Stop previous Spark processes (if any) and remove all remote files
        created by it."""

        spark_processes = [
            "Master",
            "Worker"
        ]

        force_kill = False
        for h in self.hosts:
            proc = SshProcess("jps", h)
            proc.run()

            ids_to_kill = []
            for line in proc.stdout.splitlines():
                field = line.split()
                if field[1] in spark_processes:
                    ids_to_kill.append(field[0])

            if ids_to_kill:
                force_kill = True
                ids_to_kill_str = ""
                for pid in ids_to_kill:
                    ids_to_kill_str += " " + pid

                logger.warn(
                    "Killing running Spark processes in host %s" %
                    style.host(h.address.split('.')[0]))

                proc = SshProcess("kill -9" + ids_to_kill_str, h)
                proc.run()

        if force_kill:
            logger.info(
                "Processes from previous hadoop deployments had to be killed")

        self.clean_logs()
