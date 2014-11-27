"""hadoop_g5k: Hadoop cluster management in Grid5000."""

import getpass
import os
import re
import shutil
import stat
import sys
import tempfile
try:  # Import Python 3 package, turn back to Python 2 if fails
    import configparser
except ImportError:
    import ConfigParser as configparser

from execo.action import Put, TaktukPut, Get, Remote
from execo.process import SshProcess
from execo_engine import logger
from execo_g5k.api_utils import get_host_attributes, get_host_cluster

# Constant definitions
CORE_CONF_FILE = "core-site.xml"
HDFS_CONF_FILE = "hdfs-site.xml"
MR_CONF_FILE = "mapred-site.xml"

DEFAULT_HADOOP_BASE_DIR = "/tmp/hadoop"
DEFAULT_HADOOP_CONF_DIR = DEFAULT_HADOOP_BASE_DIR + "/conf"
DEFAULT_HADOOP_LOGS_DIR = DEFAULT_HADOOP_BASE_DIR + "/logs"
DEFAULT_HADOOP_TEMP_DIR = DEFAULT_HADOOP_BASE_DIR + "/tmp"

DEFAULT_HADOOP_HDFS_PORT = 54310
DEFAULT_HADOOP_MR_PORT = 54311

DEFAULT_HADOOP_LOCAL_CONF_DIR = "conf"


class HadoopException(Exception):
    pass


class HadoopNotInitializedException(HadoopException):
    pass


class HadoopJobException(HadoopException):
    pass


class HadoopTopology(object):
    """This class is able to produce and manage a Hadoop topology."""

    def __init__(self, hosts, topo_list=None):
        """Create a hadoop topology object assigning each host to the
        corresponding rack.
        
        Args:
          hosts (list of Host): The hosts to be assigned a topology.
          topo_list (list of str, optional): The racks to be assigned to each
            host. len(hosts) should be equal to len(topo_list)
            Second line of description should be indented.
        """

        if topo_list:
            if len(hosts) == len(topo_list):
                self.topology = topo_list
                return
            else:
                logger.warn("hosts and topology have not the same length.")

        logger.info("Discovering topology automatically")
        self.topology = {}
        for h in hosts:
            nw_adapters = get_host_attributes(h)[u'network_adapters']
            for nwa in nw_adapters:
                if (u'network_address' in nwa and
                            nwa[u'network_address'] == h.address):
                    self.topology[h] = "/" + nwa[u'switch']
                    break

    def get_rack(self, host):
        """Return the rack corresponding to a host.
        
        Args:
          host (Host): The host whose rack is queried.
          
        Returns:
          str: The rack corresponding to the given host.
          
        """

        return self.topology[host]

    def __str__(self):
        return str(self.topology)

    def create_files(self, dest, data_file="topo.dat",
                     script_file="topo.sh"):
        """Create the script (topo.sh) and data (topo.dat) files used to obtain
        the topology in Hadoop.
        
        Args:
          dest (str): The name of the directory where the files will be created.
        """

        # Create topology data file
        topo_data_file = open(dest + "/" + data_file, "w")
        for h, t in self.topology.iteritems():
            topo_data_file.write(h.address + " " + t + "\n")
        topo_data_file.close()

        # Create topology script file
        script_str = """#!/bin/bash -e

script_dir=$(readlink -f $(dirname $0))
file_topo="$script_dir/topo.dat"

if [ ! -f $file_topo ]
then
    touch $file_topo
fi

output=""

for node in $@
do
    host_name=$(dig +short -x $node | rev | cut -c2- | rev)
    rack=$(grep $host_name $file_topo | cut -d' ' -f2)
    if [ -z $rack ]
    then
        rack="/default-rack"
    fi
    output="$output $rack"
done

echo $output
"""

        topo_script_file = open(dest + "/" + script_file, "w")
        topo_script_file.write(script_str)
        topo_script_file.close()

        st = os.stat(dest + "/" + script_file)
        os.chmod(dest + "/" + script_file, st.st_mode | stat.S_IEXEC)


class HadoopJarJob(object):
    state = -1
    job_id = "unknown"
    success = None

    def __init__(self, jar_path, params=None, lib_paths=None):
        """Creates a new Hadoop MapReduce jar job with the given parameters.

        Args:
          jar_path (str): The local path of the jar containing the job.
          params (list of str, optional): The list of parameters of the job.
          lib_paths (list of str, optional): The list of local paths to the
            libraries used by the job.
        """

        if not params:
            params = []
        if not lib_paths:
            lib_paths = []

        # Check if the jar file exists
        if not os.path.exists(jar_path):
            logger.error("Jar file " + jar_path + " does not exist")
            raise HadoopJobException("Jar file " + jar_path + " does not exist")

        # Check if the libraries exist
        for lp in lib_paths:
            if not os.path.exists(lp):
                logger.warn("Lib file " + lp + " does not exist")
                return  # TODO - exception

        self.jar_path = jar_path
        self.params = params
        self.lib_paths = lib_paths

    def get_files_to_copy(self):
        """Return the set of files that are used by the job and need to be
        copied to the cluster. This includes among others the job jar and the
        used libraries."""

        # Copy jar and lib files to cluster
        files_to_copy = [self.jar_path]
        for lp in self.lib_paths:
            files_to_copy.append(lp)

        return files_to_copy

    def get_command(self, exec_dir="."):
        """Return the Hadoop command that executes this job.

        Args:
          exec_dir (str, optional): The path of the directory where the job is
            to be executed.
        """

        # Get parameters
        jar_file = os.path.join(exec_dir, os.path.basename(self.jar_path))
        if self.lib_paths:
            libs_param = " -libjars "
            for lp in self.lib_paths:
                libs_param += os.path.join(exec_dir, os.path.basename(lp)) + ","
            libs_param = libs_param[:-1]
        else:
            libs_param = ""

        if isinstance(self.params, basestring):
            params_str = " " + self.params
        else:
            params_str = ""
            for p in self.params:
                params_str += " " + p

        return "jar " + jar_file + libs_param + params_str


class HadoopCluster(object):
    """This class manages the whole life-cycle of a hadoop cluster.
    
    Attributes:
      master (Host): The host selected as the master. It runs the namenode and
        jobtracker.
      hosts (list of Hosts): List of hosts composing the cluster. All run
        datanode and tasktracker processes.
      topology (HadoopTopology): The topology of the cluster hosts.
      initialized (bool): True if the cluster has been initialized, False
        otherwise.
      running (bool): True if both the namenode and jobtracker are running,
        False otherwise.
      running_dfs (bool): True if the namenode is running, False otherwise.  
      running_map_reduce (bool): True if the jobtracker is running, False
        otherwise.
        
    """

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
          hosts (list of Host): The hosts to be assigned a topology.
          topo_list (list of str, optional): The racks to be assigned to each
            host. len(hosts) should be equal to len(topo_list)
            Second line of description should be indented.
          configFile (str, optional): The path of the config file to be used.
        """

        # Load cluster properties
        config = configparser.ConfigParser(self.defaults)
        config.add_section("cluster")
        config.add_section("local")

        if config_file:
            config.readfp(open(config_file))

        self.hadoop_base_dir = config.get("cluster", "hadoop_base_dir")
        self.hadoop_conf_dir = config.get("cluster", "hadoop_conf_dir")
        self.hadoop_logs_dir = config.get("cluster", "hadoop_logs_dir")
        self.hadoop_temp_dir = config.get("cluster", "hadoop_temp_dir")
        self.hdfs_port = config.getint("cluster", "hdfs_port")
        self.mapred_port = config.getint("cluster", "mapred_port")
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")

        self.hadoop_bin_dir = self.hadoop_base_dir + "/bin"
        self.hadoop_sbin_dir = self.hadoop_base_dir + "/bin"

        # Configure master and slaves
        self.hosts = hosts
        self.master = hosts[0]

        # Create topology
        self.topology = HadoopTopology(hosts, topo_list)

        # Store cluster information
        self.host_clusters = {}
        for h in self.hosts:
            g5k_cluster = get_host_cluster(h)
            if g5k_cluster in self.host_clusters:
                self.host_clusters[g5k_cluster].append(h)
            else:
                self.host_clusters[g5k_cluster] = [h]

        logger.info("Hadoop cluster created with master " + str(self.master) +
                    ", hosts " + str(self.hosts) + " and topology " +
                    str(self.topology))

    def bootstrap(self, hadoop_tar_file):
        """Install hadoop in all cluster nodes from the specified tar.gz file.
        
        Args:
          hadoop_tar_file (str): The file containing hadoop binaries.
        """

        # 1. Remove used dirs if existing
        action = Remote("rm -rf " + self.hadoop_base_dir, self.hosts)
        action.run()
        action = Remote("rm -rf " + self.hadoop_conf_dir, self.hosts)
        action.run()
        action = Remote("rm -rf " + self.hadoop_logs_dir, self.hosts)
        action.run()
        action = Remote("rm -rf " + self.hadoop_temp_dir, self.hosts)
        action.run()

        # 1. Copy hadoop tar file and uncompress
        logger.info("Copy " + hadoop_tar_file + " to hosts and uncompress")
        action = TaktukPut(self.hosts, [hadoop_tar_file], "/tmp")
        action.run()
        action = Remote(
            "tar xf /tmp/" + os.path.basename(hadoop_tar_file) + " -C /tmp",
            self.hosts)
        action.run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        action = Remote(
            "mv /tmp/" +
            os.path.basename(hadoop_tar_file).replace(".tar.gz", "") + " " +
            self.hadoop_base_dir,
            self.hosts)
        action.run()

        # 3 Create other dirs        
        action = Remote("mkdir -p " + self.hadoop_conf_dir, self.hosts)
        action.run()

        action = Remote("mkdir -p " + self.hadoop_logs_dir, self.hosts)
        action.run()

        action = Remote("mkdir -p " + self.hadoop_temp_dir, self.hosts)
        action.run()

        # 4. Specify environment variables
        command = "cat >> " + self.hadoop_conf_dir + "/hadoop-env.sh << EOF\n"
        # TODO: is there a way to obtain JAVA_HOME automatically?
        command += "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64\n"
        command += "export HADOOP_LOG_DIR=" + self.hadoop_logs_dir + "\n"
        command += "HADOOP_HOME_WARN_SUPPRESS=\"TRUE\"\n"
        command += "EOF"
        action = Remote(command, self.hosts)
        action.run()

    def initialize(self):
        """Initialize the cluster: copy base configuration and format DFS."""

        self._pre_initialize()

        logger.info("Initializing hadoop")

        # Set basic configuration
        self._copy_base_conf()
        self._create_master_and_slave_conf()
        self.topology.create_files(self.conf_dir)

        # Configure hosts depending on resource type
        for g5k_cluster in self.host_clusters:
            hosts = self.host_clusters[g5k_cluster]
            self._configure_servers(hosts)
            self._copy_conf(self.conf_dir, hosts)

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

    def _copy_base_conf(self):
        """Copy base configuration files to tmp dir."""

        self.conf_dir = tempfile.mkdtemp("", "hadoop-", "/tmp")
        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        mandatory_files = [CORE_CONF_FILE, HDFS_CONF_FILE, MR_CONF_FILE]

        missing_conf_files = mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        logger.info("Copying missing conf files from master: " + str(
            missing_conf_files))

        remote_missing_files = [os.path.join(self.hadoop_conf_dir, f)
                                for f in missing_conf_files]

        action = Get([self.master], remote_missing_files, self.conf_dir)
        action.run()

    def _create_master_and_slave_conf(self):
        """Create master and slaves configuration files."""

        master_file = open(self.conf_dir + "/masters", "w")
        master_file.write(self.master.address + "\n")
        master_file.close()

        slaves_file = open(self.conf_dir + "/slaves", "w")
        for s in self.hosts:
            slaves_file.write(s.address + "\n")
        slaves_file.close()

    def _check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.
        
        Raises:
          HadoopNotInitializedException: If self.initialized = False
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise HadoopNotInitializedException(
                "The cluster should be initialized")

    def _configure_servers(self, hosts=None):
        """Configure servers and host-dependant parameters.

           Args:
             hosts (list of Host, optional): The list of hosts to take into
               account in the configuration. If not specified, all the hosts of
               the hadoop cluster are used. The first host of this list is
               always used as the reference.
        """

        if not hosts:
            hosts = self.hosts

        host_attrs = get_host_attributes(hosts[0])
        num_cores = host_attrs[u'architecture'][u'smt_size']
        mem_per_slot = (int(host_attrs[u'main_memory'][u'ram_size']) -
                        2 * 1024 * 1024) / \
                       (1024 * 1024 * num_cores)

        self._replace_in_file(os.path.join(self.conf_dir, CORE_CONF_FILE),
                              "fs.default.name",
                              "hdfs://" + self.master.address + ":" +
                                          str(self.hdfs_port) + "/",
                              True)
        self._replace_in_file(os.path.join(self.conf_dir, CORE_CONF_FILE),
                              "hadoop.tmp.dir",
                              self.hadoop_temp_dir, True)
        self._replace_in_file(os.path.join(self.conf_dir, CORE_CONF_FILE),
                              "topology.script.file.name",
                              self.hadoop_conf_dir + "/topo.sh", True)

        self._replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE),
                              "mapred.job.tracker",
                              self.master.address + ":" +
                              str(self.mapred_port), True)
        self._replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE),
                              "mapred.tasktracker.map.tasks.maximum",
                              str(num_cores - 1), True)
        self._replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE),
                              "mapred.tasktracker.reduce.tasks.maximum",
                              str(num_cores - 1), True)
        self._replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE),
                              "mapred.child.java.opts",
                              "-Xmx" + str(mem_per_slot) + "m", True)

    def _copy_conf(self, conf_dir, hosts=None):
        """Copy configuration files from given dir to remote dir in cluster
        hosts.
        
        Args:
          conf_dir (str): The remote configuration dir.
          hosts (list of Host, optional): The list of hosts where the
            configuration is going to be copied. If not specified, all the hosts
            of the hadoop cluster are used.
        """

        if not hosts:
            hosts = self.hosts

        conf_files = [os.path.join(conf_dir, f) for f in os.listdir(conf_dir)]

        action = TaktukPut(hosts, conf_files, self.hadoop_conf_dir)
        action.run()

        if not action.finished_ok:
            logger.warn("Error while copying configuration")
            if not action.ended:
                action.kill()

    def change_conf(self, params):
        """Modify hadoop configuration. This method copies the configuration
        files from the first host of each g5k cluster conf dir into a local
        temporary dir, do all the changes in place and broadcast the new
        configuration files to all hosts.
        
        Args:
          params (dict of str:str): The parameters to be changed in the form
            key:value.
        """

        for g5k_cluster in self.host_clusters:
            hosts = self.host_clusters[g5k_cluster]

            # Copy conf files from first host in the cluster
            action = Remote("ls " + self.hadoop_conf_dir + "/*.xml", [hosts[0]])
            action.run()
            output = action.processes[0].stdout

            remote_conf_files = []
            for f in output.split():
                remote_conf_files.append(os.path.join(self.hadoop_conf_dir, f))

            tmp_dir = "/tmp/mliroz_temp_hadoop/"
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)

            action = Get([hosts[0]], remote_conf_files, tmp_dir)
            action.run()

            # Do replacements in temp file
            temp_conf_files = [os.path.join(tmp_dir, f) for f in
                               os.listdir(tmp_dir)]

            for name, value in params.iteritems():
                for f in temp_conf_files:
                    if self._replace_in_file(f, name, value):
                        break
                else:
                    # Property not found - provisionally add it in MR_CONF_FILE
                    f = os.path.join(tmp_dir, MR_CONF_FILE)
                    self._replace_in_file(f, name, value, True)

            # Copy back the files to all hosts
            self._copy_conf(tmp_dir, hosts)

    def _replace_in_file(self, f, name, value, create_if_absent=False):
        """Assign the given value to variable name in file f.
        
        Args:
          f (str): The path of the configuration file.
          name (str): The name of the variable.
          value (str): The new value to be assigned:
          create_if_absent (bool, optional): If True, the variable will be
            created at the end of the file in case it was not already present.
        
        Returns:
          bool: True if the assignment has been made, False otherwise.
        """

        changed = False

        (_, temp_file) = tempfile.mkstemp("", "hadoopf-", "/tmp")

        inf = open(f)
        outf = open(temp_file, "w")
        line = inf.readline()
        while line != "":
            if "<name>" + name + "</name>" in line:
                if "<value>" in line:
                    outf.write(self.__replace_line(line, value))
                    changed = True
                else:
                    outf.write(line)
                    line = inf.readline()
                    if line != "":
                        outf.write(self.__replace_line(line, value))
                        changed = True
                    else:
                        logger.error("Configuration file " + f +
                                     " is not correctly formatted")
            else:
                if ("</configuration>" in line and
                        create_if_absent and not changed):
                    outf.write("  <property><name>" + name + "</name>" +
                               "<value>" + str(value) + "</value></property>\n")
                    outf.write(line)
                    changed = True
                else:
                    outf.write(line)
            line = inf.readline()
        inf.close()
        outf.close()

        if changed:
            shutil.copyfile(temp_file, f)
        os.remove(temp_file)

        return changed

    def __replace_line(self, line, value):
        return re.sub(r'(.*)<value>[^<]*</value>(.*)', r'\g<1><value>' + value +
                      r'</value>\g<2>', line)

    def format_dfs(self):
        """Format the distributed filesystem."""

        logger.info("Formatting HDFS")

        proc = SshProcess(self.hadoop_bin_dir + "/hadoop namenode -format",
                          self.master)
        proc.run()

        if proc.finished_ok:
            logger.info("HDFS formatted successfully")
        else:
            logger.warn("Error while formatting HDFS")

    def start(self):
        """Start the namenode and then the jobtracker."""

        self._check_initialization()

        self.start_dfs()
        self.start_map_reduce()

        self.running = True

    def start_and_wait(self):
        """Start the namenode and then the jobtracker. Wait for them to exit
        safemode before continuing."""

        self._check_initialization()

        self.start_dfs_and_wait()
        self.start_map_reduce_and_wait()

        self.running = True

    def start_dfs(self):
        """Start the namenode."""

        self._check_initialization()

        logger.info("Starting HDFS")

        if self.running_dfs:
            logger.warn("Dfs was already started")
            return

        proc = SshProcess(self.hadoop_sbin_dir + "/start-dfs.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting HDFS")
        else:
            self.running_dfs = True

    def start_dfs_and_wait(self):
        """Start the namenode and wait for it to exit safemode."""

        self._check_initialization()

        self.start_dfs()

        logger.info("Waiting for safe mode to be off")
        proc = SshProcess(
            self.hadoop_bin_dir + "/hadoop dfsadmin -safemode wait",
            self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting HDFS")
        else:
            self.running_dfs = True

    def start_map_reduce(self):
        """Start the jobtracker."""

        self._check_initialization()

        logger.info("Starting MapReduce")

        if self.running_map_reduce:
            logger.warn("Error while starting MapReduce")
            return

        proc = SshProcess(self.hadoop_sbin_dir + "/start-mapred.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.info("MapReduce started successfully")
        else:
            self.running_map_reduce = True

    def start_map_reduce_and_wait(self):
        """Start the jobtracker and wait for it to exit safemode."""

        self._check_initialization()

        self.start_map_reduce()

        # logger.info("Waiting for safe mode to be off")
        # proc = SshProcess(self.hadoop_base_dir +
        #                   "/bin/hadoop mradmin -safemode wait",
        # self.master)
        # proc.run()

        # TODO - does the jobtracker enter safemode?

    def stop(self):
        """Stop the jobtracker and then the namenode."""

        self._check_initialization()

        self.stop_map_reduce()
        self.stop_dfs()

        self.running = False

    def stop_dfs(self):
        """Stop the namenode."""

        self._check_initialization()

        logger.info("Stopping HDFS")

        proc = SshProcess(self.hadoop_sbin_dir + "/stop-dfs.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while stopping HDFS")
        else:
            self.running_dfs = False

    def stop_map_reduce(self):
        """Stop the jobtracker."""

        self._check_initialization()

        logger.info("Stopping MapReduce")

        proc = SshProcess(self.hadoop_sbin_dir + "/stop-mapred.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while stopping MapReduce")
        else:
            self.running_map_reduce = False

    class __ColorDecorator(object):

        defaultColor = '\033[0;0m'

        def __init__(self, component, color):
            self.component = component
            self.color = color

        def __getattr__(self, attr):
            if attr == 'write' and self.component.isatty():
                return lambda x: self.component.write(self.color + x +
                                                      self.defaultColor)
            else:
                return getattr(self.component, attr)

    def execute(self, command, node=None, should_be_running=True,
                verbose=True):
        """Execute the given command in the given node.
        
        Args:
          command (str): The command to be executed.
          node (Host, optional): The host were the command should be executed.
            If not provided, self.master is chosen.
          should_be_running (bool, optional): True if the cluster needs to be
            running in order to execute the command. If so, and it is not
            running, it is automatically started. (default: True)
          verbose: (bool, optional): If True stdout and stderr of remote process
            is displayed. (default: True)
        """

        self._check_initialization()

        if should_be_running and not self.running:
            logger.warn("The cluster was stopped. Starting it automatically")
            self.start()

        if not node:
            node = self.master

        if verbose:
            logger.info("Executing {" + self.hadoop_bin_dir + "/hadoop " +
                        command + "} in " + str(node))

        proc = SshProcess(self.hadoop_bin_dir + "/hadoop " + command, node)

        if verbose:
            red_color = '\033[01;31m'

            proc.stdout_handlers.append(sys.stdout)
            proc.stderr_handlers.append(
                self.__ColorDecorator(sys.stderr, red_color))

        proc.start()
        proc.wait()

        return (proc.stdout, proc.stderr)

    def execute_jar(self, job, node=None, verbose=True):
        """Execute the given mapreduce job included in the given jar.
        
        Args:
          job (HadoopJarJob): The job object.
          verbose: (bool, optional): If True stdout and stderr of remote process
            is displayed. (default: True)          
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
        logger.info("Executing jar job. Command = {" + self.hadoop_bin_dir +
                    "/hadoop " + command + "} in " + str(node))

        proc = SshProcess(self.hadoop_bin_dir + "/hadoop " + command, node)

        if verbose:
            red_color = '\033[01;31m'

            proc.stdout_handlers.append(sys.stdout)
            proc.stderr_handlers.append(
                self.__ColorDecorator(sys.stderr, red_color))

        proc.start()
        proc.wait()

        # Get job info
        job.stdout = proc.stdout
        job.stderr = proc.stderr
        job.success = (proc.exit_code == 0)

        for line in job.stdout.splitlines():
            if "Running job" in line:
                if "mapred.JobClient" in line:  # TODO: more possible formats?
                    try:
                        match = re.match('.*Running job: (.*)', line)
                        job.job_id = match.group(1)
                        break
                    except:
                        pass

        return (proc.stdout, proc.stderr)

    def copy_history(self, dest, job_ids=None):
        """Copy history logs from master.
        
        Args:
          dest (str): the path of the local dir where the logs will be copied.
        """

        if not os.path.exists(dest):
            logger.warning("Destination directory " + dest +
                           " does not exist. It will be created")
            os.makedirs(dest)

        history_dir = os.path.join(self.hadoop_logs_dir, "history")
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

        action = Remote("rm -rf " + self.hadoop_logs_dir + "/history",
                        [self.master])
        action.run()

        if restart:
            self.start()

    def clean_conf(self):
        """Clean configuration files used by this cluster."""

        shutil.rmtree(self.conf_dir)

    def clean_logs(self):
        """Remove all hadoop logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.hadoop_logs_dir + "/*", self.hosts)
        action.run()

        if restart:
            self.start()

    def clean_data(self):
        """Remove all data created by hadoop (including filesystem)."""

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
        """Stop previous hadoop processes (if any) and remove all remote files
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
        """Return the hadoop version.
        
        Returns:
          str: The version used by the Hadoop cluster.
        """

        proc = SshProcess(self.hadoop_bin_dir + "/hadoop version",
                          self.master)
        proc.run()
        version = proc.stdout.splitlines()[0]
        return version

        # End HadoopCluster ####################################################
