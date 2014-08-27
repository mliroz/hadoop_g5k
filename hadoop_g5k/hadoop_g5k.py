#!/usr/bin/env python

"""hadoop_g5k: Hadoop cluster management in Grid5000."""

import getpass
import os
import pickle
import re
import shutil
import stat
import sys
import tempfile
import ConfigParser

from argparse import ArgumentParser, RawTextHelpFormatter
from execo import Host
from execo.action import Put, TaktukPut, Get, Remote
from execo.log import style
from execo.process import SshProcess
from execo_engine import logger
from execo_g5k.api_utils import get_host_attributes

__user_login = getpass.getuser()

# Constant definitions
CORE_CONF_FILE = "core-site.xml"
HDFS_CONF_FILE = "hdfs-site.xml"
MR_CONF_FILE = "mapred-site.xml"

DEFAULT_HADOOP_BASE_DIR = "/opt/base/hadoop"
DEFAULT_HADOOP_CONF_DIR = DEFAULT_HADOOP_BASE_DIR + "/conf"
DEFAULT_HADOOP_LOGS_DIR = "/opt/base/logs/hadoop"
DEFAULT_HADOOP_TEMP_DIR = "/tmp/" + getpass.getuser() + "_hadoop/"

DEFAULT_HADOOP_HDFS_PORT = 54310
DEFAULT_HADOOP_MR_PORT = 54311

DEFAULT_HADOOP_LOCAL_CONF_DIR = "/home/" + getpass.getuser() + \
                                "/common/hadoop/conf"

class NotInitialized(Exception):
    pass

class HadoopTopology(object):
    """This class is able to produce and manage a Hadoop topology."""

    def __init__(self, hosts, topo_list = None):
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


    def create_files(self, dest, data_file = "topo.dat", 
                     script_file = "topo.sh"):
        """Create the script (topo.sh) and data (topo.dat) files used to obtain
        the topology in Hadoop.
        
        Args:
          dest (str): The name of the directory where the files will be created.
        """

        # Create topology data file
        topoDataFile = open(dest + "/" + data_file, "w")
        for h, t in self.topology.iteritems():
            topoDataFile.write(h.address + " " + t + "\n")
        topoDataFile.close()

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

        topoScriptFile = open(dest + "/" + script_file, "w")
        topoScriptFile.write(script_str)
        topoScriptFile.close()

        st = os.stat(dest + "/" + script_file)
        os.chmod(dest + "/" + script_file, st.st_mode | stat.S_IEXEC)

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

    def __init__(self, hosts, topo_list = None, configFile = None):
        """Create a new Hadoop cluster with the given hosts and topology.
        
        Args:
          hosts (list of Host): The hosts to be assigned a topology.
          topo_list (list of str, optional): The racks to be assigned to each
            host. len(hosts) should be equal to len(topo_list)
            Second line of description should be indented.
          configFile (str, optional): The path of the config file to be used.
        """

        # Load cluster properties
        defaults = {
            "hadoop_base_dir" : DEFAULT_HADOOP_BASE_DIR,
            "hadoop_conf_dir" : DEFAULT_HADOOP_CONF_DIR,
            "hadoop_logs_dir" : DEFAULT_HADOOP_LOGS_DIR,
            "hadoop_temp_dir" : DEFAULT_HADOOP_TEMP_DIR,
            "hdfs_port" : str(DEFAULT_HADOOP_HDFS_PORT),
            "mapred_port" : str(DEFAULT_HADOOP_MR_PORT),

            "local_base_conf_dir" : DEFAULT_HADOOP_LOCAL_CONF_DIR
        }
        config = ConfigParser.ConfigParser(defaults)
        config.add_section("cluster")
        config.add_section("local")

        if configFile:
            config.readfp(open(configFile))

        self.hadoop_base_dir = config.get("cluster","hadoop_base_dir")
        self.hadoop_conf_dir = config.get("cluster","hadoop_conf_dir")
        self.hadoop_logs_dir = config.get("cluster","hadoop_logs_dir")
        self.hadoop_temp_dir = config.get("cluster","hadoop_temp_dir")
        self.hdfs_port = config.getint("cluster","hdfs_port")
        self.mapred_port = config.getint("cluster","mapred_port")
        self.local_base_conf_dir = config.get("local","local_base_conf_dir")

        # Configure master and slaves
        self.hosts = hosts
        self.master = hosts[0]

        # Create topology
        self.topology = HadoopTopology(hosts, topo_list)

        logger.info("Hadoop cluster created with master " + str(self.master) +
                    ", hosts " + str(self.hosts) + " and topology " +
                    str(self.topology))


    def initialize(self):
        """Initialize the cluster: copy base configuration and format DFS."""

        if self.initialized:
            if self.running:
                self.stop()
            self.clean()

        self.initialized = False

        logger.info("Initializing hadoop")

        # Copy base configuration files to tmp dir
        self.conf_dir = tempfile.mkdtemp("","hadoop-","/tmp")
        baseConfFiles = [ os.path.join(self.local_base_conf_dir,f) 
                          for f in os.listdir(self.local_base_conf_dir) ]
        for f in baseConfFiles:
            shutil.copy(f,self.conf_dir)

        # Create master and slaves configuration files
        masterFile = open(self.conf_dir + "/masters", "w")
        masterFile.write(self.master.address + "\n")
        masterFile.close()

        slavesFile = open(self.conf_dir + "/slaves", "w")
        for s in self.hosts:
            slavesFile.write(s.address + "\n")
        slavesFile.close()

        # Create topology files
        self.topology.create_files(self.conf_dir)

        # Configure servers and host-dependant parameters
        self.__configure_servers()

        # Copy configuration
        self.__copy_conf(self.conf_dir)

        # Format HDFS
        self.format_dfs()

        self.initialized = True

    def __check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.
        
        Raises:
          NotInitialized: If self.initialized = False
        """
        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise NotInitialized()

    def __configure_servers(self):
        """Configure servers and host-dependant parameters (TODO - we assume all
           nodes are equal).
        """

        host_attrs = get_host_attributes(self.master)
        num_cores = host_attrs[u'architecture'][u'smt_size']
        main_mem = int(host_attrs[u'main_memory'][u'ram_size']) \
                       / (1024 * 1024 * num_cores)

        self.__replace_in_file(os.path.join(self.conf_dir, CORE_CONF_FILE), 
            "fs.default.name",
            self.master.address + ":" + str(self.hdfs_port), True)
        self.__replace_in_file(os.path.join(self.conf_dir, CORE_CONF_FILE),
            "hadoop.tmp.dir",
            self.hadoop_temp_dir, True)
        self.__replace_in_file(os.path.join(self.conf_dir, CORE_CONF_FILE),
            "topology.script.file.name",
            self.hadoop_conf_dir + "/topo.sh", True)

        self.__replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE), 
            "mapred.job.tracker",
            self.master.address + ":" + str(self.mapred_port), True)
        self.__replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE), 
            "mapred.tasktracker.map.tasks.maximum", 
            str(num_cores), True)
        self.__replace_in_file(os.path.join(self.conf_dir, MR_CONF_FILE), 
            "mapred.child.java.opts", 
            "-Xmx" + str(main_mem) + "m", True)

    def __copy_conf(self, conf_dir):
        """Copy configuration files from given dir to remote dir in cluster
        hosts.
        
        Args:
          conf_dir (str): The remote configuration dir.
        """

        confFiles = [ os.path.join(conf_dir, f) for f in os.listdir(conf_dir) ]

        action = TaktukPut(self.hosts, confFiles, self.hadoop_conf_dir)
        action.run()

        if not action.finished_ok:
            logger.warn("Error while copying configuration")
            if not action.ended:
                action.kill()


    def change_conf(self, params):
        """Modify hadoop configuration. This method copies the configuration
        files from the master conf dir into a local temporary dir, do all the 
        changes in place and broadcast the new configuration files to all hosts.
        
        Args:
          params (dict of str:str): The parameters to be changed in the form
            key:value.
        """

        # Copy conf files from master
        remoteConfFiles = [ os.path.join(self.hadoop_conf_dir, f) 
            for f in os.listdir(self.local_base_conf_dir) if f.endswith(".xml")]

        tmp_dir = "/tmp/mliroz_temp_hadoop/"
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        action = Get([self.master], remoteConfFiles, tmp_dir)
        action.run()

        # Do replacements in temp file
        tempConfFiles = [ os.path.join(tmp_dir,f) for f in os.listdir(tmp_dir)]

        for name, value in params.iteritems():
            for f in tempConfFiles:
                if self.__replace_in_file(f, name, value):
                    break
            else:
                # Property not found - provisionally add it in MR_CONF_FILE
                f = os.path.join(tmp_dir,MR_CONF_FILE)
                self.__replace_in_file(f, name, value, True)


        # Copy back the files to all hosts
        self.__copy_conf(tmp_dir)


    def __replace_in_file(self, f, name, value, create_if_absent = False):
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

        (_,temp_file) = tempfile.mkstemp("","hadoopf-","/tmp")

        inf = open(f)
        outf = open(temp_file,"w")
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
                               "<value>" + value + "</vaue></property>\n");
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

        proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop namenode -format",
                          self.master)
        proc.run()

        if proc.finished_ok:
            logger.info("HDFS formatted successfully")
        else:
            logger.warn("Error while formatting HDFS")


    def start(self):
        """Start the namenode and then the jobtracker."""

        self.__check_initialization()

        self.start_dfs()
        self.start_map_reduce()

        self.running = True

    def start_and_wait(self):
        """Start the namenode and then the jobtracker. Wait for them to exit
        safemode before continuing."""

        self.__check_initialization()

        self.start_dfs_and_wait()
        self.start_map_reduce_and_wait()

        self.running = True

    def start_dfs(self):
        """Start the namenode."""

        self.__check_initialization()

        logger.info("Starting HDFS")

        proc = SshProcess(self.hadoop_base_dir + "/bin/start-dfs.sh", 
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting HDFS")
        else:
            self.running_dfs = True

    def start_dfs_and_wait(self):
        """Start the namenode and wait for it to exit safemode."""

        self.__check_initialization()

        self.start_dfs()

        logger.info("Waiting for safe mode to be off")
        proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop dfsadmin -safemode wait",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting HDFS")
        else:
            self.running_dfs = True


    def start_map_reduce(self):
        """Start the jobtracker."""

        self.__check_initialization()

        logger.info("Starting MapReduce")

        proc = SshProcess(self.hadoop_base_dir + "/bin/start-mapred.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.info("MapReduce started successfully")
        else:
            self.running_map_reduce = True

    def start_map_reduce_and_wait(self):
        """Start the jobtracker and wait for it to exit safemode."""

        self.__check_initialization()

        self.start_map_reduce()

        #logger.info("Waiting for safe mode to be off")
        #proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop mradmin -safemode wait", 
        #                  self.master)
        #proc.run()

        # TODO - does the jobtracker enter safemode?


    def stop(self):
        """Stop the jobtracker and then the namenode."""

        self.__check_initialization()

        self.stop_map_reduce()
        self.stop_dfs()

        self.running = False


    def stop_dfs(self):
        """Stop the namenode."""

        self.__check_initialization()

        logger.info("Stopping HDFS")

        proc = SshProcess(self.hadoop_base_dir + "/bin/stop-dfs.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while stopping HDFS")
        else:
            self.running_dfs = False


    def stop_map_reduce(self):
        """Stop the jobtracker."""

        self.__check_initialization()

        logger.info("Stopping MapReduce")

        proc = SshProcess(self.hadoop_base_dir + "/bin/stop-mapred.sh",
                          self.master)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while stopping MapReduce")
        else:
            self.running_map_reduce = False


    def execute(self, command, node = None, should_be_running = True, 
                verbose = True):
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

        if not self.initialized:
            logger.error("The cluster should be initialized")
            return

        if should_be_running and not self.running:
            logger.warn("The cluster was stopped. Starting it automatically")
            self.start()

        if not node:
            node = self.master

        logger.info("Executing {" + self.hadoop_base_dir + "/bin/hadoop " + 
                    command + "} in " + str(node))

        proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop " + command, node)

        if verbose:

            class ColorDecorator(object):

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

            redColor = '\033[01;31m'

            proc.stdout_handlers.append(sys.stdout)
            proc.stderr_handlers.append(ColorDecorator(sys.stderr, redColor))

        proc.start()
        proc.wait()

    def execute_jar(self, jar_path, params = None, lib_paths = None,
                    node = None, verbose = True):
        """Execute the mapreduce job included in the given jar. A list of
        libraries to be copied along with the main jar file and a set of params
        can be indicated.
        
        Args:
          jar_path (str): The path of the jar file.
          params (list of str, optional): The list of parameters for the
            execution.
          lib_path (list of str, optional): The path to the jar files to be used
            as libraries in the execution.
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            return

        if not params:
            params = []
        if not lib_paths:
            lib_paths = []
        if not node:
            node = self.master

        # Check if the jar file exists
        if not os.path.exists(jar_path):
            logger.error("Jar file " + jar_path + " does not exist")
            return # TODO - exception

        # Check if the libraries exist
        for lp in lib_paths:
            if not os.path.exists(lp):
                logger.warn("Lib file " + lp + " does not exist")
                return # TODO - exception


        exec_dir = "/tmp"

        # Copy jar and lib files to cluster
        files_to_copy = [ jar_path ]
        for lp in lib_paths:
            files_to_copy.append(lp)

        action = Put([ node ], [ jar_path ] + lib_paths, exec_dir)
        action.run()

        # Get parameters
        jar_file = os.path.join(exec_dir,os.path.basename(jar_path))
        if lib_paths:
            libs_param = " -libjars "
            for lp in lib_paths:
                libs_param += os.path.join(exec_dir,os.path.basename(lp)) + ","
                libs_param[:-1]
        else:
            libs_param = ""


        params_str = ""
        for p in params:
            params_str += " " + p

        self.execute("jar " + jar_file + libs_param + params_str, node,
                     verbose = verbose)


    def copy_history(self, dest):
        """Copy history logs from master.
        
        Args:
          dest (str): the path of the local dir where the logs will be copied.
        """

        if not os.path.exists(dest):
            logger.warning("Destination directory " + dest + 
                           " does not exist. It will be created")

        remoteFiles = [ os.path.join(self.hadoop_logs_dir,"history") ]
        action = Get([self.master], remoteFiles, dest)
        action.run()


    def clean_conf(self):
        """Clean configuration files used by this cluster."""

        shutil.rmtree(self.conf_dir)


    def clean_logs(self):
        """Remove all hadoop logs."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        logger.info("Cleaning logs")

        restart = False
        if self.running:
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


    def get_version(self):
        """Return the hadoop version.
        
        Returns:
          str: The version used by the Hadoop cluster.
        """

        proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop version",
                          self.master)
        proc.run()
        version = proc.stdout.splitlines()[0]
        return version

    # End HadoopCluster ########################################################



def deserialize_hcluster(hc_file_name):
    """Return a Hadoop Cluster object from the given file.
    
    Args:
      hc_file_name (str): The path where Hadoop Cluster has been serialized.
     
     
    Returns:
      HadoopCluster: The deserialized cluster object.
    """

    logger.info("Deserialize hc from " + hc_file_name)

    hc_file = open(hc_file_name, 'rb')
    hc = pickle.load(hc_file)

    return hc


def serialize_hcluster(hc_file_name, hc):
    """Serialize the Hadoop Cluster object into the indicated file.
    
    Args:
      hc_file_name (str): The path where Hadoop Cluster will be serialized.  
      hc (HadoopCluster): The hadoop cluster object.
    """

    logger.info("Serialize hc in " + hc_file_name)

    hc_file = open(hc_file_name, 'wb')
    pickle.dump(hc, hc_file)



if __name__ == "__main__":

    hg5k_tmp_dir = "/tmp/" + __user_login + "_hg5k/clusters"
    if not os.path.exists(hg5k_tmp_dir):
        os.makedirs(hg5k_tmp_dir)


    def __get_default_id():
        """Return the last used id.
        
        Returns:
          int: The id of the most recently modified cluster.
        """
        files = os.listdir(hg5k_tmp_dir)

        most_recent_file = None
        most_recent_access = 0

        for f in files:
            fstat = os.stat(os.path.join(hg5k_tmp_dir,f))
            if fstat.st_atime > most_recent_access:
                most_recent_file = int(f)
                most_recent_access = fstat.st_atime

        return most_recent_file


    def __generate_new_id():
        """Return the highest generated id + 1.
        
        Returns:
          int: The new generated id.
        """

        files = os.listdir(hg5k_tmp_dir)

        if len(files) == 0:
            return 1
        else:
            highest_id = 0

            for f in files:
                highest_id = max(highest_id,int(f))

            return highest_id + 1


    def __generate_hosts(file_name):
        """Generate a list of hosts from the given file.
        
        Args:
          file_name: The path of the file containing the hosts to be used. Each
            host should be in a different line. Repeated hosts are pruned.
            Hint: in Grid5000 $OAR_NODEFILE should be used.
        
        Return:
          list of Host: The list of hosts.
        """

        hosts = []

        for line in open(file_name):
            h = Host(line.rstrip())
            if not h in hosts:
                hosts.append(h)

        return hosts


    # Main #######################################################################

    prog = 'hadoop_g5k'
    description = 'This tool helps you to manage a Hadoop cluster in Grid5000.'
    parser = ArgumentParser(prog=prog,
                            description=description,
                            formatter_class=RawTextHelpFormatter,
                            add_help=False)

    parser.add_argument("--id",
                        action="store",
                        nargs=1,
                        metavar="ID",
                        help="The identifier of the cluster. If not indicated, last used cluster will be used (if any).")

    parser.add_argument("--node",
                        action="store",
                        nargs=1,
                        metavar="NODE",
                        help="Node where the action will be executed. Applies only to --execute and --jarjob")

    parser.add_argument("--properties",
                        dest="properties",
                        nargs=1,
                        action="store",
                        help="File containing the properties to be used. Applies only to --create")

    verbose_group = parser.add_mutually_exclusive_group()

    verbose_group.add_argument("-v", "--verbose",
                               dest="verbose",
                               action="store_true",
                               help="Run in verbose mode")

    verbose_group.add_argument("-q", "--quiet",
                               dest="quiet",
                               action="store_true",
                               help="Run in quiet mode")

    object_group = parser.add_mutually_exclusive_group()

    object_group.add_argument("--create",
                              metavar="MACHINELIST",
                              nargs=1,
                              action="store",
                              help="Create the cluster object with the nodes in MACHINELIST file")

    object_group.add_argument("--delete",
                              dest="delete",
                              action="store_true",
                              help="Remove all files used by the cluster")

    parser.add_argument("--initialize",
                        dest="initialize",
                        action="store_true",
                        help="Initialize cluster: Copy configuration and format dfs")

    parser.add_argument("--start",
                        dest="start",
                        action="store_true",
                        help="Start the namenode and jobtracker")

    parser.add_argument("--stop",
                        dest="stop",
                        action="store_true",
                        help="Stop the namenode and jobtracker")

    parser.add_argument("--execute",
                        action="store",
                        nargs=1,
                        metavar="COMMAND",
                        help="Execute a hadoop command")

    parser.add_argument("--jarjob",
                        action="store",
                        nargs="+",
                        metavar=("LOCAL_JAR_PATH","PARAM"),
                        help="Copy the jar file and execute it with the specified parameters")

    parser.add_argument("--copyhistory",
                        action="store",
                        nargs=1,
                        metavar="LOCAL_PATH",
                        help="Copies history to the specified path")

    parser.add_argument("--changeconf",
                        action="store",
                        nargs="+",
                        metavar="NAME=VALUE",
                        help="Change given configuration variables")

    parser.add_argument("--clean",
                        dest="clean",
                        action="store_true",
                        help="Remove hadoop logs and clean the dfs")

    parser.add_argument("--state",
                        dest="state",
                        action="store_true",
                        help="Show the cluster state")

    parser.add_argument("-h", "--help",
                        action="help",
                        help="Show this help message and exit")

    args = parser.parse_args()

    # Get id
    if args.id:
        id = int(args.id[0])
    else:
        if args.create:
            id = __generate_new_id()
        else:
            id = __get_default_id()
            if not id:
                logger.error("There is no available cluster. You must create a new one")
                sys.exit(3)

    logger.info("Using id = " + str(id))

    verbose = True
    if args.quiet:
        verbose = False

    # Check node specification
    node_host = None
    if args.node:
        if not (args.execute or args.jarjob):
            logger.warn("--node only applies to --execute or --jarjob")
        else:
            node_host = Host(args.node[0])

    # Create or load object
    hc_file_name = os.path.join(hg5k_tmp_dir,str(id))
    if args.create:

        if os.path.exists(hc_file_name):
            logger.error("There is a hadoop cluster with that id. You must remove it before or chose another id")
            sys.exit(1)

        hosts = __generate_hosts(args.create[0])

        if args.properties:
            hc = HadoopCluster(hosts, None, args.properties[0])
        else:
            hc = HadoopCluster(hosts)

    elif args.delete:

        # Clean
        hc = deserialize_hcluster(hc_file_name)
        if hc.initialized:
            logger.warn("The cluster needs to be cleaned before removed.")
            hc.clean()

        # Remove hc dump file
        logger.info("Removing hc dump file " + hc_file_name)
        os.remove(hc_file_name)

        sys.exit(0)
    else:
        # Deserialize
        hc = deserialize_hcluster(hc_file_name)

    # Execute options
    if args.initialize:
        hc.initialize()

    if args.changeconf:
        params = {}
        for str in args.changeconf:
            parts = str.split("=")
            params[parts[0]] = parts[1]

        hc.change_conf(params)

    if args.start:
        hc.start_and_wait()

    if args.execute:
        if node_host:
            hc.execute(args.execute[0], node_host, verbose = verbose)
        else:
            hc.execute(args.execute[0], verbose = verbose)

    if args.jarjob:
        if len(args.jarjob) > 1:
            if node_host:
                hc.execute_jar(args.jarjob[0], args.jarjob[1:],
                               node = node_host, verbose = verbose)
            else:
                hc.execute_jar(args.jarjob[0], args.jarjob[1:],
                               verbose = verbose)
        else:
            if node_host:
                hc.execute_jar(args.jarjob[0], node = node_host,
                               verbose = verbose)
            else:
                hc.execute_jar(args.jarjob[0], verbose = verbose)

    if args.copyhistory:
        hc.copy_history(args.copyhistory[0])

    if args.state:
        logger.info("---------------------------------------------------------")
        logger.info(style.user2("Hadoop Cluster with ID " + str(id)))
        logger.info(style.user1("    Version: ") + hc.get_version())
        logger.info(style.user1("    Master: ") + str(hc.master))
        logger.info(style.user1("    Hosts: ") + str(hc.hosts))
        logger.info(style.user1("    Topology: "))
        for h in hc.hosts:
            logger.info("        " + str(h) + " -> " + str(hc.topology.get_rack(h)))
        if hc.initialized:
            if hc.running:
                logger.info("The cluster is running")
            else:
                logger.info("The cluster is stopped")
        else:
            logger.info("The cluster is not initialized")
        logger.info("---------------------------------------------------------")

    if args.stop:
        hc.stop()

    if args.clean:
        hc.clean()

    serialize_hcluster(hc_file_name, hc)
