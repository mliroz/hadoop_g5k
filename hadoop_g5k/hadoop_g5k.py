#!/usr/bin/env python

from argparse import ArgumentParser, RawTextHelpFormatter

import sys, os, shutil, tempfile, getpass, pickle

from execo import Host
from execo.action import Put, TaktukPut, Get, Remote
from execo.process import Process, SshProcess
from execo_engine import logger

from execo_g5k.api_utils import get_host_attributes

from os import listdir
from os.path import join

__user_login = getpass.getuser()

# Constant definitions
CORE_CONF_FILE = "core-site.xml"
HDFS_CONF_FILE = "hdfs-site.xml"
MR_CONF_FILE = "mapred-site.xml"

DEFAULT_HADOOP_BASE_DIR = "/opt/base/hadoop"
DEFAULT_HADOOP_CONF_DIR = DEFAULT_HADOOP_BASE_DIR + "/conf"
DEFAULT_HADOOP_LOGS_DIR = "/opt/base/logs/hadoop"

DEFAULT_HADOOP_HDFS_PORT = 54310
DEFAULT_HADOOP_MR_PORT = 54311

DEFAULT_HADOOP_LOCAL_CONF_DIR = "/home/" + getpass.getuser() + "/common/hadoop/conf"


class HadoopCluster:
  
  # TODO - default timeout
  #default_timeout = 1
  
  # TODO - provisionally fixed parameters
  hadoop_base_dir = DEFAULT_HADOOP_BASE_DIR
  hadoop_conf_dir = DEFAULT_HADOOP_CONF_DIR
  hadoop_logs_dir = DEFAULT_HADOOP_LOGS_DIR
  hdfs_port = DEFAULT_HADOOP_HDFS_PORT
  mapred_port = DEFAULT_HADOOP_MR_PORT
  
  local_base_conf_dir = DEFAULT_HADOOP_LOCAL_CONF_DIR
    
  # Cluster state
  initialized = False
  running = False
  running_dfs = False
  running_map_reduce = False
      
  def __init__(self, hosts, topology = None):
    """Creates a new Hadoop cluster with the given hosts and topology"""
    
    # Configure master and slaves
    self.hosts = hosts
    self.master = hosts[0]
        
    # Create topology
    if topology == None:
      self.topology = []
      for h in hosts:
        nw_adapters = get_host_attributes(h)[u'network_adapters']
        for nwa in nw_adapters:
          if u'network_address' in nwa and nwa[u'network_address'] == h.address:
            self.topology.append("/" + nwa[u'switch'])
            break
       
    logger.info("Hadoop cluster created with master " + str(self.master) + ", hosts " + str(self.hosts) + " and topology " + str(self.topology))
    
  
  def initialize(self):
    """Initializes the cluster. Copies base configuration and formats DFS"""
    
    if self.initialized:
      if self.running:
        self.stop() 
      self.clean()
      
    self.initialized = False
    
    logger.info("Initializing hadoop")
    
    # Copy base configuration files to tmp dir    
    self.conf_dir = tempfile.mkdtemp("","hadoop-","/tmp")
    baseConfFiles = [ join(self.local_base_conf_dir,f) for f in listdir(self.local_base_conf_dir) ]
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
    
    # Create topology file
    topoFile = open(self.conf_dir + "/topo.dat", "w")
    for h, t in zip(self.hosts, self.topology):
      topoFile.write(h.address + " "  + t + "\n")
    topoFile.close()    
    
    # Configure servers and host-dependant parameters
    self.__configure_servers()
    
    # Copy configuration
    self.__copy_conf(self.conf_dir)      
    
    # Format HDFS
    self.format_dfs()
    
    self.initialized = True
    
  def __configure_servers(self):  
    """Configure servers and host-dependant parameters (TODO - we assume all nodes are equal)"""
    
    host_attrs = get_host_attributes(self.master)
    num_cores = host_attrs[u'architecture'][u'smt_size']
    main_mem = int(host_attrs[u'main_memory'][u'ram_size']) / (1024 * 1024 * num_cores)
    
    self.__replace_in_file(join(self.conf_dir,CORE_CONF_FILE), "fs.default.name", self.master.address + ":" + str(self.hdfs_port))
    self.__replace_in_file(join(self.conf_dir,MR_CONF_FILE), "mapred.job.tracker", self.master.address + ":" + str(self.mapred_port))
    self.__replace_in_file(join(self.conf_dir,MR_CONF_FILE), "mapred.tasktracker.map.tasks.maximum", str(num_cores))
    self.__replace_in_file(join(self.conf_dir,MR_CONF_FILE), "mapred.child.java.opts", "-Xmx" + str(main_mem) + "m")    
  
  def __copy_conf(self, conf_dir):
    """Copy configuration files from given dir to remote dir in cluster hosts"""
      
    confFiles = [ join(conf_dir,f) for f in listdir(conf_dir) ]
    
    action = TaktukPut(self.hosts, confFiles, self.hadoop_conf_dir)
    action.run()
    
    if not action.finished_ok:
      logger.warn("Error while copying configuration")
      if not action.ended:
        action.kill()
        
    
  def change_conf(self, params):
    """Modify hadoop configuration"""
          
    # Copy conf files from master
    remoteConfFiles = [ join(self.hadoop_conf_dir,f) for f in listdir(self.local_base_conf_dir) if f.endswith(".xml")]
    
    tmp_dir = "/tmp/mliroz_temp_hadoop/"
    if not os.path.exists(tmp_dir):
      os.makedirs(tmp_dir)
      
    action = Get([self.master], remoteConfFiles, tmp_dir)
    action.run()
    
    # Do replacements in temp file
    tempConfFiles = [ join(tmp_dir,f) for f in listdir(tmp_dir)]
    
    for (name, value) in params:
      for f in tempConfFiles:
        if self.__replace_in_file(f, name, value):
          break
          
    # Copy back the files to all hosts
    self.__copy_conf(tmp_dir)
      
      
  def __replace_in_file(self, f, name, value):
    """Assign value to variable name in file f"""
        
    command = "awk 'BEGIN { RS=\"</property>\"; ORS=\"</property>\"} "
    command += "{if (match($0,/<name>" + name  + "<\/name>/)) {sub(/<value>[^<]*<\/value>/,\"<value>" + value + "</value>\");print} else print}' " + f
    
    proc = Process(command)
    proc.run()
    
    out = open(f, "w")
    out.write(proc.stdout[:proc.stdout.rfind('\n')] + "\n")
    out.close()    
    
    
  def format_dfs(self):
    """Formats HDFS"""
    
    logger.info("Formatting HDFS")
    
    proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop namenode -format", self.master)
    proc.run()
    
    if proc.finished_ok:
      logger.info("HDFS formatted successfully")
    else:
      logger.warn("Error while formatting HDFS")
      
    
  def start(self):
    """Starts the namenode and then the jobtracker"""
    self.start_dfs()
    self.start_map_reduce()
    
    self.running = True
    
  def start_and_wait(self):
    """Starts the namenode and then the jobtracker. It waits for them to exit safemode""""""Documentation"""
    self.start_dfs_and_wait()
    self.start_map_reduce_and_wait()
    
    self.running = True
    
  def start_dfs(self):
    """Starts the namenode"""
    
    logger.info("Starting HDFS")
    
    proc = SshProcess(self.hadoop_base_dir + "/bin/start-dfs.sh", self.master)
    proc.run()
    
    if not proc.finished_ok:
      logger.warn("Error while starting HDFS")
    else:
      self.running_dfs = True
      
  def start_dfs_and_wait(self):
    """Starts the namenode and wait for it to exit safemode"""
    
    self.start_dfs()
    
    logger.info("Waiting for safe mode to be off")
    proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop dfsadmin -safemode wait", self.master)
    proc.run()
    
    if not proc.finished_ok:
      logger.warn("Error while starting HDFS")
    else:
      self.running_dfs = True    
      
    
  def start_map_reduce(self):
    """Starts the jobtracker"""
    
    logger.info("Starting MapReduce")
    
    proc = SshProcess(self.hadoop_base_dir + "/bin/start-mapred.sh", self.master)
    proc.run()
    
    if not proc.finished_ok:
      logger.info("MapReduce started successfully")
    else:
      self.running_map_reduce = True
      
  def start_map_reduce_and_wait(self):
    """Starts the jobtracker and wait for it to exit safemode"""
    
    self.start_map_reduce()
    
    #logger.info("Waiting for safe mode to be off")
    #proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop mradmin -safemode wait", self.master)
    #proc.run()
    
    # TODO - does the jobtracker enter safemode?
    
    
  def stop(self):
    """Stops the jobtracker and then the namenode"""
    self.stop_map_reduce()
    self.stop_dfs()
    
    self.running = False
    
    
  def stop_dfs(self):
    """Stops the namenode"""
    
    logger.info("Stopping HDFS")
    
    proc = SshProcess(self.hadoop_base_dir + "/bin/stop-dfs.sh", self.master)
    proc.run()
    
    if not proc.finished_ok:
      logger.warn("Error while stopping HDFS") 
    else:
      self.running_dfs = False
  
  
  def stop_map_reduce(self):
    """Stopts the jobtracker"""
    
    logger.info("Stopping MapReduce")
    
    proc = SshProcess(self.hadoop_base_dir + "/bin/stop-mapred.sh", self.master)
    proc.run()
    
    if not proc.finished_ok:
      logger.warn("Error while stopping MapReduce")
    else:
      self.running_map_reduce = False      
      
  
  def execute(self, command, node = None):
    """Executes the given command in the given node.
       If it is not provided, it is executed in the master"""
       
    if not self.running:
      logger.warn("The cluster was stopped. Starting it automatically")
      self.start()
    
    if not node:
      node = self.master
    
    logger.info("Executing {" + self.hadoop_base_dir + "/bin/hadoop " + command + "} in " + str(node))
    
    proc = SshProcess(self.hadoop_base_dir + "/bin/hadoop " + command, node)
    proc.stdout_handlers.append(sys.stdout) 
    proc.stderr_handlers.append(sys.stderr) 
    proc.start()   
    proc.wait()
    
  def execute_jar(self, jar_path, params = [], lib_paths = [], node = None):
    """Executes the mapreduce job included in the given jar_path. A list of libraries
       to be copied along with the main jar file and a set of params can be in"""
       
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
          
    self.execute("jar " + jar_file + libs_param + params_str, node)

    
  def copy_history(self, dest):
    """Copy history logs from master"""
    
    remoteFiles = [ join(self.hadoop_logs_dir,"history") ]
    action = Get([self.master], remoteFiles, dest)
    action.run()  
    
    
  def clean_conf(self):
    """Clean configuration files used by this cluster"""
    
    shutil.rmtree(self.conf_dir)    
    
    
  def clean_logs(self):
    """Removes all logs"""
    
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
    """Removes all data created by hadoop (including filesystem)"""
    
    if self.running:
      logger.warn("The cluster needs to be stopped before cleaning.")
      self.stop()    
    
    logger.info("Cleaning hadoop data")
    
    restart = False
    if self.running:
      self.stop()
      restart = True
      
    action = Remote("rm -rf /tmp/" + getpass.getuser() + "_hadoop hadoop-" + getpass.getuser() + "-*", self.hosts)
    action.run()    
    
    if restart:
      self.start()    
        
        
  def clean(self):
    """Removes all files created by Hadoop (logs, filesystem, temporary files)"""
    
    if self.running:
      logger.warn("The cluster needs to be stopped before cleaning.")
      self.stop()
    
    self.clean_conf()
    self.clean_logs()
    self.clean_data()
    
    self.initialized = False
    
  # End HadoopCluster ########################################################## 
  
  
  
def deserialize_hcluster(hc_file_name):
  """Returns a Hadoop Cluster object from the given file"""
  
  logger.info("Deserialize hc from " + hc_file_name)
  
  hc_file = open(hc_file_name, 'rb')
  hc = pickle.load(hc_file) 
  
  return hc
  
  
def serialize_hcluster(hc_file_name, hc):
  """Dumps the Hadoop Cluster object into the indicated file"""
  
  logger.info("Serialize hc in " + hc_file_name)
  
  hc_file = open(hc_file_name, 'wb')
  pickle.dump(hc, hc_file)
    
    
    
if __name__ == "__main__":
  
  hg5k_tmp_dir = "/tmp/" + __user_login + "_hg5k/clusters"
  if not os.path.exists(hg5k_tmp_dir):
    os.makedirs(hg5k_tmp_dir)  
  
  
  def __get_default_id():
    """Returns the last used id"""
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
    """Returns the highest generated id + 1"""

    files = os.listdir(hg5k_tmp_dir)

    if len(files) == 0:
      return 1
    else:
      highest_id = 0

      for f in files:
        highest_id = max(highest_id,int(f))

      return highest_id + 1
  
  
  def __generate_hosts(file_name):
    """Generates a list of hosts from the given file"""

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
                metavar=("JAR_PATH","PARAM"),
                help="Copies the jar file and executes it with the specified parameters")
                
  parser.add_argument("--clean",
                dest="clean",
                action="store_true",
                help="Removes hadoop logs and cleans the dfs")
                
  parser.add_argument("-h", "--help",
                action="help",
                help="show this help message and exit")
  
  args = parser.parse_args()

  print args
  
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
  
  # Create or load object
  hc_file_name = os.path.join(hg5k_tmp_dir,str(id))
  if args.create:

    if os.path.exists(hc_file_name):
      logger.error("There is a hadoop cluster with that id. You must remove it before or chose another id")
      sys.exit(1)
    
    hosts = __generate_hosts(args.create[0])
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
  
  if args.start:
    hc.start_and_wait()
  
  if args.execute:
    hc.execute(args.execute[0])
  
  if args.jarjob:
    if len(args.jarjob) > 1:
      hc.execute_jar(args.jarjob[0], args.jarjob[1:])
    else:
      hc.execute_jar(args.jarjob[0])
    
  if args.stop:
    hc.stop()    
  
  if args.clean:
    hc.clean()

  serialize_hcluster(hc_file_name, hc)  

