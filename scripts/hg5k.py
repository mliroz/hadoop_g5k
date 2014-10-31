#!/usr/bin/env python

import getpass
import os
import pickle
import sys
import threading

from argparse import ArgumentParser, RawTextHelpFormatter

from execo.action import Get, Put, TaktukRemote
from execo.host import Host
from execo.log import style
from execo.process import SshProcess
from execo_engine import logger

from hadoop_g5k.cluster import HadoopCluster, HadoopJarJob

__user_login = getpass.getuser()

hg5k_tmp_dir = "/tmp/" + __user_login + "_hg5k/clusters"


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
            fstat = os.stat(os.path.join(hg5k_tmp_dir, f))
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
                highest_id = max(highest_id, int(f))

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

    # Main #####################################################################
    prog = "hadoop_g5k"
    description = "This tool helps you to manage a Hadoop cluster in Grid5000."
    parser = ArgumentParser(prog=prog,
                            description=description,
                            formatter_class=RawTextHelpFormatter,
                            add_help=False)

    actions = parser.add_argument_group(style.host("General options"), 
                        "Options to be used generally with hadoop action.s")
                        
    actions.add_argument("-h", "--help",
                        action="help",
                        help="Show this help message and exit")                        

    actions.add_argument("--id",
                        action="store",
                        nargs=1,
                        metavar="ID",
                        help="The identifier of the cluster. If not indicated, "
                             "last used cluster will be used (if any)")

    actions.add_argument("--node",
                        action="store",
                        nargs=1,
                        metavar="NODE",
                        help="Node where the action will be executed. Applies "
                             "only to --execute and --jarjob")
                        
    actions.add_argument("--libjars",
                        action="store",
                        nargs="+",
                        metavar="LIB_JARS",
                        help="A list of libraries to be used in job execution. "
                             "Applies only to --jarjob")
                        
    verbose_group = actions.add_mutually_exclusive_group()

    verbose_group.add_argument("-v", "--verbose",
                               dest="verbose",
                               action="store_true",
                               help="Run in verbose mode")

    verbose_group.add_argument("-q", "--quiet",
                               dest="quiet",
                               action="store_true",
                               help="Run in quiet mode")

    object_group = parser.add_argument_group(style.host("Object management options"),
                        "Options to create and destroy hadoop cluster objects.")

    object_mutex_group = object_group.add_mutually_exclusive_group()

    object_mutex_group.add_argument("--create",
                              metavar="MACHINELIST",
                              nargs=1,
                              action="store",
                              help="Create the cluster object with the nodes in"
                                   " MACHINELIST file")

    object_mutex_group.add_argument("--delete",
                              dest="delete",
                              action="store_true",
                              help="Remove all files used by the cluster")
                              
    object_group.add_argument("--version",
                        dest="version",
                        nargs=1,
                        action="store",
                        help="Hadoop version to be used.")                              
                              
    object_group.add_argument("--properties",
                        dest="properties",
                        nargs=1,
                        action="store",
                        help="File containing the properties to be used (INI "
                             "file). Applies only to --create")
                        
    object_group.add_argument("--bootstrap",
                              metavar="HADOOP_TAR",
                              nargs=1,
                              action="store",
                              help="Install hadoop in the cluster nodes taking "
                                   "into account the specified properties.\n"
                                   "HADOOP_TAR defines the path of the .tar.gz "
                                   "file containing hadoop binaries.")
                              
    actions = parser.add_argument_group(style.host("Hadoop actions"),
        "Actions to execute in the hadoop cluster. Several options can be "
        "indicated at the same time.\n" +
        "The order of execution is fixed no matter the order used in the "
        "arguments: it follows the order\n" +
        "of the options.") 

    actions.add_argument("--initialize",
                        dest="initialize",
                        action="store_true",
                        help="Initialize cluster: Copy configuration and format"
                             " dfs")
                        
    actions.add_argument("--changeconf",
                        action="store",
                        nargs="+",
                        metavar="NAME=VALUE",
                        help="Change given configuration variables")          
                        
    actions.add_argument("--start",
                        dest="start",
                        action="store_true",
                        help="Start the namenode and jobtracker")                        
                                               
    actions.add_argument("--putindfs",
                        action="store",
                        nargs="+",
                        metavar="PATH",
                        help="Copy a set of local paths into the remote path "
                             "in dfs")
                        
    actions.add_argument("--getfromdfs",
                        action="store",
                        nargs=2,
                        metavar=("DFS_PATH", "LOCAL_PATH"),
                        help="Copy a remote path in dfs into the specified "
                             "local path")

    actions.add_argument("--execute",
                        action="store",
                        nargs=1,
                        metavar="COMMAND",
                        help="Execute a hadoop command")

    actions.add_argument("--jarjob",
                        action="store",
                        nargs="+",
                        metavar=("LOCAL_JAR_PATH", "PARAM"),
                        help="Copy the jar file and execute it with the "
                             "specified parameters")

    actions.add_argument("--copyhistory",
                        action="store",
                        nargs="+",
                        metavar=("LOCAL_PATH", "JOB_ID"),
                        help="Copy history to the specified path.\n"
                             "If a list of job ids is given, just copy the "
                             "stats of those jobs.")

    actions.add_argument("--stop",
                        dest="stop",
                        action="store_true",
                        help="Stop the namenode and jobtracker")

    actions.add_argument("--clean",
                        dest="clean",
                        action="store_true",
                        help="Remove hadoop logs and clean the dfs")
                        
    queries = parser.add_argument_group(style.host("Hadoop queries"), 
        "Set of queries that can be executed in hadoop.")                         

    queries.add_argument("--state",
                        action="store",
                        nargs="?",
                        const="general",
                        metavar="general | files | dfs | dfsblocks | mrjobs",
                        help="Show the cluster state. The output depends on optional argument:\n" + 
                             "  general    Show general cluster state (default option)\n" +
                             "  files      Show dfs file hierarchy\n" +
                             "  dfs        Show filesystem state\n" +
                             "  dfsblocks  Show dfs blocks information\n" +
                             "  mrjobs     Show mapreduce state\n")

    args = parser.parse_args()

    # Get id
    if args.id:
        hc_id = int(args.id[0])
    else:
        if args.create:
            hc_id = __generate_new_id()
        else:
            hc_id = __get_default_id()
            if not hc_id:
                logger.error("There is no available cluster. You must create a"
                             " new one")
                sys.exit(3)

    logger.info("Using id = " + str(hc_id))

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
    hc_file_name = os.path.join(hg5k_tmp_dir, str(hc_id))
    if args.create:

        if os.path.exists(hc_file_name):
            logger.error("There is a hadoop cluster with that id. You must "
                         "remove it before or chose another id")
            sys.exit(1)

        hosts = __generate_hosts(args.create[0])
        
        if args.version:
            if args.version[0].startswith("0."):
                hadoop_class = HadoopCluster
            elif args.version[0].startswith("1."):
                hadoop_class = HadoopCluster
            elif args.version[0].startswith("2."):
                logger.error("Cluster for HadoopV2 not implemented yet")
                sys.exit(2)
            else:
                logger.error("Unknown hadoop version")
                sys.exit(2)
        else:
            hadoop_class = HadoopCluster
        
        if args.properties:
            hc = hadoop_class(hosts, None, args.properties[0])
        else:
            hc = hadoop_class(hosts)

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
    if args.bootstrap:
        hc.bootstrap(args.bootstrap[0])        

    # Execute options
    if args.initialize:
        hc.initialize()

    if args.changeconf:
        params = {}
        for assig in args.changeconf:
            parts = assig.split("=")
            params[parts[0]] = parts[1]

        hc.change_conf(params)

    if args.start:
        hc.start_and_wait()
        
    if args.putindfs:
        local_paths = args.putindfs[:-1]
        dest = args.putindfs[-1]
                
        for f in local_paths:
            if not os.path.exists(f):
                logger.error("Local path " + f + " does not exist")
                sys.exit(4)
                
        # Define and create temp dir
        tmp_dir = "/tmp" + dest
        hosts = hc.hosts        
        actionRemove = TaktukRemote("rm -rf " + tmp_dir, hosts)
        actionRemove.run()
        actionCreate = TaktukRemote("mkdir -p " + tmp_dir, hosts)
        actionCreate.run()
        
        def copy_function(host, files_to_copy):
            action = Put([host], files_to_copy, tmp_dir)
            action.run()
            
            for f in files_to_copy:
                src_file = os.path.join(tmp_dir, os.path.basename(f))
            
                hc.execute("fs -put " + src_file + " " +
                           os.path.join(dest, os.path.basename(src_file)),
                           host, True, False)
                
        # Assign files to hosts  
        files_per_host = [[]] * len(hosts)
        for idx in range(0, len(hosts)):
            files_per_host[idx] = local_paths[idx::len(hosts)]
            
        # Create threads and launch them
        logger.info("Copying files in parallel into " + str(len(hosts)) + " hosts")
                
        threads = []
        for idx, h in enumerate(hosts):
            if files_per_host[idx]:
                t = threading.Thread(target=copy_function,
                                     args=(h, files_per_host[idx]))
                t.start()
                threads.append(t)

        # Wait for the threads to finish
        for t in threads:
            t.join()
    
    if args.getfromdfs:
        remote_path = args.getfromdfs[0]        
        local_path = args.getfromdfs[1]

        tmp_dir = "/tmp"
        # Remove file in tmp dir if exists
        proc = SshProcess("rm -rf " +
                          os.path.join(tmp_dir, os.path.basename(remote_path)),
                          hc.master)
        proc.run()
        
        # Get files in master
        hc.execute("fs -get " + remote_path + " " + tmp_dir, verbose=False)
        
        # Copy files from master
        action = Get([hc.master],
                     [os.path.join(tmp_dir, os.path.basename(remote_path))],
                     local_path)
        action.run()

    if args.execute:
        if node_host:
            hc.execute(args.execute[0], node_host, verbose=verbose)
        else:
            hc.execute(args.execute[0], verbose=verbose)

    if args.jarjob:
        if len(args.jarjob) > 1:
            if not node_host:
                node_host = None
            if args.libjars:
                libjars = args.libjars
            else:
                libjars = None
                           
            hc.execute_jar(HadoopJarJob(args.jarjob[0], args.jarjob[1:], libjars),
                               node=node_host, verbose=verbose)
        else:
            if node_host:
                hc.execute_jar(HadoopJarJob(args.jarjob[0]), node=node_host,
                               verbose=verbose)
            else:
                hc.execute_jar(HadoopJarJob(args.jarjob[0]), verbose=verbose)

    if args.copyhistory:
        hc.copy_history(args.copyhistory[0], args.copyhistory[1:])

    if args.stop:
        hc.stop()

    if args.clean:
        hc.clean()
    
    if args.state:
        if args.state == "general":
            
            logger.info("---------------------------------------------------------")
            logger.info(style.user2("Hadoop Cluster with ID " + str(hc_id)))
            logger.info(style.user1("    Version: ") + hc.get_version())
            logger.info(style.user1("    Master: ") + str(hc.master))
            logger.info(style.user1("    Hosts: ") + str(hc.hosts))
            logger.info(style.user1("    Topology: "))
            for h in hc.hosts:
                logger.info("        " + str(h) + " -> " + str(hc.topology.get_rack(h)))
            if hc.initialized:
                if hc.running:
                    logger.info("The cluster is " + style.user3("running"))
                else:
                    logger.info("The cluster is " + style.user3("stopped"))
            else:
                logger.info("The cluster is not " + style.user3("initialized"))
            logger.info("---------------------------------------------------------")
        
        elif args.state == "files":                      
            (stdout, stderr) = hc.execute("fs -lsr /", verbose=False)
            print ""            
            for line in stdout.splitlines():
                if not "WARN fs.FileSystem" in line:
                    print line
            print ""
            
            (stdout, stderr) = hc.execute("fs -dus /", verbose=False)
            pos = stdout.rfind("\t")
            size = int(stdout[pos + 1:])
            
            human_readable_size = ""
            if 1024 < size < 1024*1024:
                human_readable_size = " (%.1f KB)" % (float(size)/1024)
            elif 1024*1024 < size < 1024*1024*1024:
                human_readable_size = " (%.1f MB)" % (float(size)/(1024*1024))
            elif size > 1024*1024*1024:
                human_readable_size = " (%.1f GB)" % (float(size)/(1024*1024*1024))
        
            print "Total Size = " + str(size) + human_readable_size + "\n"
                    
        elif args.state == "dfs":
            (stdout, stderr) = hc.execute("dfsadmin -report", verbose=False)
            print ""            
            for line in stdout.splitlines():
                if not "WARN fs.FileSystem" in line:
                    print line
            print ""
            
        elif args.state == "dfsblocks":
            (stdout, stderr) = hc.execute("fsck -blocks", verbose=False)
            print ""
            print stdout            
            print ""
                    
        elif args.state == "mrjobs":
            (stdout, stderr) = hc.execute("job -list all", verbose=False)
            print ""            
            print stdout
            print ""
        

    serialize_hcluster(hc_file_name, hc)