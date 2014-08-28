#!/usr/bin/env python

import getpass
import os
import pickle

from hadoop_g5k import HadoopCluster, HadoopJarJob

from argparse import ArgumentParser, RawTextHelpFormatter

from execo import Host
from execo.action import Get, Put
from execo.log import style
from execo.process import SshProcess
from execo_engine import logger

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

    prog = "hadoop_g5k"
    description = "This tool helps you to manage a Hadoop cluster in Grid5000."
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
                        help="Copy history to the specified path")
                        
    parser.add_argument("--putindfs",
                        action="store",
                        nargs=2,
                        metavar=("LOCAL_PATH","DFS_PATH"),
                        help="Copy a local path into the remote path in dfs")
                        
    parser.add_argument("--getfromdfs",
                        action="store",
                        nargs=2,
                        metavar=("DFS_PATH","LOCAL_PATH"),
                        help="Copy a remote path in dfs into the specified local path")                        

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
        
    if args.putindfs:
        local_path = args.putindfs[0]
        remote_path = args.putindfs[1]
        
        if not os.path.exists(local_path):
            logger.error("Local path " + local_path + " does not exist")
            sys.exit(4)
        
        # Copy files to master
        tmp_dir = "/tmp"
        action = Put([ hc.master ], [ local_path] , tmp_dir)
        action.run()
        
        # Load to dfs
        hc.execute("fs -put " + os.path.join(tmp_dir,os.path.basename(local_path)) + " " + remote_path + "/" + os.path.basename(local_path), verbose = False)
    
    if args.getfromdfs:
        remote_path = args.getfromdfs[0]        
        local_path = args.getfromdfs[1]

        tmp_dir = "/tmp"
        # Remove file in tmp dir if exists
        proc = SshProcess("rm -rf " + os.path.join(tmp_dir, os.path.basename(remote_path)), hc.master)
        proc.run()
        
        # Get files in master
        hc.execute("fs -get " + remote_path + " " + tmp_dir, verbose = False)
        
        # Copy files from master
        action = Get([hc.master], [ os.path.join(tmp_dir,os.path.basename(remote_path)) ], local_path)
        action.run()

    if args.execute:
        if node_host:
            hc.execute(args.execute[0], node_host, verbose = verbose)
        else:
            hc.execute(args.execute[0], verbose = verbose)

    if args.jarjob:
        if len(args.jarjob) > 1:
            if node_host:
                hc.execute_jar(HadoopJarJob(args.jarjob[0], args.jarjob[1:]),
                               node = node_host, verbose = verbose)
            else:
                hc.execute_jar(HadoopJarJob(args.jarjob[0], args.jarjob[1:]),
                               verbose = verbose)
        else:
            if node_host:
                hc.execute_jar(HadoopJarJob(args.jarjob[0]), node = node_host,
                               verbose = verbose)
            else:
                hc.execute_jar(HadoopJarJob(args.jarjob[0]), verbose = verbose)

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