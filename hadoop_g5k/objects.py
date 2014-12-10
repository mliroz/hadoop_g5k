import os
import stat

from execo_engine import logger
from execo_g5k.api_utils import get_host_attributes


class HadoopException(Exception):
    pass


class HadoopJobException(HadoopException):
    pass


class HadoopTopology(object):
    """This class is able to produce and manage a Hadoop topology."""

    def __init__(self, hosts, topo_list=None):
        """Create a Hadoop topology object assigning each host to the
        corresponding rack.

        Args:
          hosts (list of Host):
            The hosts to be assigned a topology.
          topo_list (list of str, optional):
            The racks to be assigned to each host. len(hosts) should be equal to
            len(topo_list).
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
          host (Host):
            The host whose rack is queried.

        Returns (str):
          The rack corresponding to the given host.

        """

        return self.topology[host]

    def __str__(self):
        return str(self.topology)

    def create_files(self, dest, data_file="topo.dat",
                     script_file="topo.sh"):
        """Create the script (topo.sh) and data (topo.dat) files used to obtain
        the topology in Hadoop.

        Args:
          dest (str):
            The name of the directory where the files will be created.
          data_file (str, optional):
            The name of the file containing the rack associated to each node.
          script_file (str, optional):
            The name of the file with the script that will be used by Hadoop to
            determine the rack of the nodes.
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
    """This class represents a Hadoop MapReduce job to be executed from a jar
    file.

    Attributes:
      jar_path (str):
        The local path of the jar containing the job.
      params (list of str):
        The list of parameters of the job.
      lib_paths (list of str):
        The list of local paths to the libraries used by the job.
      state (int):
        State of the job.
      job_id (str):
        Hadoop job identifier or "unknown" if it have not been yet assigned.
      success (bool):
        Indicates whether the job have finished successfully or not. Before
        executing its value is None.
    """

    state = -1
    job_id = "unknown"
    success = None

    def __init__(self, jar_path, params=None, lib_paths=None):
        """Creates a new Hadoop MapReduce jar job with the given parameters.

        Args:
          jar_path (str):
            The local path of the jar containing the job.
          params (list of str, optional):
            The list of parameters of the job.
          lib_paths (list of str, optional):
            The list of local paths to the libraries used by the job.
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
          exec_dir (str, optional):
            The path of the directory where the job is to be executed.
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

