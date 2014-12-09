import getpass
import os
import pickle

from execo.action import Remote
from execo.host import Host
from execo.log import style
from execo_engine import logger
from execo_g5k import get_oar_job_nodes, get_oargrid_job_nodes


def import_class(name):
    """Dynamically load a class and return a reference to it.

    Args:
      name (str): the class name, including its package hierarchy.

    Returns:
      A reference to the class.
    """

    last_dot = name.rfind(".")
    package_name = name[:last_dot]
    class_name = name[last_dot + 1:]

    mod = __import__(package_name, fromlist=[class_name])
    return getattr(mod, class_name)


def import_function(name):
    """Dynamically load a function and return a reference to it.

    Args:
      name (str): the function name, including its package hierarchy.

    Returns:
      A reference to the function.
    """

    last_dot = name.rfind(".")
    package_name = name[:last_dot]
    function_name = name[last_dot + 1:]

    mod = __import__(package_name, fromlist=[function_name])
    return getattr(mod, function_name)


def uncompress(file_name, host):
    if file_name.endswith("tar.gz"):
        decompression = Remote("tar xf " + file_name, [host])
        decompression.run()

        base_name = os.path.basename(file_name[:-7])
        dir_name = os.path.dirname(file_name[:-7])
        new_name = dir_name + "/data-" + base_name

        action = Remote("mv " + file_name[:-7] + " " + new_name, [host])
        action.run()
    elif file_name.endswith("gz"):
        decompression = Remote("gzip -d " + file_name, [host])
        decompression.run()

        base_name = os.path.basename(file_name[:-3])
        dir_name = os.path.dirname(file_name[:-3])
        new_name = dir_name + "/data-" + base_name

        action = Remote("mv " + file_name[:-3] + " " + new_name, [host])
        action.run()
    elif file_name.endswith("zip"):
        decompression = Remote("unzip " + file_name, [host])
        decompression.run()

        base_name = os.path.basename(file_name[:-4])
        dir_name = os.path.dirname(file_name[:-4])
        new_name = dir_name + "/data-" + base_name

        action = Remote("mv " + file_name[:-4] + " " + new_name, [host])
        action.run()
    elif file_name.endswith("bz2"):
        decompression = Remote("bzip2 -d " + file_name, [host])
        decompression.run()

        base_name = os.path.basename(file_name[:-4])
        dir_name = os.path.dirname(file_name[:-4])
        new_name = dir_name + "/data-" + base_name

        action = Remote("mv " + file_name[:-4] + " " + new_name, [host])
        action.run()
    else:
        logger.warn("Unknown extension")
        return file_name

    return new_name


def generate_hosts(hosts_input):
    """Generate a list of hosts from the given file.

    Args:
      hosts_input: The path of the file containing the hosts to be used,
        or a comma separated list of site:job_id or an oargrid_job_id.
        If a file is used, each host should be in a different line.
        Repeated hosts are pruned.
        Hint: in a running Grid5000 job,  $OAR_NODEFILE should be used.

    Return:
      list of Host: The list of hosts.
    """
    hosts = []
    if os.path.isfile(hosts_input):
        for line in open(hosts_input):
            h = Host(line.rstrip())
            if h not in hosts:
                hosts.append(h)
    elif ':' in hosts_input:
        # We assume the string is a comma separated list of site:job_id
        for job in hosts_input.split(','):
            site, job_id = job.split(':')
            hosts += get_oar_job_nodes(int(job_id), site)
    else:
        # If the file_name is a number, we assume this is a oargrid_job_id
        hosts = get_oargrid_job_nodes(int(hosts_input))
    logger.debug('Hosts list: \n%s',
                 ' '.join(style.host(host.address.split('.')[0])
                          for host in hosts))
    return hosts

# Serialization ###############################################################

__user_login = getpass.getuser()
serialize_base = "/tmp/" + __user_login + "_"


def enum(**enums):
    return type('Enum', (), enums)

ClusterType = enum(HADOOP="hg5k", SPARK="spark")


def __get_clusters_dir(cluster_type):
    clusters_dir = serialize_base + cluster_type + "/clusters"

    if not os.path.exists(clusters_dir):
        os.makedirs(clusters_dir)

    return clusters_dir


def __get_cluster_file(cluster_type, cid):
    return __get_clusters_dir(cluster_type) + "/" + str(cid)


def __get_hc_link_file(cluster_type, cid):
    return __get_clusters_dir(cluster_type) + "/" + str(cid) + ".hc"


def get_default_id(cluster_type):
    """Return the last used id.

    Args:
      cluster_type (ClusterType): the type of cluster.

    Returns:
      int: The id of the most recently modified cluster.
    """
    files = os.listdir(__get_clusters_dir(cluster_type))

    most_recent_file = None
    most_recent_access = 0

    for f in files:
        if not f.endswith(".hc"):
            fstat = os.stat(os.path.join(__get_clusters_dir(cluster_type), f))
            if fstat.st_atime > most_recent_access:
                most_recent_file = int(f)
                most_recent_access = fstat.st_atime

    return most_recent_file


def generate_new_id(cluster_type):
    """Return the highest generated id + 1.

    Args:
      cluster_type (ClusterType): the type of cluster.

    Returns (int):
      The new generated id.
    """

    files = os.listdir(__get_clusters_dir(cluster_type))

    if len(files) == 0:
        return 1
    else:
        highest_id = 0

        for f in files:
            highest_id = max(highest_id, int(f))

        return highest_id + 1


def cluster_exists(cluster_type, cid):
    """Determine whether the cluster for the given type and id already exists.

    Returns (bool):
      True if the cluster exists, False otherwise
    """

    fname = __get_cluster_file(cluster_type, cid)
    return os.path.exists(fname)


def deserialize_cluster(cluster_type, cid):
    """Return a cluster object from the given file.

    Args:
      cluster_type (ClusterType):
        The type of cluster to obtain.
      cid (int):
        The id of the cluster.

    Returns:
      The deserialized cluster object.
    """

    fname = __get_cluster_file(cluster_type, cid)

    logger.info("Deserialize cluster from " + fname)

    with open(fname, 'rb') as c_file:
        cluster_object = pickle.load(c_file)

    return cluster_object


def serialize_cluster(cluster_type, cid, cluster_object):
    """Serialize the cluster object. Replace also the linked Hadoop cluster if
    it exists.

    Args:
      cluster_type (ClusterType):
        The type of cluster to serialize.
      cid (int):
        The id of the cluster.
      cluster_object:
        The cluster to serialize.
    """

    fname = __get_cluster_file(cluster_type, cid)

    logger.info("Serialize cluster (" + cluster_type + ") in " + fname)

    c_file = open(fname, 'wb')
    pickle.dump(cluster_object, c_file)

    if cluster_type != ClusterType.HADOOP:
        hc_link_fname = __get_hc_link_file(cluster_type, cid)
        if os.path.exists(hc_link_fname):
            with open(hc_link_fname) as link_file:
                hc_id = int(link_file.readline())
            serialize_cluster(ClusterType.HADOOP, hc_id, cluster_object.hc)


def remove_cluster(cluster_type, cid, cluster_object):
    """Remove temporary files created for the given cluster. Remove the linked
    Hadoop cluster if it exists.

    Args:
      cluster_type (ClusterType):
        The type of cluster to serialize.
      cid (int):
        The id of the cluster.
      cluster_object:
        The cluster to remove.
    """

    fname = __get_cluster_file(cluster_type, cid)
    os.remove(fname)

    if cluster_type != ClusterType.HADOOP:
        hc_link_fname = __get_hc_link_file(cluster_type, cid)
        if os.path.exists(hc_link_fname):
            with open(hc_link_fname) as link_file:
                hc_id = int(link_file.readline())
            serialize_cluster(ClusterType.HADOOP, hc_id, cluster_object.hc)


def link_to_hadoop_cluster(cluster_type, cid, hc_id):
    """Stores a reference to a Hadoop cluster.

    Args:
      cluster_type (ClusterType):
        The type of cluster to link.
      cid (int):
        The id of the cluster.
      hc_id (int)"
        The id of the Hadoop cluster to link.
    """

    fname = __get_hc_link_file(cluster_type, cid)
    with open(fname, 'wb') as link_file:
        link_file.write(str(hc_id))


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