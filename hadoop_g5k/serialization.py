import getpass
import os
import pickle

from execo_engine import logger

from hadoop_g5k import HadoopCluster


__user_login = getpass.getuser()
serialize_base = "/tmp/" + __user_login + "_"


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
      cluster_type (str): the type of cluster.

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
      cluster_type (str): the type of cluster.

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
      cluster_type (str):
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
      cluster_type (str):
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

    if cluster_type != HadoopCluster.get_cluster_type():
        hc_link_fname = __get_hc_link_file(cluster_type, cid)
        if os.path.exists(hc_link_fname):
            with open(hc_link_fname) as link_file:
                hc_id = int(link_file.readline())
            serialize_cluster(HadoopCluster.get_cluster_type(), hc_id,
                              cluster_object.hc)


def remove_cluster(cluster_type, cid):
    """Remove temporary files created for the given cluster. Remove the linked
    Hadoop cluster if it exists.

    Args:
      cluster_type (str):
        The type of cluster to serialize.
      cid (int):
        The id of the cluster.
    """

    fname = __get_cluster_file(cluster_type, cid)
    os.remove(fname)

    if cluster_type != HadoopCluster.get_cluster_type():
        hc_link_fname = __get_hc_link_file(cluster_type, cid)
        os.remove(hc_link_fname)


def link_to_hadoop_cluster(cluster_type, cid, hc_id):
    """Stores a reference to a Hadoop cluster.

    Args:
      cluster_type (str):
        The type of cluster to link.
      cid (int):
        The id of the cluster.
      hc_id (int)"
        The id of the Hadoop cluster to link.
    """

    fname = __get_hc_link_file(cluster_type, cid)
    with open(fname, 'wb') as link_file:
        link_file.write(str(hc_id))