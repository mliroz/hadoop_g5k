from abc import ABCMeta, abstractmethod

from execo_g5k import get_host_attributes, get_host_cluster

__author__ = 'mliroz'


class PhysicalCluster(object):

    def __init__(self, name, hosts):
        self._name = name
        self._hosts = hosts

    def get_name(self):
        return self._name

    def get_hosts(self):
        return self._hosts

    @abstractmethod
    def get_memory(self):
        pass

    @abstractmethod
    def get_num_cores(self):
        pass


class G5kPhysicalCluster(PhysicalCluster):

    def __init__(self, name, hosts):
        super(G5kPhysicalCluster, self).__init__(name, hosts)

        host_attrs = get_host_attributes(hosts[0])
        self._num_cores = host_attrs[u'architecture'][u'smt_size']
        self._memory = host_attrs[u'main_memory'][u'ram_size'] / (1024 * 1024)

    def get_memory(self):
        return self._memory

    def get_num_cores(self):
        return self._num_cores


class DeploymentHardware(object):

    def __init__(self):
        self._clusters = {}
        self._total_cores = 0
        self._total_mem = 0
        self._total_nodes = 0
        self._max_memory_cluster = None
        self._max_cores_cluster = None
        self._max_nodes_cluster = None

    @abstractmethod
    def add_hosts(self, hosts):
        pass

    def add_cluster(self, cluster):
        self._clusters[cluster.get_name()] = cluster
        num_nodes = len(cluster.get_hosts())

        self._total_cores += cluster.get_num_cores() * num_nodes
        self._total_mem += cluster.get_memory() * num_nodes
        self._total_nodes += num_nodes

        if len(self._clusters) == 1: # First cluster
            self._max_memory_cluster = cluster
            self._max_cores_cluster = cluster
            self._max_nodes_cluster = cluster
        else:
            if cluster.get_memory() > self._max_memory_cluster.get_memory():
                self._max_memory_cluster = cluster

            if cluster.get_num_cores() > self._max_cores_cluster.get_num_cores():
                self._max_cores_cluster = cluster

            if num_nodes > len(self._max_nodes_cluster.get_hosts()):
                self._max_nodes_cluster = cluster

    def get_clusters(self):
        return self._clusters.values()

    def get_cluster(self, name):
        return self._clusters[name]

    def get_total_cores(self):
        return self._total_cores

    def get_total_mem(self):
        return self._total_mem

    def get_total_nodes(self):
        return self._total_nodes

    def get_max_memory_cluster(self):
        return self._max_memory_cluster

    def get_max_cores_cluster(self):
        return self._max_cores_cluster

    def get_max_nodes_cluster(self):
        return self._max_nodes_cluster


class G5kDeploymentHardware(DeploymentHardware):

    def add_hosts(self, hosts):
        host_clusters = {}
        for h in hosts:
            g5k_cluster = get_host_cluster(h)
            if g5k_cluster in host_clusters:
                host_clusters[g5k_cluster].append(h)
            else:
                host_clusters[g5k_cluster] = [h]

        print host_clusters

        for (cluster, cluster_hosts) in host_clusters.items():
            self.add_cluster(G5kPhysicalCluster(cluster, cluster_hosts))
