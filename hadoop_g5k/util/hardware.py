import os

from abc import ABCMeta, abstractmethod

from execo import SshProcess, Host, logger
from execo.log import style


# Abstract classes ############################################################

class HardwareManager(object):
    """This class is a factory for hardware related objects and methods."""

    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def make_physical_cluster(self, name, hosts):
        """Create a new instance of PhysicalCluster.

        Args:
          name (str): The name of the cluster.
          hosts (list of Host): A list with the hosts composing the cluster.

        Return:
          PhysicalCluster: An instance containing the cluster information.
        """
        pass

    @abstractmethod
    def make_deployment_hardware(self):
        """Create a new instance of DeploymentHardware.

        Return:
          DeploymentHardware: An instance with all the information about the
            hardware where the cluster is to be deployed.
        """
        pass

    @abstractmethod
    def get_memory_and_cores(self, host):
        """Obtain the total available memory in MB and number of cores of the
        given host.

        Args:
          host (Host): The host to query.

        Return:
          tuple of (int, int): A tuple containing the host available memory in
            MB and its number of cores.
        """
        pass

    @abstractmethod
    def get_switch(self, host):
        """Return the network switch to which the host is connected.

        Args:
          host (Host): The host to query.

        Return:
          str: The network switch to which the given host is connected.
        """
        pass

    @abstractmethod
    def get_hosts_list(self, hosts_str):
        """Generate a list of hosts from the given string.

        Args:
          hosts_str (str): The string defining the hosts.

        Return:
          list of Host: The list of hosts.
        """
        pass


class PhysicalCluster(object):
    """This class represents a grouping of hosts that share the same hardware
    characteristics."""

    __metaclass__ = ABCMeta

    def __init__(self, hw_manager, name, hosts):
        self._hw_manager = hw_manager

        self._name = name
        self._hosts = hosts

        (self._memory, self._num_cores) = \
            self._hw_manager.get_memory_and_cores(hosts[0])

    def get_name(self):
        """Return the cluster name.

        Return:
          str: The cluster name.
        """
        return self._name

    def get_hosts(self):
        """Return the hosts of the cluster.

        Return:
          list of Host: A list with the hosts of the cluster.
        """
        return self._hosts

    def get_memory(self):
        """Return the available memory of an individual host in the cluster.

        Return:
          int: The memory in MB.
        """
        return self._memory

    def get_num_cores(self):
        """Return the number of cores of an individual host in the cluster.

        Return:
          int: The number of cores.
        """
        return self._num_cores


class DeploymentHardware(object):
    """This class contains all hardware elements (clusters) included in a
    deployment."""

    __metaclass__ = ABCMeta

    def __init__(self, hw_manager):

        self._hw_manager = hw_manager

        self._clusters = {}
        self._total_cores = 0
        self._total_mem = 0
        self._total_nodes = 0
        self._max_memory_cluster = None
        self._max_cores_cluster = None
        self._max_nodes_cluster = None

    @abstractmethod
    def add_hosts(self, hosts):
        """Add the provided list of hosts to the deployment hardware.
        The hosts are grouped into a set of clusters.

        Args:
          hosts (list of Host): The list of hosts to add.
        """
        pass

    def add_cluster(self, cluster):
        """Add a cluster to the deployment hardware.

        Args:
          cluster (PhysicalCluster): The cluster to add.
        """

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

    def get_host_cluster(self, host):
        """Return the cluster object the given host belongs to.

        Args:
          host (Host): The host.

        Return:
          PhysicalCluster: The cluster the given host belongs to.
        """
        for c in self._clusters.itervalues():
            if host in c.get_hosts():
                return c

        return None

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


# Implementations #############################################################

class GenericHardwareManager(HardwareManager):
    """This class provides a generic implementation of HardwareManager."""

    def make_physical_cluster(self, name, hosts):
        return GenericPhysicalCluster(self, name, hosts)

    def make_deployment_hardware(self):
        return GenericDeploymentHardware(self)

    def get_memory_and_cores(self, host):
        """Obtain the total available memory in MB and number of cores of the
        given host.

        Supported systems include Linux and Max OS X.. In linux it uses nproc
        and /proc/meminfo to obtain the informtion. In Max OS X it uses
        system_profiler.

        Args:
          host (Host): The host to query.

        Return:
          tuple of (int, int): A tuple containing the host available memory in
            MB and its number of cores.
        """

        linux_cmd = "nproc && " \
                    "cat /proc/meminfo | grep MemTotal | awk '{ print $2,$3 }'"

        max_cmd = "system_profiler SPHardwareDataType | grep Cores | " \
                  "awk '{ print $NF }' && " \
                  "system_profiler SPHardwareDataType | grep Memory | " \
                  "awk '{ print $2,$3 }'"

        undef_str = "?"
        undef_cmd = "echo '" + undef_str + "'"

        command = 'if [ $(uname) == "Linux" ]; then %s; ' \
                  'elif [ $(uname) == "Darwin" ]; then %s; ' \
                  'else %s; fi' % \
                  (linux_cmd, max_cmd, undef_cmd)

        proc = SshProcess(command, host)
        proc.run()

        out = proc.stdout

        if out == undef_str:
            return None
        else:
            units = {
                "kb": lambda x: int(x) // 1024,
                "mb": lambda x: int(x),
                "gb": lambda x: int(x) * 1024,
            }

            (cores_str, mem_str) = out.splitlines()
            cores = int(cores_str)
            (num, unit) = mem_str.split()
            mem = units[unit.lower()](num)

        return mem, cores

    def get_switch(self, host):
        """Return the network switch to which the host is connected.

        In this implementation all hosts are given the switch "default-rack"

        Args:
          host (Host): The host to query.

        Return:
          str: "default-rack"
        """
        return "default-rack"

    def get_hosts_list(self, hosts_str):
        """Generate a list of hosts from the given file.

        Args:
          hosts_str (str): The following options are supported

            - The path of the file containing the hosts to be used. Each host
            should be in a different line. Repeated hosts are pruned.
            Hint: in a running Grid5000 job, $OAR_NODEFILE should be used.

            - A comma-separated list of hosts.

        Return:
          list of Host: The list of hosts.
        """
        hosts = []
        if os.path.isfile(hosts_str):
            for line in open(hosts_str):
                h = Host(line.rstrip())
                if h not in hosts:
                    hosts.append(h)
        elif "," in hosts_str:
            # We assume the string is a comma separated list of hosts
            for hstr in hosts_str.split(','):
                h = Host(hstr.rstrip())
                if h not in hosts:
                    hosts.append(h)
        else:
            # If not any of the previous, we assume is a single-host cluster
            # where the given input is the only host
            hosts = [Host(hosts_str.rstrip())]

        logger.debug('Hosts list: \n%s',
                     ' '.join(style.host(host.address.split('.')[0])
                              for host in hosts))
        return hosts


class GenericPhysicalCluster(PhysicalCluster):

    def __init__(self, hw_manager, name, hosts):
        super(GenericPhysicalCluster, self).__init__(hw_manager, name, hosts)


class GenericDeploymentHardware(DeploymentHardware):

    def __init__(self, hw_manager):
        super(GenericDeploymentHardware, self).__init__(hw_manager)

    def add_hosts(self, hosts):
        """Add the provided list of hosts to the deployment hardware.

        The hosts are grouped into a set of clusters depending on their
        hardware characteristics (memory and number of cores) so that all hosts
        within the cluster have the same ones.

        Args:
          hosts (list of Host): The list of hosts to add.
        """

        prefix = "cluster-"
        curr_cluster = -1

        cluster_names = {}
        host_clusters = {}
        for h in hosts:
            (h_mem, h_cores) = self._hw_manager.get_memory_and_cores(h)

            c_name = cluster_names.get((h_mem, h_cores))
            if not c_name:
                curr_cluster += 1
                c_name = prefix + str(curr_cluster)
                cluster_names[(h_mem, h_cores)] = c_name
                host_clusters[c_name] = []

            host_clusters[c_name].append(h)

        for (cluster, cluster_hosts) in host_clusters.items():
            self.add_cluster(GenericPhysicalCluster(self._hw_manager,
                                                    cluster, cluster_hosts))

        print cluster_names
        print host_clusters
