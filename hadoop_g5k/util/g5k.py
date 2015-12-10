import os

from execo import Host, logger
from execo.log import style
from execo_g5k import get_host_cluster, get_host_attributes, get_oar_job_nodes, \
    get_oargrid_job_nodes

from hadoop_g5k.util.hardware import PhysicalCluster, DeploymentHardware, \
    HardwareManager


class G5kHardwareManager(HardwareManager):
    """This class provides an implementation of HardwareManager that makes use
    of Grid5000 API to retrieve information about the hardware. It is based on
    execo_g5k package.
    """

    def make_physical_cluster(self, name, hosts):
        return G5kPhysicalCluster(self, name, hosts)

    def make_deployment_hardware(self):
        return G5kDeploymentHardware(self)

    def get_memory_and_cores(self, host):
        host_attrs = get_host_attributes(host)
        cores = host_attrs[u'architecture'][u'smt_size']
        mem = host_attrs[u'main_memory'][u'ram_size'] / (1024 * 1024)

        return mem, cores

    def get_switch(self, host):
        """Return the network switch to which the host is connected.

        Args:
          host (Host): The host to query.

        Return:
          str: The network switch to which the given host is connected.
        """
        nw_adapters = get_host_attributes(host)[u'network_adapters']
        for nwa in nw_adapters:
            if (u'network_address' in nwa and
                        nwa[u'network_address'] == host.address):
                return nwa[u'switch']

    def get_hosts_list(self, hosts_str):
        """Generate a list of hosts from the given string.

        Args:
          hosts_str (str): The following options are supported

            - The path of the file containing the hosts to be used. Each host
            should be in a different line. Repeated hosts are pruned.
            Hint: in a running Grid5000 job, $OAR_NODEFILE should be used.

            - A comma-separated list of  site:job_id

            - A comma-separated list of hosts.

            - An oargrid_job_id

        Return:
          list of Host: The list of hosts.
        """
        hosts = []
        if os.path.isfile(hosts_str):
            for line in open(hosts_str):
                h = Host(line.rstrip())
                if h not in hosts:
                    hosts.append(h)
        elif ':' in hosts_str:
            # We assume the string is a comma separated list of site:job_id
            for job in hosts_str.split(','):
                site, job_id = job.split(':')
                hosts += get_oar_job_nodes(int(job_id), site)
        elif "," in hosts_str:
            # We assume the string is a comma separated list of hosts
            for hstr in hosts_str.split(','):
                h = Host(hstr.rstrip())
                if h not in hosts:
                    hosts.append(h)
        elif hosts_str.isdigit():
            # If the file_name is a number, we assume this is a oargrid_job_id
            hosts = get_oargrid_job_nodes(int(hosts_str))
        else:
            # If not any of the previous, we assume is a single-host cluster
            # where the given input is the only host
            hosts = [Host(hosts_str.rstrip())]

        logger.debug('Hosts list: \n%s',
                     ' '.join(style.host(host.address.split('.')[0])
                              for host in hosts))
        return hosts


class G5kPhysicalCluster(PhysicalCluster):

    def __init__(self, hw_manager, name, hosts):
        super(G5kPhysicalCluster, self).__init__(hw_manager, name, hosts)


class G5kDeploymentHardware(DeploymentHardware):

    def __init__(self, hw_manager):
        super(G5kDeploymentHardware, self).__init__(hw_manager)

    def add_hosts(self, hosts):
        """Add the provided list of hosts to the deployment hardware.

        The hosts are grouped into a set of clusters depending on their G5k
        cluster. They are supposed to share hardware characteristics but this
        is not checked

        Args:
          hosts (list of Host): The list of hosts to add.
        """

        host_clusters = {}
        for h in hosts:
            g5k_cluster = get_host_cluster(h)
            if g5k_cluster in host_clusters:
                host_clusters[g5k_cluster].append(h)
            else:
                host_clusters[g5k_cluster] = [h]

        for (cluster, cluster_hosts) in host_clusters.items():
            self.add_cluster(G5kPhysicalCluster(self._hw_manager,
                                                cluster, cluster_hosts))
