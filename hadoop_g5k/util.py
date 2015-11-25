import os
import re
import shutil
import tempfile

from abc import ABCMeta, abstractmethod

from execo.action import Remote
from execo.host import Host
from execo.log import style
from execo_engine import logger
from execo_g5k import get_oar_job_nodes, get_oargrid_job_nodes, \
    get_host_attributes


# Imports #####################################################################

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


# Compression #################################################################

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


# Hosts #######################################################################

def generate_hosts(hosts_input):
    """Generate a list of hosts from the given file.

    Args:
      hosts_input: The path of the file containing the hosts to be used,
        or a comma separated list of site:job_id or an a comma separated list
        of hosts or an oargrid_job_id.
        If a file is used, each host should be in a different line.
        Repeated hosts are pruned.
        Hint: in a running Grid5000 job, $OAR_NODEFILE should be used.

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
    elif "," in hosts_input:
        # We assume the string is a comma separated list of hosts
        for hstr in hosts_input.split(','):
            h = Host(hstr.rstrip())
            if h not in hosts:
                hosts.append(h)
    elif hosts_input.isdigit():
        # If the file_name is a number, we assume this is a oargrid_job_id
        hosts = get_oargrid_job_nodes(int(hosts_input))
    else:
        # If not any of the previous, we assume is a single-host cluster where
        # the given input is the only host
        hosts = [Host(hosts_input.rstrip())]

    logger.debug('Hosts list: \n%s',
                 ' '.join(style.host(host.address.split('.')[0])
                          for host in hosts))
    return hosts


# Cluster info ################################################################

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


# Output formatting ###########################################################

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


# Configuration functions #####################################################

def create_xml_file(f):
    with open(f, "w") as fout:
        fout.write("<configuration>\n")
        fout.write("</configuration>")


def __replace_line(line, value):
    return re.sub(r'(.*)<value>[^<]*</value>(.*)', r'\g<1><value>' + value +
                  r'</value>\g<2>', line)


def replace_in_xml_file(f, name, value,
                        create_if_absent=False, replace_if_present=True):
    """Assign the given value to variable name in xml file f.

    Args:
      f (str):
        The path of the file.
      name (str):
        The name of the variable.
      value (str):
        The new value to be assigned:
      create_if_absent (bool, optional):
        If True, the variable will be created at the end of the file in case
        it was not already present.
      replace_if_present (bool, optional):
        If True, the current value will be replaced; otherwise, the current
        value will be maintained and the specified value ignored .

    Returns (bool):
      True if the assignment has been made, False otherwise.
    """

    changed = False

    (_, temp_file) = tempfile.mkstemp("", "xmlf-", "/tmp")

    inf = open(f)
    outf = open(temp_file, "w")
    line = inf.readline()
    while line != "":
        if "<name>" + name + "</name>" in line:
            if replace_if_present:
                if "<value>" in line:
                    outf.write(__replace_line(line, value))
                    changed = True
                else:
                    outf.write(line)
                    line = inf.readline()
                    if line != "":
                        outf.write(__replace_line(line, value))
                        changed = True
                    else:
                        logger.error("Configuration file " + f +
                                     " is not correctly formatted")
            else:
                return False
        else:
            if ("</configuration>" in line and
                    create_if_absent and not changed):
                outf.write("  <property><name>" + name + "</name>" +
                           "<value>" + str(value) + "</value></property>\n")
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


def get_xml_params(f, param_names):

    if not param_names:
        return {}

    local_param_names = param_names[:]

    params = {}
    for name in local_param_names:
        params[name] = None

    with open(f) as inf:
        line = inf.readline()
        while line != "":
            for name in local_param_names:
                if "<name>" + name + "</name>" in line:
                    if "<value>" in line:
                        match = re.match('.*<value>([^<]*)</value>.*', line)
                        params[name] = match.group(1)
                    else:
                        line = inf.readline()
                        if line != "":
                            match = re.match('.*<value>([^<]*)</value>.*', line)
                            params[name] = match.group(1)
                        else:
                            logger.error("Configuration file " + f +
                                         " is not correctly formatted")

                    del(name)
                line = inf.readline()
        inf.close()

    return params
