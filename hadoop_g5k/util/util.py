import os
import re
import socket

from execo import SshProcess

from execo.action import Remote, TaktukRemote
from execo_engine import logger


# HW management ###############################################################
def is_within_g5k():
    hostname = socket.gethostname()
    if "grid5000.fr" in hostname:
        return True
    else:
        return False

if is_within_g5k():
    from hadoop_g5k.util.g5k import G5kHardwareManager
    hw_manager = G5kHardwareManager()
else:
    from hadoop_g5k.util.hardware import GenericHardwareManager
    hw_manager = GenericHardwareManager()


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


# Requirements ################################################################

def check_java_version(java_major_version, hosts):

    tr = TaktukRemote("java -version 2>&1 | grep version", hosts)
    tr.run()

    for p in tr.processes:
        match = re.match('.*[^.0-9]1\.([0-9]+).[0-9].*', p.stdout)
        version = int(match.group(1))
        if java_major_version > version:
            msg = "Java 1.%d+ required" % java_major_version
            return False

    return True


def get_java_home(host):
    proc = SshProcess('echo $(readlink -f /usr/bin/javac | '
                               'sed "s:/bin/javac::")', host)
    proc.run()
    return proc.stdout.strip()


def check_packages(packages, hosts):
    tr = TaktukRemote("dpkg -s " + packages, hosts)
    for p in tr.processes:
        p.nolog_exit_code = p.nolog_error = True
    tr.run()
    return tr.ok


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
