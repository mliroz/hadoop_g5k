import os
import threading

from abc import ABCMeta, abstractmethod

from execo.action import Put, TaktukRemote
from execo.process import SshProcess
from execo_engine import logger
from hadoop_g5k.objects import HadoopJarJob
from hadoop_g5k.util import import_function


class Dataset(object):
    """This class defines the methods of a dataset, a set of files that can be
    loaded or generated into the dfs and then cleaned.
    """

    __metaclass__ = ABCMeta

    deployments = {}

    def __init__(self, params):
        """Create a dataset with the given params.
        
        Args:
          params (dict): Parameters of the dataset.
        
        """

        self.params = params

    @abstractmethod
    def load(self, hc, dest, desired_size=None):
        """Load the dataset in the given dfs folder.
        
        Args:
          hc (HadoopCluster):
            The Hadoop cluster where to deploy the dataset.
          dest (str):
            The dfs destination folder.
          desired_size (int, optional):
            The size of the data to be copied.
        """

        self.deployments[hc, desired_size] = dest

    def clean(self, hc):
        """Remove the dataset from dfs.
        
        Args:
          hc (HadoopCluster):
            The Hadoop cluster where the dataset has been deployed.
        """

        removed = False
        for (hcd, sized) in self.deployments:
            if hc == hcd:
                command = "fs -rmr " + self.deployments[hc, sized]
                hc.execute(command, should_be_running=True, verbose=False)
                removed = True

        if not removed:
            logger.warn("The dataset was not loaded in the given cluster")


class StaticDataset(Dataset):
    """This class manages a static dataset, i.e., a dataset that has already
    been created and is to be copied to the cluster.
    """

    def __init__(self, params):
        """Create a static dataset with the given params.
        
        Args:
          params (dict):
            A dictionary with the parameters. This dataset needs the following
            parameters:
            - local_path: The path to the directory where the dataset is stored
                          locally.
            - pre_load_function: A function to be applied after transfers and
                                 before loading to dfs (usually decompression).
        """

        super(StaticDataset, self).__init__(params)

        local_path = params["local_path"]
        if not os.path.exists(local_path):
            logger.error("The dataset local dir does not exist")

        if "pre_load_function" in params:
            pre_load_function_name = params["pre_load_function"]
            self.pre_load_function = import_function(pre_load_function_name)
        else:
            self.pre_load_function = None

        self.local_path = local_path

    def load(self, hc, dest, desired_size=None):
        """Load the dataset in the given dfs folder by copying it from the
        local folder.
        
        Args:
          hc (HadoopCluster):
            The Hadoop cluster where to deploy the dataset.
          dest (str):
            The dfs destination folder.
          desired_size (int, optional):
            The size of the data to be copied. If indicated only the first files
            of the dataset up to the given size are copied, if not, the whole
            dataset is transferred.
        """

        dataset_files = [os.path.join(self.local_path, f) for f in
                         os.listdir(self.local_path)]
        hosts = hc.hosts

        # Define and create temp dir
        tmp_dir = "/tmp" + dest
        action_remove = TaktukRemote("rm -rf " + tmp_dir, hosts)
        action_remove.run()
        action_create = TaktukRemote("mkdir -p " + tmp_dir, hosts)
        action_create.run()

        # Generate list of files to copy
        if desired_size:
            all_files_to_copy = []
            dataset_files.sort()
            real_size = 0
            while real_size < desired_size:
                if dataset_files:
                    all_files_to_copy.append(dataset_files[0])
                    real_size += os.path.getsize(dataset_files[0])
                    del dataset_files[0]
                else:
                    logger.warn(
                        "Dataset files do not fill up to desired size "
                        "(real size = " + str(real_size) + ")")
                    break

        else:
            real_size = 0
            all_files_to_copy = dataset_files
            for f in all_files_to_copy:
                real_size += os.path.getsize(f)

        # Assign files to hosts
        files_per_host = [[]] * len(hosts)
        for idx in range(0, len(hosts)):
            files_per_host[idx] = all_files_to_copy[idx::len(hosts)]

        # Create threads and launch them
        logger.info(
            "Loading dataset in parallel into " + str(len(hosts)) + " hosts")
        if not hc.running:
            hc.start()

        class SizeCollector:
            size = 0
            lock = threading.Lock()

            def increment(self, qty):
                self.lock.acquire()
                try:
                    self.size += qty
                finally:
                    self.lock.release()

        def copy_function(host, files_to_copy, collector=None):
            action = Put([host], files_to_copy, tmp_dir)
            action.run()

            local_final_size = 0

            for f in files_to_copy:
                src_file = os.path.join(tmp_dir, os.path.basename(f))
                if self.pre_load_function:
                    src_file = self.pre_load_function(src_file, host)

                    action = SshProcess("du -b " + src_file + "| cut -f1", host)
                    action.run()

                    local_final_size += int(action.stdout.strip())

                hc.execute("fs -put " + src_file + " " +
                           os.path.join(dest, os.path.basename(src_file)),
                           host, True, False)

            if collector:
                collector.increment(local_final_size)

        if self.pre_load_function:
            final_size = SizeCollector()
        else:
            final_size = None

        threads = []
        for idx, h in enumerate(hosts):
            if files_per_host[idx]:
                t = threading.Thread(target=copy_function,
                                     args=(h, files_per_host[idx], final_size))
                t.start()
                threads.append(t)

        # Wait for the threads to finish
        for t in threads:
            t.join()

        logger.info("Loading completed: real local size = " + str(real_size) +
                    ", final remote size = " + str(final_size.size))

        self.deployments[hc, desired_size] = dest


class DynamicDataset(Dataset):
    """This class manages a dynamic dataset, i.e., a dataset that is created
    dynamically by a Hadoop job.
    """

    def __init__(self, params):
        """Create a dynamic dataset with the given params.
        
        Args:
          params (dict):
            A dictionary with the parameters. This dataset needs the following
            parameters:
            - job.jar: The path to the jar containing the job to be executed.
            - job.params: The set of params of the job.
            - job.libjars: The list of jars to be used as libraries.
        """

        super(DynamicDataset, self).__init__(params)

        # Job parameters
        jobjar = params["job.jar"]

        if "libjars" in params:
            libjars = params["job.libjars"].split()
        else:
            libjars = []

        if "job.params" in params:
            jobparams = params["job.params"]
        else:
            jobparams = []

        self.job = HadoopJarJob(jobjar, jobparams, libjars)

        # Other parameters
        # TODO

    def load(self, hc, dest, desired_size=None):
        """Load the dataset in the given dfs folder by generating it
        dynamically.
        
        Args:
          hc (HadoopCluster):
            The Hadoop cluster where to deploy the dataset.
          dest (str):
            The dfs destination folder.
          desired_size (int, optional):
            The size of the data to be copied.
        """

        hc.execute_jar(self.job)

        self.deployments[hc, desired_size] = dest
