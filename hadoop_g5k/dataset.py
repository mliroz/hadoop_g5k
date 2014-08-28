import os
import threading

from abc import ABCMeta, abstractmethod

from execo.action import Put
from execo_engine import logger
from hadoop_g5k import HadoopCluster

class Dataset(object):
        
    __metaclass__ = ABCMeta
    
    deployments = {}
    
    def __init__(self, desired_size = None):
        """Documentation"""
        
        self.desired_size = desired_size
    
    @abstractmethod
    def deploy(self, hc, dest):
        """Deploy the dataset in the given dfs folder.
        
        Args:
          hc (HadoopCluster): The hadoop cluster where to deploy the dataset.
          dest (str): The dfs destination folder.
        """
        deployments[hc] = dest
    
    def clean(self, hc):
        """Remove the dataset from dfs.
        
        Args:
          hc (HadoopCluster): The hadoop cluster where the dataset has been
          deployed.
        """
        if hc in deployments:
            command = "fs -rmr " + deployments[hc]
            hc.execute(command, should_be_running = True, verbose = False)
        else:
          logger.warn("The dataset was not deployed in the given cluster")


class StaticDataset(Dataset):
    """This class manages a static dataset, i.e., a dataset that has already
    been and is to be copied to the cluster in ocreated
    """
    
    def __init__(self, local_dir, desired_size = None):
        """Create a static dataset with the desired size from the contents in
        the given directory.
        
        Args:
          local_dir (str): The path to the directory where the dataset is stored
            locally.
          desired_size (int, optional): The size of the data to be copied. If
            indicated only the first files of the dataset up to the given size
            are copied, if not, the whole dataset is transferred.
        """
        
        super(StaticDataset, self).__init__(desired_size)
        if not os.path.exists(local_dir):
            logger.error("The dataset local dir does not exist")
        
        self.local_dir = local_dir
        
    
    def deploy(self, hc, dest, uncompress_function = None):
        """Deploy the dataset in the given dfs folder by copying it from the
        local folder.
        
        Args:
          hc (HadoopCluster): The hadoop cluster where to deploy the dataset.
          dest (str): The dfs destination folder.
          uncompress_function (func): An uncompress function to be applied after
            transfers and before uploading to dfs
        """
        
        dataset_files = [os.path.join(self.local_dir,f) for f in os.listdir(self.local_dir)]
        
        #tmp_dir = os.path.join("/tmp",os.path.basename(self.local_dir))
        tmp_dir = "/tmp"
        
        # Generate list of files to copy
        if self.desired_size:
            all_files_to_copy = []
            dataset_files.sort()
            self.real_size = 0
            while self.real_size < self.desired_size:
                if dataset_files:
                    all_files_to_copy.append(dataset_files[0])
                    del dataset_files[0]
                    self.real_size += os.path.getsize(dataset_files[0])
                else:
                    logger.warn("Dataset files do not fill up to desired size (real size = " + str(self.real_size) + ")")
                    break
                
        else:
            all_files_to_copy = dataset_files
        
        # Assign files to hosts
        hosts = hc.hosts
        files_per_host = [[]] * len(hosts)
        for idx in range(0,len(hosts)):
            files_per_host[idx] = all_files_to_copy[idx::len(hosts)]
            
        # Create threads and launch them
        if not hc.running:
            hc.start()
        
        def copy_function(host, files_to_copy):
            action = Put([ host ], files_to_copy, tmp_dir)
            action.run()
            
            for f in files_to_copy:
                src_file = os.path.join(tmp_dir, os.path.basename(f))
                if uncompress_function:
                    src_file = uncompress_function(src_file, host)
            
                hc.execute("fs -put " + src_file + " " + os.path.join(dest,os.path.basename(f)), host, True, False)
            
        threads = []
        for idx, h in enumerate(hosts):
            if files_per_host[idx]:
                t = threading.Thread(target = copy_function, args = (h, files_per_host[idx]))
                t.start()
                threads.append(t)
        
        # Wait for the threads to finish
        for t in threads:
            t.join()
        
        self.deployments[hc] = dest
        

class DynamicDataset(Dataset):
    
    def __init__(self, jar_job, desired_size):
        """Documentation"""
        
    def deploy(self, hc, dest):
        """Deploy the dataset in the given dfs folder by generating it
        dynamically.
        
        Args:
          hc (HadoopCluster): The hadoop cluster where to deploy the dataset.
          dest (str): The dfs destination folder.
        """
        
        deployments[hc] = dest
