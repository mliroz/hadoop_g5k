#!/usr/bin/env python

import datetime
import os
import re
import sys
import time
import ConfigParser

from hadoop_g5k import HadoopCluster, HadoopJarJob

from execo.action import Get, Remote
from execo.process import SshProcess
from execo.time_utils import timedelta_to_seconds, format_date
from execo_g5k import get_cluster_site, get_oar_job_info, oardel, oarsub, \
    get_planning, compute_slots, get_jobs_specs, get_oar_job_nodes, \
    deploy, Deployment, get_current_oar_jobs
from execo_engine import Engine, logger, sweep, ParamSweeper
from networkx import DiGraph, NetworkXUnfeasible, topological_sort

DEFAULT_DATA_BASE_DIR = "/tests/data"
DEFAULT_OUT_BASE_DIR = "/tests/out"

class HadoopEngineException(Exception): pass
class ParameterException(HadoopEngineException): pass
class MacroException(ParameterException): pass

class MacroManager(object):
    
    def __init__(self):
        """Crate a new MacroManager object."""
    
        self.dep_graph = DiGraph()
        
        self.ds_macros = set([])
        self.xp_macros = set([])
        
        self.define_test_macros()
        
        
    def define_test_macros(self):
        """Define values and dependencies of test macros."""        
        
        self.test_macros = {
           "data_base_dir" : "/tests/data",
           "out_base_dir" : "/tests/out",
           "data_dir" : "/tests/data/0", # data_base_dir/ds_id
           "out_dir" : "/tests/out/0", # data_base_dir/comb_id
           "comb_id" : 0,
           "ds_id" : 0,
           "xp.input" : "/tests/data/0", # data_dir
           "xp.output" : "/tests/out/0" # out_dir
        }
        
        self.ds_params = set([])
        self.xp_params = set([])
        
        self.dep_graph.add_nodes_from(self.test_macros.keys())
               
        self.add_dependency("data_base_dir","data_dir")
        self.add_dependency("ds_id","data_dir")
        self.add_dependency("data_dir","xp.input")
        
        self.add_dependency("out_base_dir","out_dir")
        self.add_dependency("comb_id","out_dir")
        self.add_dependency("out_dir","xp.output")
        
        self.sorted_test_macros = topological_sort(self.dep_graph)
        
    def update_test_macros(self, ds_id = None, comb_id = None):
        """Update test macros with dataset and/or combination ids.
        
        Args:
          ds_id (int, optional): The dataset identifier.
          comb_id (int, optional): The combination identifier.
        """
        
        if ds_id:
            if "data_dir" in self.test_macros:
                self.test_macros["data_dir"] = self.test_macros["data_base_dir"] + "/" + str(ds_id)
                if "xp.input" in self.test_macros:
                    self.test_macros["xp.input"] = self.test_macros["data_dir"]
        if comb_id:            
            if "out_dir" in self.test_macros:
                self.test_macros["out_dir"] = self.test_macros["out_base_dir"] + "/" + str(comb_id)
                if "xp.output" in self.test_macros:
                    self.test_macros["xp.output"] = self.test_macros["out_dir"]          
        
    def __filter_unused_test_macros(self):
        for m in reversed(self.sorted_test_macros):
            if not self.dep_graph.successors(m):
                self.dep_graph.remove_node(m)
                self.sorted_test_macros.remove(m)
                del self.test_macros[m]
        
        
    def add_ds_params(self, params):
        """Add the list of dataset parameters.
        
        Args:
          params (dict): The list of dataset parameters.
        """
        
        self.ds_params = self.ds_params.union(params)    
        
    def add_xp_params(self, params):
        """Add the list of experiment parameters.
        
        Args:
          params (dict): The list of experiment parameters.
        """
        
        self.xp_params = self.xp_params.union(params)
        
        
    def add_dependency(self, m1, m2):
        """Includes a new macro dependency: m1 -> m2. This means that to obtain
        the value of m2 we use the value of m1.
        
        Args:
          m1 (string): The name of the param used.
          m2 (string): The name of the param being specified.
        """      
                    
        # Check if dependency is correct
        if m1 in self.ds_params:
            if m2 in self.test_macros:
                logger.error("Not allowed dependency: ds -> test")
                raise MacroException("Not allowed dependency: ds -> test")
        elif m1 in self.xp_params:
            if m2 in self.test_macros:
                logger.error("Not allowed dependency: xp -> test")
                raise MacroException("Not allowed dependency: xp -> test")
            elif m2 in self.ds_params:
                logger.error("Not allowed dependency: xp -> ds")
                raise MacroException("Not allowed dependency: xp -> ds")
            
        # Add dependency
        self.dep_graph.add_edge(m1, m2)
        
        
    def sort_macros(self):
        """Sort macros respecting dependencies.
        
        Raises:
          MacroException: if there are cycles in the dependencies between macros.
        """
        
        # Filter out unused test variables
        self.__filter_unused_test_macros()
        
        # Sort ds and xp macros
        try:
            self.sorted_ds_macros = topological_sort(self.dep_graph.subgraph(self.ds_params))
            self.sorted_xp_macros = topological_sort(self.dep_graph.subgraph(self.xp_params))
        except NetworkXUnfeasible:
            raise MacroException("Macros do not follow a DAG")
        
        logger.info("Dependencies = " + str(self.dep_graph.edges()))        
        logger.info("Test macros = " + str(self.sorted_test_macros))
        logger.info("Dataset macros = " + str(self.sorted_ds_macros))
        logger.info("Experiment macros = " + str(self.sorted_xp_macros))
        
    def _replace_macros_from_list(self, list_macros, value):
        """Replace the macros given in the list within the value if present.
        
        Args:
          list_macros (dict): The list of macros to replace and their respective
            values.
          value (string): The value where to do the replacement.
        """
        
        new_value = value
        for m in list_macros:
            new_value = new_value.replace("${" + m + "}", str(list_macros[m]))            
        return new_value
            
        
    def replace_ds_macros(self, comb):
        """Replace macros in ds combination.
        
        Args:
          comb (dict): The combination of parameters.
        """
                
        list_macros = self.test_macros
        
        for m in self.sorted_ds_macros:
            comb[m] = self._replace_macros_from_list(list_macros, comb[m])
            list_macros[m] = comb[m]
            
        
    def replace_xp_macros(self, comb):
        """Replace macros in xp combination.
        
        Args:
          comb (dict): The combination of parameters.
        """
        
        list_macros = self.test_macros
        
        for m in self.sorted_ds_macros:
            comb[m] = self._replace_macros_from_list(list_macros, comb[m])
            list_macros[m] = comb[m]
        
        for m in self.sorted_xp_macros:
            comb[m] = self._replace_macros_from_list(list_macros, comb[m])
            list_macros[m] = comb[m]        
        

class HadoopEngine(Engine):
    """This class manages thw whole workflow of a hadoop test suite."""

    def __init__(self):
        self.frontend = None
        super(HadoopEngine, self).__init__()
        
        # Parameter definition
        self.options_parser.set_usage("usage: %prog <cluster> <n_nodes> <config_file>")
        self.options_parser.add_argument("cluster",
                    "The cluster on which to run the experiment")
        self.options_parser.add_argument("n_nodes",
                    "The number of nodes in which the experiment is going to be deployed")
        self.options_parser.add_argument("config_file",
                    "The path of the file containing the test params (INI file)")                             
        self.options_parser.add_option("-k", dest="keep_alive",
                    help="keep reservation alive ..",
                    action="store_true")
        self.options_parser.add_option("-j", dest="oar_job_id",
                    help="oar_job_id to relaunch an engine",
                    type=int)
        self.options_parser.add_option("-o", dest="outofchart",
                    help="Run the engine outside days",
                    action="store_true")
        self.options_parser.add_option("-w", dest="walltime",
                    help="walltime for the reservation",
                    type="string",
                    default="1:00:00")
                    
        self.hc = None

        # Configuration variables
        self.macro_manager = MacroManager()
        self.comb_id = 0
        self.ds_id = 0        
        
        self.stats_path = None
        self.remove_output = True
        self.output_path = None
        self.summary_file_name = "summary.csv"
        self.ds_summary_file_name = "ds-summary.csv"
        self.summary_file = None
        self.ds_summary_file = None
        
        self.hadoop_props = None
        

    def run(self):
        """Inherited method, put here the code for running the engine"""

        # Get parameters
        self.cluster = self.args[0]
        self.n_nodes = int(self.args[1])
        self.config_file = self.args[2]
        self.site = get_cluster_site(self.cluster)
        
        if not os.path.exists(self.config_file):
            logger.error("Params file " + self.params_file + " does not exist")
            sys.exit(1)
        
        # Set oar job id
        if self.options.oar_job_id:
            self.oar_job_id = self.options.oar_job_id
        else:
            self.oar_job_id = None

        # TODO: Provisional
        #(self.oar_job_id, self.frontend) = get_current_oar_jobs(frontends=["lyon"])[0]
        self.oar_job_id = get_current_oar_jobs()[0][0]

        # Main
        try:
            # Creation of the main iterator which is used for the first control loop.
            # You need have a method called define_parameters, that returns a list of parameter dicts
            self.define_parameters()
            
            job_is_dead = False
            # While they are combinations to treat
            while len(self.sweeper.get_remaining()) > 0:
                
                ## SETUP
                # If no job, we make a reservation and prepare the hosts for the experiments
                #if job_is_dead or self.oar_job_id is None:
                #    self.make_reservation()
                self.hosts = get_oar_job_nodes(self.oar_job_id, self.frontend)
                (deployed, undeployed) = self.deploy_nodes()
                if len(deployed) == 0:
                    break
                #else:
                #    self.hosts = get_oar_job_nodes(self.oar_job_id, self.frontend)
                if not self.hc:
                    self.hc = HadoopCluster(self.hosts)
                ## SETUP FINISHED

                # Getting the next combination (which requires a dataset deployment)
                comb = self.sweeper.get_next()
                self.raw_comb = comb.copy()
                self.comb = comb
                self.prepare_dataset(comb)
                self.xp(comb)
                                
                # subloop over the combinations that use the same dataset
                while True:
                    newcomb = self.sweeper.get_next(lambda r:
                            filter(self._uses_same_ds, r))
                    if newcomb:
                        self.raw_comb = newcomb.copy()
                        try:
                            self.xp(newcomb)
                        except:
                            break
                    else:
                        break

                if get_oar_job_info(self.oar_job_id, self.frontend)['state'] == 'Error':
                    job_is_dead = True

        finally:
            if self.oar_job_id is not None:
                if not self.options.keep_alive:
                    pass
                    #logger.info('Deleting job')
                    #oardel([(self.oar_job_id, self.frontend)])
                else:
                    logger.info('Keeping job alive for debugging')
                    
            # Clean cluster
            if self.hc:
                if self.hc.initialized:
                    self.hc.clean()
            
            # Close summary files
            if self.summary_file:
                self.summary_file.close()
            if self.ds_summary_file:
                self.ds_summary_file.close()
                    

    def _uses_same_ds(self, candidate_comb):
        """Determine if the candidate combination uses the same dataset as the
        current one.
        
        Args:
          candidate_comb (dict): The combination candidate to be selected as the
            new combination.
        """
                
        for var in self.ds_parameters.keys():
          if candidate_comb[var] != self.raw_comb[var]:
            return False
        return True
      

    def define_parameters(self):
        """Create the iterator that contains the parameters to be explored."""
        
        config = ConfigParser.ConfigParser()
        config.readfp(open(self.config_file))
                
        # TEST PARAMETERS
        if config.has_section("test_parameters"):
            test_parameters_names = config.options("test_parameters")        
            if "test.stats_path" in test_parameters_names:
                self.stats_path = config.get("test_parameters", "test.stats_path")
                if not os.path.exists(self.stats_path):
                    os.makedirs(self.stats_path)

            if "test.remove_output" in test_parameters_names:
                self.remove_output = bool(config.get("test_parameters", "test.remove_output"))

            if "test.output_path" in test_parameters_names:
                self.output_path = config.get("test_parameters", "test.output_path")            
                if not os.path.exists(self.output_path):
                    os.makedirs(self.output_path)
            
            if "test.summary_file" in test_parameters_names:
                self.summary_file_name = config.get("test_parameters", "test.summary_file")               
                
            if "test.ds_summary_file" in test_parameters_names:
                self.ds_summary_file_name = config.get("test_parameters", "test.ds_summary_file")
                
            if "test.hadoop.properties" in test_parameters_names:
                self.hadoop_props = config.get("test_parameters", "test.hadoop.properties")
                if not os.path.exists(self.hadoop_props):
                    logger.error("Hadoop properties file " + self.hadoop_props + " does not exist")
                    raise ParameterException("Hadoop properties file " + self.hadoop_props + " does not exist")
        
        # DATASET PARAMETERS
        ds_parameters_names = config.options("ds_parameters")        
        self.ds_parameters = {}
        ds_class_parameters = {}
        ds_classes = []
        for pn in ds_parameters_names:
            pv = config.get("ds_parameters", pn).split(",")
            if pn.startswith("ds.class."):
                ds_class_parameters[pn[len("ds.class."):]] = [v.strip() for v in pv]
            elif pn == "ds.class":
                ds_classes = [v.strip() for v in pv]
            else:
                self.ds_parameters[pn] = [v.strip() for v in pv]
                
        if not config.has_option("ds_parameters","ds.dest"):
            ds_class_parameters["dest"] = "${data_dir}"
        
        # Create ds configurations        
        self.ds_config = []
        for (idx, ds_class) in enumerate(ds_classes):
            this_ds_params = {}
            for pn, pv in ds_class_parameters.iteritems():
                if len(pv) == len(ds_classes):
                    if pv[idx]:
                        this_ds_params[pn] = pv[idx]                    
                elif len(pv) == 1:
                    this_ds_params[pn] = pv[0]
                else:
                    logger.error("Number of ds_class does not much number of " + pn)
                    raise ParameterException("Number of ds_class does not much number of " + pn)

            self.ds_config.append((ds_class, this_ds_params))
        
        self.ds_parameters["ds.config"] = range(0,len(self.ds_config))
        
        # EXPERIMENT PARAMETERS
        xp_parameters_names = config.options("xp_parameters")        
        self.xp_parameters = {}
        for pn in xp_parameters_names:
            pv = config.get("xp_parameters", pn).split(",")
            self.xp_parameters[pn] = [v.strip() for v in pv]           
                       
        # GLOBAL
        self.parameters = { }
        self.parameters.update(self.ds_parameters)
        self.parameters.update(self.xp_parameters)
        
        # MACROS
        self.macro_manager.add_ds_params(self.ds_parameters)
        self.macro_manager.add_xp_params(self.xp_parameters)
        for pn in self.parameters:
            for pv in self.parameters[pn]:
                macros = re.findall('\$\{([^}]*)\}',str(pv)) # TODO: filter out non-strings
                for m in macros:
                    self.macro_manager.add_dependency(m, pn)
        self.macro_manager.sort_macros()
        
        # SUMMARY FILES
        
        # Xp summary
        self.summary_file = open(self.summary_file_name, "w")        
        self.summary_props = []
        self.summary_props.extend(self.ds_parameters.keys())
        self.summary_props.extend(self.xp_parameters.keys())
        header = "comb_id, job_id" 
        for pn in self.summary_props:
            header += ", " + str(pn)
        self.summary_file.write(header + "\n")
        self.summary_file.flush()
        
        # Ds summary
        self.ds_summary_file = open(self.ds_summary_file_name, "w")        
        header = "ds_id, ds_class, ds_class_properties" 
        self.ds_summary_file.write(header + "\n")
        self.ds_summary_file.flush()        
        
        # PRINT PARAMETERS
        print_ds_parameters = {}
        print_ds_parameters.update(self.ds_parameters)
        print_ds_parameters["ds.config"] = self.ds_config
        logger.info("Dataset parameters: " + str(print_ds_parameters))
        logger.info("Experiment parameters: " + str(self.xp_parameters))
        
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), 
                                    sweep(self.parameters))

        logger.info('Number of parameters combinations %s', len(self.sweeper.get_remaining()))

    def _import_class(self, name):
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

    def __get_ds_parameters(self, params):
        ds_params = {}
        for pn in self.ds_parameters:
            ds_params[pn] = params[pn]
        ds_params["ds.config"] = self.ds_config[params["ds.config"]]            
        return ds_params
        
    def __get_xp_parameters(self, params):
        xp_params = {}
        for pn in self.xp_parameters:
            xp_params[pn] = params[pn]
        return xp_params
    
    
    def make_reservation(self):
        """Perform a reservation of the required number of nodes."""

        logger.info('Performing reservation')
        starttime = int(time.time() + timedelta_to_seconds(datetime.timedelta(minutes=1)))
        endtime = int(starttime + timedelta_to_seconds(datetime.timedelta(days=3,
                                                                 minutes=1)))
        startdate, n_nodes = self._get_nodes(starttime, endtime)
        while not n_nodes:
            logger.info('Not enough nodes found between %s and %s, ' + \
                        'increasing time window',
                        format_date(starttime), format_date(endtime))
            starttime = endtime
            endtime = int(starttime + timedelta_to_seconds(datetime.timedelta(days=3,
                                                                minutes=1)))
            startdate, n_nodes = self._get_nodes(starttime, endtime)
            if starttime > int(time.time() + timedelta_to_seconds(
                                            datetime.timedelta(weeks=6))):
                logger.error('There are not enough nodes on %s for your ' + \
                             'experiments, abort ...', self.cluster)
                exit()
        jobs_specs = get_jobs_specs({self.cluster: n_nodes},
                                    name=self.__class__.__name__)
        sub = jobs_specs[0][0]
        sub.walltime = self.options.walltime
        sub.additional_options = '-t deploy'
        sub.reservation_date = startdate
        (self.oar_job_id, self.frontend) = oarsub(jobs_specs)[0]
        logger.info('Startdate: %s, n_nodes: %s', format_date(startdate),
                    str(n_nodes))
                    
    def _get_nodes(self, starttime, endtime):
        
        planning = get_planning(elements = [self.cluster],
                                starttime = starttime,
                                endtime = endtime,
                                out_of_chart = self.options.outofchart)
        slots = compute_slots(planning, self.options.walltime)
        startdate = slots[0][0]
        i_slot = 0
        n_nodes = slots[i_slot][2][self.cluster]
        while n_nodes < self.n_nodes:
            logger.debug(slots[i_slot])
            startdate = slots[i_slot][0]
            n_nodes = slots[i_slot][2][self.cluster]
            i_slot += 1
            if i_slot == len(slots) - 1:
                return False, False
        return startdate, self.n_nodes
    
    
    def deploy_nodes(self, min_deployed_hosts = 1, max_tries = 3):
        """Deploy nodes in the cluster. If the number of deployed nodes is less
        thatn the specified min, try again.
        
        Args:
          min_deployed_hosts (int, optional): minimum number of nodes to be
            deployed (defualt: 1).
          max_tries (int, optional): maximum number of tries to reach the
            minimum number of nodes (default: 3).
        """
        
        
        logger.info("Deploying " + str(len(self.hosts)) + " nodes")
        
        def correct_deployment(deployed, undeployed):
            return len(deployed) >= min_deployed_hosts
        
        (deployed, undeployed) = deploy(
            Deployment(self.hosts, 
            env_file = "/home/mliroz/deploys/hadoop6.env"),
            num_tries = max_tries,
            check_enough_func = correct_deployment,
            out=True
        )
            
        logger.info("%i deployed, %i undeployed" % (len(deployed), 
                                                    len(undeployed)))            
            
        if not correct_deployment(deployed, undeployed):
            logger.error("It was not possible to deploy min number of hosts")                             
                                        
        return (deployed, undeployed)
        
        
    def prepare_dataset(self, comb):
        """Prepare the dataset to be used in the next set of experiments.
        
        Args:
          comb (dict): The combination containing the dataset's parameters.        
        """
        
        logger.info("Prepare dataset with combination " + str(self.__get_ds_parameters(comb)))
        self.ds_id += 1
        self.macro_manager.update_test_macros(ds_id = self.ds_id)
        self.macro_manager.replace_ds_macros(comb)        
        logger.info("Combination after macro replacement " + str(self.__get_ds_parameters(comb)))
        
        # Initialize cluster and start
        self.hc.initialize()
        self.hc.start_and_wait()
        
        # Populate dataset
        self.deploy_ds(comb)
        
      
    def deploy_ds(self, comb):
        """Deploy the dataset corresponding to the given combination.
        
        Args:
          comb (dict): The combination containing the dataset's parameters.
        """
        
        # Create dataset
        ds_idx = comb["ds.config"]
        (ds_class_name, ds_params) = self.ds_config[ds_idx]
        ds_class = self._import_class(ds_class_name)
        self.ds = ds_class(ds_params)   
        
        # Deploy dataset
        
        # Temporarily hard coded ----------------------------------------------
        def uncompress_function(file_name, host):
            #action = Remote("gzip -d " + file_name, [ host ])
            action = Remote("bzip2 -d " + file_name, [ host ])
            action.run()
            
            base_name = os.path.basename(file_name[:-4])
            dir_name = os.path.dirname(file_name[:-4])
            
            new_name = dir_name + "/data-" + base_name
            
            action = Remote("mv " + file_name[:-4] + " " + new_name, [ host ])
            action.run()
        
            #return file_name[:-3] # Remove .gz (provisional)        
            #return file_name[:-4] # Remove .bz2 (provisional)
            return new_name
        # ---------------------------------------------------------------------
        
        self.ds.deploy(self.hc, comb["ds.dest"], int(comb["ds.size"]), uncompress_function)
        self._update_ds_summary(comb)
         
    def _update_ds_summary(self, comb):
        """Update ds summary with the deployed ds"""
        
        ds_idx = comb["ds.config"]
        (ds_class_name, ds_params) = self.ds_config[ds_idx]        
        
        line = str(self.ds_id - 1) + "," + ds_class_name + "," + str(ds_params)
        self.ds_summary_file.write(line + "\n")
        self.ds_summary_file.flush()            
      
    def xp(self, comb):
        """Perform the experiment corresponding to the given combination.
        
        Args:
          comb (dict): The combination with the experiment's parameters.
        """
        
        comb_ok = False
        try:
            logger.info("Execute experiment with combination " + str(self.__get_xp_parameters(comb)))
            self.comb_id += 1
            self.macro_manager.update_test_macros(comb_id = self.comb_id)
            self.macro_manager.replace_xp_macros(comb)            
            logger.info("Combination after macro replacement " + str(self.__get_xp_parameters(comb)))
            
            # Prepare
            self._change_hadoop_conf(comb)
            job = self._create_hadoop_job(comb)
            
            # Execute job
            self.hc.execute_jar(job)
            self._update_summary(comb, job)            
                        
            # Post-execution
            self._copy_xp_output()             
            self._remove_xp_output()
            self._copy_xp_stats()

            comb_ok = True
            
        finally:
            if comb_ok:
                self.sweeper.done(comb)
            else:
                self.sweeper.cancel(comb)
            logger.info('%s Remaining',len(self.sweeper.get_remaining()))        


    def _change_hadoop_conf(self, comb):
        """Change hadoop's configuration by using the experiment's parameters.
        
        Args:
          comb (dict): The combination with the experiment's parameters.
        """
        
        self.hc.stop() # Some parameters only take effect after restart
        mr_params = {}
        for pn in self.__get_xp_parameters(comb):
            if not pn.startswith("xp."):
                mr_params[pn] = comb[pn]

        self.hc.change_conf(mr_params)
        self.hc.start_and_wait()
        
        # TODO: provisional hack to avoid safemode
        time.sleep(10)
        
    def _create_hadoop_job(self, comb):
        """Create the hadoop job.
        
        Args:
          comb (dict): The combination with the experiment's parameters.
        """
        
        xp_job_parts = comb["xp.job"].split("||") # TODO
        jar_path = xp_job_parts[0].strip()
        if len(xp_job_parts) > 1:
            params = xp_job_parts[1].strip()
            if len(xp_job_parts) == 3:
                lib_jars = xp_job_parts[2].strip()
            else:
                lib_jars = None
        else:
            params = None
            lib_jars = None
            
        return HadoopJarJob(jar_path, params, lib_jars)
    
    def _update_summary(self, comb, job):
        """Update test summary with the executed job"""
        
        line = str(self.comb_id) + ", " + job.job_id
        for pn in self.summary_props:
            line += ", " + str(comb[pn])
        self.summary_file.write(line + "\n")
        self.summary_file.flush()        
        
    def _copy_xp_output(self):
        """Copy experiment's output"""
        
        if self.output_path:
            remote_path = self.macro_manager.test_macros["xp.output"] # TODO: what happens if not specified?
            local_path = os.path.join(self.output_path,str(self.comb_id))
            logger.info("Copying output to " + local_path)

            tmp_dir = "/tmp"
            
            # Remove file in tmp dir if exists
            proc = SshProcess("rm -rf " + os.path.join(tmp_dir, os.path.basename(remote_path)), self.hc.master)
            proc.run()

            # Get files in master
            self.hc.execute("fs -get " + remote_path + " " + tmp_dir, verbose = False)

            # Copy files from master
            action = Get([self.hc.master], [ os.path.join(tmp_dir,os.path.basename(remote_path)) ], local_path)
            action.run()            
    
    def _remove_xp_output(self):
        """Remove experiment's output."""
        
        if self.remove_output:
            logger.info("Remove output")
            self.hc.execute("fs -rmr " + self.macro_manager.test_macros["xp.output"], verbose = False) # TODO: what happens if not specified?
    
    def _copy_xp_stats(self):
        """Copy job stats and clean them in the cluster."""
        
        if self.stats_path:
            local_path = os.path.join(self.stats_path,str(self.comb_id))
            logger.info("Copying stats to " + local_path)
            self.hc.stop()
            self.hc.copy_history(local_path)
            self.hc.clean_history()

