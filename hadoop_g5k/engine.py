#!/usr/bin/env python

import os
import time
import datetime
from hadoop_g5k import HadoopCluster
import hadoop_g5k
from execo.time_utils import timedelta_to_seconds, format_date
from execo_g5k import get_cluster_site, get_oar_job_info, oardel, oarsub, \
    get_planning, compute_slots, get_jobs_specs, get_oar_job_nodes, \
    deploy, Deployment, get_host_attributes
from execo_engine import Engine, logger, sweep, ParamSweeper

class HadoopEngine(Engine):

    def __init__(self):
        super(HadoopEngine, self).__init__()
        
        self.options_parser.set_usage("usage: %prog <cluster> <n_nodes>")
        self.options_parser.add_argument("cluster",
                    "The cluster on which to run the experiment")
        self.options_parser.add_argument("n_nodes",
                    "The number of nodes in which the experiment is going to be deployed")                    
        self.options_parser.add_option("-k", dest="keep_alive",
                    help="keep reservation alive ..",
                    action="store_true")
        self.options_parser.add_option("-j", dest="oar_job_id",
                    help="oar_job_id to relaunch an engine",
                    type=int)
        self.options_parser.add_option("-o", dest="outofchart",
                    help="Run the engine outside days",
                    action="store_true")
        #self.n_nodes = 2
        self.options_parser.add_option("-w", dest="walltime",
                    help="walltime for the reservation",
                    type="string",
                    default="1:00:00")

    def prepare_dataset(self, comb):
        logger.info("Prepare dataset with combination " + str(comb))
        
      
    def xp(self, comb):
        comb_ok = False
        try:
            logger.info("Execute experiment with combination " + str(comb))

            # TODO - the experiment

            comb_ok = True
        finally:
            if comb_ok:
                self.sweeper.done(comb)
            else:
                self.sweeper.cancel(comb)
            logger.info('%s Remaining',len(self.sweeper.get_remaining()))

    def run(self):
        """Inherited method, put here the code for running the engine"""

        self.cluster = self.args[0]
        self.n_nodes = int(self.args[1])
        self.site = get_cluster_site(self.cluster)
        if self.options.oar_job_id:
            self.oar_job_id = self.options.oar_job_id
        else:
            self.oar_job_id = None

        try:
            # Creation of the main iterator which is used for the first control loop.
            # You need have a method called define_parameters, that returns a list of parameter dicts
            self.define_parameters() # TODO - repeated

            job_is_dead = False
            # While they are combinations to treat
            while len(self.sweeper.get_remaining()) > 0:
                # If no job, we make a reservation and prepare the hosts for the experiments
                if job_is_dead or self.oar_job_id is None:
                    self.make_reservation()
                # Retrieving the hosts and subnets parameters
                self.hosts = get_oar_job_nodes(self.oar_job_id, self.frontend)
                # Hosts deployment
                '''print "Just before deploying"
                deployed, undeployed = deploy(Deployment(self.hosts, 
                    env_file="/home/mliroz/deploys/hadoop6.env"))
                logger.info("%i deployed, %i undeployed" % (len(deployed), 
                                                            len(undeployed)))
                if len(deployed) == 0:
                    break'''

                # Configuration du systeme => look at the execo_g5k.topology module 
                attr = get_host_attributes(self.cluster + '-1')
                
                ## SETUP FINISHED
                
                # Getting the next combination
                comb = self.sweeper.get_next()
                self.comb = comb
                self.prepare_dataset(comb)
                self.xp(comb)
                # subloop over the combinations that have the same size
                while True:
                    #newcomb = self.sweeper.get_next(lambda r:
                    #        filter(lambda subcomb: subcomb['size'] == comb['size'], r))
                    newcomb = self.sweeper.get_next(lambda r:
                            filter(self.uses_same_combination, r))
                    if newcomb:
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
                    logger.info('Deleting job')
                    oardel([(self.oar_job_id, self.frontend)])
                else:
                    logger.info('Keeping job alive for debugging') 

    def uses_same_combination(self, newcomb):
        pop_vars = [ "size", "zipf", "pop_keys" ] 
        for var in pop_vars:
          if newcomb[var] != self.comb[var]:
            return False
        return True
      

    def define_parameters(self):
        """Create the iterator that contains the parameters to be explored"""
        
        self.parameters = {
            'size': [2, 4],
            'zipf': [1, 2],
            'pop_keys': [10, 100],
            'min_size': [500, 1000],
            'int_phases': [1, 5, 10],
            'iosf': [100]
        }
        logger.info(self.parameters)
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), 
                                    sweep(self.parameters))
        logger.info('Number of parameters combinations %s', len(self.sweeper.get_remaining()))

    def _get_nodes(self, starttime, endtime):
        
        planning = get_planning(elements=[self.cluster],
                                starttime=starttime,
                                endtime=endtime,
                                out_of_chart=self.options.outofchart)
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

    def make_reservation(self):
        """Perform a reservation of the required number of nodes"""

        logger.info('Performing reservation')
        starttime = int(time.time() + timedelta_to_seconds(datetime.timedelta(minutes=1)))
        endtime = int(starttime + timedelta_to_seconds(datetime.timedelta(days=3,
                                                                 minutes=1)))
        startdate, n_nodes = self._get_nodes(starttime, endtime)
        while not n_nodes:
            logger.info('No enough nodes found between %s and %s, ' + \
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
        #sub.additional_options = '-t deploy'
        sub.reservation_date = startdate
        (self.oar_job_id, self.frontend) = oarsub(jobs_specs)[0]
        logger.info('Startdate: %s, n_nodes: %s', format_date(startdate),
                    str(n_nodes))

if __name__ == "__main__":
    engine = HadoopEngine()    
    engine.start()