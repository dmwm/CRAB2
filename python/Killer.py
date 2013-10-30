import os, common, string
from Actor import *
from crab_util import *

class Killer(Actor):
    def __init__(self, cfg_params, range):
        self.cfg_params = cfg_params
        self.range = range
        return

    def run(self):
        """
        The main method of the class: kill a complete task
        """
        common.logger.debug( "Killer::run() called")
        task = common._db.getTask(self.range)
        toBeKilled = []
        for job  in task.jobs:
           if job.runningJob['state'] in ['SubSuccess','SubRequested']:
               toBeKilled.append(job['jobId'])
           else:
               common.logger.info("Not possible to kill Job #"+str(job['jobId'])+\
                       " : Last action was: "+str(job.runningJob['state'])+\
                       " Status is "+str(job.runningJob['statusScheduler']))
           pass

        if len(toBeKilled)>0:
            common.scheduler.cancel(toBeKilled)
            common._db.updateRunJob_(toBeKilled, [{'state':'KillSuccess'}]*len(toBeKilled))
            common._db.updateJob_(toBeKilled, [{'closed':'Y'}]*len(toBeKilled))
            common.logger.info("Jobs killed "+str(toBeKilled))
 
