from Creator import Creator
from WorkSpace import WorkSpace
from ScriptWriter import ScriptWriter
from Scheduler import Scheduler
from crab_exceptions import *
from crab_util import *
import common

import os, string, math
import time

class Requestor( Creator ):
    def __init__(self, job_type_name, cfg_params, ncjobs, skip_blocks=False,isNew=True,first_jobID=0):

        Creator.__init__(self,job_type_name, cfg_params, ncjobs, skip_blocks,isNew,first_jobID)
        common.logger.debug( "Requestor constructor finished")
        return

    def run(self):
        """
        The main method of the class.
        """
     
        common.logger.debug( "Requestor::run() called")
        common.logger.info('Creating request for server, please wait...')
        start = time.time()
        # Instantiate ScriptWriter
        script_writer = ScriptWriter(self.cfg_params,'crab_template.sh')
     
        # Create script (sh)
        script_writer.modifyTemplateScript()
       
        # SL This should be a list, rather than a string!
        concString = ','
        inSand=''
        if len(self.job_type.inputSandbox(1)): 
            inSand +=   concString.join(self.job_type.inputSandbox(1)) 
        # Sandbox, Start Dir , outputDir
        param = {'globalSandbox': inSand , 'startDirectory': common.work_space.cwdDir() , 'outputDirectory': common.work_space.resDir() }
        common._db.updateTask_(param) 
        # define requirement
        common.scheduler.sched_fix_parameter()
       
        stop = time.time()

        common.logger.info('Request created.')
        common.logger.debug( "Request Creation Time: "+str(stop - start))

        msg = ''
        common.logger.info(msg)
        return
