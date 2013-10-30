from Actor import *
from crab_util import *
import common
import string, os

class PostMortem(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        self.all_jobs=common._db.nJobs('list')

        self.fname_base = common.work_space.jobDir() + self.cfg_params['CRAB.jobtype'].upper() + '_' 

        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug( "PostMortem::run() called")

        self.collectLogging()

    def collectOneLogging(self, id):
        job=self.up_task.getJob(id)
        if not job: #id not in self.all_jobs:
            common.logger.info('Warning: job # ' + str(id) + ' does not exist! Not possible to ask for postMortem ')
            return
        elif job.runningJob['state'] == 'Created':
            common.logger.info('Warning: job # ' + str(id) + ' just Created ! Not possible to ask for postMortem ')
        else:  
            fname = self.fname_base + str(id) + '.LoggingInfo'
            if os.path.exists(fname):
                common.logger.info('Logging info for job ' + str(id) + ' already present in '+fname+'\nRemove it for update')
                return
            common.scheduler.loggingInfo(id,fname)
            fl = open(fname, 'r')
            out = "".join(fl.readlines())  
            fl.close()
            reason = self.decodeLogging(out)
            common.logger.info('Logging info for job  '+ str(id) +'  written to '+str(fname))
            common.logger.info('Reason for job status is:\n\n'+str(reason)+'\n')
        return
        

    def collectLogging(self):
        self.up_task = common._db.getTask( self.nj_list )
        for id in self.nj_list:
            self.collectOneLogging(id)
        return
        
    def decodeLogging(self, out):
        """
        """
        return  common.scheduler.decodeLogInfo(out)

