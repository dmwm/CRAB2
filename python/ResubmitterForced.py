from Submitter import Submitter
import common
from crab_util import *
from crab_exceptions import *

class ResubmitterForced(Submitter):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params

        nj_list = []

        nj_list = self.checkAllowedJob(jobs,nj_list)

        common.logger.info('Jobs '+str(nj_list)+' will be resubmitted')
        Submitter.__init__(self, cfg_params, nj_list, 'range')

        return

    def checkAllowedJob(self,jobs,nj_list):
        listRunField=[]

        task=common._db.getTask(jobs)
        for job in task.jobs:
            nj = int(job['jobId'])
            nj_list.append(nj)

        if len(nj_list) == 0 :
            msg='No jobs to resubmit'
            raise CrabException(msg)

        common._db.updateJob_(nj_list, [{'closed':'N'}]*len(nj_list))
        # Get new running instances
        common._db.newRunJobs(nj_list)

        return nj_list

