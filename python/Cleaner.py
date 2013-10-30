from Actor import *
from crab_exceptions import *
from Status import Status
import common
import string
from crab_util import *

class Cleaner(Actor):
    def __init__(self, cfg_params):
        """
        constructor
        """
        self.status = Status(cfg_params)

    def check(self):
        """
        Check whether no job is still running or not yet retrieved
        """

        task = common._db.getTask()
        upTask = common.scheduler.queryEverything(task['id'])
        self.status.compute(upTask) # compute the status

        jobTerm=[]
        jobSub=[]
        for job  in task.jobs:
            st=job.runningJob['state']
            if st not in ['KillSuccess', 'SubFailed', 'Created', 'Aborted', 'Cleared', 'Cleaned']:
                if st in ['Terminated']: jobTerm.append(job['jobId'])
                if st in ['SubSuccess']: jobSub.append(job['jobId'])
            pass

        if len(jobTerm)>0 or len(jobSub)>0:
            msg = "There are still "
            if len(jobSub)>0:
                msg= msg+" jobs submitted. Kill them '-kill %s' before '-clean'"%readableList(self,jobSub)
            if (len(jobTerm)>0 or len(jobSub)>0):
                msg = msg + "and \nalso"
            if len(jobTerm)>0:
                msg= msg+" jobs Done. Get their outputs '-get %s' before '-clean'"%readableList(self,jobTerm)
            raise CrabException(msg)
        pass

    def run(self):
        """
        remove all
        """
        if common._db.nJobs()>0:
            self.check()

        # here I should first purge boss DB if central
        #common.scheduler.clean()
        common.work_space.delete()
        print 'directory '+common.work_space.topDir()+' removed'
