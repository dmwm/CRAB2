from Submitter import Submitter
import common
from crab_util import *
from crab_exceptions import *
from PhEDExDatasvcInfo import PhEDExDatasvcInfo


class Resubmitter(Submitter):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params
              
        nj_list = []

        self.copy_data = int(cfg_params.get('USER.copy_data',0))
        self.check_RemoteDir =  int(cfg_params.get('USER.check_user_remote_dir',0))
        if (jobs=='bad'):
            nj_list = self.checkBadJob(nj_list)
        else:
            nj_list = self.checkAllowedJob(jobs,nj_list)
        common.logger.info('Jobs '+str(nj_list)+' will be resubmitted')
        Submitter.__init__(self, cfg_params, nj_list, 'range')

        return

    def checkRemoteDir(self,task):

        if self.copy_data==1: 
            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            common.scheduler.checkRemoteDir(endpoint,eval(task['outfileBasename']))


    def checkAllowedJob(self,jobs,nj_list):
        listRunField=[]
        task=common._db.getTask(jobs)
        if self.check_RemoteDir == 1 : self.checkRemoteDir(task)
        for job in task.jobs:
            st = job.runningJob['state']
            nj = int(job['jobId'])
            if st in ['KillSuccess','SubFailed','Cleared','Aborted']:
                #['K','A','SE','E','DA','NS']:
                nj_list.append(nj)
            elif st == 'Created':
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' not yet submitted: use -submit')
            elif st in ['Terminated']:
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' must be retrieved (-get) before resubmission')
            else:
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' actual status is '\
                        +str(job.runningJob['statusScheduler'])+' must be killed (-kill) before resubmission')
                if (job.runningJob['state']=='KillRequested'): common.logger.info('\t\tthe previous Kill request is being processed')


        if len(nj_list) == 0 :
            msg='No jobs to resubmit'
            raise CrabException(msg)

        common._db.updateJob_(nj_list, [{'closed':'N'}]*len(nj_list))
        # Get new running instances
        common._db.newRunJobs(nj_list)

        return nj_list

    def checkBadJob(self,nj_list):
        listRunField=[]
        jobs = common._db.nJobs('list')
        task=common._db.getTask(jobs)
        if self.check_RemoteDir == 1 : self.checkRemoteDir(task)
        for job in task.jobs:
            st = job.runningJob['state']
            nj = int(job['jobId'])
            if st in ['KillSuccess','SubFailed','Aborted','Cancelled']:
                    nj_list.append(nj)
            elif st in ['Cleared']:
                if (job.runningJob['applicationReturnCode']!=0 or (job.runningJob['wrapperReturnCode']!=0 and job.runningJob['wrapperReturnCode']!=60308 ) ) :
                    nj_list.append(nj)
            elif st == 'Created':
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' not yet submitted: use -submit')
            elif st in ['Terminated']:
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' must be retrieved (-get) before resubmission')
            else:
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' actual status is '\
                        +str(job.runningJob['statusScheduler'])+' must be killed (-kill) before resubmission')
                if (job.runningJob['state']=='KillRequested'): common.logger.info('\t\tthe previous Kill request is being processed')


        if len(nj_list) == 0 :
            msg='No jobs to resubmit'
            raise CrabException(msg)

        common._db.updateJob_(nj_list, [{'closed':'N'}]*len(nj_list))
        # Get new running instances
        common._db.newRunJobs(nj_list)

        return nj_list
