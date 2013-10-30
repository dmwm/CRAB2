from crab_exceptions import *
from crab_util import *
import common
import os, time, shutil
import traceback

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import DbError
from ProdCommon.BossLite.Common.Exceptions import TaskError

from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob


class DBinterface:
    def __init__(self, cfg_params):

        self.cfg_params = cfg_params

        self.db_type =  cfg_params.get("USER.use_db",'SQLite')
        return


    def configureDB(self):

        dbname = common.work_space.shareDir()+'crabDB'
        dbConfig = {'dbName':dbname
            }
        try: 
            common.bossSession = BossLiteAPI( self.db_type, dbConfig)
        except Exception, e :
            raise CrabException('Istantiate DB Session : '+str(e))

        try:
            common.bossSession.bossLiteDB.installDB('$CRABPRODCOMMONPYTHON/ProdCommon/BossLite/DbObjects/setupDatabase-sqlite.sql') 
        except Exception, e :
            raise CrabException('DB Installation error : '+str(e))
        return 
 
    def loadDB(self):

        dbname = common.work_space.shareDir()+'crabDB'
        dbConfig = {'dbName':dbname
            }
        try:
            common.bossSession = BossLiteAPI( self.db_type, dbConfig)
        except Exception, e :
            raise CrabException('Istantiate DB Session : '+str(e))

        return
 
    def getTask(self, jobsList='all'): 
        """
        Return task with all/list of jobs 
        """
        try:
            task = common.bossSession.load(1,jobsList)
        except Exception, e :
            common.logger.debug( "Error while getting task : " +str(traceback.format_exc()))
            raise CrabException('Error while getting task '+str(e))
        return task

    def getJob(self, n): 
        """
        Return a task with a single job 
        """ 
        try:
            task = common.bossSession.load(1,str(n))
        except Exception, e :
            common.logger.debug( "Error while getting job : " +str(traceback.format_exc()))
            raise CrabException('Error while getting job '+str(e))
        return task


    def createTask_(self, optsToSave):       
        """
        Task declaration
        with the first coniguration stuff 
        """
        opt={}
        if optsToSave.get('server_mode',0) == 1: opt['serverName']=optsToSave['server_name'] 
        opt['name']= getUserName()+ '_' + string.split(common.work_space.topDir(),'/')[-2]+'_'+common.work_space.task_uuid()
        task = Task( opt )
        try:
            common.bossSession.saveTask( task )
        except Exception, e :
            raise CrabException('Error creating task '+str(traceback.format_exc()))
            
        return 

    def updateTask_(self,optsToSave):       
        """
        Update task fields   
        """
        task = self.getTask()
   
        for key in optsToSave.keys():
            task[key] = optsToSave[key]
        try:
            common.bossSession.updateDB( task )
        except Exception, e :
            raise CrabException('Error updating task '+str(traceback.format_exc()))

        return 

    def createJobs_(self, jobsL, isNew=True):
        """  
        Fill crab DB with  the jobs filed 
        """
        task = self.getTask()

        jobs = [] 
        for id in jobsL:
            parameters = {}
            parameters['jobId'] = int(id)
            parameters['taskId'] = 1
            parameters['name'] = task['name'] + '_' + 'job' + str(id)
            job = Job(parameters)
            jobs.append(job) 
            common.bossSession.getRunningInstance(job)
            job.runningJob['status'] = 'C'
        ## added to support second step creation
        ## maybe it is not needed. TO CLARIFY
        if isNew:
            task.addJobs(jobs)
        else:
            task.appendJobs(jobs)
        try:
            common.bossSession.updateDB( task )
        except Exception, e :
            raise CrabException('Error updating task '+str(traceback.format_exc()))

        return

    def updateJob_(self, jobsL, optsToSave):       
        """
        Update Job fields   
        """
        task = self.getTask(jobsL)
        id =0 
        for job in task.jobs:
            for key in optsToSave[id].keys():
                job[key] = optsToSave[id][key]
            id+=1
        try:
            common.bossSession.updateDB( task )
        except Exception, e :
            raise CrabException('Error updating task '+str(traceback.format_exc()))
        return 

    def updateRunJob_(self, jobsL, optsToSave):       
        """
        Update Running Job fields   
        """
        task = self.getTask(jobsL)

        id=0
        for job in task.jobs:
            common.bossSession.getRunningInstance(job)
            for key in optsToSave[id].keys():
                job.runningJob[key] = optsToSave[id][key]
            id+=1
        common.bossSession.updateDB( task )
        return 

    def nJobs(self,list=''):
        
        task = self.getTask()
        listId=[]
        if list == 'list':
            for job in task.jobs:listId.append(int(job['jobId']))  
            return listId
        else:
            return len(task.jobs) 

    def dump(self,jobs):
        """
         List a complete set of infos for a job/range of jobs   
        """
        task = self.getTask(jobs)

        print "--------------------------"
        for Job in task.jobs:
            print "Id: ",Job['jobId']
            print "Dest: ", Job['dlsDestination']
            print "Output: ", Job['outputFiles']
            print "Args: ",Job['arguments']
            print "Service: ",Job.runningJob['service']
            print "--------------------------"
        return      

    def serializeTask(self, tmp_task = None):
        if tmp_task is None:
            tmp_task = self.getTask()
        return common.bossSession.serialize(tmp_task)   
 
    def queryID(self,server_mode=0, jid=False):
        '''
        Return the taskId if serevr_mode =1 
        Return the joblistId if serevr_mode =0 
        '''     
        header=''
        lines=[]
        task = self.getTask()
        if server_mode == 1:
            # init client server params...
            CliServerParams(self)       
            headerTask = "Task Id = %-40s\n" %(task['name'])
            headerTask+=  '--------------------------------------------------------------------------------------------\n'
            displayReport(self,headerTask,lines)
            common.logger.info(showWebMon(self.server_name))
        if (jid ) or (server_mode == 0):
            for job in task.jobs: 
                toPrint=''
                common.bossSession.getRunningInstance(job)
                toPrint = "%-5s %-50s " % (job['jobId'],job.runningJob['schedulerId'])
                lines.append(toPrint)
            header+= "%-5s %-50s\n " % ('Job:','ID' ) 
            header+=  '--------------------------------------------------------------------------------------------\n'
            displayReport(self,header,lines)
        return   

    def queryTask(self,attr):
        '''
        Perform a query over a generic task attribute
        '''
        task = self.getTask()
        return task[attr]

    def queryJob(self, attr, jobsL):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        task = self.getTask(jobsL)
        for job in task.jobs:
            lines.append(job[attr])
        return lines

    def queryRunJob(self, attr, jobsL):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        task = self.getTask(jobsL)
        for job in task.jobs:
            common.bossSession.getRunningInstance(job)
            lines.append(job.runningJob[attr])
        return lines

    def queryDistJob(self, attr):
        '''
        Returns the list of distinct value for a given job attributes 
        '''
        distAttr=[]
        try:
            task = common.bossSession.loadJobDist( 1, attr ) 
        except Exception, e :
            common.logger.debug( "Error loading Jobs By distinct Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By distinct Attr '+str(e))

        for i in task: distAttr.append(i[attr])   
        return  distAttr

    def queryDistJob_Attr(self, attr_1, attr_2, list):
        '''
        Returns the list of distinct value for a given job attribute 
        '''
        distAttr=[]
        try:
            task = common.bossSession.loadJobDistAttr( 1, attr_1, attr_2, list ) 
        except Exception, e :
            common.logger.debug( "Error loading Jobs By distinct Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By distinct Attr '+str(e))

        for i in task: distAttr.append(i[attr_1])  
        return  distAttr

    def queryAttrJob(self, attr, field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        try:
            task = common.bossSession.loadJobsByAttr(attr ) 
        except Exception, e :
            common.logger.debug( "Error loading Jobs By Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By Attr '+str(e))
        for i in task:
            matched.append(i[field])
        return  matched


    def queryAttrRunJob(self, attr,field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        try:
            task = common.bossSession.loadJobsByRunningAttr(attr)
        except Exception, e :
            common.logger.debug( "Error loading Jobs By Running Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By Running Attr '+str(e))
        for i in task:
            matched.append(i.runningJob[field])
        return matched 

    def newRunJobs(self,nj='all'):
        """
        Get new running instances
        """  
        task = self.getTask(nj)

        for job in task.jobs:
            common.bossSession.getNewRunningInstance(job)
            job.runningJob['status'] = 'C'
            job.runningJob['statusScheduler'] = 'Created'
            job.runningJob['state'] = 'Created'
        common.bossSession.updateDB(task)     
        return        

    def deserXmlStatus(self, reportList):

        task = self.getTask()
        if int(self.cfg_params.get('WMBS.automation',0)) == 1:
            if len(reportList) ==0:
                msg = 'You are using CRAB with WMBS the server is still creating your jobs.\n'
                msg += '\tPlease wait...'
                raise CrabException(msg)
            newJobs =  len(reportList) - len(task.jobs)
            if newJobs != 0:
                isNew=True  
                if len(task.jobs):isNew=False
                jobL=[]   
                for i in range(1,newJobs+1):
                    jobL.append(len(task.jobs)+i) 
                self.createJobs_(jobL,isNew)

        for job in task.jobs:
            if not job.runningJob:
                raise CrabException( "Missing running object for job %s"%str(job['jobId']) )

            id = str(job.runningJob['jobId'])
            rForJ = None
            nj_list= []
            for r in reportList:
                if r.getAttribute('id') in [ id, 'all']:
                    rForJ = r
                    break

            # check if rForJ is None
            if rForJ is None:
                common.logger.debug( "Missing XML element for job %s, skip update status"%str(id) )
                continue
             
            ## Check the submission number and create new running jobs on the client side
            if rForJ.getAttribute('resubmit') != 'None' and (rForJ.getAttribute('status') not in ['Cleared','Killed','Done','Done (Failed)','Not Submitted', 'Cancelled by user']) :
                if int(job.runningJob['submission']) < int(rForJ.getAttribute('resubmit')) + 1:
                    nj_list.append(id)
            if len(nj_list) > 0: self.newRunJobs(nj_list)

        task_new = self.getTask()

        for job in task_new.jobs:
            id = str(job.runningJob['jobId'])
            # TODO linear search, probably it can be optized with binary search
            rForJ = None
            for r in reportList:
                if r.getAttribute('id') in [ id, 'all']:
                    rForJ = r
                    break
                   
            # Data alignment
            if rForJ.getAttribute('status') not in ['Unknown']: # ['Created', 'Unknown']: 
                   # update the status  
                common.logger.debug("Updating DB status for job: " + str(id) + " @: " \
                                      + str(rForJ.getAttribute('status')) )
                job.runningJob['statusScheduler'] = str( rForJ.getAttribute('status') )
                if (rForJ.getAttribute('status') == 'Done' or rForJ.getAttribute('status') == 'Done (Failed)')\
                  and rForJ.getAttribute('sched_status') == 'E' :
                    job.runningJob['status'] = 'SD'
                else: 
                    job.runningJob['status'] = str( rForJ.getAttribute('sched_status') )
          
                job.runningJob['schedulerId'] = str( rForJ.getAttribute('sched_id') )
 
                job.runningJob['destination'] = str( rForJ.getAttribute('site') )
                dest = str(job.runningJob['destination']).split(':')[0]
              
                job.runningJob['applicationReturnCode'] = str( rForJ.getAttribute('exe_exit') )
                exe_exit_code = str(job.runningJob['applicationReturnCode'])
              
                job.runningJob['wrapperReturnCode'] = str( rForJ.getAttribute('job_exit') )
                job_exit_code = str(job.runningJob['wrapperReturnCode'])

                job['closed'] = str( rForJ.getAttribute('ended') )

                job.runningJob['state'] = str( rForJ.getAttribute('action') )
          
                # Needed for unique naming of the output.
                job['arguments'] = "%d %s"%(job.runningJob['jobId'], str(rForJ.getAttribute('submission')).strip() )
          
        common.bossSession.updateDB( task_new )
        return

    # FIXME temporary method to verify what kind of submission to perform towards the server
    def checkIfNeverSubmittedBefore(self):
        for j in self.getTask().jobs:
            if j.runningJob['submission'] > 1 or j.runningJob['state'] != 'Created':
                return False
        return True

    # Method to update arguments w.r.t. resubmission number in order to grant unique output
    def updateResubAttribs(self, jobsL):
        task = self.getTask(jobsL)
        for j in task.jobs:
            common.bossSession.getRunningInstance(j)
            try:
                resubNum = int(str(j['arguments']).split(' ')[1]) + 1 
            except Exception, e:
                resubNum = j.runningJob['submission']
            newArgs = "%d %d"%(j.runningJob['jobId'], resubNum)
            j['arguments'] = newArgs

        common.bossSession.updateDB(task)
        return

