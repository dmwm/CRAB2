from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
#from random import random
import time
import socket
import Scram
from ProgressBar import ProgressBar
from TerminalController import TerminalController
try:
    from hashlib import sha1
except:
    from sha import sha as sha1


class Submitter(Actor):
    def __init__(self, cfg_params, parsed_range, val):
        self.cfg_params = cfg_params
        self.limitJobs = True
        # get user request
        self.nsjobs = -1
        self.chosenJobsList = None
        if val:
            if val=='range':  # for Resubmitter
                self.chosenJobsList = parsed_range
            elif val=='all':
                pass
            elif (type(eval(val)) is int) and eval(val) > 0:
                # positive number
                self.nsjobs = eval(val)
            elif (type(eval(val)) is tuple)or( type(eval(val)) is int and eval(val)<0 ) :
                self.chosenJobsList = parsed_range
                self.nsjobs = len(self.chosenJobsList)
            else:
                msg = 'Bad submission option <'+str(val)+'>\n'
                msg += '      Must be an integer or "all"'
                msg += '      Generic range is not allowed"'
                raise CrabException(msg)
            pass
        self.seWhiteList = cfg_params.get('GRID.se_white_list',[])
        self.seBlackList = cfg_params.get('GRID.se_black_list',[])
        self.datasetPath=self.cfg_params['CMSSW.datasetpath']
        if string.lower(self.datasetPath)=='none':
            self.datasetPath = None
        self.scram = Scram.Scram(cfg_params)
        return

#wmbs
    def BuildJobList(self,type=0):
        # total jobs
        nj_list = []
        self.complete_List = common._db.nJobs('list')
        if type==1: 
            self.nj_list =[]
            if self.chosenJobsList: self.nj_list = self.chosenJobsList
            return
        # build job list
        from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser
        self.blackWhiteListParser = SEBlackWhiteListParser(self.seWhiteList, self.seBlackList, common.logger())
        common.logger.debug('nsjobs '+str(self.nsjobs))
        # get the first not already submitted
        common.logger.debug('Total jobs '+str(len(self.complete_List)))

        jobSetForSubmission = 0
        jobSkippedInSubmission = []
        tmp_jList = self.complete_List
        if self.chosenJobsList != None:
            tmp_jList = self.chosenJobsList
        for job in common._db.getTask(tmp_jList).jobs:
            cleanedBlackWhiteList = self.blackWhiteListParser.cleanForBlackWhiteList(job['dlsDestination'])
            if (cleanedBlackWhiteList != '') or (self.datasetPath == None):
                #if ( job.runningJob['status'] in ['C','RC'] and job.runningJob['statusScheduler'] in ['Created',None]):
                if ( job.runningJob['state'] in ['Created']):
                    jobSetForSubmission +=1
                    nj_list.append(job['id'])
                else:
                    continue
            else :
                jobSkippedInSubmission.append( job['id'] )
            if self.nsjobs >0 and self.nsjobs == jobSetForSubmission:
                break
            pass
        if self.nsjobs>jobSetForSubmission:
            common.logger.info('asking to submit '+str(self.nsjobs)+' jobs, but only '+\
                                  str(jobSetForSubmission)+' left: submitting those')
        if len(jobSkippedInSubmission) > 0 :
            mess =""
            for jobs in jobSkippedInSubmission:
                mess += str(jobs) + ","
            common.logger.info("Jobs:  " +str(mess) + "\n\tskipped because no sites are hosting this data\n")
            self.submissionError()
            pass
        # submit N from last submitted job
        common.logger.debug('nj_list '+str(nj_list))
        self.nj_list = nj_list
        if self.limitJobs and len(self.nj_list) > 500:
            ###### FEDE FOR BUG 85243 ############## 
            msg = "The CRAB client will not submit task with more than 500 jobs.\n"
            msg += "      Use the server mode or submit your jobs in smaller groups"
            raise CrabException(msg)
            ########################################
        return

    def run(self):
        """
        The main method of the class: submit jobs in range self.nj_list
        """
        common.logger.debug("Submitter::run() called")

        start = time.time()


        self.BuildJobList()

        check = self.checkIfCreate()

        if check == 0 :
            self.SendMLpre()

            list_matched , task = self.performMatch()
            njs = self.perfromSubmission(list_matched, task)

            stop = time.time()
            common.logger.debug("Submission Time: "+str(stop - start))

            msg = 'Total of %d jobs submitted'%njs
            if njs != len(self.nj_list) :
                msg += ' (from %d requested).'%(len(self.nj_list))
            else:
                msg += '.'
            common.logger.info(msg)

            if (njs < len(self.nj_list) or len(self.nj_list)==0):
                self.submissionError()

#wmbs
    def checkIfCreate(self,type=0):
        """
        """
        code = 0
        task=common._db.getTask()
        if type == 1 and len(task.jobs)==0:
            if task['jobType']=='Submitted':
                common.logger.info("No Request to be submitted: first create it.\n")
                code=1
        else:
            totalCreatedJobs = 0
            for job in task.jobs:
                if job.runningJob['state'] == 'Created': totalCreatedJobs +=1

            if (totalCreatedJobs==0):
                common.logger.info("No jobs to be submitted: first create them")
                code = 1
        return code


    def performMatch(self):
        """
        """
        common.logger.info("Checking available resources...")
        ### define here the list of distinct destinations sites list
        distinct_dests = common._db.queryDistJob_Attr('dlsDestination', 'jobId' ,self.nj_list)


        ### define here the list of jobs Id for each distinct list of sites
        self.sub_jobs =[] # list of jobs Id list to submit
        jobs_to_match =[] # list of jobs Id to match
        all_jobs=[]
        count=0
        for distDest in distinct_dests:
             all_jobs.append(common._db.queryAttrJob({'dlsDestination':distDest},'jobId'))
             sub_jobs_temp=[]
             for i in self.nj_list:
                 if i in all_jobs[count]: sub_jobs_temp.append(i)
             if len(sub_jobs_temp)>0:
                 self.sub_jobs.append(sub_jobs_temp)
                 jobs_to_match.append(self.sub_jobs[count][0])
             count +=1
        sel=0
        matched=[]

        task=common._db.getTask()
        for id_job in jobs_to_match :
            match = common.scheduler.listMatch(distinct_dests[sel], False)
            if len(match)>0:
                common.logger.info("Found  compatible site(s) for job "+str(id_job))
                matched.append(sel)
            else:
                common.logger.info("No compatible site found, will not submit jobs "+str(self.sub_jobs[sel]))
                self.submissionError()
            sel += 1

        return matched , task

    def perfromSubmission(self,matched,task):

        njs=0

        ### Progress Bar indicator, deactivate for debug
        if common.debugLevel == 0 :
            term = TerminalController()

        if len(matched)>0:
            common.logger.info(str(len(matched))+" blocks of jobs will be submitted")
            common.logger.debug("Delegating proxy ")
            try:
                common.scheduler.delegateProxy()
            except CrabException:
                common.logger.debug("Proxy delegation failed ")

            for ii in matched:
                common.logger.debug('Submitting jobs '+str(self.sub_jobs[ii]))

                # fix arguments for unique naming of the output
                common._db.updateResubAttribs(self.sub_jobs[ii])

                try:
                    common.scheduler.submit(self.sub_jobs[ii],task)
                except CrabException:
                    common.logger.debug('common.scheduler.submit exception. Job(s) possibly not submitted')
                    raise CrabException("Job not submitted")

                if common.debugLevel == 0 :
                    try: pbar = ProgressBar(term, 'Submitting '+str(len(self.sub_jobs[ii]))+' jobs')
                    except: pbar = None
                if common.debugLevel == 0:
                    if pbar :
                        pbar.update(float(ii+1)/float(len(self.sub_jobs)),'please wait')
                ### check the if the submission succeded Maybe not needed or at least simplified
                sched_Id = common._db.queryRunJob('schedulerId', self.sub_jobs[ii])
                listId=[]
                run_jobToSave = {'status' :'S'}
                listRunField = []
                for j in range(len(self.sub_jobs[ii])):
                    if str(sched_Id[j]) != '':
                        listId.append(self.sub_jobs[ii][j])
                        listRunField.append(run_jobToSave)
                        common.logger.debug("Submitted job # "+ str(self.sub_jobs[ii][j]))
                        njs += 1
                common._db.updateRunJob_(listId, listRunField)
                self.stateChange(listId,"SubSuccess")
                self.SendMLpost(self.sub_jobs[ii])
        else:
            common.logger.info("The whole task doesn't found compatible site ")

        return njs

    def submissionError(self):
        ## add some more verbose message in case submission is not complete
        msg =  'Submission performed using the Requirements: \n'
        ### TODO_ DS--BL
        #msg += common.taskDB.dict("jobtype")+' version: '+common.taskDB.dict("codeVersion")+'\n'
        #msg += '(Hint: please check if '+common.taskDB.dict("jobtype")+' is available at the Sites)\n'
        if self.cfg_params.has_key('GRID.se_white_list'):
            msg += '\tSE White List: '+self.cfg_params['GRID.se_white_list']+'\n'
        if self.cfg_params.has_key('GRID.se_black_list'):
            msg += '\tSE Black List: '+self.cfg_params['GRID.se_black_list']+'\n'
        if self.cfg_params.has_key('GRID.ce_white_list'):
            msg += '\tCE White List: '+self.cfg_params['GRID.ce_white_list']+'\n'
        if self.cfg_params.has_key('GRID.ce_black_list'):
            msg += '\tCE Black List: '+self.cfg_params['GRID.ce_black_list']+'\n'
        removeDefBL = self.cfg_params.get('GRID.remove_default_blacklist',0)
        if removeDefBL == '0':
            msg += '\tNote:  All CMS T1s are BlackListed by default \n'
        msg += '\t(Hint: By whitelisting you force the job to run at this particular site(s).\n'
        msg += '\tPlease check if:\n'
        msg += '\t\t -- the dataset is available at this site\n'
        msg += '\t\t -- the CMSSW version is available at this site\n'
        msg += '\t\t -- grid submission to CERN & FNAL CAFs is not allowed)\n'
        msg += '\tPlease also look at the Site Status Page for CMS sites,\n'
        msg += '\t  to check if the sites hosting your data are ok\n'
        msg += '\t  http://dashb-ssb.cern.ch/dashboard/request.py/siteviewhome\n'
        common.logger.info(msg)

        return

    def collect_MLInfo(self):
        """
        Prepare DashBoard information
        """

        taskId = common._db.queryTask('name')
        gridName = string.strip(common.scheduler.userName())
        gridScheduler = common.scheduler.name()
        if gridScheduler.upper() == 'REMOTEGLIDEIN' :
            gridScheduler = 'GLIDEIN'
        common.logger.debug("GRIDNAME: %s "%gridName)
        #### FEDE for taskType (savannah 76950)
        taskType = self.cfg_params.get('USER.tasktype','analysis') 
        #### taskType = 'analysis'

        self.executable = self.cfg_params.get('CMSSW.executable','cmsRun')
        VO = self.cfg_params.get('GRID.virtual_organization','cms')

        params = {'tool': common.prog_name,
                  'SubmissionType':'direct',
                  'JSToolVersion': common.prog_version_str,
                  'tool_ui': os.environ.get('HOSTNAME',''),
                  'scheduler': gridScheduler,
                  'GridName': gridName,
                  'ApplicationVersion': self.scram.getSWVersion(),
                  'taskType': taskType,
                  'vo': VO,
                  'CMSUser': getUserName(),
                  'user': getUserName(),
                  'taskId': str(taskId),
                  'datasetFull': self.datasetPath,
                  'resubmitter': 'user', \
                  'exe': self.executable }

        return params

    def SendMLpre(self):
        """
        Send Pre info to ML
        """
        params = self.collect_MLInfo()

        params['jobId'] ='TaskMeta'

        common.apmon.sendToML(params)

        common.logger.debug('Submission DashBoard Pre-Submission report: %s'%str(params))

        return

    def SendMLpost(self,allList):
        """
        Send post-submission info to ML
        """
        task = common._db.getTask(allList)

        params = {}
        for k,v in self.collect_MLInfo().iteritems():
            params[k] = v

        msg = ''
        Sub_Type = 'Direct'
        for job in task.jobs:
            jj = job['jobId']
            jobId = ''
            localId = ''
            jid = str(job.runningJob['schedulerId'])
            if common.scheduler.name().upper() in ['CONDOR_G']:
                rb = 'OSG'
                taskHash =  sha1(common._db.queryTask('name')).hexdigest()
                jobId = str(jj) + '_https://' + common.scheduler.name() + '/' + taskHash + '/' + str(jj)
                msg += ('JobID for ML monitoring is created for CONDOR_G scheduler: %s \n'%str(jobId))
            elif common.scheduler.name().upper() in ['GLIDEIN']:
                rb = common.scheduler.name()
                jobId = str(jj) + '_https://' + str(jid)
                msg += ('JobID for ML monitoring is created for GLIDEIN scheduler: %s \n'%str(jobId))
            elif common.scheduler.name().upper() in ['REMOTEGLIDEIN']:
                rb = str(task['serverName'])
                jobId = str(jj) + '_https://' + str(jid)
                msg += ('JobID for ML monitoring is created for REMOTEGLIDEIN scheduler: %s\n'%str(jobId))
            elif common.scheduler.name().upper() in ['LSF', 'CAF', 'PBS']:
                jobId= str(jj) + "_https://"+common.scheduler.name().upper()+":/"+jid+"-"+string.replace(str(task['name']),"_","-")
                msg += ('JobID for ML monitoring is created for %s scheduler: %s\n'%(common.scheduler.name().upper(), str(jobId)) )
                rb = common.scheduler.name()
                localId = jid
            elif common.scheduler.name().upper() in ['CONDOR']:
                taskHash = sha1(common._db.queryTask('name')).hexdigest()
                jobId = str(jj) + '_https://' + socket.gethostname() + '/' + taskHash + '/' + str(jj)
                rb = common.scheduler.name()
                msg += ('JobID for ML monitoring is created for CONDOR scheduler: %s\n'%str(jobId))
            elif common.scheduler.name().upper() in ['ARC']:
                jobId = str(jj) + '_' + str(jid)
                msg += ('JobID for ML monitoring is created for ARC scheduler: %s\n'%str(jobId))
                rb = 'ARC'
            else:
                jobId = str(jj) + '_' + str(jid)
                msg += ('JobID for ML monitoring is created for gLite scheduler %s\n'%str(jobId))
                rb = str(job.runningJob['service'])

            dlsDest = job['dlsDestination']
            if len(dlsDest) == 1 :
                T_SE=str(dlsDest[0])
            elif len(dlsDest) == 2 :
                T_SE=str(dlsDest[0])+','+str(dlsDest[1])
            else :
                T_SE=str(len(dlsDest))+'_Selected_SE'


            infos = { 'jobId': jobId, \
                      'sid': jid, \
                      'broker': rb, \
                      'bossId': jj, \
                      'SubmissionType': Sub_Type, \
                      'TargetSE': T_SE, \
                      'localId' : localId}

            for k,v in infos.iteritems():
                params[k] = v

            msg +=('Submission DashBoard report: %s\n'%str(params))
            common.apmon.sendToML(params)
        #common.logger.debug(msg)
        return


