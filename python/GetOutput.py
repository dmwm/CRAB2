from Actor import *
import common
import string, os, time
from crab_util import *

class GetOutput(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.jobs = args[1]
        
        self.log=0

        dir = os.getcwd()+'/'
        self.outDir = self.cfg_params.get('USER.outputdir' ,common.work_space.resDir())
        if ( self.outDir[-1] != '/' ) : self.outDir = self.outDir + '/'
        if ( self.outDir[0] != '/') : self.outDir = dir + self.outDir 
        self.logDir = self.cfg_params.get('USER.logdir' ,common.work_space.resDir())
        if ( self.logDir[-1] != '/' ) : self.logDir = self.logDir + '/'
        if ( self.logDir[0] != '/') : self.logDir = dir +self.logDir 
        if self.logDir != self.outDir:
            self.log=1
        self.return_data = self.cfg_params.get('USER.return_data',0)

        self.dontCheckSpaceLeft = int(self.cfg_params.get('USER.dontCheckSpaceLeft' ,0))
        return

    def run(self):
        """
        The main method of the class: Check destination dirs and 
        perform the get output
        """
        common.logger.debug( "GetOutput::run() called")

        start = time.time()
        self.getOutput()
        stop = time.time()
        common.logger.debug( "GetOutput Time: "+str(stop - start))
        pass

    def checkBeforeGet(self):
        # should be in this way... but a core dump appear... waiting for solution  
        #self.up_task = common.scheduler.queryEverything(1) 
        self.up_task = common._db.getTask() 
        list_id_done=[]
        list_id_done_not_term=[]
        self.list_id=[]
        self.all_id =[]    
        for job in self.up_task.jobs:
            if (job.runningJob['state'] == 'Terminated'):
                list_id_done.append(job['jobId'])  
            elif job.runningJob['status'] in ['Done', 'Done (Failed)']:
                list_id_done_not_term.append(job['jobId'])
            self.all_id.append(job['jobId'])  
        check = -1 
        if self.jobs != 'all': check = len( set(self.jobs).intersection(set(list_id_done)) )  
        if len(list_id_done)==0 or ( check == 0 ) :
            msg=''  
            list_jobs=self.jobs 
            if self.jobs == 'all': list_jobs=self.all_id 
            msg += 'Jobs %s are not in Done status. It is not possible yet to retrieve the output. \n'% readableList(self,list_jobs)
            if len(list_id_done) > 0:
                msg += '  Retrieve the jobs if those are in Done status and Terminatedi action. \n'
                msg += '  To know the action of a job run: "crab -status v " \n'
            raise CrabException(msg)
        else:
            if self.jobs == 'all': 
                self.list_id= list_id_done
                if len(self.up_task.jobs)>len(self.list_id): 
                    msg = 'Only %d jobs will be retrieved '% (len(self.list_id))
                    msg += ' from %d requested.\n'%(len(self.up_task.jobs))
                    msg += '\t(for details: crab -status)' 
                    common.logger.info(msg)
            else:
                for id in self.jobs:
                    if id in list_id_done: self.list_id.append(id)   
                if len(self.jobs) > len(self.list_id):
                    msg = 'Only %d jobs will be retrieved '% (len(self.list_id))
                    msg += ' from %d requested.\n'%(len(self.jobs))
                    msg += '\t(for details: crab -status)' 
                    common.logger.info(msg)
        if not os.path.isdir(self.logDir) or not os.path.isdir(self.outDir):
            msg =  ' Output or Log dir not found!! check '+self.logDir+' and '+self.outDir
            raise CrabException(msg)
        return 

    def getOutput(self):
        """
        Get output for a finished job with id.
        """
        self.checkBeforeGet()
        # Get first job of the list
        if not self.dontCheckSpaceLeft and not has_freespace(self.outDir, 10*1024): # First check for more than 10 Mb
            msg = "You have LESS than 10 MB of free space on your working dir\n"
            msg +="Please make some room before retrying\n\n"
            msg +="To bypass this check, run \n"
            msg +="crab -get -USER.dontCheckSpaceLeft=1 \n"
            raise CrabException(msg)
        list_first=self.list_id[0:1]
        task= common.scheduler.getOutput(1, list_first, self.outDir)
        lastSize = self.organizeOutput( task, list_first )    
        # here check disk space for first job
        if not self.dontCheckSpaceLeft and not has_freespace(self.outDir, lastSize*len(self.list_id)*1.2) : # add a 20% overhead
            msg = "Estimated space needed for getOutput is "+str(lastSize*len(self.list_id)*1.2)
            msg +=" which is LESS than available space on disk\n"
            msg +="Please make some room before retrying\n"
            msg +="To bypass this check, run \n"
            msg +="crab -get -USER.dontCheckSpaceLeft=1 \n"
            raise CrabException(msg)
        # get the size of the actual OSB of first job
        if (len(self.list_id)>1) :
            # check disk space for other N jobs using estimate from the first
            list_other=self.list_id[1:]
            task= common.scheduler.getOutput(1, list_other, self.outDir)
            self.organizeOutput( task, list_other )    
        return
  
    def organizeOutput(self, task, list_id): 
        """
        Untar Output  
        """
        listCode = []
        job_id = []

        success_ret = 0
        size = 0 # in kB
        for id in list_id:
            runningJob = task.getJob( id ).runningJob
            if runningJob.isError() :
	        continue
            file = 'out_files_'+ str(id)+'.tgz'
            if os.path.exists(self.outDir + file):
                self.max_id = runningJob['submission']
                if self.max_id > 1:
                    for f in os.listdir(self.outDir):
                        if (f.find('_'+str(id)+'.') != -1 ) and (f != file) and f.find('Submission_'+str(id)) == -1: 
                            self.moveOutput(id, self.max_id, self.outDir, f)
                    if self.log==1:
                        for f in os.listdir(self.logDir):
                            if f.find('_'+str(id)+'.') != -1 and f.find('Submission_'+str(id)) == -1:
                                self.moveOutput(id, self.max_id, self.logDir, f)
                            pass
                        pass
                    pass
                try:
                    size = getGZSize(self.outDir + file)/1024 # in kB
                    cmd = 'tar zxf ' + self.outDir + file + ' ' + '-C ' + self.outDir
                    cmd_out = runCommand(cmd)
                    cmd_2 ='rm ' + self.outDir + 'out_files_'+ str(id)+'.tgz'
                    cmd_out2 = runCommand(cmd_2)
                    msg = 'Results of Jobs # '+str(id)+' are in '+self.outDir
                    common.logger.info(msg)
                except IOError, eio:
                    common.logger.info("Output files for job "+ str(id) +" seems corrupted.\n")
                    continue
            else:  
                msg ="Output files for job "+ str(id) +" not available.\n"
                common.logger.info(msg)
                FieldToUpdate={}
                FieldToUpdate['state']= 'Cleared'
                FieldToUpdate["applicationReturnCode"] = str(50700)
                FieldToUpdate["wrapperReturnCode"] = str(50700)
                job_id.append(id)
                listCode.append(FieldToUpdate)
                #continue   
            input = 'crab_fjr_' + str(id) + '.xml'
            if os.path.exists(self.outDir + input):
                FiledToUpdate = self.parseFinalReport(self.outDir + input)
                FiledToUpdate['state']= 'Cleared'
                job_id.append(id)
                listCode.append(FiledToUpdate)
            else:
                msg = "Problems with "+str(input)+". File not available.\n"
                common.logger.info(msg) 
            success_ret +=1 
        common._db.updateRunJob_(job_id , listCode)

        if self.logDir != self.outDir:
            for i_id in list_id:  
                try:
                    cmd = 'mv '+str(self.outDir)+'/*'+str(i_id)+'.std* '+str(self.outDir)+'/.BrokerInfo '+str(self.logDir)
                    cmd_out =os.system(cmd)
                except:
                    msg = 'Problem with copy of job results'
                    common.logger.info(msg)
            msg = 'Results of Jobs # '+str(list_id)+' are in '+self.outDir+' (log files are in '+self.logDir+')'
            common.logger.info(msg)
        return size

    def parseFinalReport(self, input):
        """
        Parses the FJR produced by job in order to retrieve 
        the WrapperExitCode and ExeExitCode.
        Updates the BossDB with these values.

        """
        from ProdCommon.FwkJobRep.ReportParser import readJobReport
        
        codeValue = {} 

        jreports = readJobReport(input)
        if len(jreports) <= 0 :
            codeValue["applicationReturnCode"] = str(50115)
            codeValue["wrapperReturnCode"] = str(50115)
            common.logger.debug("Empty FWkobreport: error code assigned is 50115 ")
            return codeValue

        jobReport = jreports[0]

        exit_status = ''
    
        ##### temporary fix for FJR incomplete ####
        fjr = open (input)
        len_fjr = len(fjr.readlines())
        if (len_fjr <= 6):
            ### 50115 - cmsRun did not produce a valid/readable job report at runtime
            codeValue["applicationReturnCode"] = str(50115)
            codeValue["wrapperReturnCode"] = str(50115)
       
        if len(jobReport.errors) != 0 :
            for error in jobReport.errors:
                if error['Type'] == 'WrapperExitCode':
                    codeValue["wrapperReturnCode"] = error['ExitStatus']
                elif error['Type'] == 'ExeExitCode': 
                   codeValue["applicationReturnCode"] = error['ExitStatus']
                if error['Type'] == 'CMSException':
                    codeValue["applicationReturnCodeOrig"] = error['ExitStatus']
                else:
                    continue

        if not codeValue.has_key('wrapperReturnCode'):
            codeValue["wrapperReturnCode"] = ''
        if not codeValue.has_key('applicationReturnCode'):
            if codeValue.has_key('applicationReturnCodeOrig'): 
                codeValue["applicationReturnCode"] = \
                    codeValue["applicationReturnCodeOrig"]
                codeValue.pop("applicationReturnCodeOrig")
            else: 
                codeValue["applicationReturnCode"] = ''
            
        else:
            if codeValue.has_key('applicationReturnCodeOrig'):
                codeValue.pop("applicationReturnCodeOrig")
                
        #### Filling BOSS DB with SE name and LFN, for edm and not_edm files ####        
        lfns=[]
        pfns=[]

        if (len(jobReport.files) != 0):
            for f in jobReport.files:
                if f['LFN']:
                    lfns.append(f['LFN'])
                if f['PFN']:
                    #### FEDE to have the correct endpoit to use in the copyData (we modify the bossDB value and not the fjr ) 
                    if common.scheduler.name().upper() not in ['LSF', 'CAF', 'PBS'] and codeValue["wrapperReturnCode"] == 60308:
                        pfns.append(os.path.dirname(f['SurlForGrid'])+'/')
                    else:
                        pfns.append(os.path.dirname(f['PFN'])+'/')
                    ##########    
        if (len(jobReport.analysisFiles) != 0):
            for aFile in jobReport.analysisFiles:
                if aFile['LFN']:
                    lfns.append(aFile['LFN'])
                if aFile['PFN']:
                    #### FEDE to have the correct endpoit to use in the copyData (we modify the bossDB value and not the fjr ) 
                    if common.scheduler.name().upper() not in ['LSF', 'CAF', 'PBS'] and codeValue["wrapperReturnCode"] == 60308:
                        pfns.append(os.path.dirname(aFile['SurlForGrid'])+'/')
                    else:    
                        pfns.append(os.path.dirname(aFile['PFN'])+'/')
                    #########    
        codeValue["storage"] = pfns 
        codeValue["lfn"] = lfns
        return codeValue

    def moveOutput(self,id, max_id,path,file):
        """
        Move output of job already retrieved
        into the correct backup directory
        """
        Dir_Base=path +'Submission_'
        
        for i in range(1, max_id):
            if not os.path.isdir( Dir_Base + str(i) + '/'):
                cmd=('mkdir '+ Dir_Base + str(i) + '/  >& /dev/null')
                cmd_out = runCommand(cmd)   
                common.logger.debug(str(cmd_out))
        cmd='mv '+ path + file + ' ' + Dir_Base + str(max_id -1) + '/  >& /dev/null' 
        
        try:
            cmd_out = runCommand(cmd) 
            common.logger.debug(cmd_out)
        except:
            msg = 'no output to move for job '+str(id)
            common.logger.debug(msg)
            pass
        return
