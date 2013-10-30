from Scheduler import Scheduler
from crab_exceptions import *
from crab_util import getLocalDomain
import common
from PhEDExDatasvcInfo import PhEDExDatasvcInfo

import os,string

# Base class for all local scheduler

class SchedulerLocal(Scheduler) :

    def configure(self, cfg_params):
        self.environment_unique_identifier = None
        self.cfg_params = cfg_params
        Scheduler.configure(self,cfg_params)
        self.jobtypeName = cfg_params['CRAB.jobtype']

        name=string.upper(self.name())
        self.queue = cfg_params.get(name+'.queue',None)

        self.res = cfg_params.get(name+'.resource',None)

        # minimal padding time for jobs. For local schedulers is disabled.
        # Added for alignment purpose only (and for test) with Grid schedulers
        self.minimal_job_duration = 0

        if (cfg_params.has_key(self.name()+'.env_id')): self.environment_unique_identifier = cfg_params[self.name()+'.env_id']
        ## is this ok?
        localDomainName = getLocalDomain(self)
        if not cfg_params.has_key('GRID.se_white_list'):
            cfg_params['GRID.se_white_list']=localDomainName
            common.logger.info("Your domain name is "+str(localDomainName)+": only local dataset will be considered")
        else:
            common.logger.info("Your se_white_list is set to "+str(cfg_params['GRID.se_white_list'])+": only local dataset will be considered")
        return

    def userName(self):
        """ return the user name """
        import pwd,getpass
        tmp=pwd.getpwnam(getpass.getuser())[4]
        return "/CN="+tmp.strip()

    def envUniqueID(self):
        return

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        taskId = common._db.queryTask('name')
        if not self.environment_unique_identifier:
            try :
                self.environment_unique_identifier = self.envUniqueID()
            except :
                raise CrabException('environment_unique_identifier not set')
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        # start with wrapper timing
        txt  = 'export TIME_WRAP_INI=`date +%s` \n'
        txt += 'export TIME_STAGEOUT=-2 \n\n'

        txt += '# '+self.name()+' specific stuff\n'
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}; export NJob\n"
        txt += "NResub=${args[1]}; export NResub\n"
        txt += "NRand=`getRandSeed`; export NRand\n"
        txt += 'OutUniqueID=_$NRand\n'
        txt += 'OutUniqueID=_$NResub$OutUniqueID\n'
        txt += 'OutUniqueID=$NJob$OutUniqueID; export OutUniqueID\n'

        txt += "out_files=out_files_${NJob}; export out_files\n"
        txt += "echo $out_files\n"
        txt += jbt.outList()

        txt += 'SyncGridJobId=`echo '+self.environment_unique_identifier+'`\n'
        txt += 'MonitorJobID=`echo ${NJob}_${SyncGridJobId}`\n'
        txt += 'MonitorID=`echo ' + taskId + '`\n'

        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "SyncCE='+self.name()+'.`hostname -d`" | tee -a $RUNTIME_AREA/$repo \n'

        txt += 'middleware='+self.name().upper()+' \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'

        txt += 'InputSandBox=${args[3]}\n'

        txt += '\n\n'

        return txt

    def wsCopyOutput_comm(self, pool=None):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        txt = '\n'
        if int(self.copy_data) == 1:

            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            print "endpoint = ", endpoint

            ########################################################
            ##################### FEDE FOR CAF #####################
            cmscp_args = '' 
            if  common.scheduler.name().upper() == 'CAF':
                    if endpoint.find('root:') != -1:
                        SE_PATH = '/' + endpoint.split('//')[2]
                        caf_protocol = 'xrootd'
                        cmscp_args += '--protocol %s '%caf_protocol
                    elif endpoint.find('rfio:') != -1:
                        if endpoint.find('path=') != -1:
                            SE_PATH = endpoint.split('path=')[1]
                        else:
                            SE_PATH = endpoint
                        caf_protocol = 'rfio'
                        cmscp_args += '--protocol %s '%caf_protocol
            ########################################################            
            ########################################################            

            if self.check_RemoteDir == 1 :
                self.checkRemoteDir(endpoint,jbt.outList('list') )

            txt += '#\n'
            txt += '# COPY OUTPUT FILE TO '+SE_PATH+ '\n'
            txt += '#\n\n'

            txt += 'export SE='+SE+'\n'
            txt += 'echo "SE = $SE"\n'
            txt += 'export SE_PATH='+SE_PATH+'\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'
            txt += 'export LFNBaseName='+lfn+'\n'
            txt += 'echo "LFNBaseName = $LFNBaseName"\n'
            txt += 'export USER='+user+'\n'
            txt += 'echo "USER = $USER"\n'
            #txt += 'export endpoint='+"'"+endpoint+"'"+'\n'
            txt += 'export endpoint='+endpoint+'\n'
            txt += 'echo "endpoint = $endpoint"\n'
            
            if (pool) and (pool != 'None'):
                txt += 'export STAGE_SVCCLASS='+str(pool)+'\n'

            txt += 'echo ">>> Copy output files from WN = `hostname` to $SE_PATH :"\n'
            txt += 'export TIME_STAGEOUT_INI=`date +%s` \n'
            txt += 'copy_exit_status=0\n'
            #### FEDE added += ######### 
            cmscp_args += ' --destination $endpoint --inputFileList $file_list'
            #######################################################
            #######################################################
            cmscp_args +=' --middleware $middleware --se_name $SE --for_lfn $LFNBaseName %s %s '%(self.loc_stage_out,self.debugWrap)
            txt += 'echo "python cmscp.py %s "\n'%cmscp_args
            txt += 'python cmscp.py %s \n'%cmscp_args
            if self.debug_wrapper==1:
                txt += 'echo "########### details of SE interaction"\n'
                txt += 'if [ -f .SEinteraction.log ] ;then\n'
                txt += '    cat .SEinteraction.log\n'
                txt += 'else\n'
                txt += '    echo ".SEinteraction.log file not found"\n'
                txt += 'fi\n'
                txt += 'echo "#####################################"\n'
            txt += 'if [ -f $RUNTIME_AREA/resultCopyFile ] ;then\n'
            txt += '    cat $RUNTIME_AREA/resultCopyFile\n'
            txt += '    pwd\n'
            txt += 'else\n'
            ### to avoid some 70500 error ....
            txt += '    echo "ERROR ==> $RUNTIME_AREA/resultCopyFile file not found. Problem during the stageout"\n'
            txt += '    echo "RUNTIME_AREA content: "\n'
            txt += '    ls $RUNTIME_AREA \n'
            txt += '    job_exit_code=60318\n'
            txt += '    func_exit \n'
            txt += 'fi\n'
            ##########################################

            txt += 'if [ -f ${RUNTIME_AREA}/cmscpReport.sh ] ;then\n'
            txt += '    echo "-------- cat ${RUNTIME_AREA}/cmscpReport.sh "\n'
            txt += '    cat ${RUNTIME_AREA}/cmscpReport.sh\n'
            txt += '    echo "-------- end of ${RUNTIME_AREA}/cmscpReport.sh "\n'
            txt += '    source ${RUNTIME_AREA}/cmscpReport.sh\n'
            txt += '    source_result=$? \n'
            txt += '    if [ $source_result -ne 0 ]; then\n'
            txt += '        echo "problem with the source of cmscpReport.sh file"\n'
            txt += '        StageOutExitStatus=60307\n'
            txt += '    fi\n'
            txt += 'else\n'
            txt += '    echo "cmscpReport.sh file not found"\n'
            txt += '    StageOutExitStatus=60307\n'
            txt += 'fi\n'
            txt += 'if [ $StageOutExitStatus -ne 0 ]; then\n'
            txt += '    echo "Problem copying file to $SE $SE_PATH"\n'
            txt += '    copy_exit_status=$StageOutExitStatus \n'
            if not self.debug_wrapper==1:
                txt += 'if [ -f .SEinteraction.log ] ;then\n'
                txt += '    echo "########## contents of SE interaction"\n'
                txt += '    cat .SEinteraction.log\n'
                txt += '    echo "#####################################"\n'
                txt += 'else\n'
                txt += '    echo ".SEinteraction.log file not found"\n'
                txt += 'fi\n'
            txt += '    job_exit_code=$StageOutExitStatus\n'
            txt += 'fi\n'
            txt += 'export TIME_STAGEOUT_END=`date +%s` \n'
            txt += 'let "TIME_STAGEOUT = TIME_STAGEOUT_END - TIME_STAGEOUT_INI" \n'
        else:
            txt += 'export TIME_STAGEOUT=-1 \n'
        return txt
