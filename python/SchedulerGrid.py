"""
Base class for all grid schedulers
"""
__revision__ = "$Id: SchedulerGrid.py,v 1.147 2013/09/02 15:05:42 belforte Exp $"
__version__ = "$Revision: 1.147 $"

from Scheduler import Scheduler
from crab_exceptions import *
from crab_util import *
import common
from PhEDExDatasvcInfo import PhEDExDatasvcInfo
from JobList import JobList
from Downloader import Downloader

import os, sys, time

class SchedulerGrid(Scheduler):

    def __init__(self, name):
        Scheduler.__init__(self,name)
        self.states = [ "Acl", "cancelReason", "cancelling","ce_node","children", \
                      "children_hist","children_num","children_states","condorId","condor_jdl", \
                      "cpuTime","destination", "done_code","exit_code","expectFrom", \
                      "expectUpdate","globusId","jdl","jobId","jobtype", \
                      "lastUpdateTime","localId","location", "matched_jdl","network_server", \
                      "owner","parent_job", "reason","resubmitted","rsl","seed",\
                      "stateEnterTime","stateEnterTimes","subjob_failed", \
                      "user tags" , "status" , "status_code","hierarchy"]
        return

    def configure(self, cfg_params):
        self.cfg_params = cfg_params
        self.jobtypeName   = cfg_params.get('CRAB.jobtype','')
        self.schedulerName = cfg_params.get('CRAB.scheduler','')
        Scheduler.configure(self,cfg_params)
        self.proxyValid=0

        self.dontCheckProxy=int(cfg_params.get("GRID.dont_check_proxy",0)) 	 
        self.space_token = cfg_params.get("USER.space_token",None) 	 
 	try:
            self.proxyServer = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/").config("myproxy_server.conf")
            self.proxyServer = self.proxyServer.strip()
            if self.proxyServer is None:
                raise CrabException("myproxy_server.conf retrieved but empty")
        except Exception, e:
            common.logger.info("Problem setting myproxy server endpoint: using myproxy.cern.ch")
            common.logger.debug(e)
            self.proxyServer= 'myproxy.cern.ch'
        self.group = cfg_params.get("GRID.group", None) 	 
        self.role = cfg_params.get("GRID.role", None)

        removeBList = cfg_params.get("GRID.remove_default_blacklist", 0 )
        blackAnaOps = None
        if int(removeBList) == 0:
            blacklist = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/")
            result = blacklist.config("site_black_list.conf")
            if result != None:
                blackAnaOps = result
            common.logger.debug("Enforced black list: %s "%blackAnaOps)
        else:
            common.logger.info("WARNING: Skipping default black list!")

        self.EDG_ce_black_list = None
        if cfg_params.has_key('GRID.ce_black_list') and cfg_params['GRID.ce_black_list']:
            self.EDG_ce_black_list = cfg_params.get('GRID.ce_black_list')
            if int(removeBList) == 0 and blackAnaOps: 
                self.EDG_ce_black_list += ",%s"%blackAnaOps
        elif int(removeBList) == 0 and blackAnaOps:
            self.EDG_ce_black_list = blackAnaOps
        if self.EDG_ce_black_list:
            self.EDG_ce_black_list = str(self.EDG_ce_black_list).split(',')

        self.EDG_ce_white_list = cfg_params.get('GRID.ce_white_list',None)
        if (self.EDG_ce_white_list): self.EDG_ce_white_list = str(self.EDG_ce_white_list).split(',')

        self.VO = cfg_params.get('GRID.virtual_organization','cms')

        self.EDG_clock_time = cfg_params.get('GRID.max_wall_clock_time',None)

        # Default minimum CPU time to >= 130 minutes
        self.EDG_cpu_time = cfg_params.get('GRID.max_cpu_time', '130')

        ## Add EDG_WL_LOCATION to the python path
        #if not self.CRAB_useServer and not self.CRAB_serverName:
        #    if not os.environ.has_key('EDG_WL_LOCATION'):
        #        msg = "Error: the EDG_WL_LOCATION variable is not set."
        #        raise CrabException(msg)
        #    path = os.environ['EDG_WL_LOCATION']
        #    libPath=os.path.join(path, "lib")
        #    sys.path.append(libPath)
        #    libPath=os.path.join(path, "lib", "python")
        #    sys.path.append(libPath)

        self.checkProxy()
        return

    def rb_configure(self, RB):
        """
        Return a requirement to be add to Jdl to select a specific RB/WMS:
        return None if RB=None
        To be re-implemented in concrete scheduler
        """
        return None

    def sched_fix_parameter(self):
        """
        Returns string with requirements and scheduler-specific parameters
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        req = ''
        req = req + jbt.getRequirements()

        if self.EDG_requirements:
            if (not req == ' '):
                req = req +  ' && '
            req = req + self.EDG_requirements

        taskReq = {'jobType':req}
        common._db.updateTask_(taskReq)

    def listMatch(self, dest, full):
        matching='fast'
        ces=Scheduler.listMatch(self, dest, full)
        sites=[]
        for ce in ces:
            site=ce.split(":")[0]
            if site not in sites:
                sites.append(site)
            pass
        if full == True: matching='full'
        common.logger.debug("list of available site ( "+str(matching) +" matching ) : "+str(sites))
        return sites


    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        taskId =common._db.queryTask('name')
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        if not self.environment_unique_identifier:
            try :
                self.environment_unique_identifier = self.envUniqueID()
            except :
                raise CrabException('environment_unique_identifier not set')

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
        # append random code
        txt += 'OutUniqueID=_$NRand\n'
        txt += 'OutUniqueID=_$NResub$OutUniqueID\n'
        txt += 'OutUniqueID=$NJob$OutUniqueID; export OutUniqueID\n'
        txt += 'CRAB_UNIQUE_JOB_ID=%s_${OutUniqueID}; export CRAB_UNIQUE_JOB_ID\n' % taskId
        txt += 'echo env var CRAB_UNIQUE_JOB_ID set to: ${CRAB_UNIQUE_JOB_ID}\n'
        # if we want to prepend
        #txt += 'OutUniqueID=_$NResub\n'
        #txt += 'OutUniqueID=_$NJob$OutUniqueID\n'
        #txt += 'OutUniqueID=$NRand$OutUniqueID; export OutUniqueID\n'

        txt += "out_files=out_files_${NJob}; export out_files\n"
        txt += "echo $out_files\n"
        txt += jbt.outList()
      #  txt += 'if [ $JobRunCount ] && [ `expr $JobRunCount - 1` -gt 0 ] && [ $Glidein_MonitorID ]; then \n'
        txt += 'if [ $Glidein_MonitorID ]; then \n'
#        txt += '   attempt=`expr $JobRunCount - 1` \n'
#        txt += '   MonitorJobID=${NJob}_${Glidein_MonitorID}__${attempt}\n'
#        txt += '   SyncGridJobId=${Glidein_MonitorID}__${attempt}\n'
        txt += '   MonitorJobID=${NJob}_${Glidein_MonitorID}\n'
        txt += '   SyncGridJobId=${Glidein_MonitorID}\n'
        txt += 'else \n'
        txt += '   MonitorJobID=${NJob}_'+self.environment_unique_identifier+'\n'
        txt += '   SyncGridJobId='+self.environment_unique_identifier+'\n'
        txt += 'fi\n'
        txt += 'MonitorID='+taskId+'\n'
        txt += 'echo "MonitorJobID=$MonitorJobID" >> $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=$SyncGridJobId" >> $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=$MonitorID" >> $RUNTIME_AREA/$repo\n'

        txt += 'echo ">>> GridFlavour discovery: " \n'
        txt += 'if [ $OSG_GRID ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    echo "source OSG GRID setup script" \n'
        txt += '    source $OSG_GRID/setup.sh \n'
        txt += 'elif [ $NORDUGRID_CE ]; then \n' # We look for $NORDUGRID_CE before $VO_CMS_SW_DIR,
        txt += '    middleware=ARC \n'           # because the latter is defined for ARC too
        txt += 'elif [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG \n'
        txt += 'else \n'
        txt += '    echo "ERROR ==> GridFlavour not identified" \n'
        txt += '    job_exit_code=10030 \n'
        txt += '    func_exit \n'
        txt += 'fi \n'
        txt += 'echo "GridFlavour=$middleware" \n'
        txt += 'echo "GridFlavour=$middleware" >> $RUNTIME_AREA/$repo \n'

        txt += '\n'

        txt += 'echo ">>> SyncSite discovery: " \n'
        txt += 'if [ $GLIDEIN_CMSSite ]; then \n'
        txt += '    SyncSite=$GLIDEIN_CMSSite \n'
        txt += '    echo "SyncSite=$SyncSite" \n'
        txt += '    echo "SyncSite=$SyncSite" >> $RUNTIME_AREA/$repo ;\n'
        txt += 'else\n'
        txt += '    echo "not reporting SyncSite"\n'
        txt += 'fi\n';
        
        txt += '\n'
        
        txt += 'echo ">>> SyncCE discovery: " \n'
        txt += 'if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '    echo "getting SyncCE from OSG_JOB_CONTACT" \n'
        txt += '    SyncCE="$OSG_JOB_CONTACT"; \n'
        txt += 'elif [ $NORDUGRID_CE ]; then \n'
        txt += '    echo "getting SyncCE from NORDUGRID_CE" \n'
        txt += '    SyncCE="${NORDUGRID_CE}:2811/nordugrid-GE-${QUEUE:-queue}"\n '
        txt += 'elif [ $CE_ID ]; then \n'
        txt += '    echo "getting SyncCE from CE_ID" \n'
        txt += '    SyncCE="${CE_ID}" \n'
        txt += 'elif [ "$GLIDEIN_Gatekeeper" ]; then \n'        # beware: GLIDEIN_gatekeeper may have blanks
        txt += '    echo "getting SyncCE from GLIDEIN_Gaekeeper" \n'
        txt += '    GKtmp="`echo $GLIDEIN_Gatekeeper | sed -e s,http\'s\?\'://,,`"\n' # remove leading http[s]:// if any
        txt += '    SyncCE="`echo $GKtmp | cut -d: -f1`" \n'
        txt += 'else \n'
        txt += '    echo "getting SyncCE glite-brokerinfo" \n'
        txt += '    SyncCE="`glite-brokerinfo getCE`" \n'
        txt += 'fi \n'
        txt += 'echo "SyncCE=$SyncCE" \n'
        txt += 'echo "SyncCE=$SyncCE" >> $RUNTIME_AREA/$repo ;\n'
        
        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '\n'


        txt += 'export VO='+self.VO+'\n'
        ### SB START following stuff appear totally useless ##############
        #txt += 'if [ $middleware == LCG ]; then\n'
        #txt += '   if  [ $GLIDEIN_Gatekeeper ]; then\n'
        #txt += '       CloseCEs=$GLIDEIN_Gatekeeper \n'
        #txt += '   else\n'
        #txt += '       CloseCEs=`glite-brokerinfo getCE`\n'
        #txt += '   fi\n'
        #txt += '   echo "CloseCEs = $CloseCEs"\n'
        #txt += '   CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        #txt += '   echo "CE = $CE"\n'
        #txt += 'elif [ $middleware == OSG ]; then \n'
        #txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        #txt += '        CE=`echo $OSG_JOB_CONTACT | /usr/bin/awk -F\/ \'{print $1}\'` \n'
        #txt += '    else \n'
        #txt += '        echo "ERROR ==> OSG mode in setting CE name from OSG_JOB_CONTACT" \n'
        #txt += '        job_exit_code=10099\n'
        #txt += '        func_exit\n'
        #txt += '    fi \n'
        #txt += 'elif [ $middleware == ARC ]; then \n'
        #txt += '    echo "CE = $NORDUGRID_CE"\n'
        #txt += 'fi \n'
        ### SB END ###############################################

        return txt

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()

        txt = '\n'

        txt += '#\n'
        txt += '# COPY OUTPUT FILE TO SE\n'
        txt += '#\n\n'

        if int(self.copy_data) == 1:
            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, PNN, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            if self.check_RemoteDir == 1 :
                self.checkRemoteDir(endpoint,jbt.outList('list') )
            txt += 'export PNN='+PNN+'\n'
            txt += 'echo "PNN = $PNN"\n'
            txt += 'export SE='+SE+'\n'
            txt += 'echo "SE = $SE"\n'
            txt += 'export SE_PATH='+SE_PATH+'\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'
            txt += 'export LFNBaseName='+lfn+'\n'
            txt += 'echo "LFNBaseName = $LFNBaseName"\n'
            txt += 'export USER='+user+'\n'
            txt += 'echo "USER = $USER"\n'
            txt += 'export endpoint='+endpoint+'\n'
            txt += 'echo "endpoint = $endpoint"\n'

            txt += 'echo ">>> Copy output files from WN = `hostname` to $SE_PATH :"\n'
            txt += 'export TIME_STAGEOUT_INI=`date +%s` \n'
            txt += 'copy_exit_status=0\n'
            cmscp_args = ' --destination $endpoint --inputFileList $file_list'
            ### FEDE FOR MULTI ### 
            #cmscp_args +=' --middleware $middleware --lfn $LFNBaseName %s %s '%(self.loc_stage_out,self.debugWrap)
            cmscp_args +=' --middleware $middleware --se_name $SE --for_lfn $LFNBaseName %s %s '%(self.loc_stage_out,self.debugWrap)
            if self.space_token:
                cmscp_args +=' --option space_token=%s'%str(self.space_token)
            txt += 'echo "python cmscp.py %s "\n'%cmscp_args
            txt += 'python cmscp.py %s \n'%cmscp_args
            if self.debug_wrapper==1:
                txt += 'echo "which lcg-ls"\n'
                txt += 'which lcg-ls\n'
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
            ### FEDE to avoid some 70500 error ....
            txt += '    echo "ERROR ==> $RUNTIME_AREA/resultCopyFile file not found. Problem during the stageout"\n'
            txt += '    echo "RUNTIME_AREA content: " \n'
            txt += '    ls $RUNTIME_AREA \n'
            txt += '    job_exit_code=60318\n'
            txt += '    func_exit \n'
            txt += 'fi\n'
            ################################

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
            # set stageout timing to a fake value
            txt += 'export TIME_STAGEOUT=-1 \n'
        return txt

    def userName(self):
        """ return the user name """
        tmp=runCommand("eval `scram unsetenv -sh`; voms-proxy-info -identity 2>/dev/null")
        return tmp.strip()

    def configOpt_(self):
        edg_ui_cfg_opt = ' '
        if self.edg_config:
            edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo:
            edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        return edg_ui_cfg_opt



    def tags(self):
        task=common._db.getTask()
        tags_tmp=str(task['jobType']).split('"')
        tags=[str(tags_tmp[1]),str(tags_tmp[3])]
        return tags

