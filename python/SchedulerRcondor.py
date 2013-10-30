"""
Implements the vanilla (local) Remote Condor scheduler
"""

from SchedulerGrid  import SchedulerGrid
from crab_exceptions import CrabException
from crab_util import runCommand
from ServerConfig import *
from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser
import Scram


import common
import os
import socket
import re
import commands

# FUTURE: for python 2.4 & 2.6
try:
    from hashlib import sha1
except:
    from sha import sha as sha1

class SchedulerRcondor(SchedulerGrid) :
    """
    Class to implement the vanilla remote Condor scheduler
     Naming convention:  Methods starting with 'ws' provide
     the corresponding part of the job script
     ('ws' stands for 'write script').
    """

    def __init__(self):
        SchedulerGrid.__init__(self,"RCONDOR")

        self.datasetPath   = None
        self.selectNoInput = None
        self.OSBsize = 50*1000*1000 # 50 MB

        self.environment_unique_identifier = None

        return


    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """
        
#        task  = common._db.getTask()
#        #print task.__dict__
#
#        if task['serverName']!=None and task['serverName']!="":
#            # cast to string to avoid issues with unicode :-(
#            self.rcondorUserHost=str(task['serverName'])
#            common.logger.info("serverName from Task DB is %s" %
#                               self.rcondorUserHost)
#        else :
#            # get an rcondor host from config and save
#            common.logger.info("no serverName in Task DB, use env.var.")
#
#            self.rcondorHost   = os.getenv('RCONDOR_HOST')
#            if not self.rcondorHost :
#                raise CrabException('FATAL ERROR: env.var RCONDOR_HOST not defined')
#            self.rcondorUser = os.getenv('RCONDOR_USER')
#            if not self.rcondorUser :
#                common.logger.info("$RCONDOR_USER not defined, try to find out via uberftp ...")
#                command="uberftp $RCONDOR_HOST pwd|grep User|awk '{print $3}'"
#                (status, output) = commands.getstatusoutput(command)
#                if status == 0:
#                    self.rcondorUser = output
#                    common.logger.info("rcondorUser set to %s" % self.rcondorUser)
#                if self.rcondorUser==None:
#                    raise CrabException('FATAL ERROR: RCONDOR_USER not defined')
#
#                self.rcondorUserHost = self.rcondorUser + '@' + self.rcondorHost
#
#            print "will set server name to : ", self.rcondorUserHost
#            common._db.updateTask_({'serverName':self.rcondorUserHost})
#            print "ok"

        SchedulerGrid.configure(self, cfg_params)

        self.proxyValid=0
        self.dontCheckProxy=int(cfg_params.get("GRID.dont_check_proxy",0))
        self.space_token = cfg_params.get("USER.space_token",None)
        self.proxyServer= 'myproxy.cern.ch'
        self.group = cfg_params.get("GRID.group", None)
        self.role = cfg_params.get("GRID.role", None)
        self.VO = cfg_params.get('GRID.virtual_organization','cms')

        self.checkProxy()


        try:
            tmp =  cfg_params['CMSSW.datasetpath']
            if tmp.lower() == 'none':
                self.datasetPath = None
                self.selectNoInput = 1
            else:
                self.datasetPath = tmp
                self.selectNoInput = 0
        except KeyError:
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)

        if cfg_params.get('GRID.ce_black_list', None) or \
           cfg_params.get('GRID.ce_white_list', None) :
            msg="BEWARE: scheduler RGLIDEIN ignores CE black/white lists."
            msg+="\n Remove them from crab configuration to proceed."
            msg+="\n Use GRID.se_white_list and/or GRID.se_black_list instead"
            raise CrabException(msg)

        return
    
    def userName(self):
        """ return the user name """
        tmp=runCommand("voms-proxy-info -identity 2>/dev/null")
        return tmp.strip()

    def envUniqueID(self):
        taskHash = sha1(common._db.queryTask('name')).hexdigest()
        id = "https://" + socket.gethostname() + '/' + taskHash + "/${NJob}"
        return id

    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters. Used at crab -submit time
        by $CRABPYTHON/Scheduler.py
        """

#SB paste from crab ScheduerGlidein

        jobParams = ""

        (self.rcondorHost,self.rcondorUserHost) = self.pickRcondorSubmissionHost(task)

        seDest = task.jobs[i-1]['dlsDestination']

        if seDest == [''] :
            seDest = self.blackWhiteListParser.expandList("T") # all of SiteDB

        seString=self.blackWhiteListParser.cleanForBlackWhiteList(seDest)

        jobParams += '+DESIRED_SEs = "'+seString+'"; '

        scram = Scram.Scram(None)
        cmsVersion = scram.getSWVersion()
        scramArch  = scram.getArch()
        
        cmsver=re.split('_', cmsVersion)
        numericCmsVersion = "%s%.2d%.2d" %(cmsver[1], int(cmsver[2]), int(cmsver[3]))

        jobParams += '+DESIRED_CMSVersion ="' +cmsVersion+'";'
        jobParams += '+DESIRED_CMSVersionNr ="' +numericCmsVersion+'";'
        jobParams += '+DESIRED_CMSScramArch ="' +scramArch+'";'
        
        myscheddName = self.rcondorHost
        jobParams += '+Glidein_MonitorID = "https://'+ myscheddName  + '//$(Cluster).$(Process)"; '

        if (self.EDG_clock_time):
            jobParams += '+MaxWallTimeMins = '+self.EDG_clock_time+'; '
        else:
            jobParams += '+MaxWallTimeMins = %d; ' % (60*24)

        common._db.updateTask_({'jobType':jobParams})


        return jobParams


    def realSchedParams(self, cfg_params):
        """
        Return dictionary with specific parameters, to use with real scheduler
        is called when scheduler is initialized in Boss, i.e. at each crab command
        """
        #SB this method is used to pass directory names to Boss Scheduler
        # via params dictionary

        jobDir = common.work_space.jobDir()
        taskDir=common.work_space.topDir().split('/')[-2]
        shareDir = common.work_space.shareDir()
        
        params = {'shareDir':shareDir,
                  'jobDir':jobDir,
                  'taskDir':taskDir}

        return params


    def listMatch(self, seList, full):
        """
        Check the compatibility of available resources
        """

        return [True]


    def decodeLogInfo(self, fileName):
        """
        Parse logging info file and return main info
        """

        import CondorGLoggingInfo
        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(fileName)
        return reason


#    def wsCopyOutput(self):
#        """
#        Write a CopyResults part of a job script, e.g.
#        to copy produced output into a storage element.
#        """
#        txt = self.wsCopyOutput()
#        return txt


    def wsExitFunc(self):
        """
        Returns the part of the job script which runs prior to exit
        """

        txt = '\n'
        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    tmp_size=`ls -gGrta ${out_files}.tgz | awk \'{ print $3 }\'`\n'
        txt += '    rm ${out_files}.tgz\n'
        txt += '    size=`expr $tmp_size`\n'
        txt += '    echo "Total Output dimension: $size"\n'
        txt += '    limit='+str(self.OSBsize) +' \n'
        txt += '    echo "WARNING: output files size limit is set to: $limit"\n'
        txt += '    if [ "$limit" -lt "$size" ]; then\n'
        txt += '        exceed=1\n'
        txt += '        job_exit_code=70000\n'
        txt += '        echo "Output Sanbox too big. Produced output is lost "\n'
        txt += '    else\n'
        txt += '        exceed=0\n'
        txt += '        echo "Total Output dimension $size is fine."\n'
        txt += '    fi\n'

        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    if [ $exceed -ne 1 ]; then\n'
        txt += '        tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    else\n'
        txt += '        tar zcvf ${out_files}.tgz CMSSW_${NJob}.stdout CMSSW_${NJob}.stderr\n'
        txt += '    fi\n'
        txt += '    python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --errorcode $job_exit_code \n'

        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt


    def sched_fix_parameter(self):
        """
        Returns string with requirements and scheduler-specific parameters
        """

        if self.EDG_requirements:
            req = self.EDG_requirements
            taskReq = {'commonRequirements':req}
            common._db.updateTask_(taskReq)

    def pickRcondorSubmissionHost(self, task):
    
        task  = common._db.getTask()

        if task['serverName']!=None and task['serverName']!="":
            # rcondorHost is already defined and stored for this task
            # so pick it from DB
            # cast to string to avoid issues with unicode :-(
            rcondorUserHost=str(task['serverName'])
            common.logger.info("serverName from Task DB is %s" %
                               rcondorUserHost)
            if '@' in rcondorUserHost:
                rcondorHost = rcondorUserHost.split('@')[1]
            else:
                rcondorHost = rcondorUserHost
        else:
            if self.cfg_params.has_key('CRAB.submit_host'):
                # get an rcondor host from crab config file 
                srvCfg=ServerConfig(self.cfg_params['CRAB.submit_host']).config()
                rcondorHost=srvCfg['serverName']
                common.logger.info("rcondorhost from crab.cfg = %s" % rcondorHost)
            else:
                # pick from Available Servers List
                srvCfg=ServerConfig('default').config()
                print srvCfg
                rcondorHost = srvCfg['serverName']
                common.logger.info("rcondorhost from Avail.List = %s" % rcondorHost)

            if not rcondorHost:
                raise CrabException('FATAL ERROR: condorHost not defined')
                # fall back to env.
                #common.logger.info("no serverName in Task DB, use env.var.")
                #rcondorHost   = os.getenv('RCONDOR_HOST')
                #if not rcondorHost :
                #    raise CrabException('FATAL ERROR: env.var RCONDOR_HOST not defined')
            
            #rcondorUser = os.getenv('RCONDOR_USER')
            #if not rcondorUser :
            common.logger.info("try to find out RCONDOR_USER via uberftp ...")
            command="uberftp %s pwd|grep User|awk '{print $3}'" % rcondorHost
            (status, output) = commands.getstatusoutput(command)
            if status == 0:
                rcondorUser = output
                common.logger.info("rcondorUser set to %s" % rcondorUser)
                if rcondorUser==None:
                    raise CrabException('FATAL ERROR: RCONDOR_USER not defined')

            rcondorUserHost = rcondorUser + '@' + rcondorHost

        print "will set server name to : ", rcondorUserHost
        common._db.updateTask_({'serverName':rcondorUserHost})

        return (rcondorHost, rcondorUserHost)
