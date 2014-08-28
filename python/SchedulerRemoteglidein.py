"""
Implements the Remote Glidein scheduler
"""

from SchedulerGrid  import SchedulerGrid
from crab_exceptions import CrabException
from crab_util import runCommand
from crab_util import gethnUserNameFromSiteDB
from ServerConfig import *
from NodeNameUtils import *

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

class SchedulerRemoteglidein(SchedulerGrid) :
    """
    Class to implement the vanilla remote Condor scheduler
     for a glidein frontend, the created JDL will not work
     on valilla local condor.
     Naming convention:  Methods starting with 'ws' provide
     the corresponding part of the job script
     ('ws' stands for 'write script').
    """

    def __init__(self):
        SchedulerGrid.__init__(self,"REMOTEGLIDEIN")

        self.datasetPath   = None
        self.selectNoInput = None
        self.OSBsize = 50*1000*1000 # 50 MB

        self.environment_unique_identifier = None
        self.submissionDay = time.strftime("%y%m%d",time.localtime())
        
        return


    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """
   
        # this line needs to be before the call to SchedulerGrid.configure
        # because that calls SchedulerRemoteglidin in turn and
        # sshControlPersist needs to be defined then :-(
        self.sshControlPersist =  cfg_params.get('USER.ssh_control_persist','3600')
        if self.sshControlPersist.lower() == "no" or \
                self.sshControlPersist.lower() == "yes" or \
                self.sshControlPersist.isdigit() :
            pass
        else:
            msg = "Error: invalid value '%s' for USER.ssh_control_persist " % \
                self.sshControlPersist
            raise CrabException(msg)

        SchedulerGrid.configure(self, cfg_params)

        self.proxyValid=0
        self.dontCheckProxy=int(cfg_params.get("GRID.dont_check_proxy",'0'))
        self.space_token = cfg_params.get("USER.space_token",None)
        self.proxyServer= 'myproxy.cern.ch'
        self.group = cfg_params.get("GRID.group", None)
        self.role = cfg_params.get("GRID.role", None)
        self.VO = cfg_params.get('GRID.virtual_organization','cms')
        self.allowOverflow = cfg_params.get('GRID.allow_overflow', '1')
        self.max_rss = cfg_params.get('GRID.max_rss','2000')

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
            msg="BEWARE: scheduler REMOTEGLIDEIN ignores CE black/white lists."
            msg+="\n Remove them from crab configuration to proceed."
            msg+="\n Use GRID.se_white_list and/or GRID.se_black_list instead"
            raise CrabException(msg)


        # make sure proxy FQAN has not changed since last time
        command =  "eval `scram unsetenv -sh`; voms-proxy-info -identity -fqan 2>/dev/null"
        command += " | head -2"
        identity = runCommand(command)
        idfile = common.work_space.shareDir() + "GridIdentity"
        if os.access(idfile, os.F_OK) :
            # identity file exists from previous commands
            f=open(idfile, 'r')
            idFromFile=f.read()
            f.close()
        else :
            # create it
            f=open(idfile, 'w')
            f.write(identity)
            f.close()
            idFromFile = identity

        if identity != idFromFile:
            msg =  "Wrong Grid Credentials:\n%s" % identity
            msg += "\nMake sure you have "
            msg += " DN, FQAN =\n%s" % idFromFile
            raise CrabException(msg)

        return

#
    def envUniqueID(self):
        taskHash = sha1(common._db.queryTask('name')).hexdigest()
        id = "https://" + socket.gethostname() + '/' + taskHash + "/${NJob}"
        return id

    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters. Used at crab -submit time
        by $CRABPYTHON/Scheduler.py
        """

#SB paste from crab SchedulerGlidein

        jobParams = ""

        (self.remoteHost,self.remoteUserHost) = self.pickRemoteSubmissionHost(task)

        psnDest = task.jobs[i-1]['dlsDestination']

        if psnDest == [''] :
            # crab.cfg had datasetpath = None
            pnn2psn = getMapOfPhedexNodeName2ProcessingNodeNameFromSiteDB()
            allPSNs = set(pnn2psn.values())   # set removes duplicates
            psnDest = allPSNs

        blackList =  parseIntoList(self.cfg_params.get("GRID.se_black_list", []))
        whiteList =  parseIntoList(self.cfg_params.get("GRID.se_white_list", []))

        #raise Exception
        #a=1/0
        
        psnDest = cleanPsnListForBlackWhiteLists(psnDest, blackList, whiteList)
        if not psnDest or psnDest == [] or psnDest == ['']:
            msg = "No Processing Site Name after applying black/white list."
            msg += " Can't submit"
            common.logger.info(msg)
            raise CrabException(msg)

        msg = "list of PSN's for submission: %s" % psnDest
        common.logger.info(msg)

        jobParams += '+DESIRED_Sites = "%s";' % psnDest

        #raise Exception
        #a=1/0


        scram = Scram.Scram(None)
        cmsVersion = scram.getSWVersion()
        scramArch  = scram.getArch()
        
        cmsver=re.split('_', cmsVersion)
        if cmsver[3]=='DEVEL': cmsver[3]='0'
        numericCmsVersion = "%s%.2d%.2d" %(cmsver[1], int(cmsver[2]), int(cmsver[3]))

        if "slc5" in scramArch:
            cmsOpSys = "LINUX"
            cmsOpSysMajorVer = "5,6"  # SL5 exe's also run on SL6
        if "slc6" in scramArch:
            cmsOpSys = "LINUX"
            cmsOpSysMajorVer = "6"
        if "ia32" in scramArch:
            cmsArch = "INTEL,X86_64" # 32bit exe's also run on 64bit
        if "amd64" in scramArch:
            cmsArch = "X86_64"

        # protect against datasetPath being None
        jobParams += '+DESIRED_CMSDataset ="' + str(self.datasetPath) + '";'
            
        jobParams += '+DESIRED_CMSVersion ="' +cmsVersion+'";'
        jobParams += '+DESIRED_CMSVersionNr ="' +numericCmsVersion+'";'
        jobParams += '+DESIRED_CMSScramArch ="' +scramArch+'";'
        jobParams += '+DESIRED_OpSyses ="' +cmsOpSys+'";'
        jobParams += '+DESIRED_OpSysMajorVers ="' +cmsOpSysMajorVer+'";'
        jobParams += '+DESIRED_Archs ="' +cmsArch+'";'

        userName = gethnUserNameFromSiteDB()
        jobParams += '+AccountingGroup ="' + userName+'";'
        
        myscheddName = self.remoteHost

        jobParams += '+Glidein_MonitorID = "https://'+ myscheddName + \
                     '//' + self.submissionDay + '//$(Cluster).$(Process)"; '

        if (self.EDG_clock_time):
            glideinTime = "%d" % (int(self.EDG_clock_time)+20) # 20 min to wrapup
            jobParams += '+MaxWallTimeMins = '+ glideinTime + '; '
        else:
            jobParams += '+MaxWallTimeMins = %d; ' % (21*60+55) #  21:55h  (unit = min)


        if self.max_rss :
            jobParams += 'request_memory = '+self.max_rss+';'

        if self.allowOverflow == "0":
            jobParams += '+CMS_ALLOW_OVERFLOW = False; '

        if self.EDG_addJdlParam:
            if self.EDG_addJdlParam[-1] == '':
                self.EDG_addJdlParam = self.EDG_addJdlParam[:-1]
            for p in self.EDG_addJdlParam:
                jobParams += p.strip()+';\n'

        common._db.updateTask_({'jobType':jobParams})

        return jobParams


    def realSchedParams(self, cfg_params):
        """
        Return dictionary with specific parameters, to use with real scheduler
        is called when scheduler is initialized in Boss, i.e. at each crab command
        """
        #SB this method is used to pass informatinos to Boss Scheduler
        # via params dictionary

        jobDir = common.work_space.jobDir()
        taskDir=common.work_space.topDir().split('/')[-2]
        shareDir = common.work_space.shareDir()
        
        params = {'shareDir':shareDir,
                  'jobDir':jobDir,
                  'taskDir':taskDir,
                  'submissionDay':self.submissionDay,
                  'sshControlPersist':self.sshControlPersist}

        return params


    def listMatch(self, seList, full):
        """
        Check the compatibility of available resources
        """
        blackList = parseIntoList(self.cfg_params.get("GRID.se_black_list", []))
        whiteList = parseIntoList(self.cfg_params.get("GRID.se_white_list", []))

        if seList == ['']: # datasetpath=None in crab.cfg any site wil do
            psnDest = [True]
        else:
            psnDest = cleanPsnListForBlackWhiteLists(seList, blackList, whiteList)

        return psnDest


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

    def pickRemoteSubmissionHost(self, task):
    
        task  = common._db.getTask()

        if task['serverName']!=None and task['serverName']!="":
            # remoteHost is already defined and stored for this task
            # so pick it from DB
            # cast to string to avoid issues with unicode :-(
            remoteUserHost=str(task['serverName'])
            common.logger.info("serverName from Task DB is %s" %
                               remoteUserHost)
        else:
            if self.cfg_params.has_key('CRAB.submit_host'):
                # get a remote submission host from crab config file 
                srvCfg=ServerConfig(self.cfg_params['CRAB.submit_host']).config()
                remoteUserHost=srvCfg['serverName']
                common.logger.info("remotehost from crab.cfg = %s" % remoteUserHost)
            else:
                # pick from Available Servers List
                srvCfg=ServerConfig('default').config()
                remoteUserHost = srvCfg['serverName']
                common.logger.info("remotehost from Avail.List = %s" % remoteUserHost)

            if not remoteUserHost:
                raise CrabException('FATAL ERROR: remoteHost not defined')

        if '@' in remoteUserHost:
            remoteHost = remoteUserHost.split('@')[1]
        else:
            remoteHost = remoteUserHost

        common._db.updateTask_({'serverName':remoteUserHost})

        return (remoteHost, remoteUserHost)
