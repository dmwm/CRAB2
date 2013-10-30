#!/usr/bin/env python
"""
_ServerCommunicator_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "farinafa@cern.ch"

from crab_exceptions import *
from crab_util import *
import common
import Scram
from ProdCommon.Credential.CredentialAPI import CredentialAPI
from xml.dom import minidom
import os, sys
import commands
import traceback
from Downloader import Downloader

if (sys.version_info[0] + .1 * sys.version_info[1]) < 2.6 : 
    from CRAB_Server_API import CRAB_Server_Session as C_AS_Session
elif os.environ["SCRAM_ARCH"].split("_")[1].find('32')>1:
    try:
        from CRAB_Server_API_1_1 import CRAB_Server_Session as C_AS_Session
    except :
        try:
            from CRAB_Server_API_36X import CRAB_Server_Session as C_AS_Session 
        except :
            from CRAB_Server_API_311X import CRAB_Server_Session as C_AS_Session 
else:
    try:
        from lib64.CRAB_Server_API_311X_amd64 import CRAB_Server_Session as C_AS_Session
    except:
        try:
            from lib64.CRAB_Server_API_5X_amd64 import CRAB_Server_Session as C_AS_Session
        except: 
            from lib64.CRAB_Server_API_6X_amd64 import CRAB_Server_Session as C_AS_Session
 
class ServerCommunicator:
    """
    Common interface for the interaction between the Crab client and the server Web Service
    """
    def __init__(self, serverName, serverPort, cfg_params, proxyPath=None):
        """
        Open the communication with an Analysis Server by passing the server URL and the port
        """

        self.ServerTwiki = 'https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabServerForUsers#Server_available_for_users'

        self.asSession = C_AS_Session(serverName, serverPort)
        self.cfg_params = cfg_params
        self.userSubj = ''
        self.serverName = serverName
        credentialType = 'Proxy'
        if common.scheduler.name().upper() in ['CAF','LSF']:
            credentialType = 'Token'
        CliServerParams(self)
        self.crab_task_name = common.work_space.topDir().split('/')[-2] # nice task name "crab_0_..."

        configAPI = {'credential' : credentialType, \
                     'logger' : common.logger() }

        CredAPI =  CredentialAPI( configAPI )
        try:
            self.userSubj = CredAPI.getSubject()
        except Exception, err:
            common.logger.debug("Getting Credential Subject: " +str(traceback.format_exc()))
            raise CrabException("Error Getting Credential Subject")

        self.scram=Scram.Scram(cfg_params)
###################################################
    # Interactions with the server
###################################################

# wmbs
    def submitNewTask(self, blTaskName, blXml, rng, TotJob,taskType='fullySpecified'):
        """
        _submitNewTask_
        Send a new task to the server to be submitted.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """


        if not blXml:
            raise CrabException('Error while serializing task object to XML')
            return -2

        if not blTaskName:
            raise CrabException('Error while extracting the Task Unique Name string')
            return -3

        cmdXML = None
        cmdXML = self._createXMLcommand(blTaskName, 'submit', rng, newTaskAddIns=True,jobs=TotJob,type=taskType)

        if not cmdXML:
            raise CrabException('Error while creating the Command XML')
            return -4

        ret = -1
        ret = self.asSession.transferTaskAndSubmit(blXml, cmdXML, blTaskName)

        if ret == 0:
             # success
             logMsg = 'Task %s successfully submitted to server %s'%(self.crab_task_name, self.serverName)
             common.logger.info(logMsg+'\n')
        else:
             self.checkServerResponse(ret)
             # reset server choice
             opsToBeSaved={'serverName' : '' }
             common._db.updateTask_(opsToBeSaved)

        return ret

    def checkServerResponse(self, ret):
        """
        analyze the server return codes
        """

        logMsg = ''
        if ret == 10:
            # overlaod
            logMsg = 'Error The server %s refused to accept the task %s because it is overloaded\n'%(self.serverName, self.crab_task_name)
            logMsg += '\t For Further infos please contact the server Admin: %s'%self.server_admin
        elif ret == 14:
            # Draining
            logMsg  = 'Error The server %s refused to accept the task %s because it is Draining out\n'%(self.serverName, self.crab_task_name)
            logMsg += '\t remaining jobs due to scheduled maintainence\n'
            logMsg += '\t For Further infos please contact the server Admin: %s'%self.server_admin
        elif ret == 101:
            # overlaod
            logMsg = 'Error The server %s refused the submission %s because you asked a too large task. Please submit by range'%(self.serverName, self.crab_task_name)
        elif ret == 11:
            # failed to push message in DB
            logMsg = 'Server unable to release messages into DB. Task %s won\'t be submitted.'%self.crab_task_name
        elif ret == 12:
            # failed SOAP communication
            logMsg = 'Error The server %s refused to accept the task %s. It could be under maintainance. \n'%(self.serverName, self.crab_task_name)
            logMsg += '\t For Further infos please contact the server Admin: %s'%self.server_admin
        elif ret == 20:
            # failed to push message in PA
            logMsg = 'Server unable to release messages to other components. Task %s won\'t be submitted.'%self.crab_task_name
        elif ret == 22:
            # failed SOAP communication
            logMsg = 'Error during SOAP communication with server %s'%self.serverName
        elif ret == 33:
            # uncompatible client version
            logMsg  = 'Error You are using a wrong client version for server: %s\n'%self.serverName
            logMsg += '\t For further informations about "Servers available for users" please check here:\n \t%s '%self.ServerTwiki
        else:
            logMsg = 'Unexpected return code from server %s: %d'%(self.serverName, ret)

        # print loggings
        if logMsg != '':
            common.logger.info(logMsg)
        return ret

    def subsequentJobSubmit(self, blTaskName, rng):
        """
        _subsequentJobSubmit_
        Let the submission of other jobs of a task that has been already sent to a server.
        This method is used for subsequent submission of ranged sets of jobs.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        
        return self._genericCommand('submit', blTaskName, rng)

    def killJobs(self, blTaskName, rng):
        """
        _killJobs_
        Send a kill command to one or more jobs running on the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('kill', blTaskName, rng)

    def StopWorkflow(self, blTaskName):
        """
        _StopWorkflow_
        """
        return self._genericCommand('StopWorkflow', blTaskName, 'all')

    def cleanTask(self, blTaskName):
        """
        _cleanTask_
        Force the server to clean the jobs on the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('clean', blTaskName, 'all')

    def getStatus(self, blTaskName, statusFile=None, statusFamilyType='status'):
        """
        _getStatus_
        Retrieve the task status from the server. It can recover any kind of status (version, loggingInfos,...)
        """

        # fill the filename
        filename = str(statusFile)

        if not blTaskName:
            raise CrabException('Exception while getting the task unique name')
            return ''

        # get the data and fill the file content
        statusMsg = self.asSession.getTaskStatus(statusFamilyType, blTaskName)
        if 'Error:' in  statusMsg[:6] or len(statusMsg)==0:
             if statusFamilyType != 'isServerDrained':
                 raise CrabException('Error in retrieving task %s status from server %s'%(self.crab_task_name, self.serverName) )
             else:
                 statusFile = None
                 statusMMsg = ""

        if statusFile is not None:
            f = open(statusFile, 'w')
            f.write(statusMsg)
            f.close()
            return statusFile
        return statusMsg

    def outputRetrieved(self, blTaskName, rng):
        """
        _getJobsOutput_
        Get from the server the output file locations to be transfered back.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('outputRetrieved', blTaskName, rng)

    def checkDrainMode(self, blTaskName='null'):
        return self.getStatus( blTaskName, statusFamilyType='isServerDrained')

    def postMortemInfos(self, blTaskName, rng):
        """
        _postMortemInfos_
        Retrieve the job postmortem information from the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        # get the status in
        raise NotImplementedError
        return None

###################################################
    # Auxiliary methods
###################################################

    def _createXMLcommand(self, taskUName, cmdSpec='status', rng='all', newTaskAddIns=False, flavour='analysis', type='fullySpecified',jobs='-1'):

        xmlString = ''
        cfile = minidom.Document()

        ver = common.prog_version_str
        node = cfile.createElement("TaskCommand")
        node.setAttribute("Task", str(taskUName) )
        node.setAttribute("Subject", str(self.userSubj) )
        node.setAttribute("Command", str(cmdSpec) )
        node.setAttribute("Range", str(rng) )
        node.setAttribute("TotJob", str(jobs) )
        node.setAttribute("Scheduler", str(self.cfg_params['CRAB.scheduler']) )
        node.setAttribute("Flavour", str(flavour) )
        node.setAttribute("Type", str(type) )
        node.setAttribute("ClientVersion", str(ver) )

        ## Only Temporary. it should be at Server level
        removeBList = self.cfg_params.get("GRID.remove_default_blacklist", 0 )
        blackAnaOps = "None.Nowhere" 
        if int(removeBList) == 0:
            blacklist = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/")
            result = blacklist.config("site_black_list.conf")
            if result != None:
                blackAnaOps = result
            common.logger.debug("Enforced black list: %s "%str(blacklist))
        else:
            common.logger.info("WARNING: Skipping default black list!")

        try:
            myproxyserver = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/").config("myproxy_server.conf")
            myproxyserver = myproxyserver.strip()
            if myproxyserver is None:
                raise CrabException("myproxy_server.conf retrieved but empty")
        except Exception, e:
            common.logger.info("Problem setting myproxy server endpoint: using myproxy.cern.ch")
            common.logger.debug(e)
            myproxyserver = 'myproxy.cern.ch'

        # create a mini-cfg to be transfered to the server
        miniCfg = {}

        ## migrate CE/SE infos
        miniCfg['EDG.ce_white_list'] = ""
        if 'GRID.ce_white_list' in self.cfg_params:
            miniCfg['EDG.ce_white_list'] = str( self.cfg_params['GRID.ce_white_list'] )

        miniCfg['EDG.ce_black_list'] = blackAnaOps
        if 'GRID.ce_black_list' in self.cfg_params:
            if blackAnaOps:
                miniCfg['EDG.ce_black_list'] += ", "
            miniCfg['EDG.ce_black_list'] += str( self.cfg_params['GRID.ce_black_list'] )

        miniCfg['EDG.se_white_list'] = ""
        if 'GRID.se_white_list' in self.cfg_params:
            miniCfg['EDG.se_white_list'] = str( self.cfg_params['GRID.se_white_list'] )

        miniCfg['EDG.se_black_list'] = ""
        if 'GRID.se_black_list' in self.cfg_params:
            miniCfg['EDG.se_black_list'] = str( self.cfg_params['GRID.se_black_list'] )

        miniCfg['EDG.group'] = ""
        if 'GRID.group' in self.cfg_params:
            miniCfg['EDG.group'] = str( self.cfg_params['GRID.group'] )

        miniCfg['EDG.role'] = ""
        if 'GRID.role' in self.cfg_params:
            miniCfg['EDG.role'] = str( self.cfg_params['GRID.role'] )

        miniCfg['cfgFileNameCkSum'] = makeCksum(common.work_space.cfgFileName())
        if 'cfgFileNameCkSum' in self.cfg_params:
            miniCfg['cfgFileNameCkSum'] = str(self.cfg_params['cfgFileNameCkSum'])

        miniCfg['CRAB.se_remote_dir'] = ''
        if 'CRAB.se_remote_dir' in self.cfg_params:
            miniCfg['CRAB.se_remote_dir'] = str(self.cfg_params['CRAB.se_remote_dir'])

        miniCfg['CAF.queue'] = self.cfg_params.get('CAF.queue','cmscaf1nw')
        miniCfg['CAF.resources'] = self.cfg_params.get('CAF.resource', 'cmscaf')
        miniCfg['CAF.group'] = self.cfg_params.get('CAF.group', None)
        ## JDL requirements specific data. Scheduler dependant
        miniCfg['EDG.max_wall_time'] = self.cfg_params.get('GRID.max_wall_clock_time', None)
        miniCfg['EDG.max_cpu_time'] = self.cfg_params.get('GRID.max_cpu_time', '130')
        miniCfg['proxyServer'] = myproxyserver 
        miniCfg['VO'] = self.cfg_params.get('GRID.virtual_organization', 'cms')
        miniCfg['EDG_retry_count'] = 0
        miniCfg['EDG_shallow_retry_count'] = -1

        ## Additional field for DashBoard
        miniCfg['CMSSW.datasetpath'] = self.cfg_params.get('CMSSW.datasetpath', 'None')

        ###################################################################################
        ### FEDE for tasktype, savannah 76950 
        miniCfg['USER.tasktype'] = self.cfg_params.get("USER.tasktype", 'analysis')
        ###################################################################################
        ## Additional fields for Notification by the server
        miniCfg['eMail'] = self.cfg_params.get('USER.email', None)
        miniCfg['threshold'] = self.cfg_params.get('USER.thresholdlevel', 100)

        miniCfg['CMSSW_version'] = self.scram.getSWVersion()
#WMBS
        miniCfg['feeder'] = self.cfg_params.get('WMBS.feeder', 'Feeder')
        miniCfg['parent_task'] = self.cfg_params.get('WMBS.parent_task', '')

        miniCfg['processing'] = self.cfg_params.get('WMBS.processing', 'bulk')
        miniCfg['startrun'] = self.cfg_params.get('WMBS.startrun', 'None')

        miniCfg['splitting_algorithm'] = self.cfg_params.get('WMBS.splitting_algorithm','FileBased')
        miniCfg['split_per_job'] = self.cfg_params.get('WMBS.split_per_job','files_per_job')
        miniCfg['split_value'] = self.cfg_params.get('WMBS.split_value',1000000)

        miniCfg['CMSSW_version'] = self.scram.getSWVersion()

        ## put here other fields if needed
        node.setAttribute("CfgParamDict", str(miniCfg) )
        cfile.appendChild(node)
        xmlString += str(cfile.toprettyxml())
        return xmlString

    def _genericCommand(self, cmd, blTaskName, rng):
        if not blTaskName:
            raise CrabException('Error while extracting the Task Unique Name string')
            return -2

        cmdXML = None
        cmdXML = self._createXMLcommand(blTaskName, cmd, rng)
        if not cmdXML:
            raise CrabException('Error while creating the Command XML')
            return -3

        ret = -1
        ret = self.asSession.sendCommand(cmdXML, blTaskName)
        logMsg = ''
        debugMsg = ''
        if ret == 0:
             # success
             debugMsg = 'Command successfully sent to server %s for task %s'%(self.serverName, self.crab_task_name)
        else:
             self.checkServerResponse(ret)
        return ret

