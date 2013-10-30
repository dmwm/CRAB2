"""
Scheduler implementation used by CondorG and Glidein (Common parts)
This class was originally SchedulerCondor_g.
For a history of this code, see that file.
"""

from SchedulerGrid import SchedulerGrid
from crab_exceptions import CrabException
from crab_util import runCommand

from ProdCommon.BDII.Bdii import getJobManagerList, listAllCEs
from WMCore.SiteScreening.BlackWhiteListParser import CEBlackWhiteListParser

import Scram
import CondorGLoggingInfo
import common

#import popen2
import os

# FUTURE: for python 2.4 & 2.6
try:
    from hashlib import sha1
except:
    from sha import sha as sha1

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.45 2013/01/17 14:50:18 belforte Exp $"
__version__ = "$Revision: 1.45 $"

class SchedulerCondorCommon(SchedulerGrid):
    """
    Scheduler implementation used by CondorG and Glidein (Common parts)
    """

    def __init__(self, name):
        SchedulerGrid.__init__(self, name)
        self.environment_unique_identifier = None
        self.msgPre = '[Condor-G Scheduler]: '

        return


    def configure(self, cfgParams):
        """
        Configure the scheduler with the config settings from the user
        """
        # FIXME: Get rid of try/except and use get() instead

        if not os.environ.has_key('EDG_WL_LOCATION'):
            # This is an ugly hack needed for SchedulerGrid.configure() to
            # work!
            os.environ['EDG_WL_LOCATION'] = ''

        if not os.environ.has_key('X509_USER_PROXY'):
            # Set X509_USER_PROXY to the default location.  We'll do this
            # because in functions called by Scheduler.checkProxy()
            # voms-proxy-info will be called with '-file $X509_USER_PROXY',
            # so if X509_USER_PROXY isn't set, it won't work.
            os.environ['X509_USER_PROXY'] = '/tmp/x509up_u' + str(os.getuid())

        SchedulerGrid.configure(self, cfgParams)
        if cfgParams.get('CRAB.server_name',None) or cfgParams.get('CRAB.use_server',None):
            pass
        else:
            self.checkCondorSetup()

        # init BlackWhiteListParser
        ceWhiteList = cfgParams.get('GRID.ce_white_list',[])
        ceBlackList = cfgParams.get('GRID.ce_black_list',[])
        self.ceBlackWhiteListParser = \
            CEBlackWhiteListParser(ceWhiteList, ceBlackList, common.logger())

        try:
            self.GLOBUS_RSL = cfgParams['CONDORG.globus_rsl']
        except KeyError:
            self.GLOBUS_RSL = ''

        # Provide an override for the batchsystem that condor_g specifies
        # as a grid resource. This is to handle the case where the site
        # supports several batchsystem but bdii only allows a site
        # to public one.
        try:
            self.batchsystem = cfgParams['CONDORG.batchsystem']
            msg = self.msgPre + 'batchsystem overide specified in your crab.cfg'
            common.logger.debug(msg)
        except KeyError:
            self.batchsystem = ''

        self.datasetPath = ''

        try:
            tmp =  cfgParams['CMSSW.datasetpath']
            if tmp.lower() == 'none':
                self.datasetPath = None
                self.selectNoInput = 1
            else:
                self.datasetPath = tmp
                self.selectNoInput = 0
        except KeyError:
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)

        return

    def envUniqueID(self):
        taskHash = sha1(common._db.queryTask('name')).hexdigest()
        id = 'https://' + self.name() + '/' + taskHash + '/${NJob}'
        msg = 'JobID for ML monitoring is created for OSG scheduler: %s'%id
        common.logger.debug( msg)
        return id

    def realSchedParams(self, cfgParams):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """

        tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
        jobDir = common.work_space.jobDir()
        params = {'tmpDir':tmpDir,
                  'jobDir':jobDir}
        return  params


    def sched_parameter(self, i, task):
        """
        Returns scheduler-specific parameters
        """
        jobParams = ''

        return jobParams


    def decodeLogInfo(self, theFile):
        """
        Parse logging info file and return main info
        """

        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(theFile)
        return reason


    def listMatch(self, seList, full, onlyOSG=True):
        """
        Check the compatibility of available resources
        """

        scram = Scram.Scram(None)
        versionCMSSW = scram.getSWVersion()
        arch = scram.getArch()

        if self.selectNoInput:
            availCEs = listAllCEs(versionCMSSW, arch, onlyOSG=onlyOSG)
        else:
            seDest = self.blackWhiteListParser.cleanForBlackWhiteList(seList, "list")
            availCEs = getJobManagerList(seDest, versionCMSSW,
                                         arch, onlyOSG=onlyOSG)

        uniqCEs = []
        for ce in availCEs:
            if ce not in uniqCEs:
                uniqCEs.append(ce)

        ceDest = self.ceBlackWhiteListParser.cleanForBlackWhiteList(uniqCEs, "list")

        return ceDest

    def ce_list(self):
        """
        Returns string with requirement CE related, dummy for now
        """

        req = ''

        return req, self.EDG_ce_white_list, self.EDG_ce_black_list

    def seListToCElist(self, seList, onlyOSG=True):
        """
        Convert the list of SEs into a list of CEs
        """

        ceDest = self.listMatch(seList, onlyOSG)

        if (not ceDest):
            msg = 'No sites found hosting the data or ' \
                + 'all sites blocked by CE/SE white/blacklisting'
            print msg
            raise CrabException(msg)

        return ceDest


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


    def checkCondorSetup(self):
        """
        Check the local machine for a properly set up and running condor install
        """

        # check for locally running schedd
        cmd = 'ps xau | grep -i condor_schedd | grep -v grep'
        cmdOut = runCommand(cmd)
        if cmdOut == None:
            msg  = self.msgPre + 'condor_schedd is not running on this machine.\n'
            msg += self.msgPre + 'Please use a machine with condor installed and running\n'
            msg += self.msgPre + 'condor_schedd or change the Scheduler in your crab.cfg.'
            common.logger.debug( msg)
            raise CrabException(msg)

        self.checkExecutableInPath('condor_q')
        self.checkExecutableInPath('condor_submit')
        self.checkExecutableInPath('condor_version')

        # get version number
        cmd = 'condor_version'
        cmdOut = runCommand(cmd)
        if cmdOut != None :
            pass
            #tmp = cmdOut.find('CondorVersion') + 15
            #version = cmdOut[tmp:tmp+6].split('.')
            #version_master = int(version[0])
            #version_major  = int(version[1])
            #version_minor  = int(version[2])
        else :
            msg  = self.msgPre + 'condor_version was not able to determine the installed condor version.\n'
            msg += self.msgPre + 'Please use a machine with a properly installed condor\n'
            msg += self.msgPre + 'or change the Scheduler in your crab.cfg.'
            common.logger.debug( msg)
            raise CrabException(msg)

        self.checkExecutableInPath('condor_config_val')
        self.checkCondorVariablePointsToFile('GRIDMANAGER')
        self.checkCondorVariablePointsToFile('GT2_GAHP', alternateName='GAHP')
        self.checkCondorVariablePointsToFile('GRID_MONITOR')
        self.checkCondorVariableIsTrue('ENABLE_GRID_MONITOR')

        return

    def checkExecutableInPath(self, name):
        """
        check if executable is in PATH
        """

        cmd = 'which '+name
        cmdOut = runCommand(cmd)
        if cmdOut == None:
            msg  = self.msgPre + name + ' is not in the $PATH on this machine.\n'
            msg += self.msgPre + 'Please use a machine with a properly installed condor\n'
            msg += self.msgPre + 'or change the Scheduler in your crab.cfg.'
            common.logger.debug( msg)
            raise CrabException(msg)

    def checkCondorVariablePointsToFile(self, name, alternateName=None):
        """
        check for condor variable
        """

        cmd = 'condor_config_val ' + name
        cmdOut = runCommand(cmd)
        if alternateName and not cmdOut:
            cmd = 'condor_config_val ' + alternateName
            cmdOut = runCommand(cmd)
        if cmdOut:
            cmdOut = cmdOut.strip()
        if not cmdOut or not os.path.isfile(cmdOut) :
            msg  = self.msgPre + 'the variable ' + name
            msg += ' is not properly set for the condor installation.\n'
            msg += self.msgPre + 'Please ask the administrator of the local condor '
            msg += 'installation  to set the variable ' + name + ' properly, '
            msg += 'use another machine with a properly installed condor\n'
            msg += 'or change the Scheduler in your crab.cfg.'
            common.logger.debug( msg)
            raise CrabException(msg)

    def checkCondorVariableIsTrue(self, name):
        """
        check for condor variable
        """

        cmd = 'condor_config_val '+name
        cmdOut = runCommand(cmd)
        if cmdOut == 'TRUE' :
            msg  = self.msgPre + 'the variable ' + name
            msg += ' is not set to true for the condor installation.\n'
            msg += self.msgPre + 'Please ask the administrator of the local condor installation '
            msg += 'to set the variable ' + name + ' to true, '
            msg += 'use another machine with a properly installed condor or '
            msg += 'change the Scheduler in your crab.cfg.'
            common.logger.debug( msg)
            raise CrabException(msg)

    def queryCondorVariable(self, name, default):
        """
        check for condor variable
        """

        cmd = 'condor_config_val '+name
        cmdOut = runCommand(cmd)
        if cmdOut:
       # out = popen2.Popen3(cmd, 1)
       # exitCode = out.wait()
       # cmdOut = out.fromchild.readline().strip()
       # if exitCode != 0 :
            cmdOut = str(default)

        return cmdOut

