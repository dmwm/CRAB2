from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
import common
import pprint
import os,string


class SchedulerSlurm(SchedulerLocal):

    def __init__(self):
        Scheduler.__init__(self, "SLURM")


    def configure(self, cfg_params):
        self.use_proxy = 0
        SchedulerLocal.configure(self, cfg_params)
        queueConfigName = "%s.queue" % self.name().upper()
        if queueConfigName in cfg_params:
            if len(cfg_params[queueConfigName]) == 0 or cfg_params[queueConfigName] == "default":
                common.logger.info(" The default queue of local SLURM configuration will be used")
        else:
            common.logger.info(" The default queue of local SLURM configuration will be used")

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
        self.VO = cfg_params.get('GRID.virtual_organization','cms')
        self.OSBsize = 0
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

        if int(cfg_params.get(self.name().upper() + '.use_proxy',1)):
            common.logger.info("Using a proxy")
            self.use_proxy = 1
            self.dontCheckProxy = 0
        else:
            common.logger.info("Not using a proxy")
            self.dontCheckProxy = 1
            self.use_proxy = 0
            self.dontCheckMyProxy = 1

        if self.use_proxy:
            self.checkProxy()

    def envUniqueID(self):
        id = "https://"+common.scheduler.name()+":/${SLURM_JOBID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """
        params={'jobScriptDir':common.work_space.jobDir(),
                'jobResDir':common.work_space.resDir()
               }

		# update parameters
        for s in ('resources', 'queue', 'workernodebase', 'hostname', 'forcetransferfiles', 'grouplist'):
            params[s] = cfg_params.get( self.name().upper()+'.'+s,'' )

        return params

    def listMatch(self, dest, full):
        return [str(getLocalDomain(self))]

    def wsCopyOutput(self):
        return self.wsCopyOutput_comm()

    def wsExitFunc(self):
        """
        """
        s=[]
        s.append('func_exit(){')
        s.append(self.wsExitFunc_common())
        s.append('tar zcvf ${out_files}.tgz ${filesToCheck}')
        s.append('exit $job_exit_code')
        s.append('}')
        return '\n'.join(s)

    def envUniqueID(self):
        id = "https://"+common.scheduler.name()+":/${SLURM_JOBID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id
