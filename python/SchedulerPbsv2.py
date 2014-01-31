from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
import common
import pprint
import os,string

# PBS/torque interface for CRAB (dave.newbold@cern.ch, June 09)
#    - rewritten by Andrew Melo (andrew.m.melo@vanderbilt.ed, Mar 12)
#
# In the [CRAB] section of crab.cfg, use:
#
# scheduler = pbsv2
#
# In the [PBS] section of crab.cfg, use the following optional parameters
#
# queue= pbs_queue_to_use [default, use the default queue in your local PBS config]
# resources = resource_1=value,resource_2=value, etc [like qsub -l syntax]
# workernodebase = /tmp (top level directory where CRAB should unpack on worker nodes. $HOME is a bad default for many)
# hostname = hostname of submit host (only needed if you have trouble transferring stdout/stderr)
# forceTransferFiles - if you're not running on a shared filesystem, set this to 1 to make the scheduler move files for you
# NB: - the scheduler uses a wrapper script to create a local dir (see BossLite scheduler module)
# Both wrapper stdout/stderr and job script output files are placed in your crab_*/res directory by default
#

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerPbsv2(SchedulerLocal):

    def __init__(self):
        Scheduler.__init__(self, "PBSV2")


    def configure(self, cfg_params):
        self.use_proxy = 0
        SchedulerLocal.configure(self, cfg_params)
        queueConfigName = "%s.queue" % self.name().upper()
        if queueConfigName in cfg_params:
            if len(cfg_params[queueConfigNAme]) == 0 or cfg_params[queueConfigName] == "default":
                common.logger.info(" The default queue of local PBS configuration will be used")
        else:
            common.logger.info(" The default queue of local PBS configuration will be used")

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
        id = "https://"+common.scheduler.name()+":/${PBS_JOBID}-"+ \
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
        id = "https://"+common.scheduler.name()+":/${PBS_JOBID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

