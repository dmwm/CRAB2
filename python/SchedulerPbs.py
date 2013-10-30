from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
import common

import os,string

# PBS/torque interface for CRAB (dave.newbold@cern.ch, June 09)
#
#
# In the [CRAB] section of crab.cfg, use:
#
# scheduler = pbs
#
# In the [PBS] section of crab.cfg, use the following optional parameters
#
# queue= pbs_queue_to_use [default, use the default queue in your local PBS config]
# resources = resource_1=value,resource_2=value, etc [like qsub -l syntax]
# wnBase = /tmp (top level directory where CRAB should unpack on worker nodes. $HOME is a bad default for many)
#
# NB: - the scheduler uses a wrapper script to create a local dir (see BossLite scheduler module)
# Both wrapper stdout/stderr and job script output files are placed in your crab_*/res directory by default
#

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerPbs(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"PBS")
        self.OSBsize = None

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)
        if "PBS.queue" in cfg_params:
            if len(cfg_params["PBS.queue"]) == 0 or cfg_params["PBS.queue"] == "default":
                common.logger.info(" The default queue of local PBS configuration will be used")
        else:
            common.logger.info(" The default queue of local PBS configuration will be used")

        return


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
        for s in ('resources', 'queue'):
            params[s] = cfg_params.get( self.name().upper()+'.'+s,'' )

        params['workDir'] = cfg_params.get(self.name().upper()+'.wnbase','')

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
        s.append('tar zcvf '+common.work_space.resDir()+'${out_files}.tgz ${filesToCheck}')
        s.append('exit $job_exit_code')
        s.append('}')
        return '\n'.join(s)

    def envUniqueID(self):
        id = "https://"+common.scheduler.name()+":/${PBS_JOBID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

