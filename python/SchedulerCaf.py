from Scheduler import Scheduler
from SchedulerLsf import SchedulerLsf
from crab_exceptions import *
import common

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerCaf(SchedulerLsf) :

    def __init__(self):
        SchedulerLsf.__init__(self)
        Scheduler.__init__(self,"CAF")
        self.OSBsize = 55*1000*1000  # 55 MB

        return

    def configure(self, cfg_params):
        """
        CAF is just a special queue and resources for LSF at CERN
        """
        SchedulerLsf.configure(self, cfg_params)
        self.queue = cfg_params.get(self.name().upper()+'.queue','cmscaf1nw')
        self.res = cfg_params.get(self.name().upper()+'.resource','"type==SLC5_64 || type==SLC4_64"')
        self.group = cfg_params.get(self.name().upper()+'.group', None)

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        sched_param= ''

        if (self.queue):
            sched_param += '-q '+self.queue +' '
        if (self.res): sched_param += ' -R '+self.res +' '
        if (self.group): sched_param += ' -G '+str(self.group).upper() +' '
        return sched_param

    def wsSetupEnvironment(self):
        #Returns part of a job script which does scheduler-specific work.
        txt = SchedulerLsf.wsSetupEnvironment(self)
        txt += '# CAF specific stuff\n'
        txt += 'echo "----- ENV CAF BEFORE sourcing /afs/cern.ch/cms/caf/setup.sh  -----"\n'
        txt += 'echo "CMS_PATH = $CMS_PATH"\n'
        txt += 'echo "STAGE_SVCCLASS = $STAGE_SVCCLASS"\n'
        txt += 'echo "STAGER_TRACE = $STAGER_TRACE"\n' 
        txt += 'source /afs/cern.ch/cms/caf/setup.sh \n'
        txt += '\n'
        txt += 'echo "----- ENV CAF AFTER sourcing /afs/cern.ch/cms/caf/setup.sh  -----"\n'
        txt += 'echo "CMS_PATH = $CMS_PATH"\n'
        txt += 'echo "STAGE_SVCCLASS = $STAGE_SVCCLASS"\n'
        txt += 'echo "STAGER_TRACE = $STAGER_TRACE"\n' 
        txt += '\n'
        return txt

    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'
        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    cp *.${LSB_BATCH_JID}.out CMSSW_${NJob}.stdout \n'
        txt += '    cp *.${LSB_BATCH_JID}.err CMSSW_${NJob}.stderr \n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'
        return txt
