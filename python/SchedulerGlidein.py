"""
Glidein specific portions of the interface to the BossLite scheduler
"""

__revision__ = "$Id: SchedulerGlidein.py,v 1.19 2012/06/14 15:53:44 belforte Exp $"
__version__ = "$Revision: 1.19 $"

from SchedulerCondorCommon import SchedulerCondorCommon
import common

class SchedulerGlidein(SchedulerCondorCommon):
    """
    Glidein specific portions of the interface to the BossLite scheduler
    """

    def __init__(self):
        SchedulerCondorCommon.__init__(self,"GLIDEIN")
        self.OSBsize = 55*1000*1000 # 55MB
        self.environment_unique_identifier = '$Glidein_MonitorID'
        return


    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters
        """
        jobParams = SchedulerCondorCommon.sched_parameter(self, i, task)
        seDest = task.jobs[i-1]['dlsDestination']
        ceDest = self.seListToCElist(seDest, onlyOSG=False)
        ceString = ','.join(ceDest)

        jobParams += '+DESIRED_Gatekeepers = "'+ceString+'"; '
        jobParams += '+DESIRED_Archs = "INTEL,X86_64"; '
        jobParams += "Requirements = stringListMember(GLIDEIN_Gatekeeper,DESIRED_Gatekeepers) &&  stringListMember(Arch,DESIRED_Archs); "
        if (self.EDG_clock_time):
            jobParams += '+MaxWallTimeMins = '+self.EDG_clock_time+'; '
        else:
            jobParams += '+MaxWallTimeMins = 1440; '

        common._db.updateTask_({'jobType':jobParams})
        return jobParams # Not sure I even need to return anything


    def listMatch(self, seList, full, onlyOSG=False):
        """
        Check the compatibility of available resources
        """
        ceDest = SchedulerCondorCommon.listMatch(self, seList, full, onlyOSG=False)
        return ceDest


    def envUniqueID(self):
        msg = 'JobID for ML monitoring is created for Glidein scheduler: %s'%id
        common.logger.debug(msg)
        return '$Glidein_MonitorID'
