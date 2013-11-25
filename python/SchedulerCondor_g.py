"""
CondorG specific parts of the scheduler
Non-specific code comes from SchedulerCondorCommon
"""

from SchedulerCondorCommon import SchedulerCondorCommon

__revision__ = "$Id: SchedulerCondor_g.py,v 1.109 2012/06/14 15:47:29 belforte Exp $"
__version__ = "$Revision: 1.109 $"

# All of the content moved to SchedulerCondorCommon.

class SchedulerCondor_g(SchedulerCondorCommon):
    """
    CondorG specific parts of the CondorG scheduler
    """


    def __init__(self):
        SchedulerCondorCommon.__init__(self,"CONDOR_G")
        self.OSBsize = None
        return


    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters
        """
        jobParams = SchedulerCondorCommon.sched_parameter(self, i, task)

        ceDest = self.seListToCElist(task.jobs[i-1]['dlsDestination'])

        if len(ceDest) == 1:
            # 06NOV2013 Bockjoo there is no gt2 in OSG anymore
            jobParams += "grid_resource = gt5 "+ceDest[0]+"; "
        else:
            jobParams += "schedulerList = "+','.join(ceDest)+"; "

        globusRSL = self.GLOBUS_RSL
        if (self.EDG_clock_time):
            globusRSL += '(maxWalltime='+self.EDG_clock_time+')'
        # 20NOV2013 Bockjoo: IT DOES NOT WORK
        # Users should know how to set CONDORG.globus_rsl
        # if not job will be submitted with the site default maxWalltime etc
        # Make sure not to interfere with set globus_rsl in the CONDORG section
        #else:
        #    globusRSL += '(maxWalltime=120)'
        

        if (globusRSL != ''):
            jobParams +=  'globusrsl = ' + globusRSL + '; '

        return jobParams
