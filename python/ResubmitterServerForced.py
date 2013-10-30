from SubmitterServer import SubmitterServer
from ResubmitterForced  import ResubmitterForced
import common
from crab_util import *

class ResubmitterServerForced(SubmitterServer, ResubmitterForced):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params

        nj_list = []

        # get updated status from server 
        try:
            from StatusServer import StatusServer
            stat = StatusServer(self.cfg_params)
            warning_msg = stat.resynchClientSide()
            if warning_msg is not None:
                common.logger.info(warning_msg)
        except:
            pass

        nj_list = self.checkAllowedJob(jobs,nj_list)
       
        SubmitterServer.__init__(self, cfg_params, nj_list, 'range')
 
        return

        

