from SubmitterServer import SubmitterServer
from Resubmitter  import Resubmitter
import common
from crab_util import *

class ResubmitterServer(SubmitterServer, Resubmitter):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params

        self.copy_data = int(cfg_params.get('USER.copy_data',0))
        self.check_RemoteDir =  int(cfg_params.get('USER.check_user_remote_dir',0))

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

        if (jobs=='bad'):
            nj_list = self.checkBadJob(nj_list)
        else:
            nj_list = self.checkAllowedJob(jobs,nj_list)
       
        SubmitterServer.__init__(self, cfg_params, nj_list, 'range')
 
        return

        
