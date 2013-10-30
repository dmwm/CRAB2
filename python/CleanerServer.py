from Actor import *
from crab_exceptions import *
from crab_util import *
import common
import string
from ServerCommunicator import ServerCommunicator
from StatusServer import StatusServer
from Cleaner import Cleaner

class CleanerServer(Cleaner):

    def __init__(self, cfg_params):
        """
        constructor
        """
        Cleaner.__init__(self, cfg_params)
        self.cfg_params = cfg_params

        # init client server params...
        CliServerParams(self)
        return

    def run(self):
        # get updated status from server
        try:
            stat = StatusServer(self.cfg_params)
            warning_msg = stat.resynchClientSide()
            if warning_msg is not None:
                common.logger.info(warning_msg)
        except:
            pass
        
        # check whether the action is allowable
        self.check()

        # notify the server to clean the task 
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
        taskuuid = str(common._db.queryTask('name'))

        try:
            csCommunicator.cleanTask(taskuuid)
        except Exception, e:
            msg = "Client Server comunication failed about cleanJobs: task \n" + taskuuid
            msg += "Only local working directory will be removed."
            common.logger.debug( msg)
            pass

        # TODO remove these lines once the integration completed
        msg=''
        msg+='functionality not yet available for the server. Work in progres \n'
        msg+='only local working directory will be removed'
        common.logger.info(msg)
        # TODO - end

        # remove local structures
        common.work_space.delete()
        print 'directory '+common.work_space.topDir()+' removed'
        return

