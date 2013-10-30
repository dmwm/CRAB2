from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator


class WorkflowHandler(Actor):
    def __init__(self, cfg_params):
        self.cfg_params = cfg_params

        # init client server params...
        CliServerParams(self)       

        return

    def run(self):
        """
        The main method of the class: WorkflowHandler 
        """
        common.logger.debug( "WorkflowStopper::run() called")

        taskuuid = str(common._db.queryTask('name'))
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)

        ret = csCommunicator.StopWorkflow( taskuuid )
        del csCommunicator
 
        if ret != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%ret
            raise CrabException(msg)

        common.logger.info("Stop Workflow request succesfully sent to the server\n" ) 

        return
                
