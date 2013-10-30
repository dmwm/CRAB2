from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from Status import Status

from xml.parsers.expat import *

import zlib

class StatusServer(Status):

    def __init__(self, *args):

        Status.__init__(self, *args) 
 
        # init client server params...
        CliServerParams(self)       

        return

    def query(self,display=True):

        warning_msg = self.resynchClientSide()
        
        upTask = common._db.getTask()  
        self.compute(upTask,display)

    def resynchClientSide(self):
        """
        get status from the server and
        aling back data on client
        """ 
        task = common._db.getTask()
        self.task_unique_name = str(task['name'])

        # communicator allocation
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)

        # align back data and print
        reportXML = None
        warning_msg = None

        max_get_status_tries = 5
        for retry in xrange(max_get_status_tries):
            if retry > 0 :
                delay = pow(2,retry-1)
                common.logger.info("Server status decoding problem. Try again in %d seconds"%delay)
                runCommand("/bin/sleep %d"%delay)
            handledXML = csCommunicator.getStatus( self.task_unique_name )
            reportXML, warning_msg = self.statusDecoding(handledXML)
            if reportXML is not None and warning_msg is None:
                break
            common.logger.debug(warning_msg)


        if warning_msg is not None:
            warning_msg = "WARNING: Unable to decompress status from server. Please issue crab -status again"
            common.logger.info(warning_msg)
            return warning_msg

        try:
            xmlStatus = minidom.parseString(reportXML)
            reportList = xmlStatus.getElementsByTagName('Job')
            common._db.deserXmlStatus(reportList)
        except Exception, e:
            warning_msg = "WARNING: Unable to extract status from XML file. Please issue crab -status again"
            common.logger.info(warning_msg)
            common.logger.debug("DUMP STATUS XML: %s"%str(reportXML))
            common.logger.debug( str(e) )
            common.logger.debug( traceback.format_exc() )
            return warning_msg

        return warning_msg

    def statusDecoding(self, handledXML):
        import base64
        import urllib
        reportXML, warning_msg = None, None

        # WS channel
        paddedXML =''
        try:
            paddedXML = base64.urlsafe_b64decode(handledXML)
        except Exception, e:
            warning_msg = "WARNING: Problem while decoding base64 status. %s" % traceback.format_exc()

        #padding 
        paddedXML += "="*( len(handledXML)%4 )
        if warning_msg is not None:
            warning_msg = None
            try:
                paddedXML = base64.urlsafe_b64decode(paddedXML)
            except Exception, e:
                warning_msg = "WARNING: Padding fallback failed: %s" % traceback.format_exc()
                return reportXML, warning_msg

        # decompression
        try:
            reportXML = zlib.decompress(paddedXML)
            warning_msg = None
        except Exception, e:
            reportXML = None
            warning_msg = "WARNING: Problem while decompressing status from the server: %s"%str(e)

        if warning_msg is None:
            return reportXML, warning_msg
        common.logger.debug("StatusServer failed: "+warning_msg + '\n' + traceback.format_exc() )

        # HTTP channel not usable since firewalled
#        try:
#            xmlStatusURL  = 'http://%s:8888/visualog/'%self.server_name
#            xmlStatusURL += '?taskname=%s&logtype=Xmlstatus'%common._db.queryTask('name')
#            common.logger.debug("Accessing URL for status fallback: %s"%xmlStatusURL)
#            reportXML = ''.join(urllib.urlopen(xmlStatusURL).readlines())
#            warning_msg = None
#        except Exception, e:
#            reportXML = None
#            warning_msg = "WARNING: Unable to retrieve status from server. Please issue crab -status again"
#            common.logger.debug(warning_msg + '\n' + traceback.format_exc() )
#
        return reportXML, warning_msg 

    def showWebMon(self):
        msg  = 'You can also check jobs status at: http://%s:8888/logginfo\n'%self.server_name
        msg += '\t( Your task name is: %s )\n'%common._db.queryTask('name') 
        common.logger.debug(msg)
        #common.logger.info("Web status at: http://%s:8888/visualog/?taskname=%s&logtype=Status\n"%(self.server_name,self.task_unique_name))
