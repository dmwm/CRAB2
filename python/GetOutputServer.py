"""
Get output for server mode
"""

__revision__ = "$Id: GetOutputServer.py,v 1.51 2010/07/27 15:56:13 ewv Exp $"
__version__ = "$Revision: 1.51 $"

from GetOutput import GetOutput
from StatusServer import StatusServer
from crab_util import *
import common
import time

from ServerCommunicator import ServerCommunicator

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

class GetOutputServer( GetOutput, StatusServer ):

    def __init__(self, *args):

        GetOutput.__init__(self, *args)

        # init client server params...
        CliServerParams(self)

        if self.storage_path[0] != '/':
            self.storage_path = '/'+self.storage_path

        self.copyTout= setLcgTimeout()
        if common.scheduler.name().upper() in ['LSF', 'CAF']:
            self.copyTout= ' '

        return


    def getOutput(self):

        # get updated status from server #inherited from StatusServer
        warning_msg = self.resynchClientSide()
        if warning_msg is not None:
            common.logger.info(warning_msg)


        # understand whether the required output are available
        self.checkBeforeGet()

        # retrive files
        filesAndJodId = { }

        filesAndJodId.update( self.retrieveFiles(self.list_id) )
        common.logger.debug( "Files to be organized and notified " + str(filesAndJodId))

        # load updated task
        task = common._db.getTask()

        self.organizeOutput( task, self.list_id )

        self.notifyRetrievalToServer(filesAndJodId)
        return

    def retrieveFiles(self, filesToRetrieve):
        """
        Real get output from server storage
        """

        self.taskuuid = str(common._db.queryTask('name'))
        common.logger.debug( "Task name: " + self.taskuuid)

        # create the list with the actual filenames
        remotedir = os.path.join(self.storage_path, self.taskuuid)

        # list of file to retrieve
        osbTemplate = remotedir + '/out_files_%s.tgz'
        osbFiles = [osbTemplate % str(jid) for jid in filesToRetrieve]
        if self.cfg_params['CRAB.scheduler'].lower() in ["condor_g"]:
            osbTemplate = remotedir + '/CMSSW_%s.stdout'
            osbFiles.extend([osbTemplate % str(jid) for jid in filesToRetrieve])
            osbTemplate = remotedir + '/CMSSW_%s.stderr'
            osbFiles.extend([osbTemplate % str(jid) for jid in filesToRetrieve])
        common.logger.debug( "List of OSB files: " +str(osbFiles) )

        copyHere = self.outDir
        destTemplate = copyHere+'/out_files_%s.tgz'
        destFiles = [ destTemplate % str(jid) for jid in filesToRetrieve ]
        if self.cfg_params['CRAB.scheduler'].lower() in ["condor_g"]:
            destTemplate = copyHere + '/CMSSW_%s.stdout'
            destFiles.extend([destTemplate % str(jid) for jid in filesToRetrieve])
            destTemplate = copyHere + '/CMSSW_%s.stderr'
            destFiles.extend([destTemplate % str(jid) for jid in filesToRetrieve])

        common.logger.info("Starting retrieving output from server "+str(self.storage_name)+"...")

        try:
            seEl = SElement(self.storage_name, self.storage_proto, self.storage_port)
        except Exception, ex:
            common.logger.debug( str(ex))
            msg = "ERROR : Unable to create SE source interface \n"
            raise CrabException(msg)
        try:
            loc = SElement("localhost", "local")
        except Exception, ex:
            common.logger.debug( str(ex))
            msg = "ERROR : Unable to create destination  interface \n"
            raise CrabException(msg)

        ## copy ISB ##
        sbi = SBinterface(seEl, loc, logger = common.logger.logger)

        filesAndJodId = {}

        if self.storage_proto in ['globus']:
            # construct a list of absolute paths of input files
            # and the destinations to copy them to
            sourcesList = []
            destsList = []
            for i in xrange(len(osbFiles)):
                sourcesList.append(osbFiles[i])
                destsList.append(destFiles[i])
                #if i < len(filesToRetrieve):
                #    filesAndJodId[ filesToRetrieve[i] ] = osbFiles[i]

            # construct logging information
            toCopy = "\n".join([t[0] + " to " + t[1] for t in map(None, sourcesList, destsList)]) + "\n"
            common.logger.debug("Retrieving:\n " + toCopy)

            # try to do the copy
            copy_res = None
            try:
                copy_res = sbi.copy( sourcesList, destsList, opt="tout=300")
            except Exception, ex:
                msg = "WARNING: Unable to retrieve output file %s \n" % osbFiles[i]
                msg += str(ex)
                common.logger.debug(msg)
                import traceback
                common.logger.debug( str(traceback.format_exc()) )
            if copy_res is not None:
                ## evaluating copy results
                copy_err_list = []
                count = 0
                for ll in map(None, copy_res, sourcesList):
                    exitcode = int(ll[0][0])
                    if exitcode == 0:
                        filesAndJodId[ filesToRetrieve[count] ] = osbFiles[count]
                    else:
                        copy_err_list.append( [ ll[1], ll[0][1] ] )
                    count += 1
                if len(copy_err_list) > 0:
                    msg = "ERROR : Unable to retrieve output file \n"
                    for problem in copy_err_list:
                        msg += "              Problem transferring [%s]:  '%s'\n" %(problem[0],problem[1])
        else:
            # retrieve them from SE
            for i in xrange(len(osbFiles)):
                source = osbFiles[i]
                dest = destFiles[i]
                common.logger.debug( "retrieving "+ str(source) +" to "+ str(dest) )
                try:
                    sbi.copy( source, dest , opt=self.copyTout)
                    if i < len(filesToRetrieve):
                        filesAndJodId[ filesToRetrieve[i] ] = dest
                except Exception, ex:
                    msg = "WARNING: Unable to retrieve output file %s \n" % osbFiles[i]
                    msg += str(ex)
                    common.logger.debug(msg)
                    import traceback
                    common.logger.debug( str(traceback.format_exc()) )
                    continue

        return filesAndJodId

    def notifyRetrievalToServer(self, fileAndJobList):
        retrievedFilesJodId = []

        for jid in fileAndJobList:
            if not os.path.exists(fileAndJobList[jid]):
                # it means the file has been untarred
                retrievedFilesJodId.append(jid)

        common.logger.debug( "List of retrieved files notified to server: %s"%str(retrievedFilesJodId) )

        # notify to the server that output have been retrieved successfully. proxy from StatusServer
        if len(retrievedFilesJodId) > 0:
            csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
            try:
                csCommunicator.outputRetrieved(self.taskuuid, retrievedFilesJodId)
            except Exception, e:
                msg = "Client Server comunication failed about outputRetrieved: jobs "+(str(retrievedFilesJodId))
                common.logger.debug( msg)
                pass
        return

