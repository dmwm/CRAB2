from Actor import *
import common
from datetime import datetime
from crab_report import run_upload
from crab_util import *
import os
import socket

class ReportUploader( Actor ):

    uploadFileServer = "http://analysisops.cern.ch/cmserrorreports"
    dashbtaskmon = 'http://dashb-cms-job-task.cern.ch/taskmon.html'
    #centralservermon = 'http://glidein-mon.t2.ucsd.edu:8080/dashboard/ajaxproxy.jsp?p='

    def __init__(self, cfg_params, jobid = -1): 
        """
        init class variables

         - checking if task exists
         - adding task generic files
         - adding job specific files
        """

        common.logger.debug('Initializing uploader...')

        self.cfg_params = cfg_params

        try:
            if common.scheduler.name().upper() != 'REMOTEGLIDEIN':
                CliServerParams(self)
            self.requestedserver = 'default'
            if self.cfg_params.has_key("CRAB.server_name"):
                self.requestedserver = self.cfg_params['CRAB.server_name']
            if self.cfg_params.has_key("CRAB.use_server"):
                if int(self.cfg_params['CRAB.use_server']) == 0:
                    self.server_name = 'No server'
        except Exception, ex:
            common.logger.debug("Problem '%s'" % str(ex))
            import traceback
            common.logger.debug(str(traceback.format_exc()))
            self.server_name = 'No server'
            self.requestedserver = 'No server'

        self.taskpath = common.work_space.topDir()

        if os.path.exists( self.taskpath ) is True:

            self.filetoup = [ \
                              common.work_space.shareDir() + '/crab.cfg', \
                              common.work_space.logDir() + '/crab.log' \
                            ] 

            if jobid > -1:
                self.filetoup.append( common.work_space.resDir() + 'CMSSW_%i.stdout' % jobid )
                self.filetoup.append( common.work_space.jobDir() + 'CMSSW_%i.LoggingInfo' % jobid )
                self.filetoup.append( common.work_space.resDir() + 'crab_fjr_%i.xml' % jobid )

        else:
            raise CrabException( 'Error: task [%s] not found in the path!' % self.taskname )


        self.taskname = common._db.queryTask('name')

        self.task = common._db.getTask()

        self.hostname = socket.getfqdn()

        self.username = getUserName()

        self.scheduler = common.scheduler.name()

        if (self.scheduler.upper() in 'REMOTEGLIDEIN') :
            self.server_name = str(self.task['serverName'])
            cmd = "gsissh %s pwd" % self.server_name
            self.remote_path = runCommand(cmd).rstrip()
        else :
            self.remote_path=''

        val = getCentralConfigLink('reportLogURL')
        if val is not None and len(val) > 0:
            self.uploadFileServer = val
        common.logger.debug('Using %s as remote repository server for uploading logs' % self )

        val = getCentralConfigLink('dashbTaskMon')
        if val is not None and len(val) > 0:
            self.dashbtaskmon = val
        common.logger.debug('Using %s as link for dashboard task monitoring' % self.dashbtaskmon )


    def __prepareMetadata( self, datafile ):
        """
        __prepareMetadata

        preparing metadata file content for errorreport server
        """

        fmeta = open(datafile, 'w')
        strmeta = 'username:%s\n' % self.username + \
                  'version:%s\n' % '%s_%s' % (common.prog_name.upper(), common.prog_version_str) + \
                  'jobuuid:%s\n' % self.taskname + \
                  'monitoringlink:Dashboard Task Mon,%s%s \n' %(self.dashbtaskmon,self.taskname) # + \
        if self.server_name != 'No server' and \
               self.scheduler.upper() != 'REMOTEGLIDEIN' :
            cserverStatus = 'http://%s:8888/visualog/?logtype=Status&taskname=%s\n' % (self.server_name, self.taskname)
            strmeta += 'monitoringlink:CrabServer Status,%s\n' % cserverStatus
            cserverLog = 'http://%s:8888/visualog/?logtype=Logging&taskname=%s\n' % (self.server_name, self.taskname)
            strmeta += 'monitoringlink:CrabServer Logging,%s\n' % cserverLog

        fmeta.write( strmeta )
        fmeta.close()

        common.logger.debug( "Metadatafile created as %s " % fmeta.name )

        return fmeta.name


    def __prepareSummary( self, datafile ):
        """
        __prepareSummary

        preparing Summary file for errorreport server
        """

        fsummary = open(datafile, 'w')
        ## version could be replaced by common.prog_name + common.prog_version_str
        fsummary.write(
                     'username:         %s\n' % self.username + \
                     'running on:       %s\n' % self.hostname + \
                     'version:          %s\n' % os.path.basename(os.path.normpath(os.getenv('CRABDIR'))) + \
                     'user working dir: %s\n' % self.taskname + \
                     'scheduler:        %s\n' % self.scheduler + \
                     'requested server: %s\n' % self.requestedserver + \
                     'used server:      %s\n' % self.server_name + \
                     'task:             %s\n' % self.taskname + \
                     'remote path:      %s\n' % (self.remote_path+'/'+self.taskname)
                   )
        fsummary.close()

        common.logger.debug( "Summary file created as %s " % fsummary.name )

        return fsummary.name

    def run( self ):
        """
        _run_

        Method that performs the upload with the various steps:
        - prepares metadata file
        - prepare summary info file
        - checks if the input files exists
        - prepares the package
        - uploads the package 
        """

        common.logger.info("Preparing directory and files to report...")

        archivereport = self.username + '-' + 'uploader.zip'
        common.logger.debug("Archivereport %s" % archivereport)

        basename = os.path.basename(os.path.normpath(self.taskpath))
        dirname = 'crabreport_' + (str(datetime.today())).replace(' ', '-')
        self.crabreportdir = self.taskpath + '/' + dirname

        cmd = 'cd %s; mkdir %s; cd %s; ' % (self.taskpath, dirname, dirname)
        common.logger.debug("Running '%s' " % cmd)
        out = runCommand( cmd )
        common.logger.debug("Result '%s'" % str(out))

        metadataFile = self.__prepareMetadata(self.crabreportdir + '/__metadata.txt')
        summaryFile = self.__prepareSummary(self.crabreportdir + '/_summary.txt')
        
        cmd = "cd %s;" % self.crabreportdir
        for filetemp in self.filetoup:
            if os.path.exists( filetemp ) is True :
                cmd += 'ln -s %s %s.txt; ' % ( filetemp, os.path.basename( filetemp ) )
                common.logger.debug("File %s found and added" % filetemp)
            else:
                common.logger.debug("File %s not found, skipping it!" % filetemp)

        common.logger.debug("Running '%s' " % cmd)
        out = runCommand( cmd )
        common.logger.debug("Result '%s'" % str(out))

        common.logger.debug("Zipping...")
        cmd = 'cd %s; zip -l %s %s/*' % (self.taskpath, archivereport, self.crabreportdir)
        common.logger.debug("Running '%s' " % cmd)
        out = runCommand( cmd )
        common.logger.debug("Result '%s'" % str(out))

        try:
           common.logger.info("Starting to upload the report...")
           link_result = run_upload(server = self.uploadFileServer, path = '%s/%s' % (self.taskpath, archivereport) )
           if link_result is not None and len(link_result) > 0:
               common.logger.info("Your report was uploaded to the central repository")
               common.logger.info("Please include this URL in your bug report or in the support e-mail")
               common.logger.info( "\t %s\n" % str(link_result))
           else:
               common.logger.error("A problem occurred while uploading your report to the central repository!")
        except Exception, ex:
           raise CrabException("Problem %s uploading log files to %s" % (str(ex), self.uploadFileServer) )
        finally:
           common.logger.debug("Start cleaning report...")
           cmd = 'rm -rf %s %s/%s' % (self.crabreportdir, self.taskpath, archivereport)
           common.logger.debug("Running '%s' " % cmd)
           out = runCommand( cmd )
           common.logger.debug("Result '%s'" % str(out))

        common.logger.info("Report upload finished.")

