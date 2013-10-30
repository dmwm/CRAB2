from PostMortem import PostMortem

from crab_util import *
import common
import string, os

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

class PostMortemServer(PostMortem):
    def __init__(self, cfg_params, nj_list):

        PostMortem.__init__(self, cfg_params, nj_list)

        # init client server params...
        CliServerParams(self)

        self.copyTout= setLcgTimeout()
        if common.scheduler.name().upper() in ['LSF', 'CAF']:
            self.copyTout= ' '

        if self.storage_path[0]!='/':
            self.storage_path = '/'+self.storage_path

        return

    def collectLogging(self):
        # get updated status from server
        try:
            from StatusServer import StatusServer
            stat = StatusServer(self.cfg_params)
            warning_msg = stat.resynchClientSide()
            if warning_msg is not None:
                common.logger.info(warning_msg)
        except:
            pass

        #create once storage interaction object
        seEl = None
        loc = None
        try:
            seEl = SElement(self.storage_name, self.storage_proto, self.storage_port)
        except Exception, ex:
            common.logger.debug( str(ex))
            msg = "ERROR: Unable to create SE source interface \n"
            raise CrabException(msg)
        try:
            loc = SElement("localhost", "local")
        except Exception, ex:
            common.logger.debug( str(ex))
            msg = "ERROR: Unable to create destination interface \n"
            raise CrabException(msg)

        ## coupling se interfaces
        sbi = SBinterface(seEl, loc, logger = common.logger.logger)

        ## get the list of jobs to get logging.info skimmed by failed status
        logginable = self.skimDeadList()

        if self.storage_proto in ['globus']:
            for id in self.nj_list:
                if id not in self.all_jobs:
                    common.logger.info('Warning: job # ' + str(id) + ' does not exist! Not possible to ask for postMortem ')
                elif id not in logginable:
                    common.logger.info('Warning: job # ' + str(id) + ' not killed or aborted! Will get loggingInfo manually ')
                    PostMortem.collectOneLogging(self,id)
            # construct a list of absolute paths of input files
            # and the destinations to copy them to
            sourcesList = []
            destsList = []
            self.taskuuid = str(common._db.queryTask('name'))
            common.logger.debug( "Starting globus retrieval for task name: " + self.taskuuid)
            remotedir = os.path.join(self.storage_path, self.taskuuid)
            for i in logginable:
                remotelog = remotedir + '/loggingInfo_'+str(i)+'.log'
                sourcesList.append(remotelog)
                fname = self.fname_base + str(i) + '.LoggingInfo'
                destsList.append(fname)

            # try to do the copy
            copy_res = None
            try:
                copy_res = sbi.copy( sourcesList, destsList, opt=self.copyTout)
            except Exception, ex:
                msg = "WARNING: Unable to retrieve logging" #ainfo file %s \n" % osbFiles[i]
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
                        ## decode logging info
                        fl = open(destsList[count], 'r')
                        out = "".join(fl.readlines())
                        fl.close()
                        reason = self.decodeLogging(out)
                        common.logger.info('Logging info for job '+ str(logginable[count]) +': '+str(reason)+'\n      written to '+str(destsList[count])+' \n' )
                    else:
                        common.logger.info('Logging info for job '+ str(logginable[count]) +' not retrieved. Tring to get loggingInfo manually')
                        PostMortem.collectOneLogging(self,logginable[count])
                    count += 1
        else:
            ## iter over each asked job and print warning if not in skimmed list
            for id in self.nj_list:
                if id not in self.all_jobs:
                    common.logger.info('Warning: job # ' + str(id) + ' does not exist! Not possible to ask for postMortem ')
                    continue
                elif id in logginable:
                    fname = self.fname_base + str(id) + '.LoggingInfo'
                    if os.path.exists(fname):
                        common.logger.info('Logging info for job ' + str(id) + ' already present in '+fname+'\nRemove it for update')
                        continue
                    ## retrieving & processing logging info
                    if self.retrieveFile( sbi, id, fname):
                        ## decode logging info
                        fl = open(fname, 'r')
                        out = "".join(fl.readlines())
                        fl.close()
                        reason = self.decodeLogging(out)
                        common.logger.info('Logging info for job '+ str(id) +': '+str(reason)+'\n      written to '+str(fname)+' \n' )
                    else:
                        common.logger.info('Logging info for job '+ str(id) +' not retrieved. Tring to get loggingInfo manually')
                        PostMortem.collectOneLogging(self,id)
                else:
                    common.logger.info('Warning: job # ' + str(id) + ' not killed or aborted! Will get loggingInfo manually ')
                    PostMortem.collectOneLogging(self,id)
        return


    def skimDeadList(self):
        """
        __skimDeadList__
        return the list of jobs really failed: K, A
        """
        skimmedlist = []
        self.up_task = common._db.getTask( self.nj_list )
        for job in self.up_task.jobs:
            if job.runningJob['status'] in ['K','A']:
                skimmedlist.append(job['jobId'])
        return skimmedlist

    def retrieveFile(self, sbi, jobid, destlog):
        """
        __retrieveFile__

        retrieves logging.info file from the server storage area
        """
        self.taskuuid = str(common._db.queryTask('name'))
        common.logger.debug( "Task name: " + self.taskuuid)

        # full remote dir
        remotedir = os.path.join(self.storage_path, self.taskuuid)
        remotelog = remotedir + '/loggingInfo_'+str(jobid)+'.log'

        common.logger.info("Starting retrieving logging-info from server " \
                               + str(self.storage_name) + " for job " \
                               + str(jobid) + "...")

        # retrieve logging info from storage
        common.logger.debug( "retrieving "+ str(remotelog) +" to "+ str(destlog) )
        try:
            sbi.copy( remotelog, destlog)
        except Exception, ex:
            msg = "WARNING: Unable to retrieve logging-info file %s \n"%remotelog
            msg += str(ex)
            common.logger.debug(msg)
            return False
        # cleaning remote logging info file
        try:
            common.logger.debug( "Cleaning remote file [%s] " %( str(remotelog) ) )
            sbi.delete(remotelog)
        except Exception, ex:
            msg = "WARNING: Unable to clean remote logging-info file %s \n"%remotelog
            msg += str(ex)
            common.logger.debug(msg)
        return True
