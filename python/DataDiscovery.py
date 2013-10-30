#!/usr/bin/env python

__revision__ = "$Id: DataDiscovery.py,v 1.50 2013/09/12 13:45:22 belforte Exp $"
__version__ = "$Revision: 1.50 $"

import exceptions
import DBSAPI.dbsApi
from DBSAPI.dbsApiException import *
import common
from crab_util import *
try: # Can remove when CMSSW 3.7 and earlier are dropped
    from FWCore.PythonUtilities.LumiList import LumiList
except ImportError:
    from LumiList import LumiList

import os



class DBSError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)



class DBSInvalidDataTierError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)



class DBSInfoError:
    def __init__(self, url):
        print '\nERROR accessing DBS url : '+url+'\n'
        pass



class DataDiscoveryError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)



class NotExistingDatasetError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)



class NoDataTierinProvenanceError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)



class DataDiscovery:
    """
    Class to find and extact info from published data
    """
    def __init__(self, datasetPath, cfg_params, skipAnBlocks):

        #       Attributes
        self.datasetPath = datasetPath
        # Analysis dataset is primary/processed/tier/definition
        self.ads = len(self.datasetPath.split("/")) > 4
        self.cfg_params = cfg_params
        self.skipBlocks = skipAnBlocks

        self.eventsPerBlock = {}  # DBS output: map fileblocks-events for collection
        self.eventsPerFile = {}   # DBS output: map files-events
#         self.lumisPerBlock = {}   # DBS output: number of lumis in each block
#         self.lumisPerFile = {}    # DBS output: number of lumis in each file
        self.blocksinfo = {}      # DBS output: map fileblocks-files
        self.maxEvents = 0        # DBS output: max events
        self.maxLumis = 0         # DBS output: total number of lumis
        self.parent = {}          # DBS output: parents of each file
        self.lumis = {}           # DBS output: lumis in each file
        self.lumiMask = None
        self.splitByLumi = False
        self.splitDataByEvent = 0

    def fetchDBSInfo(self):
        """
        Contact DBS
        """
        ## get DBS URL
        global_url="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        dbs_url=  self.cfg_params.get('CMSSW.dbs_url', global_url)
        common.logger.info("Accessing DBS at: "+dbs_url)

        ## check if runs are selected
        runselection = []
        if (self.cfg_params.has_key('CMSSW.runselection')):
            runselection = parseRange2(self.cfg_params['CMSSW.runselection'])
            if len(runselection)>1000000:
                common.logger.info("ERROR: runselection range has more then 1M numbers")
                common.logger.info("ERROR: Too large. runselection is ignored")
                runselection=[]

        ## check if various lumi parameters are set
        self.lumiMask = self.cfg_params.get('CMSSW.lumi_mask',None)
        self.lumiParams = self.cfg_params.get('CMSSW.total_number_of_lumis',None) or \
                          self.cfg_params.get('CMSSW.lumis_per_job',None)

        lumiList = None
        if self.lumiMask:
            lumiList = LumiList(filename=self.lumiMask)
        if runselection:
            runList = LumiList(runs = runselection)

        self.splitByRun = int(self.cfg_params.get('CMSSW.split_by_run', 0))
        self.splitDataByEvent = int(self.cfg_params.get('CMSSW.split_by_event', 0))
        common.logger.log(10-1,"runselection is: %s"%runselection)

        if not self.splitByRun:
            self.splitByLumi = self.lumiMask or self.lumiParams or self.ads

        if self.splitByRun and not runselection:
            msg = "Error: split_by_run must be combined with a runselection"
            raise CrabException(msg)

        ## service API
        args = {}
        args['url']     = dbs_url
        args['level']   = 'CRITICAL'

        ## check if has been requested to use the parent info
        useparent = int(self.cfg_params.get('CMSSW.use_parent',0))

        ## check if has been asked for a non default file to store/read analyzed fileBlocks
        defaultName = common.work_space.shareDir()+'AnalyzedBlocks.txt'
        fileBlocks_FileName = os.path.abspath(self.cfg_params.get('CMSSW.fileblocks_file',defaultName))

        api = DBSAPI.dbsApi.DbsApi(args)
        self.files = self.queryDbs(api,path=self.datasetPath,runselection=runselection,useParent=useparent)

        # Check to see what the dataset is
        pdsName = self.datasetPath.split("/")[1]
        primDSs = api.listPrimaryDatasets(pdsName)
        dataType = primDSs[0]['Type']
        common.logger.debug("Datatype is %s" % dataType)
        if dataType == 'data' and not \
            (self.splitByRun or self.splitByLumi or self.splitDataByEvent):
            msg = 'Data must be split by lumi or by run. ' \
                  'Please see crab -help for the correct settings'
            raise  CrabException(msg)



        anFileBlocks = []
        if self.skipBlocks: anFileBlocks = readTXTfile(self, fileBlocks_FileName)

        # parse files and fill arrays
        for file in self.files :
            parList  = []
            fileLumis = [] # List of tuples
            # skip already analyzed blocks
            fileblock = file['Block']['Name']
            if fileblock not in anFileBlocks :
                filename = file['LogicalFileName']
                # asked retry the list of parent for the given child
                if useparent==1:
                    parList = [x['LogicalFileName'] for x in file['ParentList']]
                if self.splitByLumi:
                    fileLumis = [ (x['RunNumber'], x['LumiSectionNumber'])
                                 for x in file['LumiList'] ]
                self.parent[filename] = parList
                # For LumiMask, intersection of two lists.
                if self.lumiMask and runselection:
                    self.lumis[filename] = runList.filterLumis(lumiList.filterLumis(fileLumis))
                elif runselection:
                    self.lumis[filename] = runList.filterLumis(fileLumis)
                elif self.lumiMask:
                    self.lumis[filename] = lumiList.filterLumis(fileLumis)
                else:
                    self.lumis[filename] = fileLumis
                if filename.find('.dat') < 0 :
                    events    = file['NumberOfEvents']
                    # Count number of events and lumis per block
                    if fileblock in self.eventsPerBlock.keys() :
                        self.eventsPerBlock[fileblock] += events
                    else :
                        self.eventsPerBlock[fileblock] = events
                    # Number of events per file
                    self.eventsPerFile[filename] = events

                    # List of files per block
                    if fileblock in self.blocksinfo.keys() :
                        self.blocksinfo[fileblock].append(filename)
                    else :
                        self.blocksinfo[fileblock] = [filename]

                    # total number of events
                    self.maxEvents += events
                    self.maxLumis  += len(self.lumis[filename])

        if  self.skipBlocks and len(self.eventsPerBlock.keys()) == 0:
            msg = "No new fileblocks available for dataset: "+str(self.datasetPath)
            raise  CrabException(msg)


        if len(self.eventsPerBlock) <= 0:
            raise NotExistingDatasetError(("\nNo data for %s in DBS\nPlease check"
                                            + " dataset path variables in crab.cfg")
                                            % self.datasetPath)


    def queryDbs(self,api,path=None,runselection=None,useParent=None):


        allowedRetriveValue = []
        if self.splitByLumi or self.splitByRun or useParent == 1:
            allowedRetriveValue.extend(['retrive_block', 'retrive_run'])
        if self.splitByLumi:
            allowedRetriveValue.append('retrive_lumi')
        if useParent == 1:
            allowedRetriveValue.append('retrive_parent')
        common.logger.debug("Set of input parameters used for DBS query: %s" % allowedRetriveValue)
        try:
            if self.splitByRun:
                files = []
                for arun in runselection:
                    try:
                        if self.ads:
                            filesinrun = api.listFiles(analysisDataset=path,retriveList=allowedRetriveValue,runNumber=arun)
                        else:
                            filesinrun = api.listFiles(path=path,retriveList=allowedRetriveValue,runNumber=arun)
                        files.extend(filesinrun)
                    except:
                        msg="WARNING: problem extracting info from DBS for run %s "%arun
                        common.logger.info(msg)
                        pass

            else:
                if allowedRetriveValue:
                    if self.ads:
                        files = api.listFiles(analysisDataset=path, retriveList=allowedRetriveValue)
                    else :
                        files = api.listFiles(path=path, retriveList=allowedRetriveValue)
                else:
                    files = api.listDatasetFiles(self.datasetPath)

        except DbsBadRequest, msg:
            raise DataDiscoveryError(msg)
        except DBSError, msg:
            raise DataDiscoveryError(msg)

        return files


    def getMaxEvents(self):
        """
        max events
        """
        return self.maxEvents


    def getMaxLumis(self):
        """
        Return the number of lumis in the dataset
        """
        return self.maxLumis


    def getEventsPerBlock(self):
        """
        list the event collections structure by fileblock
        """
        return self.eventsPerBlock


    def getEventsPerFile(self):
        """
        list the event collections structure by file
        """
        return self.eventsPerFile


    def getFiles(self):
        """
        return files grouped by fileblock
        """
        return self.blocksinfo


    def getParent(self):
        """
        return parent grouped by file
        """
        return self.parent


    def getLumis(self):
        """
        return lumi sections grouped by file
        """
        return self.lumis


    def getListFiles(self):
        """
        return parent grouped by file
        """
        return self.files
