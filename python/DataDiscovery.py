#!/usr/bin/env python

__revision__ = "$Id: DataDiscovery.py,v 1.50 2013/09/12 13:45:22 belforte Exp $"
__version__ = "$Revision: 1.50 $"

import exceptions
import common
from crab_util import *
try: # Can remove when CMSSW 3.7 and earlier are dropped
    from FWCore.PythonUtilities.LumiList import LumiList
except ImportError:
    from LumiList import LumiList

import os
import urlparse


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
        # make assumption that same host won't be used for both
        # this check should catch most deployed servers
        DBS2HOST = 'cmsdbsprod.cern.ch'
        DBS3HOST = 'cmsweb.cern.ch'
        useDBS2 = False
        useDBS3 = False
        useDAS = False

        ## get DBS URL
        global_dbs2="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        global_dbs3="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"

        if self.cfg_params.get('CMSSW.use_dbs3'):
            useDBS3 = int(self.cfg_params.get('CMSSW.use_dbs3'))==1

        if useDBS3:
            dbs_url=  self.cfg_params.get('CMSSW.dbs_url', global_dbs3)
        else:
            dbs_url=  self.cfg_params.get('CMSSW.dbs_url', global_dbs2)
            
        common.logger.info("Accessing DBS at: "+dbs_url)

        endpoint_components = urlparse.urlsplit(dbs_url)

        if endpoint_components.hostname == DBS3HOST:
            useDBS3=True
        elif endpoint_components.hostname == DBS2HOST:
            useDBS2=True

        if useDBS2 and useDBS3:
            print "trying to use DBS2 and DBS3 at same time ?"
            raise exception

        if self.cfg_params.get('CMSSW.use_das'):
            useDAS = int(self.cfg_params.get('CMSSW.use_das'))==1

        #if self.cfg_params.get('CMSSW.use_dbs3'):
        #    useDBS3 = int(self.cfg_params.get('CMSSW.use_dbs3'))==1
        #    if useDBS3 : useDBS2=False

        if useDBS2:
            common.logger.info("Will do Data Discovery using  DBS2")
        if useDBS3:
            common.logger.info("Will do Data Discovery using  DBS3")
            localScopeDBS3 = not "global" in dbs_url
        if useDAS :
            common.logger.info("will use DAS to talk to DBS")


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

        if useDBS2 :
            import DBSAPI.dbsApi
            #from DBSAPI.dbsApiException import *
            import DBSAPI.dbsApiException
            api = DBSAPI.dbsApi.DbsApi(args)
            self.files = self.queryDbs(api,path=self.datasetPath,runselection=runselection,useParent=useparent)
        elif useDBS3 :
            from dbs.apis.dbsClient import DbsApi
            api = DbsApi(dbs_url)
            self.files = self.queryDbs3(api,path=self.datasetPath,runselection=runselection,useParent=useparent,localScopeDBS3=localScopeDBS3)
        elif useDAS :
            self.files = self.queryDas(path=self.datasetPath,runselection=runselection,useParent=useparent)

        # Check to see what the dataset is
        pdsName = self.datasetPath.split("/")[1]
        if useDBS2 :
            primDSs = api.listPrimaryDatasets(pdsName)
            dataType = primDSs[0]['Type']
        elif useDBS3 :
            dataType=api.listDataTypes(dataset=self.datasetPath)[0]['data_type']

        common.logger.info("Datatype is %s" % dataType)
        if dataType == 'data' and not \
            (self.splitByRun or self.splitByLumi or self.splitDataByEvent):
            msg = 'Data must be split by lumi or by run. ' \
                  'Please see crab -help for the correct settings'
            raise  CrabException(msg)



        anFileBlocks = []
        if self.skipBlocks: anFileBlocks = readTXTfile(self, fileBlocks_FileName)

        print "SB+++++++++++++++++++++++++++++++++++++++++++"
        print self.files[0]['ParentList']
        print "SB+++++++++++++++++++++++++++++++++++++++++++"

        # parse files and fill arrays
        for file in self.files :
            parList  = []
            fileLumis = [] # List of tuples
            # skip already analyzed blocks
            fileblock = file['Block']['Name']
            #print "SB fileblock = ", fileblock
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

        print "WILL RETURN FILES OF LENGTH ", len(files)
        
        return files


    def queryDbs3(self,api,path=None,runselection=None,useParent=None,
                  localScopeDBS3=False):
        

        files=[]  # structure to return, named "files" in the caller
        lfnList=[] # running list of files processed for this dataset
        lfn2files={} # keep a map of lfn to entry in files lfn[i]=file[i]['lfn']
        blocks=[]

        #if runselection:
        #    result=api.listBlocks(dataset=self.datasetPath, run_num=runselection)
        #else:
        result=api.listBlocks(dataset=self.datasetPath)

        for block in result:
            blockName = block['block_name']
            print "SB blockName: ", blockName

            # get list of files, and for each #events and parent, then
            # will get filelumis


        #    if runselection:
        #        filesInBlock=api.listFiles(block_name=blockName, detail=True, run_num=runselection)
        #    else:
            filesInBlock=api.listFiles(block_name=blockName, detail=True)
            for file in filesInBlock:
                lfn=file['logical_file_name']
                # prepare entry for files structure for this file
                # LumiList will be filled later as lumidics for this file
                # are found in DBS3 query output and parsed
                fileEntry={}
                fileEntry['LogicalFileName']=lfn
                fileEntry['NumberOfEvents']=file['event_count']
                fileEntry['Block']={}
                fileEntry['Block']['Name']=blockName
                fileEntry['Block']['StorageElementList']=[]  # needed so that can extend in Splitter.py
                #if localScopeDBS3:
                #    fileEntry['Block']['StorageElementList'].append(block['origin_site_name'])
                fileEntry['LumiList']=[]
                fileEntry['ParentList']=[]
                # this is simple, byt very slow
                """
                if useParent:
                    parentList=api.listFileParents(logical_file_name=lfn)
                    for parent in parentList:
                        parentDict={'LogicalFileName':parent['parent_logical_file_name']}
                        fileEntry['ParentList'].append(parentDict)
                """
                files.append(fileEntry)
                lfn2files[lfn]=len(files)-1
                
            # get list of file parents
            # getting for the full block at once requires some work with list,
            # but it is much faster
            if useParent:
                parentList=api.listFileParents(block_name=blockName)
                for parent in parentList:
                    lfn = parent['logical_file_name']
                    parentLfn = parent['parent_logical_file_name']
                    indx=lfn2files[lfn]
                    files[indx]['ParentList'].append({'LogicalFileName':parentLfn})
            
            # now query for lumis

            if runselection:
                res=api.listFileLumis(block_name=blockName, run_num=runselection)
            else:
                res=api.listFileLumis(block_name=blockName)
            #print "filelumis in this block: ", len(res)
            #print "res[0] = ", res[0]
            for lumidic in res:
                lfn=lumidic['logical_file_name']
                #print 'SB lfn ', lfn
                # add the info from this lumidic to files
                indx=lfn2files[lfn]
                #print indx, files[indx]['LogicalFileName']
                run=lumidic['run_num']
                for lumi in lumidic['lumi_section_num']:
                    lD={}
                    lD['RunNumber']=run
                    lD['LumiSectionNumber']=lumi
                    files[indx]['LumiList'].append(lD)
            
                
        print "WILL RETURN FILES OF LENGTH ", len(files)

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
