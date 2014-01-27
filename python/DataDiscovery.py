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


from DBSAPI.dbsApiException import *
import os, time
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
        verifyDBS23 = False
        useDAS = False

        # knwon DBS end-points
        known_dbs_urls = []
        global_dbs2 = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        global_dbs3 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        caf_dbs2_01 = "http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet"
        local_dbs2_01 = "http://cmsdbsprod.cern.ch/cms_dbs_ph_analysis_01/servlet/DBSServlet"
        local_dbs2_02 = "http://cmsdbsprod.cern.ch/cms_dbs_ph_analysis_02/servlet/DBSServlet"
        caf_dbs3_01   = "https://cmsweb.cern.ch/dbs/prod/caf01/DBSReader"
        local_dbs3_01 = "https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader"
        local_dbs3_02 = "https://cmsweb.cern.ch/dbs/prod/phys02/DBSReader"
        local_dbs3_03 = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader"
        known_dbs_urls = [ \
            global_dbs2, caf_dbs2_01, local_dbs2_01, local_dbs2_02, \
            global_dbs3, caf_dbs3_01, local_dbs3_01, local_dbs2_01, local_dbs3_03 \
            ]

        ## correspondence maps of DBS2/3 isntances
        dbs2to3={}
        dbs3to2={}
        dbs2to3[global_dbs2] = global_dbs3
        dbs2to3[caf_dbs2_01]   = caf_dbs3_01
        dbs2to3[local_dbs2_01] = local_dbs3_01
        dbs2to3[local_dbs2_02] = local_dbs3_02
        dbs2to3[local_dbs3_01] = local_dbs3_01
        dbs2to3[local_dbs3_02] = local_dbs3_02
        dbs2to3[local_dbs3_03] = local_dbs3_03
        # reverse map:
        for key,value in dbs2to3.iteritems():
            dbs3to2[value]=key



        ## get DBS URL specified by user (default to global DBS2)
        dbs_url = self.cfg_params.get('CMSSW.dbs_url', global_dbs2)
        
        if self.cfg_params.get('CMSSW.use_dbs3'):
            useDBS3 = int(self.cfg_params.get('CMSSW.use_dbs3'))==1

        if self.cfg_params.get('CMSSW.verify_dbs23'):
            verifyDBS23 = int(self.cfg_params.get('CMSSW.verify_dbs23'))==1
        if verifyDBS23 and not dbs_url in knwon_dbs_urls:
            common.logger.info ("automatic verification DBS2/3 not possible for non standard dbs_url=%s"%dbs_url)
            verifyDBS23 = False

        # support shortcuts for local scope DBS's
        if dbs_url == "dbs2_caf_01" :  dbs_url=caf_dbs2_01
        if dbs_url == "analysis_01" :  dbs_url=local_dbs2_01
        if dbs_url == "analysis_02" :  dbs_url=local_dbs2_02
        if dbs_url == "caf01"  :       dbs_url=caf_dbs3_01
        if dbs_url == "phys01" :       dbs_url=local_dbs3_01
        if dbs_url == "phys02" :       dbs_url=local_dbs3_02
        if dbs_url == "phys03" :       dbs_url=local_dbs3_03

        # if user asked for DBS3, remap DBS url if needed
        # and possible, i.e. using a known URL
        if useDBS3 and dbs_url in knwon_dbs_urls:
            dbs_url = dbs2to3 [dbs_url]
        common.logger.info("Accessing DBS at: "+dbs_url)


        endpoint_components = urlparse.urlsplit(dbs_url)

        if endpoint_components.hostname == DBS3HOST or useDBS3:
            useDBS3=True
            dbs_url_3 = dbs_url
            if dbs_url in known_dbs_urls:
                dbs_url_2 = dbs3to2[dbs_url]
        elif endpoint_components.hostname == DBS2HOST:
            useDBS2=True
            dbs_url_2 = dbs_url
            if dbs_url in known_dbs_urls:
                dbs_url_3 = dbs2to3[dbs_url]
        else:
            # if we do not know this URL, better be a DBS3 test instance
            useDBS3=True
            dbs_url_3 = dbs_url

        
        if useDBS2 and useDBS3:
            msg = "trying to use DBS2 and DBS3 at same time ?"
            raise  CrabException(msg)

        if self.cfg_params.get('CMSSW.use_das'):
            useDAS = int(self.cfg_params.get('CMSSW.use_das'))==1


        if useDBS2:
            common.logger.info("Will do Data Discovery using  DBS2")
        if useDBS3:
            common.logger.info("Will do Data Discovery using  DBS3")
        if useDAS :
            common.logger.info("will use DAS to talk to DBS")
        if verifyDBS23:
            common.logger.info("Will verify that DBS2 and DBS3 return same information")


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
        if useDBS2 or verifyDBS23:
            args = {}
            args['url']     = dbs_url_2
            args['level']   = 'CRITICAL'

        ## check if has been requested to use the parent info
        useparent = int(self.cfg_params.get('CMSSW.use_parent',0))


        defaultName = common.work_space.shareDir()+'AnalyzedBlocks.txt'
        ## check if has been asked for a non default file to store/read analyzed fileBlocks
        #SB no no, we do not want this, it is not even documented !
        #fileBlocks_FileName = os.path.abspath(self.cfg_params.get('CMSSW.fileblocks_file',defaultName))
        if self.cfg_params.get('CMSSW.fileblocks_file') :
            msg = "CMSSW.fileblocks_file option non supported"
            raise CrabException(msg)
        fileBlocks_FileName = os.path.abspath(defaultName)

        if useDBS2 or verifyDBS23:
            #common.logger.info("looking up DBS2 ...")
            import DBSAPI.dbsApi
            import DBSAPI.dbsApiException
            start_time=time.time()
            api2 = DBSAPI.dbsApi.DbsApi(args)
            files2 = self.queryDbs(api2,path=self.datasetPath,runselection=runselection,useParent=useparent)
            elapsed=time.time() - start_time
            common.logger.info("DBS2 lookup took %5.2f sec" % elapsed)
            if useDBS2:
                self.files = files2
        if useDBS3 or verifyDBS23:
            #common.logger.info("looking up DBS3 ...")
            from dbs.apis.dbsClient import DbsApi
            start_time=time.time()
            api3 = DbsApi(dbs_url_3)
            files3 = self.queryDbs3(api3,path=self.datasetPath,runselection=runselection,useParent=useparent)
            elapsed=time.time() - start_time
            common.logger.info("DBS3 lookup took %5.2f sec" % elapsed)
            if useDBS3:
                self.files = files3
        if useDAS :
            self.files = self.queryDas(path=self.datasetPath,runselection=runselection,useParent=useparent)

        if verifyDBS23:
            if not self.compareFilesStructure(files2,files3):
                common.logger.info("ERROR: DBS2 - DB3 comparsion failed, please run crab -uploadLog and report to crabFeedback")
        

        # Check to see what the dataset is
        pdsName = self.datasetPath.split("/")[1]
        if useDBS2 :
            primDSs = api2.listPrimaryDatasets(pdsName)
            dataType = primDSs[0]['Type']
        elif useDBS3 :
            dataType=api3.listDataTypes(dataset=self.datasetPath)[0]['data_type']

        common.logger.info("Datatype is %s" % dataType)
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
            msg="No data for %s in DBS\n Check datasetpath parameter in crab.cfg" % self.datasetPath
            raise  CrabException(msg)


    def queryDbs(self,api,path=None,runselection=None,useParent=None):


        allowedRetriveValue = []
        if self.splitByLumi or self.splitByRun or useParent == 1:
            allowedRetriveValue.extend(['retrive_block', 'retrive_run'])
        #if self.splitByLumi:
        #    allowedRetriveValue.append('retrive_lumi')
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
            raise crabException(msg)
        except DBSError, msg:
            raise crabException(msg)

        
        return files


    def queryDbs3(self,api,path=None,runselection=None,useParent=None):
        

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
                fileEntry['LumiList']=[]
                fileEntry['ParentList']=[]

                files.append(fileEntry)
                lfn2files[lfn]=len(files)-1
                
            # get list of file parents
            # getting for the full block at once requires some work with list,
            # but it is much faster
            if useParent:
                FileParentList=api.listFileParents(block_name=blockName)
                for FileParentEntry in FileParentList:
                    lfn = FileParentEntry['logical_file_name']
                    indx=lfn2files[lfn]  # ptr to this LFN info in the files structure

                    # need to turn this list of LFN's into a list of dict. for
                    # compatibility with code used to DBS2
                    for parentFile in FileParentEntry['parent_logical_file_name']:
                        files[indx]['ParentList'].append({'LogicalFileName':parentFile})
            
            # now query for lumis

            # to be able to verify with DBS2, do not use runselection
            #if runselection:
            #    res=api.listFileLumis(block_name=blockName, run_num=runselection)
            #else:
            #    res=api.listFileLumis(block_name=blockName)
            res=api.listFileLumis(block_name=blockName)
            for lumidic in res:
                lfn=lumidic['logical_file_name']
                # add the info from this lumidic to files
                indx=lfn2files[lfn]
                run=lumidic['run_num']
                for lumi in lumidic['lumi_section_num']:
                    lD={}
                    lD['RunNumber']=run
                    lD['LumiSectionNumber']=lumi
                    files[indx]['LumiList'].append(lD)
            
                
        #common.logger.info("DBS3 IS RETURNING FILES OF LENGTH %s" % len(files))

        return files

    def compareFilesStructure(self, f2, f3):
        """
        compare self.files structures
        limit comparison to the parts that are actually used
        and thus filled by queryDBS3
        """
        compareStatus = False
        common.logger.info("Compare dataset information retrieved from DBS2 and DBS3 ... on %d files" % len(f2))

        # at the very least there must be same number of files
        if len(f2) == len(f3):
            compareStatus = True
        else:
            common.logger.info("DBS2/DBS3 mismatch: number of files differ")

        if compareStatus:
            # make sure list of LFN is the same
            lfn_list_2 = [x['LogicalFileName'] for x in f2]
            lfn_list_2.sort()
            lfn_list_3 = [x['LogicalFileName'] for x in f3]
            lfn_list_3.sort()
            if lfn_list_2 == lfn_list_3:
                "List of LFN's is the same for DBS2/DBS3"
                compareStatus = True
            else:
                common.logger.info("DBS2/DBS3 mismatch: list of LFN's differ")

        if compareStatus:
            # go through file dictionary list from DBS2
            for fDic2 in f2:
                # compare the two dictionaries for each LFN

                lfn=fDic2['LogicalFileName']
                # find dictionary from DBS3 for same file
                lfnMatch=False
                for fileDic in f3:
                    if fileDic['LogicalFileName'] == lfn:
                        fDic3=fileDic
                        break

                # compare block name
                if not fDic2['Block']['Name'] == fDic3['Block']['Name'] :
                    compareStatus = False
                    msg = "DBS2/DBS3 mismatch: BlockName differs"
                    msg += " for file: \n%s\n" % lfn
                    msg += "\nfDic2['Block']['Name']=%s" % fDic2['Block']['Name']
                    msg += "\nfDic3['Block']['Name']=%s" % fDic3['Block']['Name']
                    common.logger.info(msg)
                    break

                # compare number of events
                if not fDic2['NumberOfEvents'] == fDic3['NumberOfEvents'] :
                    compareStatus = False
                    msg = "DBS2/DBS3 mismatch: Number Of Events differs"
                    msg += " for file: \n%s\n" % lfn
                    msg += "\n=fDic2['NumberOfEvents']= %s" % fDic2['NumberOfEvents']
                    msg += "\n=fDic3['NumberOfEvents']= %s" % fDic3['NumberOfEvents']
                    common.logger.info(msg)
                    break

                # compare number of lumis
                ll3 = fDic3['LumiList']
                ll2 = []
                for ld in fDic2['LumiList']:
                    trimmed_lumidict={}
                    for k in ['RunNumber','LumiSectionNumber']:
                        trimmed_lumidict[k]=ld[k]
                    ll2.append(trimmed_lumidict)
                if not len(ll2) == len(ll3) :
                    compareStatus = False
                    msg = "DBS2/DBS3 mismatch: Number Of Lumis differs"
                    msg += " for file: \n%s\n" % lfn
                    msg += "\n #DBS2= %s" % len(ll2)
                    msg += "\n #DBS3= %s" % len(ll3)
                    common.logger.info(msg)
                    break

                # compare list of lumis
                # from list of dictionaries to list of ntuples so can sort
                lu2=[]
                for ld in ll2:
                    lu2.append((ld['RunNumber'],ld['LumiSectionNumber']))
                lu3=[]
                for ld in ll3:
                    lu3.append((ld['RunNumber'],ld['LumiSectionNumber']))
                lu2.sort()
                lu3.sort()
                if not lu2 == lu3 :
                    compareStatus = False
                    msg = "DBS2/DBS3 mismatch: LumiList differs"
                    msg += " for file: \n%s\n" % lfn
                    msg += "\n=fDic2['LumiList']=\n%s" % lu2
                    msg += "\n=fDic3['LumiList']=\n%s" % lu3
                    common.logger.info(msg)
                    break


        if compareStatus:
            common.logger.info("Comparison was succesfull")
        
        return compareStatus




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
