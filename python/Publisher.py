import getopt, string
import common
import time, glob
from Actor import *
from crab_util import *
from crab_exceptions import *
from crab_dbs3publish import *

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter,DBSWriterObjects
import sys
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsMigrateApi import DbsMigrateApi
from DBSAPI.dbsApiException import *

class Publisher(Actor):
    def __init__(self, cfg_params):
        """
        Publisher class:

        - parses CRAB FrameworkJobReport on UI
        - returns <file> section of xml in dictionary format for each xml file in crab_0_xxxx/res directory
        - publishes output data on DBS and DLS
        """

        self.cfg_params=cfg_params
        self.fjrDirectory = cfg_params.get('USER.outputdir' ,
                                           common.work_space.resDir()) + '/'
       
        if not cfg_params.has_key('USER.publish_data_name'):
            raise CrabException('Cannot publish output data, because you did not specify USER.publish_data_name parameter in the crab.cfg file')
        self.userprocessedData = cfg_params['USER.publish_data_name']
        self.processedData = None

        if (not cfg_params.has_key('USER.copy_data') or int(cfg_params['USER.copy_data']) != 1 ) or \
            (not cfg_params.has_key('USER.publish_data') or int(cfg_params['USER.publish_data']) != 1 ):
            msg  = 'You can not publish data because you did not selected \n'
            msg += '\t*** copy_data = 1 and publish_data = 1  *** in the crab.cfg file'
            raise CrabException(msg)

        if not cfg_params.has_key('CMSSW.pset'):
            raise CrabException('Cannot publish output data, because you did not specify the psetname in [CMSSW] of your crab.cfg file')
        self.pset = cfg_params['CMSSW.pset']

        self.globalDBS=cfg_params.get('CMSSW.dbs_url',"http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")

        #if not cfg_params.has_key('USER.dbs_url_for_publication'):
        #    msg = "Warning. The [USER] section does not have 'dbs_url_for_publication'"
        #    msg = msg + " entry, necessary to publish the data.\n"
        #    msg = msg + "Use the command **crab -publish -USER.dbs_url_for_publication=dbs_url_for_publication*** \nwhere dbs_url_for_publication is your local dbs instance."
        #    raise CrabException(msg)

        self.DBSURL=cfg_params.get('USER.dbs_url_for_publication','DBS3/phys03')
        #common.logger.info('<dbs_url_for_publication> = '+self.DBSURL)
        if (self.DBSURL == "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet") or (self.DBSURL == "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"):
            msg = "You can not publish your data in the globalDBS = " + self.DBSURL + "\n" 
            msg = msg + "Please write your local one in the [USER] section 'dbs_url_for_publication'"
            raise CrabException(msg)
            
        self.content=file(self.pset).read()
        self.resDir = common.work_space.resDir()
        
        self.dataset_to_import=[]
        
        self.datasetpath=cfg_params['CMSSW.datasetpath']
        if (self.datasetpath.upper() != 'NONE'):
            self.dataset_to_import.append(self.datasetpath)
        
        ### Added PU dataset
        tmp = cfg_params.get('CMSSW.dataset_pu',None)
        if tmp :
            datasets = tmp.split(',')
            for dataset in datasets:
                dataset=string.strip(dataset)
                self.dataset_to_import.append(dataset)
        ###        
            
        self.import_all_parents = cfg_params.get('USER.publish_with_import_all_parents',1)
        
        if ( int(self.import_all_parents) == 0 ):
            common.logger.info("WARNING: The option USER.publish_with_import_all_parents=0 has been deprecated. The import of parents is compulsory and done by default")
        self.skipOcheck=cfg_params.get('CMSSW.publish_zero_event',1)
        if ( int(self.skipOcheck) == 0 ):
            common.logger.info("WARNING: The option CMSSW.publish_zero_event has been deprecated. The publication is done by default also for files with 0 events")
        self.SEName=''
        self.CMSSW_VERSION=''
        self.exit_status=''
        self.time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        self.problemFiles=[]
        self.noEventsFiles=[]
        self.noLFN=[]

        #### FEDE to allow publication without input data in <file>
        if cfg_params.has_key('USER.no_inp'):
            self.no_inp = cfg_params['USER.no_inp']
        else:
            self.no_inp = 0
        ############################################################
    def importParentDataset(self,globalDBS, datasetpath):
        """
           WARNING: it works only with DBS_2_0_9_patch_6
        """

        args={'url':globalDBS}
        try:
            api_reader = DbsApi(args)
        except DbsApiException, ex:
            msg = "%s\n" % formatEx(ex)
            raise CrabException(msg)

        args={'url':self.DBSURL}
        try:
            api_writer = DbsApi(args)
        except DbsApiException, ex:
            msg = "%s\n" % formatEx(ex)
            raise CrabException(msg)

        try:
            common.logger.info("--->>> Importing all parents level")
            start = time.time()
            common.logger.debug("start import parents time: " + str(start))
            for block in api_reader.listBlocks(datasetpath):
                if (str(block['OpenForWriting']) != '1'):
                    api_writer.dbsMigrateBlock(globalDBS,self.DBSURL,block['Name'] )
                else:
                    common.logger.debug("Skipping the import of " + block['Name'] + " it is an open block")
                    continue
                ################
            stop = time.time()
            common.logger.debug("stop import parents time: " + str(stop))
            common.logger.info("--->>> duration of all parents import (sec): "+str(stop - start))
        except DbsApiException, ex:
            msg = "Error importing dataset to be processed into local DBS\n"
            msg += "Source Dataset: %s\n" % datasetpath
            msg += "Source DBS: %s\n" % globalDBS
            msg += "Destination DBS: %s\n" % self.DBSURL
            common.logger.info(msg)
            common.logger.info(str(ex))
            return 1
        return 0

    def publishDataset(self,file):
        """
        """
        try:
            jobReport = readJobReport(file)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+file+" file"
            common.logger.info(msg)
            return self.exit_status

        if (len(self.dataset_to_import) != 0):
           for dataset in self.dataset_to_import:
               common.logger.info("--->>> Importing parent dataset in the dbs: " +dataset)
               status_import=self.importParentDataset(self.globalDBS, dataset)
               if (status_import == 1):
                   common.logger.info('Problem with parent '+ dataset +' import from the global DBS '+self.globalDBS+ 'to the local one '+self.DBSURL)
                   self.exit_status='1'
                   return self.exit_status
               else:
                   common.logger.info('Import ok of dataset '+dataset)

        
        if (len(jobReport.files) <= 0) :
            self.exit_status = '1'
            msg = "Error: No EDM file to publish in xml file"+file+" file"
            common.logger.info(msg)
            return self.exit_status
        else:
            msg = "fjr contains some files to publish" 
            common.logger.debug(msg)

        #### datasets creation in dbs
        #// DBS to contact write and read of the same dbs
        dbsReader = DBSReader(self.DBSURL,level='ERROR')
        dbswriter = DBSWriter(self.DBSURL)
        #####

        self.published_datasets = [] 
        for fileinfo in jobReport.files:
            datasets_info=fileinfo.dataset
            if len(datasets_info)<=0:
                self.exit_status = '1'
                msg = "Error: No info about dataset in the xml file "+file
                common.logger.info(msg)
                return self.exit_status
            else:
                for dataset in datasets_info:
                    #### for production data
                    self.processedData = dataset['ProcessedDataset']
                    if (dataset['PrimaryDataset'] == 'null'):
                        dataset['PrimaryDataset'] = self.userprocessedData
                    elif self.datasetpath.upper() != 'NONE':
                        dataset['ParentDataset']= self.datasetpath

                    dataset['PSetContent']=self.content
                    cfgMeta = {'name' : self.pset , 'Type' : 'user' , 'annotation': 'user cfg', 'version' : 'private version'} # add real name of user cfg
                    common.logger.info("PrimaryDataset = %s"%dataset['PrimaryDataset'])
                    common.logger.info("ProcessedDataset = %s"%dataset['ProcessedDataset'])
                    common.logger.info("<User Dataset Name> = /"+dataset['PrimaryDataset']+"/"+dataset['ProcessedDataset']+"/USER")
                    
                    self.dataset_to_check="/"+dataset['PrimaryDataset']+"/"+dataset['ProcessedDataset']+"/USER"


                    self.published_datasets.append(self.dataset_to_check)

                    common.logger.log(10-1,"--->>> Inserting primary: %s processed : %s"%(dataset['PrimaryDataset'],dataset['ProcessedDataset']))
                    
                    #### check if dataset already exists in the DBS
                    result = dbsReader.matchProcessedDatasets(dataset['PrimaryDataset'], 'USER', dataset['ProcessedDataset'])
                    if (len(result) != 0):
                       result = dbsReader.listDatasetFiles(self.dataset_to_check)

                    primary = DBSWriterObjects.createPrimaryDataset( dataset, dbswriter.dbs)
                    common.logger.log(10-1,"Primary:  %s "%primary)
                    print "primary = ", primary 

                    algo = DBSWriterObjects.createAlgorithm(dataset, cfgMeta, dbswriter.dbs)
                    common.logger.log(10-1,"Algo:  %s "%algo)

                    processed = DBSWriterObjects.createProcessedDataset(primary, algo, dataset, dbswriter.dbs)
                    common.logger.log(10-1,"Processed:  %s "%processed)
                    print "processed = ", processed 

                    common.logger.log(10-1,"Inserted primary %s processed %s"%(primary,processed))
                    #######################################################################################
                
        common.logger.log(10-1,"exit_status = %s "%self.exit_status)
        return self.exit_status

    def publishAJobReport(self,file,procdataset):
        """
           input:  xml file, processedDataset
        """
        common.logger.debug("FJR = %s"%file)
        try:
            jobReport = readJobReport(file)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+file+" file"
            raise CrabException(msg)
        ### skip publication for 0 events files
        filestopublish=[]
        for file in jobReport.files:
            #### added check for problem with copy to SE and empty lfn
            if (string.find(file['LFN'], 'copy_problems') != -1):
                self.problemFiles.append(file['LFN'])
            elif (file['LFN'] == ''):
                self.noLFN.append(file['PFN'])
            else:
                if int(file['TotalEvents']) == 0:
                    self.noEventsFiles.append(file['LFN'])
                for ds in file.dataset:
                    ### Fede for production
                    if (ds['PrimaryDataset'] == 'null'):
                        ds['PrimaryDataset']=self.userprocessedData
                filestopublish.append(file)
       
        jobReport.files = filestopublish
        for file in filestopublish:
            common.logger.debug("--->>> LFN of file to publish =  " + str(file['LFN']))
        ### if all files of FJR have number of events = 0
        if (len(filestopublish) == 0):
            return None
           
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL)
        # insert files
        Blocks=None
        try:
            ### FEDE added insertDetectorData = True to propagate in DBS info about run and lumi 
            Blocks=dbswriter.insertFiles(jobReport, insertDetectorData = True)
            #Blocks=dbswriter.insertFiles(jobReport)
            common.logger.debug("--->>> Inserting file in blocks = %s"%Blocks)
        except DBSWriterError, ex:
            common.logger.debug("--->>> Insert file error: %s"%ex)
        return Blocks

    def remove_input_from_fjr(self, list_of_good_files):
            """
              to remove the input file from fjr in the case of problem with lfn
            """
            from xml.etree.ElementTree import ElementTree, Element
            new_good_list = []               
            no_inp_dir = self.fjrDirectory + 'no_inp'
            if not os.path.isdir(no_inp_dir):
                try:
                    os.mkdir(no_inp_dir)
                    print "no_inp_dir = ", no_inp_dir
                except:
                    print "problem during no_inp_dir creation: ", no_inp_dir
            for file in  list_of_good_files:
                name_of_file = os.path.basename(file)
                #print "name_of_file = " , name_of_file
                oldxmlfile = ElementTree()
                oldxmlfile.parse(file)
                newxmlfile = ElementTree(Element(oldxmlfile.getroot().tag))
                self.recurse(oldxmlfile.getroot(), newxmlfile.getroot())
                new_good_file = no_inp_dir + '/' + name_of_file
                newxmlfile.write(new_good_file)
                new_good_list.append(new_good_file)
            print "new_good_list = ", new_good_list    
            return new_good_list   

    def recurse(self,oldnode,newnode):
            """
               recursive function to remove 
            """
            from xml.etree.ElementTree import ElementTree, Element
            try: 
                newnode.text = oldnode.text
            except AttributeError: pass
            try: 
                newnode.attrib = oldnode.attrib
            except AttributeError: pass
            try: 
                newnode.tail = oldnode.tail
            except AttributeError: pass

            for oldi in oldnode.getchildren():
                if oldi.tag != "Inputs" and oldi.tag == "DatasetInfo":
                    newi = Element(oldi.tag)
                    newtag = Element("Entry")
                    newtag.attrib = {'Name':'Description'}
                    newtag.text = 'Unknown provenance'
                    newi.append(newtag)
                    newnode.append(newi)
                    self.recurse(oldi, newi)
                elif oldi.tag != "Inputs" and oldi.tag != "DatasetInfo":    
                    newi = Element(oldi.tag)
                    newnode.append(newi)
                    self.recurse(oldi, newi)

    def run(self):
        """
        parse of all xml file on res dir and creation of dictionary
        """
        
        task = common._db.getTask()
        good_list=[]   # list of fjr's to publish

        common.logger.info("Listing crab_fjr files")
        nj=0
        for job in task.getJobs():
            nj += 1
            if nj%100 == 0 :
                common.logger.info("checking job %d" % nj)
            fjr = self.fjrDirectory + job['outputFiles'][-1]
            if (job.runningJob['applicationReturnCode']!=0 or job.runningJob['wrapperReturnCode']!=0): continue
            # get FJR filename
            fjr = self.fjrDirectory + job['outputFiles'][-1]
            reports = readJobReport(fjr)
            if len(reports)>0 and reports[0].status == "Success":
                goodReport = True
                # sanity check : is there at least one run  ?
                for outFile in reports[0].files:
                    if len(outFile['Runs']) == 0 :
                        msg="ERROR: no run/lumi info. Skip FJR file %s" % fjr
                        common.logger.info(msg)
                        goodReport = False
                if not goodReport: continue
                good_list.append(fjr)

        if len(good_list) == 0:
            common.logger.info("No fjr with exit code =0 to be published")
            status = '0'
            return status

        common.logger.info("Task has %d jobs"%len(task.getJobs()))
        common.logger.info("Found %d fjr's with exit code=0 to be considere for publication"%len(good_list))

        pubToDBS2 = False
        pubToDBS3 = True
        if  self.cfg_params.get('CMSSW.publish_dbs2',None)=="1":
            pubToDBS2 = True
            pubToDBS3 = False
        if pubToDBS2 :
            status = self.DBS2Publish(good_list)
        elif pubToDBS3:
            argsForDbs3 = self.PrepareForDBS3Publish(good_list)
            globalApi    = argsForDbs3['globalApi']
            sourceApi    = argsForDbs3['sourceApi']
            inputDataset = argsForDbs3['inputDataset']
            toPublish    = argsForDbs3['toPublish']
            destApi      = argsForDbs3['destApi']
            destReadApi  = argsForDbs3['destReadApi']
            migrateApi   = argsForDbs3['migrateApi']
            originSite   = argsForDbs3['origin_site_name']
            (failed,published,results) = publishInDBS3(\
                sourceApi, globalApi, inputDataset, toPublish, destApi, destReadApi, migrateApi, originSite)
            if len(failed) == 0:
                status='0'
            else:
                status='1'
        else:
            raise CrabException('Could not define wether to publish to DBS2 or DBS3')

        return status

    def PrepareForDBS3Publish(self,good_list):
        from dbs.apis.dbsClient import DbsApi as Dbs3Api
        from ProdCommon.FwkJobRep.ReportParser import readJobReport

        # extract information from self and good_list and format as liked by publishInDBS3 function
        originSite = None

        (isDbs2, isDbs3, dbs2_url, dbs3_url) =  verify_dbs_url(self)
        sourceUrl = dbs3_url
        prdDBSurl = 'https://cmsweb.cern.ch/dbs/prod/phys03/'
        intDBSurl = 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/'
        devDBSurl = 'https://dbs3-dev01.cern.ch/dbs/dev/phys03/'
        devDBSurl = 'https://dbs3-testbed.cern.ch/dbs/dev/phys03/'
        destDBSurl = prdDBSurl

        if  self.cfg_params.get('CMSSW.dbs3-int',None)=="1":
            destDBSurl = intDBSurl
            
        if  self.cfg_params.get('CMSSW.dbs3-dev',None)=="1":
            destDBSurl = devDBSurl

        pubUrl= self.cfg_params.get('CMSSW.dbs-pub',None)
        if pubUrl:
            if not '/' in pubUrl:
                destDBSurl = 'https://'+ pubUrl +'.cern.ch/dbs/dev/phys03/'
            else:
                destDBSurl = 'https://'+ pubUrl +'/'

        common.logger.info('your dataset will be published in %s' % destDBSurl)

        destUrl = destDBSurl + 'DBSWriter'
        destReadUrl  = destDBSurl + 'DBSReader'
        migrateUrl = destDBSurl + 'DBSMigrate'
        inputDataset = self.cfg_params.get('CMSSW.datasetpath','None')

        sourceApi = Dbs3Api(url=sourceUrl)
        
        # when looking up parents may need to look in global DBS as well
        globalUrl = sourceUrl   # if this is not global, next lines will make it
        globalUrl = globalUrl.replace('phys01','global')
        globalUrl = globalUrl.replace('phys02','global')
        globalUrl = globalUrl.replace('phys03','global')
        globalUrl = globalUrl.replace('caf','global')
        globalApi = Dbs3Api(url=globalUrl)

        destinationApi = Dbs3Api(url=destUrl)
        # be safe, use RO Api unless really want to write
        destinationReadApi = Dbs3Api(url=destReadUrl)
        migrateApi = Dbs3Api(url=migrateUrl)
        
        # the publishInDBS3 copied from CRAB3 needs the info in the toPublish dictionary
        # format is: {outdataset:files}
        #   outdataset = dataset to be published (full name /prim/proc/tier)
        #   files is a list of dictionaries, one per LFN
        
        toPublish={}

        common.logger.info("parsing crab_fjr files...")
        nfjr=0
        for crabFjr in good_list:                 # this is the list of FJR's in crab res
            fjr=readJobReport(crabFjr)[0]         # parse into python
            nfjr += 1
            if nfjr%100 == 0:
                common.logger.info ("parsed %d crab_fjr files..." %nfjr)
            if not fjr.files:
                msg = "WARNING: No EDM file to be published in %s" % crabFjr.split('/')[-1]
                common.logger.info(msg)

            for outFile in fjr.files:             # one fjr may have multiple output LFN's
                dset_info=outFile.dataset[0]      # better there is only one dataset per file !
                procds=dset_info['ProcessedDataset']
                primds=dset_info['PrimaryDataset']
                if primds =='null':
                    # user MC, get publishdatane stripping username and hash from procds
                    primds='-'.join(procds.split('-')[1:-1])
                tier=dset_info['DataTier']
                outdataset="/%s/%s/%s" % (primds, procds,tier)
                if not toPublish.has_key(outdataset):
                    toPublish[outdataset]=[]
                fileDic={}                          # prepare dictionary to publish this LFN
                fileDic['cksum']=outFile['Checksum']
                fileDic['md5']="NOTSET"
                fileDic['adler32']="NOTSET"
                fileDic['acquisitionera']="null"
                fileDic['globaltag']="None"
                fileDic['publishname']=procds
                fileDic['outdataset']=outdataset
                fileDic['swversion']=dset_info['ApplicationVersion']
                fileDic['lfn']=outFile['LFN']
                fileDic['filesize']=outFile['Size']
                fileDic['runlumi']=outFile['Runs']
                # beware duplicate parents in FJR and parent='' when FJR had no LFN
                fileDic['parents']=[p for p in set(outFile.parentLFNs()) if p]
                if originSite:
                    if outFile['PNN'] != originSite:
                        msg = "ERROR: not all files to be published have same location"
                        msg += "file %s has origin %s, while previous files have %s\n" %\
                            (outFile['LFN'], outFile['PNN'], originSite)
                        raise CrabException(msg)
                else:
                    originSite = outFile['PNN']
                fileDic['inevents']=outFile['TotalEvents']

                toPublish[outdataset].append(fileDic)    # add dictionary to files list


        # all done
        common.logger.info("parsing of crab_fjr files completed")
        argsForDbs3 = { \
            'sourceApi' : sourceApi,
            'globalApi' : globalApi,
            'inputDataset' : inputDataset,
            'toPublish' : toPublish,
            'destApi' : destinationApi,
            'destReadApi' : destinationReadApi,
            'migrateApi' : migrateApi,
            'origin_site_name' : originSite
        }
        return argsForDbs3
        
    def DBS2Publish(self, good_list):
        ####################################################
        if self.no_inp == 1:
            file_list = self.remove_input_from_fjr(good_list)
        else:
            file_list=good_list
        print "file_list = ", file_list    
        ####################################################    

        common.logger.log(10-1, "fjr with FrameworkJobReport Status='Success', file_list = "+str(file_list))
        common.logger.log(10-1, "len(file_list) = "+str(len(file_list)))

        if (len(file_list)>0):
            BlocksList=[]
            common.logger.info("--->>> Start dataset publication")
            self.exit_status=self.publishDataset(file_list[0])
            if (self.exit_status == '1'):
                return self.exit_status
            common.logger.info("--->>> End dataset publication")


            common.logger.info("--->>> Start files publication")
            for file in file_list:
                Blocks=self.publishAJobReport(file,self.processedData)
                if Blocks:
                    for x in Blocks: # do not allow multiple entries of the same block
                        if x not in BlocksList:
                           BlocksList.append(x)

            # close the blocks
            common.logger.log(10-1, "BlocksList = %s"%BlocksList)
            dbswriter = DBSWriter(self.DBSURL)

            for BlockName in BlocksList:
                try:
                    closeBlock=dbswriter.manageFileBlock(BlockName,maxFiles= 1)
                    common.logger.log(10-1, "closeBlock %s"%closeBlock)
                except DBSWriterError, ex:
                    common.logger.info("Close block error %s"%ex)

            if (len(self.noEventsFiles)>0):
                common.logger.info("--->>> WARNING: "+str(len(self.noEventsFiles))+" published files contain 0 events are:")
                for lfn in self.noEventsFiles:
                    common.logger.info("------ LFN: %s"%lfn)
            if (len(self.noLFN)>0):
                common.logger.info("--->>> WARNING: there are "+str(len(self.noLFN))+" files not published because they have empty LFN")
                for pfn in self.noLFN:
                    common.logger.info("------ pfn: %s"%pfn)
            if (len(self.problemFiles)>0):
                common.logger.info("--->>> WARNING: "+str(len(self.problemFiles))+" files not published because they had problem with copy to SE")
                for lfn in self.problemFiles:
                    common.logger.info("------ LFN: %s"%lfn)
            common.logger.info("--->>> End files publication")

            #### FEDE for MULTI ####
            for dataset_to_check in self.published_datasets:
                self.cfg_params['USER.dataset_to_check']=dataset_to_check
                from InspectDBS import InspectDBS
                check=InspectDBS(self.cfg_params)
                check.checkPublication()
            #########################

            return self.exit_status

        else:
            common.logger.info("--->>> No valid files to publish on DBS. Your jobs do not report exit codes = 0")
            self.exit_status = '1'
            return self.exit_status
    
