#!/usr/bin/env python
import sys
for p in sys.path:
    if p.find( "python2.3/lib-dynload" ) != -1 :
        sys.path.pop( sys.path.index(p) )

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx
from DBSAPI.dbsApiException import DbsException
import os,getopt
from Actor import *
import common


class InspectDBS(Actor):
    def __init__(self, cfg_params):
        """
        InspectDBS class: 

        - check data publication in a DBS
        """

        try:
            self.DBSURL=cfg_params['USER.dbs_url_for_publication']
        except KeyError:
            msg = "Warning. You have to specify the url of DBS in the USER.dbs_url_for_publication parameter of crab.cfg or as command line option: \n"
            msg += "crab -checkPublication -USER.dbs_url_for_publication=<DBS url where data are published> -USER.dataset_to_check=<datasetpath to check>\n"
            raise CrabException(msg)
            
        try:
            self.dataset_to_check=cfg_params['USER.dataset_to_check']
        except KeyError:
            msg = "Warning. You have to speficy the dataset you want to check in the USER.dataset_to_check parameter of crab.cfg or as command line option: \n"
            msg += "crab -checkPublication -USER.dbs_url_for_publication=<DBS url where data are published> -USER.dataset_to_check=<datasetpath to check>\n"
            raise CrabException(msg)

    
    def checkPublication(self):
        """
           check dataset publication in a dbs  
        """

        common.logger.info('--->>> Check data publication: dataset '+self.dataset_to_check+' in DBS url '+ self.DBSURL+'\n')
        #  //
        # // Get API to DBS
        #//
        dbsreader = DBSReader(self.DBSURL)
        #  //
        # // Get list of datasets
        #//
        if len(self.dataset_to_check.split('/')) < 4:
            msg = "the provided dataset name is not correct"
            raise CrabException(msg)
        else:   
            primds=self.dataset_to_check.split('/')[1]
            procds=self.dataset_to_check.split('/')[2]
            tier=self.dataset_to_check.split('/')[3]
            datasets=dbsreader.matchProcessedDatasets(primds,tier,procds)
            if common.debugLevel:
                print "PrimaryDataset = ", primds
                print "ProcessedDataset = ", procds
                print "DataTier = ", tier
                print "datasets matching your requirements= ", datasets

        for dataset in datasets:
        #  //
        # // Get list of blocks for the dataset and their location
        #//
            if len(dataset.get('PathList'))==0:
                print "===== Empty dataset yet /%s/%s with tiers %s"%(dataset.get('PrimaryDataset')['Name'],dataset.get('Name'),dataset.get('TierList'))
            else:
                for datasetpath in dataset.get('PathList'):
                    nevttot=0
                    print "=== dataset %s"%datasetpath
                    ### FEDE #######
                    if dataset['Description'] != None:
                        print "=== dataset description = ", dataset['Description']
                    ################    
                    blocks=dbsreader.getFileBlocksInfo(datasetpath)
                    for block in blocks:
                        SEList=dbsreader.listFileBlockLocation(block['Name'])  # replace that with DLS query
                        print "===== File block name: %s" %block['Name']
                        print "      File block located at: ", SEList
                        print "      File block status: %s" %block['OpenForWriting']
                        print "      Number of files: %s"%block['NumberOfFiles']
                        print "      Number of Bytes: %s"%block['BlockSize']
                        print "      Number of Events: %s"%block['NumberOfEvents']
                        if common.debugLevel:
                            print "--------- info about files --------"
                            print " Size \t Events \t LFN \t FileStatus "
                            files=dbsreader.listFilesInBlock(block['Name'])
                            for file in files:
                                print "%s %s %s %s"%(file['FileSize'],file['NumberOfEvents'],file['LogicalFileName'],file['Status'])
                        nevttot = nevttot + block['NumberOfEvents']
                    print "\n total events: %s in dataset: %s\n"%(nevttot,datasetpath)
        if not common.debugLevel:
            common.logger.info('You can obtain more info about files of the dataset using: crab -checkPublication -USER.dataset_to_check='+self.dataset_to_check+' -USER.dbs_url_for_publication='+self.DBSURL+' -debug')
        
    def run(self):
        """
        parse of all xml file on res dir and creation of distionary
        """
        self.checkPublication()
