#!/usr/bin/env python
import sys
for p in sys.path:
    if p.find( "python2.3/lib-dynload" ) != -1 :
        sys.path.pop( sys.path.index(p) )
##

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx
from DBSAPI.dbsApiException import DbsException
#
import os,getopt

#   //
#  // Get DBS instance to use
# //
usage="\n Usage: python InspectDBS2.py <options> \n Options: \n --datasetPath=/primarydataset/procdataset/tier \t\t dataset path \n --DBSURL=<URL> \t\t DBS URL \n --help \t\t\t\t print this help \n"
valid = ['DBSURL=','datasetPath=','full','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

#url = "http://cmssrv17.fnal.gov:8989/DBS/servlet/DBSServlet"
#url = "http://cmssrv17.fnal.gov:8989/DBS108LOC1/servlet/DBSServlet"
url = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

datasetPath = None
full = False 

for opt, arg in opts:
    if opt == "--DBSURL":
        url = arg
    if opt == "--datasetPath":
        datasetPath = arg
    if opt == "--full":
        full = True
    if opt == "--help":
        print usage
        sys.exit(1)

if datasetPath == None:
    print "--datasetPath option not provided. For example : --datasetPath /primarydataset/processeddataset/datatier"
    print usage
    sys.exit()
print ">>>>> DBS URL : %s"%(url,)


import logging
logging.disable(logging.INFO)

#  //
# // Get API to DBS
#//
dbsreader = DBSReader(url)
#  //
# // Get list of datasets
#//
if datasetPath:
     primds=datasetPath.split('/')[1]
     procds=datasetPath.split('/')[2]
     tier=datasetPath.split('/')[3]
#     print " matchProcessedDatasets(%s,%s,%s)"%(primds,tier,procds)
     datasets=dbsreader.matchProcessedDatasets(primds,tier,procds)
else:
     datasets=dbsreader.matchProcessedDatasets("*","*","*")


for dataset in datasets:
#  //
# // Get list of blocks for the dataset and their location
#//
 for datasetpath in dataset.get('PathList'):
   nevttot=0
   print "===== dataset %s"%datasetpath
   blocks=dbsreader.getFileBlocksInfo(datasetpath)
   for block in blocks:
     SEList=dbsreader.listFileBlockLocation(block['Name'])  # replace that with DLS query
     print "== File block %s is located at: %s"%(block['Name'],SEList)
     print "File block name: %s" %block['Name']
     print "File block status: %s" %block['OpenForWriting']
     print "Number of files: %s"%block['NumberOfFiles']
     print "Number of Bytes: %s"%block['BlockSize']
     print "Number of Events: %s"%block['NumberOfEvents']
     if full:
      print "--------- info about files --------"
      print " Size \t Events \t LFN \t FileStatus "
      files=dbsreader.listFilesInBlock(block['Name'])
      for file in files:
        print "%s %s %s %s"%(file['FileSize'],file['NumberOfEvents'],file['LogicalFileName'],file['Status'])
     nevttot = nevttot + block['NumberOfEvents']

   print "\n total events: %s in dataset: %s\n"%(nevttot,datasetpath)

 if len(dataset.get('PathList'))==0:
   print "===== Empty dataset yet /%s/%s with tiers %s"%(dataset.get('PrimaryDataset')['Name'],dataset.get('Name'),dataset.get('TierList'))




