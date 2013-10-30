#!/usr/bin/env python
"""
_DBSDeleteData_
                                                                                
Command line tool to delete a dataset or a fileblock.

"""
import sys
for p in sys.path:
    if p.find( "python2.3/lib-dynload" ) != -1 :
        sys.path.pop( sys.path.index(p) )

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx

import string,getopt

usage="\n Usage: python DBSDeleteDataset.py <options> \n Options: \n --DBSURL=<URL> \t\t DBS URL \n --datasetPath=<dataset> \t\t dataset \n --block=<blockname> \t\t blockname \n"
valid = ['DBSURL=','datasetPath=','block=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

url = None
datasetPath = None
block = None

for opt, arg in opts:
    if opt == "--datasetPath":
        datasetPath = arg
    if opt == "--block":
        block = arg
    if opt == "--DBSURL":
        url = arg

if url == None:
    print "--DBSURL option not provided. For example :\n --DBSURL http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet"
    print usage
    sys.exit(1)

if (block == None) and (datasetPath == None):
    print "\n either  --datasetPath or --block option has to be provided"
    print usage
    sys.exit(1)
if (block != None) and (datasetPath != None):
    print "\n options --block or --datasetPath are mutually exclusive"
    print usage
    sys.exit(1)


print ">>>>> DBS URL : %s"%(url)

import logging
logging.disable(logging.INFO)

#  //
# // Get API to DBS
#//
args = {'url' : url , 'level' : 'ERROR'}
dbsapi = DbsApi(args)

#  //
# // Delete dataset
#//
if (datasetPath):
 print "Deleting datasetPath=%s"%datasetPath
 dbsapi.deleteProcDS(datasetPath)

if (block):
 dbsreader = DBSReader(url)
 getdatasetPath = dbsreader.blockToDatasetPath(block)
 print "Deleting block=%s from datasetPath=%s"%(block,getdatasetPath)
 dbsapi.deleteBlock(getdatasetPath,block)



