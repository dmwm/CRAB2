#!/usr/bin/env python
"""
_DBSInvalidateDataset_
                                                                                
Command line tool to invalidate a dataset.

"""
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsOptions import DbsOptionParser

import string,sys,os

#
# Check if the parents of a dataset are all valid (or really not invalid)
#
def parentsValid(dbsapi,ads):
  parents=dbsapi.listDatasetParents(ads)
  parentsOK=True

  for p in parents:
    path=p['PathList'][0]
    dsStatus=datasetStatus(dbsapi,path)
    # do we want to check other statuses?
    if dsStatus == "INVALID" or dsStatus == "INVALID-RO":
      parentsOK=False
      print "Parent dataset %s is %s, must be a valid status to validate child" % (path, dsStatus)

  return parentsOK
  
#
# Check that the children of a dataset are all invalid
#
def childrenInvalid(dbsapi,ads):
  childrenOK=True
  dsBlocks=dbsapi.listBlocks(dataset=ads)
  childPaths = {}

#
# To find the children we have to go through the block parentage, so accumulate the list of parent
# datasets and check each one only once
#
  for b in dsBlocks:
    blockChildren=dbsapi.listBlockChildren(b['Name'])
    for c in blockChildren:
      childPaths[c['Path']] = 1

  for c in childPaths.keys():
    dsStatus=datasetStatus(dbsapi,c)
    if dsStatus != "INVALID" and dsStatus != "INVALID-RO":
      childrenOK=False
      print "Child dataset %s is %s, must be invalid to invalidate parent" % (c, dsStatus)

  return childrenOK

#
# Get the current status of a dataset
#
def datasetStatus(dbsapi,ads):

  params = ads.split('/')
  proc = dbsapi.listProcessedDatasets(patternPrim=params[1],  patternProc=params[2], patternDT=params[3])

  if proc and proc[0]['Status']:
    dsStatus = proc[0]['Status']
  else:
    # listProcessedDatasets will not list INVALID datasets, so check the list of all datasets to see
    # if it is there
    paths = dbsapi.listDatasetPaths()
    pathset = set(paths)
    if ads in pathset:
      dsStatus = 'INVALID'
    else:
      dsStatus = 'NOT FOUND'

  return dsStatus

# 
# Invalidate/validate dataset
#
def setDatasetStatus(dbsapi,ads,valid):
  oldStatus = datasetStatus(dbsapi, ads)
  didit=False

  if valid:
    if oldStatus == 'INVALID':
      if parentsValid(dbsapi,ads):
        print "Validating Dataset %s"%ads
        dbsapi.updateProcDSStatus(ads,"VALID")
        didit=True
    else:
      print "Dataset %s status is %s, must be INVALID to validate" % (ads, oldStatus)

  else:
    if oldStatus == 'VALID':
      if childrenInvalid(dbsapi,ads):
        print "Invalidating Dataset %s"%ads
        dbsapi.updateProcDSStatus(ads,"INVALID")
        didit=True
    else:
      print "Dataset %s status is %s, must be VALID to invalidate" % (ads, oldStatus)

  return didit

#
# Invalidate/validate all the files in a dataset
#
def setDatasetFilesStatus(dbsapi,ads,valid):

  retrieveList=['retrive_status']

  if valid:
    retrieveList.append('retrive_invalid_files')
    newStatus='VALID'
    oldStatus='INVALID'
  else:
    newStatus='INVALID'
    oldStatus='VALID'

  files=dbsapi.listFiles(path=ads,retriveList=retrieveList)

  if files:
    print "Setting files to status " + newStatus

  for f in files:
    if f['Status'] == oldStatus:
      dbsapi.updateFileStatus(f['LogicalFileName'], newStatus)

  return

#
# Process a list of datasets
#
def updateDatasetStatus(dbsapi,lds,valid,files):
  if (lds != None):
    datasetList=lds.split(',')
    for ads in datasetList:
      if setDatasetStatus(dbsapi,ads,valid) and files:
        setDatasetFilesStatus(dbsapi,ads,valid)

  return

def main ():
  from optparse import OptionParser

  usage="""\npython DBSInvalidateDataset <options> \nOptions: \n --DBSURL=<URL> \t\t DBS URL \n --datasetPath=<dataset> \t dataset \n --valid \t\t\t re-validate an invalid dataset\n --files \t\t\t change status of all the files in the dataset"""
  parser = OptionParser(usage=usage)

  parser.add_option('-D', '--DBSURL', dest='url', default='https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet', help='DBS URL')
  parser.add_option('-d', '--datasetPath', dest='dataset', default=None, help='Dataset')
  parser.add_option('-v', '--valid', action="store_true", default=False,dest='valid', help='Validate status instead of invalidate')
  parser.add_option('-f', '--files', action="store_true", default=False,dest='files', help='Validate or invalidate all files in dataset')

  (opts, args) = parser.parse_args()

  if opts.url == None:
    print "--url option not provided."
    print "Using %s"%opts.url

  if opts.dataset == None:
    print "--dataset option must be provided"
    print usage;
    sys.exit(1)

  dbsargs = {'url' : opts.url}
  dbsapi = DbsApi(dbsargs)

  try:
    if opts.dataset != None:
      updateDatasetStatus(dbsapi, opts.dataset, opts.valid, opts.files)

  except Exception, ex:
    print "Caught exception %s:"%str(ex)
    sys.exit(1)

  sys.exit(0)

if __name__ == "__main__":
  main()

