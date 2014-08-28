#!/usr/bin/env python
import os, string, re
import common
import json
import cjson
import subprocess
from DLSInfo import *

# ####################################
class DataLocationError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
# class to extact data location
class DataLocation:
    def __init__(self, Listfileblocks, cfg_params):

#       Attributes
        self.Listfileblocks = Listfileblocks  # DLS input: list of fileblocks lists

        self.cfg_params = cfg_params
        self.datasetPath = cfg_params['CMSSW.datasetpath']

        self.SelectedSites = {}        # DLS output: list of sites hosting fileblocks
                                       # can be retrieved using method getSites

# #######################################################################
    def fetchDLSInfo(self):
        """
        Contact DLS
        """

        # Force show_prod=1 for everybody, grid jobs rely on central black list and t1access role to limit access to T1's
        self.cfg_params['CMSSW.show_prod'] = 1

        # make assumption that same host won't be used for both
        # this check should catch most deployed servers

        (isDbs2, isDbs3, dbs2_url, dbs3_url) = verify_dbs_url(self)
        dbs_url=dbs3_url

        global_dbs3 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"

        # first try PhEDEx

        PhEDExUrl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
        apiUrl = PhEDExUrl + "BlockReplicaSummary?dataset=%s&complete=y" % self.datasetPath
        cmd = 'curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "%s"' % apiUrl
        common.logger.debug("Retrieve block locations with\n%s" % cmd)

        try:
            j=None
            j=subprocess.check_output(cmd,shell=True)
            dict=json.loads(j)
        except:
            import sys
            msg = "ERROR in $CRABPYTHON/DataLocation.py trying to retrieve data locations with\n%s" %cmd
            if j:
                msg += "\n       command stdout is:\n%s" % j
            msg += "\n       which raised:\n%s" % str(sys.exc_info()[1])
            raise CrabException(msg)
        
        blockLocations=dict['phedex']['block']
        # blockLocations is a list of dictionaries, one per block
        # format of each entry is like
        # {u'name': u'/SingleMu/Run2012B-TOPMuPlusJets-22Jan2013-v1/AOD#42cbaf9c-715f-11e2-af21-00221959e72f',
        # u'replica': [{u'complete': u'y', u'node': u'T1_IT_CNAF_MSS'},
        #      {u'complete': u'y', u'node': u'T3_FR_IPNL'},
        #      {u'complete': u'y', u'node': u'T1_IT_CNAF_Disk'},
        #      {u'complete': u'y', u'node': u'T1_US_FNAL_Disk'},
        #      {u'complete': u'y', u'node': u'T2_US_Purdue'},
        #      {u'complete': u'y', u'node': u'T1_IT_CNAF_Buffer'}]}

        # retrieve from PhEDEx the storage type of each node to tell Disk from Tape
        PhEDExUrl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
        apiUrl = PhEDExUrl + "BlockReplicaSummary?dataset=%s&complete=y" % self.datasetPath
        cmd = 'curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "%s"' % apiUrl
        common.logger.debug("Retrieve block locations with\n%s" % cmd)

        try:
            j=None
            j=subprocess.check_output(cmd,shell=True)
            dict=json.loads(j)
        except:
            import sys
            msg = "ERROR in $CRABPYTHON/DataLocation.py trying to retrieve data locations with\n%s" %cmd
            if j:
                msg += "\n       command stdout is:\n%s" % j
            msg += "\n       which raised:\n%s" % str(sys.exc_info()[1])
            raise CrabException(msg)
        
        blockLocations=dict['phedex']['block']
        # blockLocations is a list of dictionaries, one per block
        # format of each entry is like
        # {u'name': u'/SingleMu/Run2012B-TOPMuPlusJets-22Jan2013-v1/AOD#42cbaf9c-715f-11e2-af21-00221959e72f',
        # u'replica': [{u'complete': u'y', u'node': u'T1_IT_CNAF_MSS'},
        #      {u'complete': u'y', u'node': u'T3_FR_IPNL'},
        #      {u'complete': u'y', u'node': u'T1_IT_CNAF_Disk'},
        #      {u'complete': u'y', u'node': u'T1_US_FNAL_Disk'},
        #      {u'complete': u'y', u'node': u'T2_US_Purdue'},
        #      {u'complete': u'y', u'node': u'T1_IT_CNAF_Buffer'}]}

        # retrieve from PhEDEx the storage type of each node to tell Disk from Tape
        PhEDExUrl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
        apiUrl = PhEDExUrl + "BlockReplicaSummary?dataset=%s&complete=y" % self.datasetPath
        cmd = 'curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "%s"' % apiUrl
        common.logger.debug("Retrieve block locations with\n%s" % cmd)

        try:
            j=None
            j=subprocess.check_output(cmd,shell=True)
            dict=json.loads(j)
        except:
            import sys
            msg = "ERROR in $CRABPYTHON/DataLocation.py trying to retrieve data locations with\n%s" %cmd
            if j:
                msg += "\n       command stdout is:\n%s" % j
            msg += "\n       which raised:\n%s" % str(sys.exc_info()[1])
            raise CrabException(msg)
        
        blockLocations=dict['phedex']['block']
        # blockLocations is a list of dictionaries, one per block
        # format of each entry is like
        # {u'name': u'/SingleMu/Run2012B-TOPMuPlusJets-22Jan2013-v1/AOD#42cbaf9c-715f-11e2-af21-00221959e72f',
        # u'replica': [{u'complete': u'y', u'node': u'T1_IT_CNAF_MSS'},
        #      {u'complete': u'y', u'node': u'T3_FR_IPNL'},
        #      {u'complete': u'y', u'node': u'T1_IT_CNAF_Disk'},
        #      {u'complete': u'y', u'node': u'T1_US_FNAL_Disk'},
        #      {u'complete': u'y', u'node': u'T2_US_Purdue'},
        #      {u'complete': u'y', u'node': u'T1_IT_CNAF_Buffer'}]}

        # retrieve from PhEDEx the storage type of each node to tell Disk from Tape
        # use cjson format this time (more convenient for the needd parsing)
        PhEDExUrl = "https://cmsweb.cern.ch/phedex/datasvc/cjson/prod/"
        apiUrl = PhEDExUrl + "nodes"
        cmd = 'curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "%s"' % apiUrl
        common.logger.debug("Retrieve PNNs type with\n%s" % cmd)
        try:
            cj=None
            cj=subprocess.check_output(cmd,shell=True)
            dict=cjson.decode(cj)
        except:
            import sys
            msg = "ERROR in $CRABPYTHON/DataLocation.py trying to retrieve PNNs type with\n%s" %cmd
            if j:
                msg += "\n       command stdout is:\n%s" % j
            msg += "\n       which raised:\n%s" % str(sys.exc_info()[1])
            raise CrabException(msg)
        
        columns = dict['phedex']['node']['column']
        values = dict['phedex']['node']['values']
        for i in range(len(columns)):
            if columns[i] == 'name': namInd = i

        # build a list of Pheded Node Names which are Disk
        diskNodes=[]
        for row in values:
            if 'Disk' in row:
                diskNodes.append(row[namInd])

        # convert to the blockSites format required by this code
        blockSites={}
        for block in blockLocations:
            bname = block['name']
            blockSites[bname]=[]
            for replica in block['replica']:
                site=replica['node']
                if site in diskNodes:
                    blockSites[bname].append(site)

        if len(blockSites) == 0 :
            common.logger.info("No dataset location information found in PhEDEx")
            if dbs_url == global_dbs3:
                common.logger.info("Dataset in global DBS without location information")
            else:
                common.logger.info("Use origin site location recorded in local scope DBS")
                blockSites = self.getBlockSitesFromLocalDBS3(dbs_url)
                try:
                    blockSites = self.getBlockSitesFromLocalDBS3(dbs_url)
                except:
                    msg = "CAN'T GET LOCATION INFO FROM DBS END POINT: %s\n" % dbs_url
                    raise CrabException(msg)

        self.SelectedSites = blockSites

# #######################################################################

    def getBlockSitesFromLocalDBS3(self,dbs_url):

        ## find the location for each block in the list
        from dbs.apis.dbsClient import DbsApi
        api = DbsApi(dbs_url)

        from NodeNameUtils import getMapOfSEHostName2PhedexNodeNameFromPhEDEx

        se2pnn = getMapOfSEHostName2PhedexNodeNameFromPhEDEx()

        blockSites = {}
        for block in self.Listfileblocks:
            blockInfo=api.listBlocks(block_name=block,detail=True)
            location=blockInfo[0]['origin_site_name']
            if location == 'UNKNOWN':
                blockSites[block] = []
            else:
                #if locationIsValidPNN:
                if location.startswith('T2_') or location.startswith('T3_'):
                    blockSites[block] = [location]
                else:
                    if location in se2pnn.keys():
                        blockSites[block] = [se2pnn[location]]
                    else:
                        msg = "ERROR: unknown location for block: %s. Skip this block" % location
                        common.logger.info(msg)
                        blockSites[block] = []

        return blockSites

# #######################################################################
    def getSites(self):
        """
        get the sites hosting all the needed data
        """
        return self.SelectedSites

