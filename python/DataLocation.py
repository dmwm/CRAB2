#!/usr/bin/env python
import os, string, re
import common
import urlparse
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
        DLS_type="DLS_TYPE_PHEDEX"
        dls=DLSInfo(DLS_type,self.cfg_params)
        blockSites = dls.getReplicasBulk(self.Listfileblocks)
        if len(blockSites) == 0 :
            common.logger.info("No dataset location information found in PhEDEx")
            if dbs_url == global_dbs3:
                common.logger.info("Dataset in global DBS without location information")
            else:
                common.logger.info("Use origin site location recorded in local scope DBS")
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

        blockSites = {}
        for block in self.Listfileblocks:
            blockInfo=api.listBlocks(block_name=block,detail=True)
            location=blockInfo[0]['origin_site_name']
            if location == 'UNKNOWN':
                blockSites[block] = []
            else:
                blockSites[block] = [location]

        return blockSites

# #######################################################################
    def getSites(self):
        """
        get the sites hosting all the needed data
        """
        return self.SelectedSites

