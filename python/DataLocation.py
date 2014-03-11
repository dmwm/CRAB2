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
                                       #  retrieved using method getSites

# #######################################################################
    def fetchDLSInfo(self):
        """
        Contact DLS
        """

        # Force show_prod=1 for everybody, grid jobs rely on central black list and t1access role to limit access to T1's
        self.cfg_params['CMSSW.show_prod'] = 1

        # make assumption that same host won't be used for both
        # this check should catch most deployed servers
        DBS2HOST = 'cmsdbsprod.cern.ch'
        DBS3HOST = 'cmsweb.cern.ch'
        (isDbs2, isDbs3, dbs2_url, dbs3_url) = verify_dbs_url(self)
        if isDbs2: dbs_url=dbs2_url
        if isDbs3: dbs_url=dbs3_url

        global_dbs2 = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        global_dbs3 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"

        if dbs_url==global_dbs2 or dbs_url==global_dbs3:
            # global DBS has no location info
            DLS_type="DLS_TYPE_PHEDEX"
            dls=DLSInfo(DLS_type,self.cfg_params)
            blockSites = dls.getReplicasBulk(self.Listfileblocks)
        else:
            # assume it is some local scope DBS
            if isDbs2:
                DLS_type="DLS_TYPE_DBS"
                dls=DLSInfo(DLS_type,self.cfg_params)
                blockSites = self.PrepareDict(dls)
            else:
                # assume it is some DBS3 end point
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

    def PrepareDict(self,dls):
        ## find the replicas for each block
        failCount = 0
        countblock=0
        blockSites = {}
        for fileblocks in self.Listfileblocks:
            countblock=countblock+1
            try:
                replicas=dls.getReplicas(fileblocks)
                common.logger.debug("sites are %s"%replicas)
                if len(replicas)!=0:
                    blockSites[fileblocks] = replicas
                else:
                    # add empty entry if no replicas found
                    blockSites[fileblocks] = ''

            except DLSNoReplicas, ex:
                common.logger.debug(str(ex.getErrorMessage()))
                common.logger.debug("Block not hosted by any site, continuing.\n")
                blockSites[fileblocks] = ''
                failCount = failCount + 1
            except:
                raise CrabException('')

        if countblock == failCount:
            msg = "All data blocks encountered a DLS error.  Quitting."
            raise CrabException(msg)
        return blockSites
# #######################################################################
    def getSites(self):
        """
        get the sites hosting all the needed data
        """
        return self.SelectedSites

#######################################################################
    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()
