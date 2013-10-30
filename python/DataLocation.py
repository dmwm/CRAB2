#!/usr/bin/env python
import os, string, re
import common
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
        dlstype=''

        useDBS    = self.cfg_params.get('CMSSW.dbs_url',None)
        scheduler = self.cfg_params.get('CRAB.scheduler',None).lower()
        global_url = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

        # Force show_prod=1 for everybody, grid jobs rely on central black list and t1access role to limit access to T1's

        # Don't switch to DBS just because user specified the global URL in config

        self.cfg_params['CMSSW.show_prod'] = 1
        if useDBS == global_url:
            useDBS = None

        if useDBS:
            dlstype='dbs'
            DLS_type="DLS_TYPE_%s"%dlstype.upper()
            dls=DLSInfo(DLS_type,self.cfg_params)
            blockSites = self.PrepareDict(dls)
        else:
            dlstype='phedex'
            DLS_type="DLS_TYPE_%s"%dlstype.upper()
            dls=DLSInfo(DLS_type,self.cfg_params)
            blockSites = dls.getReplicasBulk(self.Listfileblocks)

        self.SelectedSites = blockSites

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
                raise DataLocationError('')

        if countblock == failCount:
            msg = "All data blocks encountered a DLS error.  Quitting."
            raise DataLocationError(msg)
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
