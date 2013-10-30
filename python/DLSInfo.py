#!/usr/bin/env python
import sys, os, commands,string, re
import exceptions
from crab_exceptions import *
from crab_util import *
import common

import dlsApi
import dlsClient
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
                                                                                            
class DLSError:
    def __init__(self, fileblocks):
        print '\nERROR accessing DLS for fileblock '+fileblocks+'\n'
        pass


class DLSNoReplicas(exceptions.Exception):
    def __init__(self, FileBlock):
        self.args ="No replicas exists for fileblock: %s \n"%str(FileBlock)
        exceptions.Exception.__init__(self, self.args)
        pass

    def getClassName(self):
        """ Return class name. """
        return "%s" % (self.__class__.__name__)

    def getErrorMessage(self):
        """ Return exception error. """
        return "%s" % (self.args)


##############################################################################
# Class to extract info from DLS 
##############################################################################

class DLSInfo:
    def __init__(self, type, cfg_params):
        self.cfg_params = cfg_params
        self.showCAF = False
        self.showProd = False
        self.subscribed = False    
        subscribed =int(self.cfg_params.get('CMSSW.subscribed', 0))  
        if subscribed == 1: self.subscribed = True
        showProd = int(self.cfg_params.get('CMSSW.show_prod', 0)) 
        if showProd == 1: self.showProd = True

        phedexURL='https://cmsweb.cern.ch/phedex/datasvc/xml/prod/'
        global_url="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

        if type=="DLS_TYPE_DBS":
            # use dbs_url as dls_endpoint if dls_type is dbs
            endpoint=self.cfg_params.get('CMSSW.dbs_url', global_url) 
        elif type=="DLS_TYPE_PHEDEX":
            endpoint=self.cfg_params.get('CMSSW.dls_phedex_url',phedexURL)
            if self.cfg_params['CRAB.scheduler'].upper() == 'CAF':  self.showCAF = True
        else:
            msg = "DLS type %s not among the supported DLS ( DLS_TYPE_DLI and DLS_TYPE_MYSQL ) "%type
            raise CrabException(msg)
        common.logger.debug("DLS interface: %s Server %s"%(type,endpoint))       
        try:
            self.api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
        except dlsApi.DlsApiError, inst:
            msg = "Error when binding the DLS interface: %s  Server %s"%(str(inst),endpoint)
            #print msg
            raise CrabException(msg)

    def getReplicasBulk(self,fileblocks):
        """
        query DLS to get replicas
        """               
        ##
        try:
            entryList=self.api.getLocations(fileblocks,longList=True,showCAF=self.showCAF,showProd=self.showProd,subscribed=self.subscribed)
        except dlsApi.DlsApiError, inst:
            raise DLSNoReplicas(fileblocks)
        results = {} 
        for entry in entryList: 
            ListSites=[] 
            for loc in entry.locations:
                ListSites.append(str(loc.host))
            if len(ListSites)<=0:
                msg ="No replicas exists for fileblock: %s \n"%str(fileblocks)
                raise CrabException(msg)
            results[entry.fileBlock.name]=ListSites

        return results         
# ####################################
    def getReplicas(self,fileblocks):
        """
        query DLS to get replicas
        """
        ##
        try:
            entryList=self.api.getLocations([fileblocks],showCAF=self.showCAF,subscribed=self.subscribed)
        except dlsApi.DlsApiError, inst:
            raise DLSNoReplicas(fileblocks)

        ListSites=[] 
        for entry in entryList:
            for loc in entry.locations:
                ListSites.append(str(loc.host))
        if len(ListSites)<=0:
            raise DLSNoReplicas(fileblocks)

        return ListSites         
