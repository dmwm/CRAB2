#!/usr/bin/env python

import os
import common
import imp
import pickle

from crab_util import *
from crab_exceptions import *

from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
# FIXME: Cleanup includes from FWCore. Most of this is not needed.
#from FWCore.ParameterSet.Config    import include
from FWCore.ParameterSet.DictTypes import SortedKeysDict
from FWCore.ParameterSet.Modules   import OutputModule
from FWCore.ParameterSet.Modules   import Service
from FWCore.ParameterSet.Types     import *

import FWCore.ParameterSet.Types   as CfgTypes
import FWCore.ParameterSet.Modules as CfgModules
import FWCore.ParameterSet.Config  as cms

class PsetManipulator:
    def __init__(self, pset):
        """
        Read in Pset object and initialize
        """

        self.pset = pset

        common.logger.debug("PsetManipulator::__init__: PSet file = "+self.pset)
        handle = open(self.pset, 'r')
        try:   # Nested form for Python < 2.5
            try:
                self.cfo = imp.load_source("pycfg", self.pset, handle)
                self.cmsProcess = self.cfo.process
            except Exception, ex:
                msg = "Your config file is not valid python: %s" % str(ex)
                raise CrabException(msg)
        finally:
            handle.close()

        self.cfg = CfgInterface(self.cmsProcess)
        try: # Quiet the output
            if self.cfg.data.MessageLogger.cerr.FwkReport.reportEvery.value() < 100:
                self.cfg.data.MessageLogger.cerr.FwkReport.reportEvery = cms.untracked.int32(100)
        except AttributeError:
            pass

    def maxEvent(self, maxEv):
        """
        Set max event in the standalone untracked module
        """
        self.cfg.maxEvents.setMaxEventsInput(maxEv)
        return

    def skipEvent(self, skipEv):
        """
        Set max event in the standalone untracked module
        """
        if self.cfg.inputSource.sourceType not in ['EmptySource']:
            self.cfg.inputSource.setSkipEvents(skipEv)
        return

    def psetWriter(self, name):
        """
        Write out modified CMSSW.py
        """

        pklFileName = common.work_space.jobDir() + name + ".pkl"
        pklFile = open(pklFileName, "wb")
        myPickle = pickle.Pickler(pklFile)
        myPickle.dump(self.cmsProcess)
        pklFile.close()

        outFile = open(common.work_space.jobDir()+name, "w")
        outFile.write("import FWCore.ParameterSet.Config as cms\n")
        outFile.write("import pickle\n")
        outFile.write("process = pickle.load(open('%s', 'rb'))\n" % (name + ".pkl"))
        outFile.close()


        return

    def getTFileService(self):
        """ Get Output filename from TFileService and return it. If not existing, return None """
        if not self.cfg.data.services.has_key('TFileService'):
            return None
        tFileService = self.cfg.data.services['TFileService']
        if "fileName" in tFileService.parameterNames_():
            fileName = getattr(tFileService,'fileName',None).value()
            return fileName
        return None

    def getPoolOutputModule(self):
        """ Get Output filename from PoolOutputModule and return it. If not existing, return None """
        outputFinder = PoolOutputFinder()
        for p  in self.cfg.data.endpaths.itervalues():
            p.visit(outputFinder)
        return outputFinder.getDict()
        #return outputFinder.getList()

    def getBadFilesSetting(self):
        setting = False
        try:
            if self.cfg.data.source.skipBadFiles.value():
                setting = True
        except AttributeError:
            pass # Either no source or no setting of skipBadFiles
        return setting

class PoolOutputFinder(object):

    def __init__(self):
        self._poolList = []
        self._poolDict = {}

    def enter(self,visitee):
        if isinstance(visitee,OutputModule) and visitee.type_() == "PoolOutputModule":
            filename=visitee.fileName.value().split(":")[-1]
            self._poolList.append(filename)

            try:
                selectEvents = visitee.SelectEvents.SelectEvents.value()
            except AttributeError:
                selectEvents = None
            try:
                dataset = visitee.dataset.filterName.value()
            except AttributeError:
                dataset = None
            self._poolDict.update({filename:{'dataset':dataset, 'selectEvents':selectEvents}})

    def leave(self,visitee):
        pass

    def getList(self):
        return self._poolList

    def getDict(self):
        #### FEDE FOR MULTI ####
        return self._poolDict
