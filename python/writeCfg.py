#!/usr/bin/env python

"""
Re-write config file and optionally convert to python
"""

__revision__ = "$Id: writeCfg.py,v 1.33 2011/03/23 16:48:53 spiga Exp $"
__version__ = "$Revision: 1.33 $"

import getopt
import imp
import os
import pickle
import sys
import xml.dom.minidom

from random import SystemRandom

from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
import FWCore.ParameterSet.Types as CfgTypes

MyRandom  = SystemRandom()

class ConfigException(Exception):
    """
    Exceptions raised by writeCfg
    """

    def __init__(self, msg):
        Exception.__init__(self, msg)
        self._msg = msg
        return

    def __str__(self):
        return self._msg

def main(argv) :
    """
    writeCfg

    - Read in existing, user supplied pycfg or pickled pycfg file
    - Modify job specific parameters based on environment variables and arguments.xml
    - Write out pickled pycfg file

    required parameters: none

    optional parameters:
    --help             :       help
    --debug            :       debug statements

    """

    # defaults
    inputFileNames  = None
    parentFileNames = None
    debug           = False
    _MAXINT         = 900000000

    try:
        opts, args = getopt.getopt(argv, "", ["debug", "help"])
    except getopt.GetoptError:
        print main.__doc__
        sys.exit(2)

    try:
        CMSSW  = os.environ['CMSSW_VERSION']
        parts = CMSSW.split('_')
        CMSSW_major = int(parts[1])
        CMSSW_minor = int(parts[2])
    except (KeyError, ValueError):
        msg = "Your environment doesn't specify the CMSSW version or specifies it incorrectly"
        raise ConfigException(msg)

    # Parse command line options
    for opt, arg in opts :
        if opt  == "--help" :
            print main.__doc__
            sys.exit()
        elif opt == "--debug" :
            debug = True

    # Parse remaining parameters
    try:
        fileName    = args[0]
        outFileName = args[1]
    except IndexError:
        print main.__doc__
        sys.exit()

  # Read in Environment, XML and get optional Parameters

    nJob       = int(os.environ.get('NJob',      '0'))
    preserveSeeds  = os.environ.get('PreserveSeeds','')
    incrementSeeds = os.environ.get('IncrementSeeds','')

  # Defaults

    maxEvents  = 0
    skipEvents = 0
    firstEvent = -1
    compHEPFirstEvent = 0
    firstRun   = 0
    # FUTURE: Remove firstRun
    firstLumi  = 0

    dom = xml.dom.minidom.parse(os.environ['RUNTIME_AREA']+'/arguments.xml')

    for elem in dom.getElementsByTagName("Job"):
        if nJob == int(elem.getAttribute("JobID")):
            if elem.getAttribute("MaxEvents"):
                maxEvents = int(elem.getAttribute("MaxEvents"))
            if elem.getAttribute("SkipEvents"):
                skipEvents = int(elem.getAttribute("SkipEvents"))
            if elem.getAttribute("FirstEvent"):
                firstEvent = int(elem.getAttribute("FirstEvent"))
            if elem.getAttribute("FirstRun"):
                firstRun = int(elem.getAttribute("FirstRun"))
            if elem.getAttribute("FirstLumi"):
                firstLumi = int(elem.getAttribute("FirstLumi"))

            generator      = str(elem.getAttribute('Generator'))
            inputBlocks    = str(elem.getAttribute('InputBlocks'))
            inputFiles     = str(elem.getAttribute('InputFiles'))
            parentFiles    = str(elem.getAttribute('ParentFiles'))
            lumis          = str(elem.getAttribute('Lumis'))

    report(inputBlocks,inputFiles,parentFiles,lumis)

  # Read Input python config file

    handle = open(fileName, 'r')
    try:   # Nested form for Python < 2.5
        try:
            print "Importing .py file"
            cfo = imp.load_source("pycfg", fileName, handle)
            cmsProcess = cfo.process
        except Exception, ex:
            msg = "Your pycfg file is not valid python: %s" % str(ex)
            raise ConfigException(msg)
    finally:
        handle.close()

    cfg = CfgInterface(cmsProcess)

    # Set parameters for job
    print "Setting parameters"
    inModule = cfg.inputSource
    if maxEvents:
        cfg.maxEvents.setMaxEventsInput(maxEvents)

    if skipEvents and inModule.sourceType not in ['EmptySource']:
        inModule.setSkipEvents(skipEvents)

    # Set "skip events" for various generators
    if generator == 'comphep':
        cmsProcess.source.CompHEPFirstEvent = CfgTypes.int32(firstEvent)
    elif generator == 'lhe':
        cmsProcess.source.skipEvents = CfgTypes.untracked(CfgTypes.uint32(firstEvent))
        cmsProcess.source.firstEvent = CfgTypes.untracked(CfgTypes.uint32(firstEvent+1))
    elif firstEvent != -1: # (Old? Madgraph)
        cmsProcess.source.firstEvent = CfgTypes.untracked(CfgTypes.uint32(firstEvent))

    if inputFiles:
        inputFileNames = inputFiles.split(',')
        inModule.setFileNames(*inputFileNames)

    # handle parent files if needed
    if parentFiles:
        parentFileNames = parentFiles.split(',')
        inModule.setSecondaryFileNames(*parentFileNames)

    if lumis:
        if CMSSW_major < 3: # FUTURE: Can remove this check
            print "Cannot skip lumis for CMSSW 2_x"
        else:
            lumiRanges = lumis.split(',')
            inModule.setLumisToProcess(*lumiRanges)

    # Pythia parameters
    if (firstRun):
        inModule.setFirstRun(firstRun)
    if (firstLumi):
        inModule.setFirstLumi(firstLumi)

    # Check if there are random #'s to deal with
    if cfg.data.services.has_key('RandomNumberGeneratorService'):
        print "RandomNumberGeneratorService found, will attempt to change seeds"
        from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
        ranGenerator = cfg.data.services['RandomNumberGeneratorService']

        ranModules   = getattr(ranGenerator, "moduleSeeds", None)
        oldSource    = getattr(ranGenerator, "sourceSeed",  None)
        if ranModules != None or oldSource != None:
            msg = "Your random number seeds are set in an old,\n"
            msg += "deprecated style. Please change to new style:\n"
            msg += "https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideEDMRandomNumberGeneratorService"
            raise ConfigException(msg)

        randSvc = RandomNumberServiceHelper(ranGenerator)

        incrementSeedList = []
        preserveSeedList  = []

        if incrementSeeds:
            incrementSeedList = incrementSeeds.split(',')
        if preserveSeeds:
            preserveSeedList  = preserveSeeds.split(',')

        # Increment requested seed sets
        for seedName in incrementSeedList:
            curSeeds = randSvc.getNamedSeed(seedName)
            newSeeds = [x+nJob for x in curSeeds]
            randSvc.setNamedSeed(seedName, *newSeeds)
            preserveSeedList.append(seedName)

        # Randomize remaining seeds
        randSvc.populate(*preserveSeedList)

    # Write out new config file
    pklFileName = outFileName + '.pkl'
    outFile = open(outFileName,"w")
    outFile.write("import FWCore.ParameterSet.Config as cms\n")
    outFile.write("import pickle\n")
    outFile.write("process = pickle.load(open('%s', 'rb'))\n" % pklFileName)
    outFile.close()

    pklFile = open(pklFileName,"wb")
    myPickle = pickle.Pickler(pklFile)
    myPickle.dump(cmsProcess)
    pklFile.close()

    if (debug):
        print "writeCfg output (May not be exact):"
        print "import FWCore.ParameterSet.Config as cms"
        print cmsProcess.dumpPython()


def report( inputBlocks='', inputFiles='', parentFiles='', lumis='' ):
    """
    Writes the 4 parameters to a file, one parameter per line.
    """
    outFile = open('%s/inputsReport.txt'%os.environ['RUNTIME_AREA'],"a")

  #  InputFileList=inputFiles.split(',') 
  #  parentFilesList= parentFiles.split(',')
  #  lumisList= lumis.split(',')

    ## replacing , with ; otherwise report.py will split it as a new parameter
    txt = ''
    txt += 'inputBlocks='+inputBlocks.replace(',',';')+'\n'
    txt += 'inputFiles='+inputFiles.replace(',',';')+'\n'
    txt += 'parentFiles='+parentFiles.replace(',',';')+'\n'
    txt += 'lumisRange='+lumis.replace(',',';')+'\n'
    outFile.write(str(txt))
    outFile.close()
    return


if __name__ == '__main__' :
    exit_status = main(sys.argv[1:])
    sys.exit(exit_status)
