########################################################
#
# functions to access and maniputlate informations from cmsweb
# (SiteDB and PhEDEx dataservice) about node names
# PNN = PhEDEx Node Name = phedex node where data is
# PSN = Processing Site Name = CMS Site where jobs are sent
#
########################################################
#
import subprocess
import json
import cjson
import Lexicon
import sys
from crab_exceptions import *
from Downloader import Downloader
import common


def  expandIntoListOfPhedexNodeNames(location_list):
    """
    take as input a list of locations, returns a list of PNN's
    raise CrabExceptoin if input is not a valid PNN abbreviation
    use https://cmsweb.cern.ch/phedex/datasvc/doc/nodes
    """

    # build API node filter, add wildcards wich are not required by Crab2
    args = ''
    for loc in location_list:
        phedexNode = loc.strip()
        try:
            Lexicon.cmsname(phedexNode)
        except Exception, text:
            msg =  "%s\n'%s' is not a valid Phedex Node Name" % (text,phedexNode)
            raise CrabException(msg)
        args += "&node=%s*" % phedexNode
    # first char of arg to API is ?, not &
    args = '?'+args[1:]
    apiUrl = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/Nodes' + args
    cmd = 'curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "%s"' % apiUrl
    try:
        j = None
        j = subprocess.check_output(cmd,shell=True)
        nodeDict = json.loads(j)
    except:
        msg = "ERROR in $CRABPYTHON/cms_cmssw.py trying to retrieve Phedex Node list  with\n%s" %cmd
        if j:
            msg += "\n       command stdout is:\n%s" % j
        msg += "\n       which raised:\n%s" % str(sys.exc_info()[1])
        raise CrabException(msg)

    listOfPNNs = []
    PNNdicts = nodeDict['phedex']['node']
    for node in PNNdicts:
        # beware SiteDB V2 API, cast to string to avoid unicode
        listOfPNNs.append(str(node['name']))
        
    return listOfPNNs


def getMapOfPhedexNodeName2ProcessingNodeNameFromSiteDB():
    """
 returns a dictionary, key is PhedexNodeName, value is ProcessingNodeName
  {'PNN':'PSN', ...}
  """

    cmd = 'curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "https://cmsweb.cern.ch/sitedb/data/prod/data-processing"'
    try:
        cj = subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
    except :
        msg = "ERROR trying to talk to SiteDB\n%s"%str(sys.exc_info()[1])
        raise CrabException(msg)
    try:
        dataProcessingDict = cjson.decode(cj)
    except:
        msg = "ERROR decoding SiteDB output\n%s"%cj
        raise CrabException(msg)
    pnn2psn={}
    for s in dataProcessingDict['result']:
        # beware SiteDB V2 API, cast to string to avoid unicode
        pnn2psn[s[0]] = str(s[1])
        
    return pnn2psn

def cleanPsnListForBlackWhiteLists(inputList, blackList, whiteList):

    """
    Take the input list and apply the blacklist, then the whitelist
    B/W lists accept the formats T1, T2_IT etc. to select all sites
    of that king. validation of crab.cfg paramter must be done before
    calling this
    It also applied global black list
    All arguments must be passed as LIST (not strings)
    """

    #print "iL, BL, WL:"
    #print inputList
    #print blackList
    #print whiteList
    #print "----------------------------"
    tmpList = list(inputList)
    for dest in inputList:
        for black in blackList:
            if dest.startswith(black) :
                tmpList.remove(dest)
    
    #print "after BL: ", tmpList
    if not whiteList:
        cleanedList = tmpList
    else:
        cleanedList = []
        for dest in tmpList:
            for white in whiteList:
                if dest.startswith(white):
                    cleanedList.append(dest)

    #print "after WL: ", cleanedList
    return cleanedList
            
def applyGloablBlackList(cfg_params):
    
    removeBList = cfg_params.get("GRID.remove_default_blacklist", 0 )
    blackAnaOps = None
    if int(removeBList) == 0:
        try:
            blacklist = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/")
            result = blacklist.config("site_black_list.conf").strip().split(',')
        except:            
            msg = "ERROR talking to cmsdoc\n%s"%str(sys.exc_info()[1])
            common.logger.info(msg)
            common.logger.info("WARNING: Could not download default site black list")
            result = []
        if result :
            blackAnaOps = result
            common.logger.debug("Enforced black list: %s "%blackAnaOps)
        else:
            common.logger.info("WARNING: Skipping default black list!")
    if blackAnaOps:
        blackUser = cfg_params.get("GRID.se_black_list", [] )
        cfg_params['GRID.se_black_list'] = blackUser + blackAnaOps 


def validateBWLists(cfg_params):
    # make sure to have lists, not string
    blackList = cfg_params.get("GRID.se_black_list", [] )
    if type(blackList) == type("string") :
        blackList = blackList.strip().split(',')
    whiteList = cfg_params.get("GRID.se_white_list", [] )
    if type(whiteList) == type("string") :
        whiteList = whiteList.strip().split(',')

    cfg_params['GRID.se_black_list'] = blackList
    cfg_params['GRID.se_white_list'] = whiteList

    pass

