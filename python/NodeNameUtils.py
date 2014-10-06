########################################################
#
# functions to access and maniputlate informations from cmsweb
# (SiteDB and PhEDEx dataservice) about node names
# PNN = PhEDEx Node Name = phedex node where data is
# PSN = Processing Site Name = CMS Site where jobs are sent
#
########################################################
#
import json
import cjson
import Lexicon
import sys
import commands
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
        status, j = commands.getstatusoutput(cmd)
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
        status, cj = commands.getstatusoutput(cmd)
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

    # beware: not touch list we are iterating on, make a deep copy
    tmpList = list(inputList)

    for dest in inputList:
        for black in blackList:
            if dest.startswith(black) :
                if dest in tmpList:
                    tmpList.remove(dest)

    if not whiteList:
        cleanedList = tmpList
    else:
        cleanedList = []
        for dest in tmpList:
            for white in whiteList:
                if dest.startswith(white):
                    cleanedList.append(dest)

    return cleanedList
            
def applyGloablBlackList(cfg_params):
    
    removeBList = cfg_params.get("GRID.remove_default_blacklist", 0 )
    blackAnaOps = None
    if int(removeBList) == 0:
        try:
            blacklist = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/")
            result = blacklist.config("site_black_list.conf")
        except:            
            msg = "ERROR talking to cmsdoc\n%s"%str(sys.exc_info()[1])
            common.logger.info(msg)
            common.logger.info("WARNING: Could not download default site black list")
            result = []
        if result :
            blackAnaOps = result.strip()
            common.logger.debug("Enforced black list: %s "%blackAnaOps)
        else:
            common.logger.info("WARNING: Skipping default black list!")
    if blackAnaOps:
        blackUser = cfg_params.get("GRID.se_black_list", '' )
        if blackUser:
            cfg_params['GRID.se_black_list'] = blackUser + ',' + blackAnaOps 
        else:
            cfg_params['GRID.se_black_list'] = blackAnaOps
        msg = "PSN black list: %s" % cfg_params['GRID.se_black_list']
        common.logger.info(msg)

def validateSiteName(site):
    """
    make sure site has the Tx_cc_nnnn format or valid abbreviation
    returns True or False
    """
    return True

def validateBWLists(cfg_params):
    # convert to lists for processing. But leave cfg_params
    # as strings, since this is what Crab2 code expects
    blackList = cfg_params.get("GRID.se_black_list", [] )
    if type(blackList) == type("string") :
        blackList = blackList.strip().split(',')
    whiteList = cfg_params.get("GRID.se_white_list", [] )
    if type(whiteList) == type("string") :
        whiteList = whiteList.strip().split(',')

    # make sure each item in the list is a valid cms node name
    # or possibly a shortcut like T3

    for site in blackList:
        try:
            Lexicon.cmsname(site)
        except Exception, text:
            msg = "ERROR in GRID.se_black_list: %s\n" % blackList
            msg += "%s\n'%s' is not a valid Phedex Node Name" % (text,site)
            raise CrabException(msg)

    for site in whiteList:
        try:
            Lexicon.cmsname(site)
        except Exception, text:
            msg = "ERROR in GRID.se_white_list: %s\n" % whiteList
            msg += "%s\n'%s' is not a valid Phedex Node Name" % (text,site)
            raise CrabException(msg)


def parseIntoList(param):
    """
    to be used to make sure that one crab config parameter is usable as a list of strings,
    eve if it is a string with comma insides in the config. file
    """
    if type(param) == type("string") :
        list = param.split(',')
        for item in list:
            item = item.strip()
    else:
        list = param

    return list
    

def getMapOfSEHostName2PhedexNodeNameFromPhEDEx():
    """
    returns a dictionary, key is SE host name, value is PhedexNodeName
    {'SE':'PNN', ...}
    """

    # retrieve information from PhEDEx API and reformat
    # if one same SE is used in more PNN's (CERN e.g.) it will
    # appear multiple times, one for each SE/PNN combination
    
    cmd = 'curl -ks "https://cmsweb.cern.ch/phedex/datasvc/json/prod/senames?protocol=srmv2"'
    
    try:
        j = None
        status, j = commands.getstatusoutput(cmd)
        phedexDict = json.loads(j)
    except:
        msg = "ERROR in $CRABPYTHON/NodeUtils.py trying to retrieve Phedex SE/Node map  with\n%s" %cmd
        if j:
            msg += "\n       command stdout is:\n%s" % j
        msg += "\n       which raised:\n%s" % str(sys.exc_info()[1])
        raise CrabException(msg)

    senameDictionaries=phedexDict['phedex']['senames']
    se2pnn={}
    for dict in senameDictionaries:
        node = str(dict['node'])  # beware Unicode
        # skip phedex nodes we would not submit to anyhow
        if node.startswith('T0'): continue
        if node.startswith('T1') and not node.endswith('Disk'): continue
        se = str(dict['sename'])  # beware Unicode
        if not se in se2pnn.keys():
            se2pnn[se] = node

    return se2pnn

def getListOfPSNsForThisDomain(fqdn):
    """
    return a list of Processing Site Names
    which belong to this domain, i.e. which are connected
    to PhEDEx Node names that have SE's with same fqdn
    """
    se2pnn = getMapOfSEHostName2PhedexNodeNameFromPhEDEx()
    pnn2psn = getMapOfPhedexNodeName2ProcessingNodeNameFromSiteDB()

    listOfPSNs = []
    for se in se2pnn.keys():
        if fqdn in se:
            pnn = se2pnn[se]
            psn = pnn2psn[pnn]
            if not psn in listOfPSNs:
                listOfPSNs.append(psn)

    return listOfPSNs
