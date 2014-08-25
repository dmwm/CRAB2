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

    tmpList = list(inputList)
    for dest in inputList:
        for black in blackList:
            if dest.startswith(black) :
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

def getMapOfSEHostName2PhedexNodeNameFromPhEDEx():
    """
    returns a dictionary, key is SE host name, value is PhedexNodeName
    {'SE':'PNN', ...}
    """

    """
    at the moment new phedex API is only available on CERN LAN,
    so hardcode here the situation as August 19, 2014
    The following dictionary was obtained with:
    cmd = 'curl -ks "https://phedex-web-dev.cern.ch/phedex/datasvc/json/prod/senames?protocol=srmv2"'
    jN=subprocess.check_output(cmdPhedex,shell=True)
    dictN=json.loads(jN)
    senames=dictN['phedex']['senames']
    se2pnn={}
    for sename in senames:
        se = str(sename['sename'])
        node = str(sename['node'])
        if node.startswith('T1'):
            disk = node.rpartition('_')[0] + '_Disk'
            node = disk
        if node.startswith('T0'): continue
        if node == 'T1_CH_CERN_Disk':
            node = 'T2_CH_CERN'
        if not se in se2pnn.keys():
            se2pnn[se] = node
    """

    se2pnn = {'bonner-grid.rice.edu': 'T3_US_Rice',
 'bsrm-3.t2.ucsd.edu': 'T2_US_UCSD',
 'ccsrm.in2p3.fr': 'T1_FR_CCIN2P3_Disk',
 'ccsrmt2.in2p3.fr': 'T2_FR_CCIN2P3',
 'charm.ucr.edu': 'T3_US_UCR',
 'cit-se.ultralight.org': 'T2_US_Caltech',
 'cluster.pnpi.nw.ru': 'T2_RU_PNPI',
 'cluster142.knu.ac.kr': 'T2_KR_KNU',
 'cms-0.mps.ohio-state.edu': 'T3_US_OSU',
 'cms-se.hep.uprm.edu': 'T3_US_PuertoRico',
 'cms-se.sc.chula.ac.th': 'T3_TH_CHULA',
 'cms-se.sdfarm.kr': 'T3_KR_KISTI',
 'cms-se0.kipt.kharkov.ua': 'T2_UA_KIPT',
 'cms-xen19.fnal.gov': 'T3_US_FNALXEN',
 'cmsdca2.fnal.gov': 'T1_US_FNAL_Disk',
 'cmsdcache.pi.infn.it': 'T2_IT_Pisa',
 'cmseos.fnal.gov': 'T3_US_FNALLPC',
 'cmslmon.fnal.gov': 'T1_US_FNAL_Disk',
 'cmsrm-se01.roma1.infn.it': 'T2_IT_Rome',
 'cmssrm-kit.gridka.de': 'T1_DE_KIT_Disk',
 'cmssrm.fnal.gov': 'T1_US_FNAL_Disk',
 'cmssrm.hep.wisc.edu': 'T2_US_Wisconsin',
 'cmssrmdisk.fnal.gov': 'T1_US_FNAL_Disk',
 'cmssw.Princeton.EDU': 'T3_US_Princeton_ICSE',
 'dc2-grid-64.brunel.ac.uk': 'T2_UK_London_Brunel',
 'dcache-se-cms.desy.de': 'T2_DE_DESY',
 'dcache07.unl.edu': 'T2_US_Nebraska',
 'dp0015.m45.ihep.su': 'T2_RU_IHEP',
 'eoscmsftp.cern.ch': 'T2_CH_CERN',
 'eymir.grid.metu.edu.tr': 'T2_TR_METU',
 'f-dpm001.grid.sinica.edu.tw': 'T2_TW_Taiwan',
 'ff-se.unl.edu': 'T3_US_Omaha',
 'ganymede.hep.kbfi.ee': 'T2_EE_Estonia',
 'gc1-se.spa.umn.edu': 'T3_US_Minnesota',
 'gfe02.grid.hep.ph.ic.ac.uk': 'T2_UK_London_IC',
 'glite-se.ceres.auckland.ac.nz': 'T3_NZ_UOA',
 'grid-srm.physik.rwth-aachen.de': 'T2_DE_RWTH',
 'grid02.physics.uoi.gr': 'T2_GR_Ioannina',
 'grid09.phy.pku.edu.cn': 'T3_CN_PKU',
 'grid143.kfki.hu': 'T2_HU_Budapest',
 'grid71.phy.ncu.edu.tw': 'T3_TW_NCU',
 'gridse2.pg.infn.it': 'T3_IT_Perugia',
 'gridsrm.ts.infn.it': 'T3_IT_Trieste',
 'grse001.inr.troitsk.ru': 'T2_RU_INR',
 'gw-3.ccc.ucl.ac.uk': 'T3_UK_London_UCL',
 'hepcms-0.umd.edu': 'T3_US_UMD',
 'hephyse.oeaw.ac.at': 'T2_AT_Vienna',
 'heplnx204.pp.rl.ac.uk': 'T2_UK_SGrid_RALPP',
 'hepse01.colorado.edu': 'T3_US_Colorado',
 'ingrid-se02.cism.ucl.ac.be': 'T2_BE_UCL',
 'kodiak-se.baylor.edu': 'T3_US_Baylor',
 'lcg58.sinp.msu.ru': 'T2_RU_SINP',
 'lcgse02.phy.bris.ac.uk': 'T2_UK_SGrid_Bristol',
 'lcgsedc01.jinr.ru': 'T2_RU_JINR',
 'llrpp01.in2p3.fr': 'T2_FR_GRIF_LLR',
 'lorienmaster.irb.hr': 'T3_HR_IRB',
 'lyogrid06.in2p3.fr': 'T3_FR_IPNL',
 'madhatter.csc.fi': 'T2_FI_HIP',
 'maite.iihe.ac.be': 'T2_BE_IIHE',
 'meson.fis.cinvestav.mx': 'T3_MX_Cinvestav',
 'moboro.uniandes.edu.co': 'T3_CO_Uniandes',
 'ndcms.crc.nd.edu': 'T3_US_NotreDame',
 'node12.datagrid.cea.fr': 'T2_FR_GRIF_IRFU',
 'ntugrid4.phys.ntu.edu.tw': 'T3_TW_NTU_HEP',
 'ntugrid6.phys.ntu.edu.tw': 'T3_TW_NTU_HEP',
 'osg-hep.phys.virginia.edu': 'T3_US_UVA',
 'osg-se.cac.cornell.edu': 'T3_US_Cornell',
 'osg-se.sprace.org.br': 'T2_BR_SPRACE',
 'pcncp22.ncp.edu.pk': 'T2_PK_NCP',
 'polgrid4.in2p3.fr': 'T2_FR_GRIF_LLR',
 'red-srm1.unl.edu': 'T2_US_Nebraska',
 'ruhex-osgce.rutgers.edu': 'T3_US_Rutgers',
 'sbgse1.in2p3.fr': 'T2_FR_IPHC',
 'se.grid.icm.edu.pl': 'T2_PL_Warsaw',
 'se.grid.kiae.ru': 'T2_RU_RRC_KI',
 'se.hep.fsu.edu': 'T3_US_FSU',
 'se.hepgrid.uerj.br': 'T2_BR_UERJ',
 'se.tier3.ucdavis.edu': 'T3_US_UCD',
 'se01.cmsaf.mit.edu': 'T2_US_MIT',
 'se01.indiacms.res.in': 'T2_IN_TIFR',
 'se03.esc.qmul.ac.uk': 'T3_UK_London_QMUL',
 'se1.accre.vanderbilt.edu': 'T3_US_Vanderbilt_EC2',
 'se1.particles.ipm.ac.ir': 'T3_IR_IPM',
 'se2.grid.lebedev.ru': 'T3_RU_FIAN',
 'se2.ppgrid1.rhul.ac.uk': 'T3_UK_London_RHUL',
 'se3.accre.vanderbilt.edu': 'T3_US_Vanderbilt_EC2',
 'se3.itep.ru': 'T2_RU_ITEP',
 'sebo-t3-01.cr.cnaf.infn.it': 'T3_IT_Bologna',
 'sigmorgh.hpcc.ttu.edu': 'T3_US_TTU',
 'srm-cms-disk.gridpp.rl.ac.uk': 'T1_UK_RAL_Disk',
 'srm-cms.cern.ch': 'T2_CH_CERN',
 'srm-cms.gridpp.rl.ac.uk': 'T1_UK_RAL_Disk',
 'srm-cms.jinr-t1.ru': 'T1_RU_JINR_Disk',
 'srm-eoscms.cern.ch': 'T2_CH_CERN',
 'srm.brazos.tamu.edu': 'T3_US_TAMU',
 'srm.ciemat.es': 'T2_ES_CIEMAT',
 'srm.glite.ecdf.ed.ac.uk': 'T3_UK_ScotGrid_ECDF',
 'srm.hep.brown.edu': 'T3_US_Brown',
 'srm.hep.fiu.edu': 'T3_US_FIU',
 'srm.ihep.ac.cn': 'T2_CN_Beijing',
 'srm.ihepa.ufl.edu': 'T2_US_Florida',
 'srm.rcac.purdue.edu': 'T2_US_Purdue',
 'srm.unl.edu': 'T2_US_Nebraska',
 'srm01.ifca.es': 'T2_ES_IFCA',
 'srm01.ncg.ingrid.pt': 'T2_PT_NCG_Lisbon',
 'srm2.grid.sinica.edu.tw': 'T1_TW_ASGC_Disk',
 'srmcms.pic.es': 'T1_ES_PIC_Disk',
 'storage01.lcg.cscs.ch': 'T2_CH_CSCS',
 'storm-fe-cms.cr.cnaf.infn.it': 'T1_IT_CNAF_Disk',
 'storm-se-01.ba.infn.it': 'T2_IT_Bari',
 'storm.ifca.es': 'T2_ES_IFCA',
 'storm.mib.infn.it': 'T3_IT_MIB',
 'stormfe1.pi.infn.it': 'T2_IT_Pisa',
 'svr018.gla.scotgrid.ac.uk': 'T3_UK_ScotGrid_GLA',
 't2-srm-02.lnl.infn.it': 'T2_IT_Legnaro',
 't2se01.physics.ox.ac.uk': 'T3_UK_SGrid_Oxford',
 't3se01.psi.ch': 'T3_CH_PSI',
 't3serv006.mit.edu': 'T3_US_MIT',
 'terbium.lsr.nectec.or.th': 'T2_TH_CUNSTDA',
 'ttgrid04.ci.northwestern.edu': 'T3_US_NU',
 'umiss005.hep.olemiss.edu': 'T3_US_UMiss',
 'uosaf0007.sscc.uos.ac.kr': 'T3_KR_UOS',
 'uscms1-se.fltech-grid3.fit.edu': 'T3_US_FIT'}

    
    return se2pnn

