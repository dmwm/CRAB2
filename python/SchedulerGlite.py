"""
CRAB interface to BossLite gLite Scheduler
"""

__revision__ = "$Id: SchedulerGlite.py,v 1.86 2013/01/17 15:15:03 belforte Exp $"
__version__ = "$Revision: 1.86 $"

from SchedulerGrid import SchedulerGrid
from crab_exceptions import *
from crab_util import *
import EdgLoggingInfo
import common
from WMCore.SiteScreening.BlackWhiteListParser import CEBlackWhiteListParser

import os, sys, time

class SchedulerGlite(SchedulerGrid):
    def __init__(self, name="GLITE"):
        SchedulerGrid.__init__(self,name)

        self.EDG_retry_count        = 0
        self.EDG_shallow_retry_count= -1
        self.OSBsize = 55*1000*1000  # 55MB

    def configure(self,cfg_params):
        SchedulerGrid.configure(self, cfg_params)
        self.environment_unique_identifier = '$GLITE_WMS_JOBID'

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """
        self.rb_param_file=''
        if (not cfg_params.has_key('GRID.rb')):
            cfg_params['GRID.rb']='CERN'
        self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("GRID.rb"))
        self.wms_service=cfg_params.get("GRID.wms_service",'')
        self.skipWMSAuth=cfg_params.get("GRID.skipwmsauth",1)
        params = { 'service' : self.wms_service, \
                   'config' : self.rb_param_file, \
                   'skipWMSAuth' : self.skipWMSAuth
                 }
        return  params


    def rb_configure(self, RB):
        url ='http://cmsdoc.cern.ch/cms/LCG/crab/config/'
        from Downloader import Downloader
        import httplib
        common.logger.debug('Downloading config files for WMS: '+url)
        ## 25-Jun-2009 SL: patch to use Cream enabled WMS
        if ( self.cfg_params.get('GRID.use_cream',None) ):
            RB='CREAM'
        if not RB: return None
        rb_param_file = None
        configFileName = 'glite_wms_'+str(RB)+'.conf'

        results = Downloader(url)
        try:
            gliteConfig  = results.filePath(configFileName)
        except httplib.HTTPException, ex: 
            raise CrabException( "Problem getting RB config file: %s, reason:"%(configFileName, ex) )

        if (gliteConfig ):
            rb_param_file = gliteConfig
        return rb_param_file

    def ce_list(self):
        """
        Returns string with requirement CE related
        """
        ceParser = CEBlackWhiteListParser(self.EDG_ce_white_list,
                                          self.EDG_ce_black_list, common.logger())
        req = ''
        ce_white_list = []
        ce_black_list = []
        if self.EDG_ce_white_list:
            ce_white_list = ceParser.whiteList()
            tmpCe=[]
            concString = '&&'
            for ce in ce_white_list:
                tmpCe.append('RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId)')
            if len(tmpCe) == 1:
                req +=  " && (" + concString.join(tmpCe) + ") "
            elif len(tmpCe) > 1:
                firstCE = 0
                for reqTemp in tmpCe:
                    if firstCE == 0:
                        req += " && ( (" + reqTemp + ") "
                        firstCE = 1
                    elif firstCE > 0:
                        req += " || (" + reqTemp + ") "
                if firstCE > 0:
                    req += ") "

        if self.EDG_ce_black_list:
            ce_black_list = ceParser.blackList()
            tmpCe=[]
            concString = '&&'
            for ce in ce_black_list:
                tmpCe.append('(!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))')
            if len(tmpCe): req += " && (" + concString.join(tmpCe) + ") "

        # requirement added to skip gliteCE
        # not more needed
 #       req += '&& (!RegExp("blah", other.GlueCEUniqueId))'
        retWL = ','.join(ce_white_list)
        retBL = ','.join(ce_black_list)
        if not retWL:
            retWL = None
        if not retBL:
            retBL = None

        return req, retWL, retBL

    def se_list(self, dest):
        """
        Returns string with requirement SE related
        """
        hostList=self.findSites_(dest)
        req=''
        reqtmp=[]
        concString = '||'

        for arg in hostList:
            reqtmp.append(' Member("'+arg+'" , other.GlueCESEBindGroupSEUniqueID) ')

        if len(reqtmp): req += " && (" + concString.join(reqtmp) + ") "

        return req

    def jdlParam(self):
        """
        Returns
        """
        req=''
        if self.EDG_addJdlParam:
            if self.EDG_addJdlParam[-1] == '': self.EDG_addJdlParam= self.EDG_addJdlParam[:-1]
            for p in self.EDG_addJdlParam:
                req+=string.strip(p)+';\n'
        return req

    def specific_req(self):
        """
        Returns string with specific requirements
        """
        req=''
        if self.EDG_clock_time:
            if (not req == ' '): req = req + ' && '
            req = req + 'other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time

        if self.EDG_cpu_time:
            if (not req == ' '): req = req + ' && '
            req = req + ' other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time

        return req

    def sched_parameter(self,i,task):
        """
        Returns string with requirements and scheduler-specific parameters
        """
        dest=  task.jobs[i-1]['dlsDestination']

        req=''
        #req +=task['jobType']
        ####### FEDE FOR BUG 73010 ############
        try: 
            #print "task['jobType'] = ", task['jobType']
            req +=task['jobType']
        except TypeError:
            msg = "Error: wrong or missing task info. Your created task can not be submitted. Please check your configuration file and create the task again.  \n "
            raise CrabException(msg) 
        ####################################### 

        sched_param=''
        sched_param+='Requirements = ' + req +self.specific_req() + self.se_list(dest) +\
                                        self.ce_list()[0] +';\n'
        if self.EDG_addJdlParam: sched_param+=self.jdlParam()
        sched_param+='MyProxyServer = "' + self.proxyServer + '";\n'
        sched_param+='VirtualOrganisation = "' + self.VO + '";\n'
        sched_param+='RetryCount = '+str(self.EDG_retry_count)+';\n'
        sched_param+='DefaultNodeRetryCount = '+str(self.EDG_retry_count)+';\n'
        sched_param+='ShallowRetryCount = '+str(self.EDG_shallow_retry_count)+';\n'
        sched_param+='DefaultNodeShallowRetryCount = '+str(self.EDG_shallow_retry_count)+';\n'

        return sched_param

    def decodeLogInfo(self, file):
        """
        Parse logging info file and return main info
        """
        loggingInfo = EdgLoggingInfo.EdgLoggingInfo()
        reason = loggingInfo.decodeReason(file)
        return reason

    def findSites_(self, sites):
        itr4 =[]
        if len(sites)>0 and sites[0]=="":
            return itr4
        if sites != [""]:
            replicas = self.blackWhiteListParser.checkBlackList(sites)
            if len(replicas)!=0:
                replicas = self.blackWhiteListParser.checkWhiteList(replicas)

            itr4 = replicas
        return itr4

    def delegateProxy(self):
        self.boss().delegateProxy()   
        return

    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()
        txt += '    exit $job_exit_code\n'

        txt += '}\n'
        return txt

    def listMatch(self, dest, full):
        matching='fast'
        
        if self.boss().schedulerConfig['name'] == 'SchedulerGLite' :
            taskId=common._db.getTask()
            req=str(self.sched_parameter(1,taskId))
            sites = self.boss().schedSession().matchResources(taskId, requirements=req)
        else :
            sites = SchedulerGrid.listMatch(self, dest, full)
            
        if full == True: matching='full'
        common.logger.debug("list of available site ( "+str(matching) +" matching ) : "+str(sites))
        
        return sites
