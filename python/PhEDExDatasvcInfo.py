from Actor import *
import urllib
from xml.dom.minidom import parse
from crab_exceptions import *
from WorkSpace import *
from urlparse import urlparse 
from LFNBaseName import *
from crab_util import getUserName

class PhEDExDatasvcInfo:
    def __init__( self , cfg_params=None, config=None ):
 
        ## PhEDEx Data Service URL
        self.datasvc_url="https://cmsweb.cern.ch/phedex/datasvc/xml/prod"

        self.FacOps_savannah = 'https://savannah.cern.ch/support/?func=additem&group=cmscompinfrasup'
        self.stage_out_faq='https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrabFaq#Stageout_and_publication' 
        self.dataPub_faq = 'https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabForPublication'

        self.usePhedex = True 
        self.sched = common.scheduler.name().upper() 

        if config!=None:
            self.checkConfig(config)  
        else:
            self.checkCfgConfig(cfg_params)  

        self.protocol = self.srm_version
 

    def checkConfig(self,config):
        """
        """
        self.srm_version = config.get("srm_version",'srmv2')
        self.node = config.get('storage_element',None)
        self.lfn='/store/'

    def checkCfgConfig(self,cfg_params):
        """
        """
        self.datasvc_url = cfg_params.get("USER.datasvc_url",self.datasvc_url)
        self.srm_version = cfg_params.get("USER.srm_version",'srmv2')
        self.node = cfg_params.get('USER.storage_element',None)

        self.publish_data = cfg_params.get("USER.publish_data",0)
        self.usenamespace = cfg_params.get("USER.usenamespace",0)
        self.user_remote_dir = cfg_params.get("USER.user_remote_dir",'')
        if self.user_remote_dir:
            if ( self.user_remote_dir[-1] != '/' ) : self.user_remote_dir = self.user_remote_dir + '/'
          
        self.datasetpath = cfg_params.get("CMSSW.datasetpath")
        self.publish_data_name = cfg_params.get('USER.publish_data_name','')

        self.pset = cfg_params.get('CMSSW.pset',None)

        self.user_port = cfg_params.get("USER.storage_port",'8443')
        self.user_se_path = cfg_params.get("USER.storage_path",'')
        if self.user_se_path:
            if ( self.user_se_path[-1] != '/' ) : self.user_se_path = self.user_se_path + '/'
                                                    
        #check if using "private" Storage
        if not self.node :
            msg = 'Please specify the storage_element name in your crab.cfg section [USER].\n'
            msg +='\tFor further information please visit : %s'%self.stage_out_faq 
            raise CrabException(msg)
        if (self.node.find('T1_') + self.node.find('T2_')+self.node.find('T3_')) == -3: self.usePhedex = False 

        if not self.usePhedex and ( self.user_remote_dir == '' or self.user_se_path == '' ):
            ####### FEDE FOR BUG 73010 ############
            msg =  'Error: task ' + common.work_space._top_dir + ' not correctly created. Please remove it. \n'
            msg += '      You are asking to stage out without using CMS Storage Name convention. In this case you \n' 
            msg += '      must specify both user_remote_dir and storage_path in the crab.cfg section [USER].\n'
            msg += '      For further information please visit : \n\t%s'%self.stage_out_faq
            task = common._db.getTask()
            #add = '\n\n'
            #import shutil
            #try:
            #    add += '      Task not correctly created: removing the working_dir ' +  common.work_space._top_dir + ' \n'
            #    shutil.rmtree(common.work_space._top_dir)
            #except OSError:
            #    add += '      Warning: problems removing the working_dir ' + common.work_space._top_dir + ' \n'
            #    add += '      Please remove it by hand'
            #msg += add
            raise CrabException(msg)

        self.forced_path = '/store/user/'
        if self.sched in ['LSF','PBS']:
            self.srm_version = 'direct'
            self.SE = {'LSF':'', 'PBS':''}
            
        if self.sched == 'CAF': 
            #### FEDE TEST FOR XROOTD
            ######### first solution ################
            #eos = cfg_params.get("USER.caf_eos_area", 0)
            #if eos == 0:
            #    self.forced_path = '/store/caf/user/'
            #else:    
            #    self.forced_path = '/store/eos/user'
            #########################################
            ######### second solution ###############
            self.forced_path = cfg_params.get("USER.caf_lfn", '/store/caf/user')
            #########################################
            #print "--->>> FORCING THE FIRST PART OF LFN WITH ", self.forced_path
            self.SE = {'CAF':'caf.cern.ch'}
            self.srm_version = 'stageout'
            #print "--->>> query with 'stageout' "
            #########################################

        if not self.usePhedex: 
            self.forced_path = self.user_remote_dir
        return
 
    def getEndpoint(self):   
        '''
        Return full SE endpoint and related infos
        '''
        self.lfn = self.getLFN()
 
        #extract the PFN for the given node,LFN,protocol
        endpoint = self.getStageoutPFN()
        if ( endpoint[-1] != '/' ) : endpoint = endpoint + '/'
        ### FEDE bug fix 93573 
        if ( self.lfn[-1] != '/' ) : self.lfn = self.lfn + '/'

        if int(self.publish_data) == 1 or  int(self.usenamespace) == 1:
            self.lfn = self.lfn  + '${PSETHASH}/'
            endpoint = endpoint  + '${PSETHASH}/'
   
        #extract SE name an SE_PATH (needed for publication)
        SE, SE_PATH, User = self.splitEndpoint(endpoint)

        #### FEDE FOR XROOTD #####
        #print "in getEndpoint di PhEDExDatasvcInfo.py: "
        #print "    SE = ", SE
        #print "    SE_PATH = ", SE_PATH
        #print "    User = ", User
        #print "    endpoint = ", endpoint
        ##############################

        return endpoint, self.lfn , SE, SE_PATH, User         
       
    def splitEndpoint(self, endpoint):
        '''
        Return relevant infos from endpoint  
        '''
        SE = ''
        SE_PATH = ''
        USER = getUserName()
        if self.usePhedex: 
            ### FEDE PER TEST WITH XROOTD
            if (self.protocol == 'direct' or self.protocol == 'stageout'):
                SE = self.SE[self.sched]
                SE_PATH = endpoint 
                #############################   
                #print "    SE_PATH = ", SE_PATH
            else: 
                url = 'http://'+endpoint.split('://')[1]
                scheme, host, path, params, query, fragment = urlparse(url)
                SE = self.getAuthoritativeSE()
                SE_PATH = endpoint.split(host)[1]
        else:
            SE = self.node
            SE_PATH = self.user_se_path + self.user_remote_dir 
            if self.lfn.find('group') != -1:
                try:
                    USER = (self.lfn.split('group')[1]).split('/')[1]
                except:
                    pass
        return SE, SE_PATH, USER 

    def getLFN(self):
        """
        define the LFN composing the needed pieces
        """
        lfn = ''
        l_User = False
        if not self.usePhedex and (int(self.publish_data) == 0 and int(self.usenamespace) == 0) :
            ### add here check if user is trying to force a wrong LFN using a T2  TODO
            ## check if storage_name is a T2 (siteDB query)
            ## if yes :match self.user_lfn with LFNBaseName...
            ##     if NOT : raise (you are using a T2. It's not allowed stage out into self.user_path+self.user_lfn)   
            lfn = self.user_remote_dir
            return lfn
	if self.publish_data_name == '' and int(self.publish_data) == 1:
            msg = "Error. The [USER] section does not have 'publish_data_name'\n"
            msg += '\tFor further information please visit : \n\t%s'%self.dataPub_faq
            raise CrabException(msg)
        if self.publish_data_name == '' and int(self.usenamespace) == 1:
           self.publish_data_name = "DefaultDataset"
        if int(self.publish_data) == 1:
            if self.sched in ['CAF']: l_User=True 
            primaryDataset = self.computePrimaryDataset()
            ### added the case lfn = LFNBase(self.forced_path, primaryDataset, self.publish_data_name, publish=True)
            ### for the publication in order to be able to check the lfn length  
            lfn = LFNBase(self.forced_path, primaryDataset, self.publish_data_name, publish=True)
        elif int(self.usenamespace) == 1:
            if self.sched in ['CAF']: l_User=True 
            primaryDataset = self.computePrimaryDataset()
            lfn = LFNBase(self.forced_path, primaryDataset, self.publish_data_name)
        else:
            if self.sched in ['CAF','LSF']: l_User=True 
            lfn = LFNBase(self.forced_path,self.user_remote_dir)

        if ( lfn[-1] != '/' ) : lfn = lfn + '/'

        return lfn
 
    def computePrimaryDataset(self):
        """
        compute the last part for the LFN in case of publication     
        """
        if (self.datasetpath.upper() != 'NONE'):
            primarydataset = self.datasetpath.split("/")[1]
        else:
            primarydataset = self.publish_data_name
        return primarydataset
    
    def domPhedex(self,params,datasvc_baseUrl):
        """
        PhEDEx Data Service lfn2pfn call
 
        input:   params,datasvc_baseUrl
        returns: DOM object with the content of the PhEDEx Data Service call
        """  
        params = urllib.urlencode(params)
        try:
            urlresults = urllib.urlopen(datasvc_baseUrl, params)
            urlresults = parse(urlresults)
        except IOError:
            msg="Unable to access PhEDEx Data Service at %s"%datasvc_baseUrl
            raise CrabException(msg)
        except:
            urlresults = None

        return urlresults
 
    def parse_error(self,urlresults):
        """
        look for errors in the DOM object returned by PhEDEx Data Service call
        """
        errormsg = None 
        errors=urlresults.getElementsByTagName('error')
        for error in errors:
            errormsg=error.childNodes[0].data
            if len(error.childNodes)>1:
               errormsg+=error.childNodes[1].data
        return errormsg
 
    def parse_lfn2pfn(self,urlresults):
        """
        Parse the content of the result of lfn2pfn PhEDEx Data Service  call
 
        input:    DOM object with the content of the lfn2pfn call
        returns:  PFN  
        """
        result = urlresults.getElementsByTagName('phedex')
               
        if not result:
              return []
        result = result[0]
        pfn = None
        mapping = result.getElementsByTagName('mapping')
        for m in mapping:
            pfn=m.getAttribute("pfn")
            if pfn:
              return pfn
 
    def getStageoutPFN( self ):
        """
        input:   LFN,node name,protocol
        returns: PFN 
        """
        if self.usePhedex:
            params = {'node' : self.node , 'lfn': self.lfn , 'protocol': self.protocol}
            datasvc_lfn2pfn="%s/lfn2pfn"%self.datasvc_url
            fullurl="%s/lfn2pfn?node=%s&lfn=%s&protocol=%s"%(self.datasvc_url,self.node,self.lfn,self.protocol) 
            #print "--->>> fullurl = ", fullurl
            domlfn2pfn = self.domPhedex(params,datasvc_lfn2pfn)
            if not domlfn2pfn :
                msg="Unable to get info from %s"%fullurl
                raise CrabException(msg)
  
            errormsg = self.parse_error(domlfn2pfn)
            if errormsg: 
                msg="Error extracting info from %s due to: %s"%(fullurl,errormsg)
                raise CrabException(msg)
  
            stageoutpfn = self.parse_lfn2pfn(domlfn2pfn)
            if not stageoutpfn:
                msg ='Unable to get stageout path from TFC at Site %s \n'%self.node
                msg+='      Please alert the CompInfraSup group through their savannah %s \n'%self.FacOps_savannah
                msg+='      reporting: \n'
                msg+='       Summary: Unable to get user stageout from TFC at Site %s \n'%self.node
                msg+='       OriginalSubmission: stageout path is not retrieved from %s \n'%fullurl
                raise CrabException(msg)
        else:
            if self.sched in ['CAF','LSF','PBS'] :
                if (self.user_se_path[-1]=='/') and (self.lfn[0]=='/'):
                    stageoutpfn = self.user_se_path+(self.lfn).lstrip('/') 
                else:             
                    stageoutpfn = self.user_se_path+self.lfn 
            else: 
                stageoutpfn = 'srm://'+self.node+':'+self.user_port+self.user_se_path+self.lfn 

        if ( stageoutpfn[-1] != '/' ) : stageoutpfn = stageoutpfn + '/'
        return stageoutpfn 

    def getAuthoritativeSE(self):
        """
        input:   node name
        returns: AuthoritativeSE 
        """
        params = {'node' : self.node }
        datasvc_nodes="%s/nodes"%self.datasvc_url
        fullurl="%s/nodes/?node=%s"%(self.datasvc_url,self.node) 
        domnodes = self.domPhedex(params,datasvc_nodes)

        if not domnodes :
            msg="Unable to get info from %s"%fullurl
            raise CrabException(msg)

        errormsg = self.parse_error(domnodes)
        if errormsg: 
            msg="Error extracting info from %s due to: %s"%(fullurl,errormsg)
            raise CrabException(msg)
        result = domnodes.getElementsByTagName('phedex')
        if not result:
              return []
        result = result[0]
        se = None
        node = result.getElementsByTagName('node')
        for m in node:
            se=m.getAttribute("se")
            if se:
                return se


if __name__ == '__main__':
  """
  Sort of unit testing to check Phedex API for whatever site and/or lfn.
  Usage:
     python PhEDExDatasvcInfo.py --node T2_IT_Bari --lfn /store/maremma

  """
  import getopt,sys
  from crab_util import *
  import common
  klass_name = 'SchedulerGlite'
  klass = importName(klass_name, klass_name)
  common.scheduler = klass()

  lfn="/store/user/"
  node='T2_IT_Bari'
  valid = ['node=','lfn=']
  try:
       opts, args = getopt.getopt(sys.argv[1:], "", valid)
  except getopt.GetoptError, ex:
       print str(ex)
       sys.exit(1)
  for o, a in opts:
        if o == "--node":
            node = a
        if o == "--lfn":
            lfn = a
  
  mycfg_params = { 'USER.storage_element': node }
  dsvc = PhEDExDatasvcInfo(mycfg_params)
  dsvc.lfn = lfn
  print dsvc.getStageoutPFN()

