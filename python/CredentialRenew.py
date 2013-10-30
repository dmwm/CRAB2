from Actor import *
from crab_util import *
import common
import traceback
from ProdCommon.Credential.CredentialAPI import CredentialAPI
from SubmitterServer import SubmitterServer


class CredentialRenew(Actor):

    def __init__(self, cfg_params):
        self.cfg_params=cfg_params
        self.credentialType = 'Proxy'
        if common.scheduler.name().upper() in ['LSF', 'CAF']:
            self.credentialType = 'Token'
         
        # init client server params...
        CliServerParams(self)       

    def run(self):
        """
        """

        common.logger.debug("CredentialRenew::run() called")

        # FIXME With MyProxy delegation this part is completely overlapped with the method manageDelegation
        # in SubmitServer. We should to maintain just one version of the method in a common part  

        try:
            myproxyserver = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/").config("myproxy_server.conf")
            myproxyserver = myproxyserver.strip()
            if myproxyserver is None:
                raise CrabException("myproxy_server.conf retrieved but empty")
        except Exception, e:
            common.logger.info("Problem setting myproxy server endpoint: using myproxy.cern.ch")
            common.logger.debug(e)
            myproxyserver = 'myproxy.cern.ch'

        configAPI = {'credential' : self.credentialType, \
                     'myProxySvr' : myproxyserver,\
                     'serverDN'   : self.server_dn,\
                     'shareDir'   : common.work_space.shareDir() ,\
                     'userName'   : getUserName(),\
                     'serverName' : self.server_name, \
                     'logger' : common.logger() \
                     }
        try:
            CredAPI =  CredentialAPI( configAPI )
        except Exception, err :
            common.logger.debug( "Configuring Credential API: " +str(traceback.format_exc()))
            raise CrabException("ERROR: Unable to configure Credential Client API  %s\n"%str(err))

        if self.credentialType == 'Proxy':
             # Proxy delegation through MyProxy, 4 days lifetime minimum
             if not CredAPI.checkMyProxy(Time=4, checkRetrieverRenewer=True) :
                common.logger.info("Please renew MyProxy delegated proxy:\n")
                try:
                    CredAPI.credObj.serverDN = self.server_dn
                    CredAPI.ManualRenewMyProxy()
                except Exception, ex:
                    common.logger.debug("Delegating Credentials to MyProxy : " +str(traceback.format_exc()))
                    raise CrabException(str(ex))
        else:
            self.renewer()
            if not CredAPI.checkCredential(Time=100) :
                common.logger.info("Please renew your %s :\n"%self.credentialType)
                try:
                    CredAPI.ManualRenewCredential()
                except Exception, ex:
                    raise CrabException(str(ex))
            try:
                dict = CredAPI.registerCredential() 
            except Exception, err:
                common.logger.debug( "Registering Credentials : " +str(traceback.format_exc()))
                raise CrabException("ERROR: Unable to register %s delegating server: %s\n"%\
                    (self.credentialType,self.server_name ))

        common.logger.info("Credential successfully delegated to the server.\n")
        return
