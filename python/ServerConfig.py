from crab_exceptions import *
from crab_util import *
import common
from Downloader import Downloader
import os, time

class ServerConfig:
    def __init__(self, serverName):
        import string
        self.serverName = string.lower(serverName)
        common.logger.debug('Calling ServerConfig '+self.serverName)

        url ='http://cmsdoc.cern.ch/cms/LCG/crab/config/'

        self.downloader = Downloader(url)
          

    def config(self):
        """
        """
        if 'default' in  self.serverName:
            self.serverName = self.selectServer() 
        if 'server_' in self.serverName:
            configFileName = '%s.conf'%self.serverName
        else: 
            configFileName = 'server_%s.conf'%self.serverName
 
        serverConfig = eval(self.downloader.config(configFileName))
 
        if not serverConfig:
            serverConfig = {} 
        serverConfig['serverGenericName']=self.serverName

        return serverConfig
 
    def selectServer(self):
        """
        """
        common.logger.debug('getting serverlist from web')
        # get a list of available servers 
        serverListFileName ='AvailableServerList'
 
        serverListFile = self.downloader.config(serverListFileName)
 
        if not serverListFile:
            msg = 'List of avalable Server '+serverListFileName+' from '+self.url+' is empty\n'
            msg += 'Please report to CRAB feedback hypernews hn-cms-crabFeedback@cern.ch'
            raise CrabException(msg)
        # clean up empty lines and comments
        serverList=[]
        [serverList.append(string.split(string.strip(it))) for it in serverListFile.split('\n') if (it.strip() and not it.strip()[0]=="#")]
        common.logger.debug('All avaialble servers: '+str(serverList))
 
        # select servers from client version
        compatibleServerList=[]
        for s in serverList:
            #vv=string.split(s[1],'-')
            if len(s)<2:
                continue
            vv=s[1].split('-')
            if len(vv[0])==0: vv[0]='0.0.0'
            if len(vv[1])==0: vv[1]='99.99.99'
            for i in 0,1:
                tmp=[]
                [tmp.append(int(t)) for t in vv[i].split('.')]
                vv[i]=tuple(tmp)
            
            if vv[0]<=common.prog_version and common.prog_version<=vv[1] and common.scheduler.name()==string.lower(s[2]):
                compatibleServerList.append(s[0])
 
        common.logger.debug('All avaialble servers compatible with %s: '%common.prog_version_str +str(compatibleServerList))
        if len(compatibleServerList)==0: 
            msg = "No compatible server for client version %s and scheduler %s\n"%(common.prog_version_str,common.scheduler.name())
            msg += "Exiting"
            common.logger.info(msg)
            raise CrabException(msg)
 
        # if more than one, pick up a random one, waiting for something smarter (SiteDB)
        import random
        serverName = random.choice(compatibleServerList)
        common.logger.debug('Avaialble servers: '+str(compatibleServerList)+' choosen: '+serverName)
 
        return serverName
