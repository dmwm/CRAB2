from Actor import *
import common
import string, os, time, sys, glob
from crab_util import *
import traceback
#from ServerCommunicator import ServerCommunicator

class CacheCleaner(Actor):
    def __init__(self):
       """
       A class to clean:
       - SiteDB cache
       - Crab cache
       """
       return

    def run(self):
        common.logger.debug("CacheCleaner::run() called")
        try:
            sitedbCache= self.findSiteDBcache()
          #  sitedbCache= '%s/.cms_sitedbcache'%os.getenv('HOME')
            if os.path.isdir(sitedbCache):
               cmd = 'rm -f  %s/*'%sitedbCache
               cmd_out = runCommand(cmd)
               common.logger.info('%s Cleaned.'%sitedbCache)
            else:
               common.logger.info('%s not found'%sitedbCache)
        except Exception, e:
            common.logger.debug("WARNING: Problem cleaning the SiteDB cache.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() )

           # Crab  cache 
        try: 
            crabCache= self.findCRABcache()
         #   crabCache= '%s/.cms_crab'%os.getenv('HOME')
            if os.path.isdir(crabCache):
               cmd = 'rm -f  %s/*'%crabCache
               cmd_out = runCommand(cmd)
               common.logger.info('%s Cleaned.'%crabCache)
            else:
               common.logger.debug('%s not found'%crabCache)
        except Exception, e:
            common.logger.debug("WARNING: Problem cleaning the cache.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() )
        return

    def findSiteDBcache(self):

        sitedbCache = None
 
        if os.getenv('CMS_SITEDB_CACHE_DIR'):
            sitedbCache = os.getenv('CMS_SITEDB_CACHE_DIR') + '/.cms_sitedbcache'
        elif os.getenv('HOME'):
            sitedbCache = os.getenv('HOME') + '/.cms_sitedbcache'
        else:
            sitedbCache = '/tmp/sitedbjson_' + pwd.getpwuid(os.getuid())[0]

        return sitedbCache


    def findCRABcache(self):

        crabCache = None

        if os.getenv('CMS_CRAB_CACHE_DIR'):
            crabCache ='%s/.cms_crab'%os.getenv('CMS_CRAB_CACHE_DIR') 
        elif os.getenv('HOME'):
            crabCache ='%s/.cms_crab'%os.getenv('HOME')
        else:
            crabCache  = '/tmp/crab_cache_' + pwd.getpwuid(os.getuid())[0]

        return crabCache
