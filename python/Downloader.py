from WMCore.Services.Service import Service
import common

class Downloader:

    def __init__(self, endpoint, cachepath='', cacheduration = 0.5, timeout = 20, \
                 type = "txt/csv", logger = None ):
        ## if not already defined set default CachePath to $HOME/.cms_crab   
        if cachepath =='':
            import os
            if os.getenv('CMS_CRAB_CACHE_DIR'):
                cachepath ='%s/.cms_crab'%os.getenv('CMS_CRAB_CACHE_DIR') 
            elif os.getenv('HOME'):
                cachepath='%s/.cms_crab'%os.getenv('HOME')
            else:
                import pwd
                cachepath = '/tmp/crab_cache_' + pwd.getpwuid(os.getuid())[0]
 
        if not os.path.isdir(cachepath):
            try:
                os.mkdir(cachepath)
            except:
                common.logger.info('Warning cannot create %s. Using current directory'%cachepath)
                cachepath=os.getcwd() 
       
        if not logger: logger = common.logger()

        self.wmcorecache = {}
        self.wmcorecache['logger'] = logger
        self.wmcorecache['cachepath'] = cachepath   ## cache area
        self.wmcorecache['cacheduration'] = 0.5     ## half an hour
        self.wmcorecache['timeout'] = 20            ## seconds
        self.wmcorecache['endpoint'] = endpoint

    def downloadConfig(self, cacheFile, type = "txt/csv",openf=True, useVerb='GET'):
        self.wmcorecache['type'] = type
        common.logger.debug("Downloading file [%s] to [%s]." %(str(self.wmcorecache['endpoint']),(str(self.wmcorecache['cachepath'])+"/"+cacheFile)))
        servo = Service( self.wmcorecache )
        servo['usestalecache'] = True
        return servo.refreshCache( cacheFile, cacheFile, openfile=openf, verb=useVerb )

    def aconfig(self, fileName = "prova"):
        f = self.downloadConfig(fileName)
        l = ''.join( f.readlines() )
        f.close()

        value = None
        try:
            result = eval(l)
        except SyntaxError, se:
            common.logger.debug("Problem reading downloaded file %s "%str(fileName))

        return result

    def config(self, fileName = "prova"):
        f = self.downloadConfig(fileName)
        try:
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % self.wmcorecache['endpoint'] )
        return result

    def filePath(self, fileName = "prova"):
        return  self.downloadConfig(fileName, openf=False)
