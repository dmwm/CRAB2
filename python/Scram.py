import common
import os, os.path
from crab_exceptions import *
from crab_util import *

class Scram:
    def __init__(self, cfg_params):
        self.build = 'tgz'
        self.tgz_name = 'default.tgz'
        self.tgzNameWithPath = None

        self.scramArch = ''
        self.scramVersion = ''
        scramArea = ''

        if os.environ.has_key("SCRAM_ARCH"):
            self.scramArch = os.environ["SCRAM_ARCH"]
        if os.environ.has_key("SCRAMRT_LOCALRT"):
            # try scram v1
            self.scramArea = os.environ["SCRAMRT_LOCALRT"]
            self.scramVersion = 1
        elif os.environ.has_key("LOCALRT"):
            # try scram v0
            self.scramArea = os.environ["LOCALRT"]
            reVer=re.compile( r'V(\d*)_' )
            if (os.path.exists(self.scramArea+'/config/scram_version')):
                verFile=open(self.scramArea+'/config/scram_version','r')
                lines = verFile.readlines()
                for line in lines:
                    if reVer.search(line):
                        self.scramVersion=int(reVer.search(line).groups()[0])
                        break
                    pass
                verFile.close()
            pass
        elif os.environ.has_key("BASE_PATH"):
            # try scram v0
            self.scramArea = os.environ["BASE_PATH"]
            reVer=re.compile( r'V(\d*)_' )
            if (os.path.exists(self.scramArea+'/config/scram_version')):
                verFile=open(self.scramArea+'/config/scram_version','r')
                lines = verFile.readlines()
                for line in lines:
                    if reVer.search(line):
                        self.scramVersion=int(reVer.search(line).groups()[0])
                        break
                    pass
                verFile.close()
            pass
        else:
            msg = 'Did you do cmsenv from your working area ?\n'
            raise CrabException(msg)
        common.logger.debug("Scram::Scram() version is "+str(self.scramVersion))
        common.logger.debug("Scram::Scram() arch is "+str(self.scramArch))
        common.logger.log(10-1, "Scram::Scram() area is "+self.scramArea)
        pass

    def commandName(self):
        """ return scram command name """
        return 'scram'

    def getSWArea_(self):
        """
        Get from SCRAM the local working area location
        """
        return string.strip(self.scramArea)

    def getSWVersion(self):
        """
        Get the version of the sw
        """

        ver = ''
        envFileName=self.scramArea+"/.SCRAM/Environment"
        if os.environ.has_key("CMSSW_VERSION"):
            ver= os.environ["CMSSW_VERSION"]
        ##SL Else take  sw version from scramArea/.SCRAM/Environment
        if ver == '':
            common.logger.debug("$CMSSW_VERSION variable not defined...trying with scramArea/.SCRAM/Environment file.")
            if os.path.exists(envFileName):
                reVer=re.compile( r'SCRAM_PROJECTVERSION=(\S*)' )
                envFile = open(envFileName,'r')
                lines = envFile.readlines()
                for line in lines:
                    if reVer.search(line):
                        ver=reVer.search(line).groups()[0]
                        break
                    pass
                envFile.close()
            else: common.logger.debug('file %s not found'%envFileName)
        if ver == '':
            msg  = 'Cannot find sw version:\n'
            msg += 'Did you do cmsenv from your working area or is your area corrupt?\n'
            raise CrabException(msg)
        return string.strip(ver)

    def getReleaseTop_(self):
        """ get release top """

        result = ''
        archenvFile = "%s/.SCRAM/%s/Environment"%(self.scramArea,self.scramArch)
        if (os.path.exists(archenvFile)):
            envFileName = archenvFile
        else:
            envFileName = self.scramArea+"/.SCRAM/Environment"

        try:
            envFile = open(envFileName, 'r')
            for line in envFile:
                line = string.strip(line)
                (k, v) = string.split(line, '=')
                if k == 'RELEASETOP':
                    result=v
                    break
                pass
            pass
        except IOError:
            msg = 'Cannot open scram environment file '+envFileName
            raise CrabException(msg)

        return string.strip(result)

    def findFile_(self, filename):
        """
        Find the file in $PATH
        """
        search_path=os.environ["PATH"]
        file_found = 0
        paths = string.split(search_path, os.pathsep)
        for path in paths:
            if os.path.exists(os.path.join(path, filename)):
                file_found = 1
                break
        if file_found:
            return os.path.abspath(os.path.join(path, filename))
        else:
            return None

    def getArch(self):
        """
        Return the architecture of the current scram project
        """
        if os.environ.has_key("SCRAM_ARCH"):
            return os.environ["SCRAM_ARCH"]
        else:
            cmd=self.commandName() + ' arch'
            out = runCommand(cmd)
            return out
