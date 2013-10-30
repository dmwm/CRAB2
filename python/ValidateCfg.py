import imp, sys
import traceback
import common
from crab_exceptions import *


class ValidateCfg:
    def __init__(self, args):

        self.pset = args.get('pset','0')
        if self.pset == '0' or self.pset.upper() == 'NONE' :
            msg = 'Error: No configuration file has been specified in your crab.cfg file.'
            raise CrabException(msg)

    def run(self):

        if self.pset.endswith('py'):
            self.DumpPset()
        else:
            self.IncludePset()

        return

    def ImportFile( self ):
        """
        Read in Pset object
        """
        common.logger.info( "Importing file %s"%self.pset)
        handle = open(self.pset, 'r')

        try:
            cfo = imp.load_source("pycfg", self.pset, handle)
            cmsProcess = cfo.process
        except Exception, ex:
            goodcfg = False
            msg = "%s file is not valid python: %s" % \
                (self.pset,str(traceback.format_exc()))
            msg += "\n\nPlease post on the EDM hypernews if this " + \
                   "information doesn't help solve this problem."
            raise CrabException( msg )
        handle.close()
        return cmsProcess

    def DumpPset( self ):
        """
        """
        cmsProcess = self.ImportFile()

        common.logger.info( 'Starting the dump.....' )
        try:
            cmsProcess.dumpPython()
        except Exception, ex:
            msg = "Python parsing failed: \n\n%s" % \
                str(traceback.format_exc())
            msg += "\n\nPlease post on the EDM hypernews if this " + \
                   "information doesn't help solve this problem."
            raise CrabException( msg )
        msg = "Python parsing succeeded. File is valid.\n"
        common.logger.info( msg )

    def IncludePset(self):
        """
        """
        from FWCore.ParameterSet.Config import include
        common.logger.info( 'Starting include.....' )
        try:
            cfo = include(self.pset)
        except Exception, ex:
            msg = "Python parsing failed: \n\n%s" % \
                str(traceback.format_exc())
            msg += "\n\nPlease post on the EDM hypernews if this " + \
                   "information doesn't help solve this problem."
            raise CrabException(msg)
        msg = "Python parsing succeeded. File is valid.\n"
        common.logger.info( msg )


if __name__ == '__main__' :

    pset = sys.argv[1]
    check = ValidateCfg({'pset':pset})
    check.run()
