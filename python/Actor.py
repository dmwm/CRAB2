#
#  This is an abstract class for all CRAB actions.
#

from crab_exceptions import *

class Actor:
    def run(self):
        raise CrabException, "Actor::run() must be implemented"
    
    def stateChange(self, subrange, action):
        """
        _stateChange_
        """
        import common
        common.logger.debug( "Updating [%s] state %s"%(str(subrange),action))
        updlist = [{'state': action}] * len(subrange)
        common._db.updateRunJob_(subrange, updlist)

