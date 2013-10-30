import logging,time,sys
import common

class CrabLogger:
    def __init__(self, args):

        # print 'Creating LOGGER',logging._handlers
        logging.DEBUG_VERBOSE = logging.DEBUG - 1
        logging.addLevelName(logging.DEBUG_VERBOSE,'debug_verbose')
        logging.root.setLevel(logging.CRITICAL)

        self.logger = logging.getLogger("crab:")
        self.logger.setLevel(logging.DEBUG_VERBOSE)

        # FileHandler
        log_fname =common.work_space.logDir()+common.prog_name+'.log'
        self.fh=logging.FileHandler(log_fname)
        fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] \t%(message)s")
        fh_level=logging.DEBUG
        self.fh.setLevel(fh_level)
        self.fh.setFormatter(fh_formatter)
        self.logger.addHandler(self.fh)

        # StreamerHandler: to be dded _only_ if not already present: otherwise duplicated
        streamenPresent=False
        for x in self.logger.handlers:
            if x.__class__==logging.StreamHandler: streamenPresent=True
        if not streamenPresent:
            self.ch=logging.StreamHandler(sys.stdout)
            ch_formatter = logging.Formatter("%(name)s  %(message)s")
            ch_level=logging.INFO
            if common.debugLevel > 0:ch_level=logging.DEBUG
            if common.debugLevel > 2:
                fh_level=logging.DEBUG_VERBOSE
                ch_level=logging.DEBUG_VERBOSE

            self.ch.setLevel(ch_level)
            self.ch.setFormatter(ch_formatter)

            # add StreamerHandler only if is not yet there
            self.logger.addHandler(self.ch)

        self.debug('%s\n'%args)
        # print 'LOGGER',logging._handlers
        return

    def __call__(self):
        return self.logger

    def delete(self):
        # The trick her is to  flush, close, remove from handler list and finally delete _only_ the FileHandler,
        # NOT the StreamerHandler as well, since it is apparently used asynchrounously and will give error in emit(...)
        if self.fh in logging._handlers :
            self.fh.flush()
            self.fh.close()
            self.logger.removeHandler(self.fh)
            del self.fh
        common.logger=None

    def info(self, msg):
        #print msg
        self.logger.info(msg)

    def debug(self, msg):
        #print msg
        self.logger.debug(msg)

    def log(self,i, msg):
        #print msg
        self.logger.log(i,msg)

