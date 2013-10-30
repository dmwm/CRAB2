from crab_exceptions import *
import logging
import common
import os, string, time
from crab_util import loadConfig

class WorkSpace:
    def __init__(self, top_dir, cfg_params):

        self._cwd_dir = os.getcwd()+'/'
        self._top_dir = top_dir    # top working directory

        self._user_out_dir = ''
        self._user_log_dir = ''
        if 'USER.outputdir' in cfg_params.keys():    
            self._user_out_dir = os.path.abspath(cfg_params['USER.outputdir'])
            if cfg_params['CRAB.scheduler'].upper() == 'CAF' :
                msg = "Cannot use 'outputdir' parameter with CAF scheduler\n"
                msg += 'please remove it from your crab.cfg\n'
                raise CrabException(msg)
        if 'USER.logdir' in cfg_params.keys():    
            self._user_log_dir = os.path.abspath(cfg_params['USER.logdir'])
        #Matteo: Necessary to manage user ui_working_dir
        if 'USER.ui_working_dir' in cfg_params.keys():    
            self._top_dir = os.path.abspath(cfg_params['USER.ui_working_dir'])

       # self._res_dir = cfg_params.get("USER.outputdir", self._top_dir + '/res')     # dir to store job results
       # self._log_dir = ("USER.logdir",self._top_dir + '/log')    # log-directory
        self._log_dir = self._top_dir + '/log'    # log-directory
        self._job_dir = self._top_dir + '/job'     # job pars, scripts, jdl's
        self._res_dir = self._top_dir + '/res'     # dir to store job results
        self._share_dir = self._top_dir + '/share' # directory for common stuff

        #self._pathForTgz = string.split(top_dir, '/')[-1]
        self._pathForTgz = self._share_dir

        self.uuid = self.uuidGen()
 
    def uuidGen(self):
        import random

        letters = "abcdefghijklmnopqrstuvwxyz"
        numbers = "0123456789"
        genid_bits = random.sample(letters, 3) + random.sample(numbers, 3)
        random.shuffle(genid_bits)
        combination = ''.join(genid_bits)

        return combination   

    def create(self):
        # Matteo change in order to ban only "data" in "CMSSW" dir and 
        # not crash when short path is given    
        subpath = self._top_dir.split('CMSSW')
        if len(subpath)!=1 and ("data" in subpath[-1].split("/")):
            msg = 'Cannot run CRAB from "data" directory.\n'
            msg += 'please change direcotry\n'
            raise CrabException(msg)

        if not os.path.exists(self._top_dir):
            try:
                os.makedirs(self._top_dir)
            except OSError:
                msg = 'Cannot create '+str(self._top_dir) +' directory.\n'
                raise CrabException(msg)
            pass
        if not os.listdir(self._top_dir):
            os.mkdir(self._job_dir)
            os.mkdir(self._share_dir)
            os.mkdir(self._res_dir)
            os.mkdir(self._log_dir)
            pass

        # Some more check for _res_dir, since it can be user defined
        if self._user_out_dir != '':
            if not os.path.exists(self._user_out_dir):
                try: 
                    os.makedirs(self._user_out_dir)
                except:
                    msg = 'Cannot mkdir ' + self._user_out_dir + ' Check permission'
                    raise CrabException(msg)
            if os.listdir(self._user_out_dir):
                msg = self._user_out_dir + ' already exists and is not empty. Please remove it before create new task'
                raise CrabException(msg)
        # ditto for _log_dir
        if self._user_log_dir != '':
            if not os.path.exists(self._user_log_dir):
                try:
                    os.makedirs(self._user_log_dir)
                except:
                    msg = 'Cannot mkdir ' + self._user_log_dir + ' Check permission'
                    raise CrabException(msg)
                pass 
            if os.listdir(self._user_log_dir):
                msg = self._user_log_dir + ' already exists and is not empty. Please remove it before create new task'
                raise CrabException(msg)
        return

    def delete(self):
        """
        delete the whole workspace without doing any test!!!
        """
        common.logger.delete()
        common.logger = None

        if os.path.exists(self._top_dir):
            import shutil
            try:
                shutil.rmtree(self._top_dir)
            except :
                msg = 'Problem removing whole workspace. Please remove the remaining files manually.'
                raise CrabException(msg)
        return

    def cwdDir(self):
        if (self._cwd_dir[-1] != '/'):
           self._cwd_dir = self._cwd_dir + '/'
        return self._cwd_dir

    def topDir(self):
        if (self._top_dir[-1] != '/'):
           self._top_dir = self._top_dir + '/'
        return self._top_dir 

    def logDir(self):
        if (self._log_dir[-1] != '/'):
           self._log_dir = self._log_dir + '/'
        return self._log_dir

    def jobDir(self):
        if (self._job_dir[-1] != '/'):
           self._job_dir = self._job_dir + '/'
        return self._job_dir

    def resDir(self):
        if (self._res_dir[-1] != '/'):
           self._res_dir = self._res_dir + '/'
        return self._res_dir

    def shareDir(self):
        if (self._share_dir[-1] != '/'):
           self._share_dir = self._share_dir + '/'
        return self._share_dir
        
    def pathForTgz(self):
        if (self._pathForTgz[-1] != '/'):
           self._pathForTgz = self._pathForTgz + '/'
        return self._pathForTgz
        
    def task_uuid(self):
        return self.uuid

    def setResDir(self, dir):
        self._res_dir = dir
        return

    def saveFileName(self):
        return self.shareDir() + common.prog_name + '.sav'

    def cfgFileName(self):
        return self.shareDir() + common.prog_name + '.cfg'

    def saveConfiguration(self, opts, pars):

        # Save options
        
        save_file = open(self.saveFileName(), 'w')

        for k in opts.keys():
            if opts[k] : save_file.write(k+'='+opts[k]+'\n')
            else       : save_file.write(k+'\n')
            pass
        
        save_file.close()

        # Save cfg-file

        secs={}
        for k in pars.keys():
            sec, value = string.split(k,".")
            if sec not in secs.keys():
                secs[sec]={}
            secs[sec][value]=pars[k]
            pass
        
        config_file = open(self.cfgFileName(), 'w')
        for k in secs.keys():
            config_file.write('['+k+']\n')
            for v in secs[k].keys():
                if v!='debug': config_file.write(v+'='+secs[k][v]+'\n')
            else : config_file.write('\n')
        config_file.close()

        return

    def addToSavedCfg(self, opt):
        pars={}
        pars = loadConfig(self.shareDir() + common.prog_name + '.cfg', pars)
        pars[opt.keys()[0]]=opt.values()[0]

        secs={}
        for k in pars.keys():
            sec, value = string.split(k,".")
            if sec not in secs.keys():
                secs[sec]={}
            secs[sec][value]=pars[k]
            pass

        config_file = open(self.cfgFileName(), 'w')
        for k in secs.keys():
            config_file.write('['+k+']\n')
            for v in secs[k].keys():
                if v!='debug': config_file.write(v+'='+secs[k][v]+'\n')
            else : config_file.write('\n')
        config_file.close()

        return
    def loadSavedOptions(self):
        
        # Open save-file

        try:
            save_file = open(self.saveFileName(), 'r')
        except IOError, e:
            msg = 'Misconfigured continuation directory:\n'
            msg += str(e)
            raise CrabException(msg)

        # Read saved options

        save_opts = {}
        for line in save_file:
            line = line[:-1]  # drop '\n'
            try:
                (k, v) = string.split(line, '=')
            except:
                k=line
                v=''
                pass
            save_opts[k] = v
            pass
        
        save_file.close()
        return save_opts
    
