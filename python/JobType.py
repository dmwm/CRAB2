from crab_exceptions import *

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#


class JobType:
    def __init__(self, name):
        self._name = name
        self._params={}
        return

    def split(self, jobList):
        """
        Returns the list of jobtype specific attriute for each job
        """
        return     
        
    def numberOfJobs(self):
        """
        Returns the numberof job to be created
        """
        return 0
    
    def jobsToDB(self, nJobs):
        """
        Write the JobType specific Entres into JobDB
        """
        return
    
    def name(self):
        return self._name

    def prepareSteeringCards(self):
        """
        Make initial modifications of the user's steering card file.
        These modifications are common for all jobs.
        """
        # The default is no action,
        # i.e. user's steering card file used as is.
        return

    def modifySteeringCards(self, nj):
        """
        Make individual modifications of the user's steering card file
        for one job.
        """
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::modifySteeringCards() from '+__file__
        raise CrabException(msg)

    def wsSetupCMSEnvironment_(self):
        """
        Returns part of a job script which is prepares
        the execution environment and which is common for all CMS jobs.
        """
        txt = '\n'
        txt += 'echo ">>> setup CMS environment:"\n'
        txt += '   echo "JOB_EXIT_STATUS = 0"\n'
        txt += 'if [ ! $VO_CMS_SW_DIR ] ;then\n'
        #txt += '   echo "SET_CMS_ENV 10031 ==> ERROR CMS software dir not found on WN `hostname`"\n'
        #txt += '   echo "JOB_EXIT_STATUS = 10031" \n'
        #txt += '   echo "JobExitCode=10031" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '   exit 1\n'
        txt += '   echo "ERROR ==> CMS software dir not found on WN `hostname`"\n'
        txt += '   job_exit_code=10031\n'
        txt += '   func_exit\n'
        txt += 'else\n'
        txt += '   echo "Sourcing environment... "\n'
        txt += '   if [ ! -s $VO_CMS_SW_DIR/cmsset_default.sh ] ;then\n'
        #txt += '       echo "SET_CMS_ENV 10020 ==> ERROR cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        #txt += '       echo "JOB_EXIT_STATUS = 10020"\n'
        #txt += '       echo "JobExitCode=10020" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '       dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '       exit 1\n'
        txt += '       echo "ERROR ==> cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        txt += '       job_exit_code=10020\n'
        txt += '       func_exit\n'
        txt += '   fi\n'
        txt += '   echo "sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '   source $VO_CMS_SW_DIR/cmsset_default.sh\n'
        txt += '   result=$?\n'
        txt += '   if [ $result -ne 0 ]; then\n'
        #txt += '       echo "SET_CMS_ENV 10032 ==> ERROR problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        #txt += '       echo "JOB_EXIT_STATUS = 10032"\n'
        #txt += '       echo "JobExitCode=10032" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '       dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '       exit 1\n'
        txt += '       echo "ERROR ==> problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '       job_exit_code=10032\n'
        txt += '       func_exit\n'
        txt += '   fi\n'
        txt += 'fi\n'
        txt += '\n'
        txt += 'echo "==> setup cms environment ok"\n'
        return txt
    
    def wsSetupEnvironment(self, nj):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """
        return ''

    def wsBuildExe(self, nj):
        """
        Returns part of a job script which builds the binary executable.
        """
        return ''

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """
        return ''

    def executableName(self):
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::executableName() from '+__file__
        raise CrabException(msg)

    def executableArgs(self):
        return ''

    def setParam_(self, param, value):
        """
        Set relevant job type parameters
        """
        self._params[param] = value

    def getParams(self):
        """
        Get relevant job type parameters dictionary
        """
        return self._params

    # marco

    def getRequirements(self, nj):
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::executableName() from '+__file__
        raise CrabException(msg)

    def configFilename(self):
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::configFilename() from '+__file__
        raise CrabException(msg)
