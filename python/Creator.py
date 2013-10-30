from Actor import Actor
from WorkSpace import WorkSpace
from JobList import JobList
from ScriptWriter import ScriptWriter
from Scheduler import Scheduler
from crab_exceptions import *
from crab_util import *
import common

import os, string, math
import time

class Creator(Actor):
    def __init__(self, job_type_name, cfg_params, ncjobs, skip_blocks=False,isNew=True,first_jobID=0):
        self.job_type_name = job_type_name
        self.job_type = None
        self.cfg_params = cfg_params
        self.total_njobs = 0
        self.total_number_of_events = 0
        self.job_number_of_events = 0
        self.first_event = 0
        self.jobParamsList=[]
        self.first_jobID = first_jobID 
        self.isNew = isNew

        self.createJobTypeObject(ncjobs,skip_blocks,self.isNew)
        common.logger.debug( __name__+": JobType "+self.job_type.name()+" created")


        self.total_njobs = self.job_type.numberOfJobs();
        common.logger.debug( __name__+": total # of jobs = "+`self.total_njobs`)

        self.ncjobs = ncjobs
        if ncjobs == 'all' : self.ncjobs = self.total_njobs
        if ncjobs > self.total_njobs : self.ncjobs = self.total_njobs
        
        self.job_type_name = self.job_type.name()
 
        common.logger.debug( "Creator constructor finished")
        return

    def writeJobsSpecsToDB(self, first_jobID=0):
        """
        Write firstEvent and maxEvents in the DB for future use
        """

        self.job_type.split(self.jobParamsList,first_jobID)
        return

    def nJobs(self):
        return self.total_njobs
    

    def nJobsL(self):
        jobsL=[]
        for i in range(self.total_njobs):
            jobsL.append(self.first_jobID+i+1)   
        return jobsL

    def createJobTypeObject(self,ncjobs,skip_blocks,isNew):
        file_name = 'cms_'+ string.lower(self.job_type_name)
        klass_name = string.capitalize(self.job_type_name)

        try:
            klass = importName(file_name, klass_name)
        except KeyError:
            msg = 'No `class '+klass_name+'` found in file `'+file_name+'.py`'
            raise CrabException(msg)
        except ImportError, e:
            msg = 'Cannot create job type '+self.job_type_name
            msg += ' (file: '+file_name+', class '+klass_name+'):\n'
            msg += str(e)
            raise CrabException(msg)

        self.job_type = klass(self.cfg_params,ncjobs,skip_blocks,isNew)
        return

    def jobType(self):
        return self.job_type

    def run(self):
        """
        The main method of the class.
        """

        common.logger.debug( "Creator::run() called")
        start = time.time()
        # Instantiate ScriptWriter
        script_writer = ScriptWriter(self.cfg_params,'crab_template.sh')

        # Loop over jobs
        argsList = []
        njc = 0
        listID=[]
        listField=[]
        listRunField=[]
        run_jobToSave = {'status' :'C', \
                         'statusScheduler' : 'Created', \
                         'state' : "Created"  }

        msg = ("Creating jobs :\n")
        for nj in range(self.total_njobs):
            nj=nj+self.first_jobID
            output=[]
            if njc == self.ncjobs : break

            msg += "... job # %s "%str(nj+1)
            listRunField.append(run_jobToSave)

            # Prepare configuration file

            output.append('out_files_'+str(nj+1)+'.tgz')
            job_ToSave={'outputFiles': output}
            listField.append(job_ToSave)             

            listID.append(nj+1)
            njc = njc + 1
            pass
        common.logger.log(10-1,msg) 
       # ## Not clear why here.. DS
       # self.job_type.setArgsList()
        common._db.updateRunJob_(listID , listRunField ) 
        common._db.updateJob_(listID, listField ) 

        if self.isNew:
            # Create script (sh)
            script_writer.modifyTemplateScript()
       
            # SL This should be a list, rather than a string!
            concString = ','
            inSand=''
            if len(self.job_type.inputSandbox(1)): 
                inSand +=   concString.join(self.job_type.inputSandbox(1)) 
            # Sandbox, Start Dir , outputDir
            param = {'globalSandbox': inSand , 'startDirectory': common.work_space.cwdDir() , 'outputDirectory': common.work_space.resDir() }
            common._db.updateTask_(param) 
            # define requirement
            common.scheduler.sched_fix_parameter()
        common.scheduler.declare(listID) 
       
        stop = time.time()
        common.logger.info('Creating '+str(self.total_njobs)+' jobs, please wait...')

        stop = time.time()
        common.logger.debug( "Creation Time: "+str(stop - start))

        msg = 'Total of %d jobs created'%njc
        if njc != self.ncjobs: msg = msg + ' from %d requested'%self.ncjobs
        msg = msg + '.\n'
        common.logger.info(msg)
        
        return
