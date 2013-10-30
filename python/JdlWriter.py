from Actor import *
from crab_util import *
import common

import os, errno, time, sys, re 
import commands

class JdlWriter( Actor ):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params
        self.nj_list = jobs 
        from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser
        seWhiteList = cfg_params.get('GRID.se_white_list',[])
        seBlackList = cfg_params.get('GRID.se_black_list',[])
        self.blackWhiteListParser = SEBlackWhiteListParser(seWhiteList, seBlackList, common.logger())
        self.datasetpath=self.cfg_params['CMSSW.datasetpath']
        if string.lower(self.datasetpath)=='none':
            self.datasetpath = None

        return

    def run(self):
        """
        The main method of the class: write JDL for jobs in range self.nj_list
        """
        common.logger.debug( "JdlWriter::run() called")
       
        start = time.time()

        jobs_L = self.listOfjobs()
 
        self.writer(jobs_L) 
     
        stop = time.time()

        common.logger.log(10-1,"JDL writing time :"+str(stop - start))
      
        return


    def listOfjobs(self):
  
        ### define here the list of distinct destinations sites list    
        distinct_dests = common._db.queryDistJob_Attr('dlsDestination', 'jobId' ,self.nj_list)


        ### define here the list of jobs Id for each distinct list of sites
        self.sub_jobs =[] # list of jobs Id list to submit
        jobs_to_match =[] # list of jobs Id to match
        all_jobs=[] 
        count=0
        for distDest in distinct_dests: 
            dest = self.blackWhiteListParser.cleanForBlackWhiteList(distDest)
            if not dest and self.datasetpath: 
                common.logger.info('No destination available: will not create jdl \n' )
                continue
            all_jobs.append(common._db.queryAttrJob({'dlsDestination':distDest},'jobId'))
            sub_jobs_temp=[]
            for i in self.nj_list:
                if i in all_jobs[count]: sub_jobs_temp.append(i) 
            if len(sub_jobs_temp)>0:
                self.sub_jobs.append(sub_jobs_temp)   
            count +=1
        return self.sub_jobs

    def writer(self,list):
        """
        Materialize JDL into file  
        """
        if len(list)==0:
            common.logger.info('No destination available for any job: will not create jdl \n' )
        
        task = common._db.getTask() 
        c1 = 1
        c2 = 1
        for sub_list in list: 
            jdl = common.scheduler.writeJDL(sub_list, task)

            for stri in jdl:
                #self.jdlFile='File-'+str(c1)+'_'+str(c2)+'.jdl'
                self.jdlFile='File-'+str(c1)+'_'+str(c2)+'.jdl'
                j_file = open(common.work_space.shareDir()+'/'+self.jdlFile, 'w')
                j_file.write( stri )
                j_file.close()
                c2 += 1
            c1 += 1

        common.logger.info('JDL files are  written to '+str(common.work_space.shareDir())+'File-*.jdl \n' )

        return
