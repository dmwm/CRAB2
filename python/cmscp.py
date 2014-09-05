#!/usr/bin/env python
import sys, os
import time, random
try:
    import json
except:    
    import simplejson as json
from ProdCommon.Storage.SEAPI.SElement import SElement, FullPath
from ProdCommon.Storage.SEAPI.SBinterface import *
from ProdCommon.Storage.SEAPI.Exceptions import *
from ProdCommon.FwkJobRep.SiteLocalConfig import loadSiteLocalConfig



class cmscp:
    def __init__(self, args):
        """
        cmscp
        safe copy of local file  to/from remote SE via lcg_cp/srmcp,
        including success checking  version also for CAF using rfcp command to copy the output to SE
        input:
           $1 middleware (CAF, LSF, LCG, OSG)
           $2 local file (the absolute path of output file or just the name if it's in top dir)
           $3 if needed: file name (the output file name)
           $5 remote SE (complete endpoint)
           $6 srm version
           --for_lfn $LFNBaseName
        output:
             return 0 if all ok
             return 60307 if srmcp failed
             return 60303 if file already exists in the SE
        """
        self.params = {"source":'', "destination":'','destinationDir':'', "inputFileList":'', "outputFileList":'', \
                           "protocol":'', "option":'', "middleware":'', "srm_version":'srmv2', "for_lfn":'', "se_name":'', "surl_for_grid":''}
        self.debug = 0
        #### for fallback copy
        self.local_stage = 0
        self.params.update( args )
        ## timeout needed for subprocess command of SEAPI
        ## they should be a bit higher then the corresponding passed by command line  
        ## default values
        self.subprocesstimeout = { \
                                   'copy':   3600, \
                                   'exists': 1200, \
                                   'delete': 1200, \
                                   'size':   1200 \
                                 }

        return

    def processOptions( self ):
        """
        check command line parameter
        """
        if 'help' in self.params.keys(): HelpOptions()
        if 'debug' in self.params.keys(): self.debug = 1
        if 'local_stage' in self.params.keys(): self.local_stage = 1

        # source and dest cannot be undefined at same time
        if not self.params['source']  and not self.params['destination'] :
            HelpOptions()
        # if middleware is not defined --> protocol cannot be empty
        if not self.params['middleware'] and not self.params['protocol'] :
            HelpOptions()

        # input file must be defined
        if not self.params['inputFileList'] : 
            HelpOptions()
        else:
            file_to_copy=[]
            if self.params['inputFileList'].find(','):
                [file_to_copy.append(x.strip()) for x in self.params['inputFileList'].split(',')]
            else:
                file_to_copy.append(self.params['inputFileList'])
            self.params['inputFileList'] = file_to_copy

        if not self.params['for_lfn'] and self.local_stage == 1 : HelpOptions()
        

    def run( self ):
        """
        Check if running on UI (no $middleware) or
        on WN (on the Grid), and take different action
        """
        self.processOptions()
        if self.debug: print 'calling run() : \n'
        # stage out from WN
        if self.params['middleware'] :
            results = self.stager(self.params['middleware'],self.params['inputFileList'])
            self.writeJsonFile(results)
            self.finalReport(results)
        # Local interaction with SE
        else:
            results = self.copy(self.params['inputFileList'], self.params['protocol'], self.params['option'] )
            self.writeJsonFile(results)
            return results
            
    def writeJsonFile( self, results ):
        """
        write a json file containing copy results for each file
        """
        if self.debug: 
            print 'in writeJsonFile() : \n'
            print "---->>>> in writeJsonFile results =  ", results
        jsonOut = "resultCopyFile"
        if os.getenv("RUNTIME_AREA"):
            jsonOut = "%s/resultCopyFile"%os.getenv("RUNTIME_AREA")
        fp = open(jsonOut, 'w')
        json.dump(results, fp)
        fp.close()
        if self.debug: 
            print '    reading resultCopyFile : \n'
            lp = open(jsonOut, "r")
            inputDict = json.load(lp)
            lp.close()
            print "    inputDict = ", inputDict
        return

    def checkLcgUtils( self ):
        """
        _checkLcgUtils_
        check the lcg-utils version and report
        """
        import commands
        cmd = "lcg-cp --version | grep lcg_util"
        try:
            status, output = commands.getstatusoutput( cmd )
            num_ver = -1
            if output.find("not found") == -1 or status == 0:
                temp = output.split("-")
                version = ""
                if len(temp) >= 2:
                    version = output.split("-")[1]
                    temp = version.split(".")
                    if len(temp) >= 1:
                        num_ver = int(temp[0])*10
                        num_ver += int(temp[1])
        except:
            # if above failed, better try lcg-cp anyhow, at least
            # will get decent log and message
            num_ver = 99999
        return num_ver

    def setProtocol( self, middleware ):
        """
        define the allowed potocols based on $middlware
        which depend on scheduler
        """
        # default To be used with "middleware"
        if self.debug: 
            print 'setProtocol() :\n'
            print '\tmiddleware =  %s utils \n'%middleware

        lcgOpt={'srmv1':'-b -D srmv1  -t 2400 --verbose',
                'srmv2':'-b -D srmv2  -t 2400 --verbose'}
        if self.checkLcgUtils() >= 17:
            lcgOpt={'srmv1':'-b -D srmv1 --srm-timeout 2400 --sendreceive-timeout 2400 --connect-timeout 300 --verbose',
                    'srmv2':'-b -D srmv2 --srm-timeout 2400 --sendreceive-timeout 2400 --connect-timeout 300 --verbose'}

        srmOpt={'srmv1':' -report ./srmcp.report -retry_timeout 480000 -retry_num 3 -streams_num=1 ',
                'srmv2':' -report=./srmcp.report -retry_timeout=480000 -retry_num=3 -storagetype=permanent '}
        rfioOpt=''
        #### FEDE FOR XROOTD #########
        xrootdOpt=''
        #############################

        supported_protocol = None
        if middleware.lower() in ['osg','lcg','condor','sge','pbsv2', 'slurm']:
            supported_protocol = [('srm-lcg',lcgOpt[self.params['srm_version']])]#,\
                               #  (self.params['srm_version'],srmOpt[self.params['srm_version']])]
        #elif middleware.lower() in ['lsf','caf']:
        elif middleware.lower() in ['lsf']:
            supported_protocol = [('rfio',rfioOpt)]
        elif middleware.lower() in ['pbs']:
            supported_protocol = [('rfio',rfioOpt),('local','')]
        elif middleware.lower() in ['arc']:
            supported_protocol = [('srmv2','-debug'),('srmv1','-debug')]
        #### FEDE FOR XROOTD ##########
        elif middleware.lower() in ['caf']:
            if self.params['protocol']:
                supported_protocol = [(self.params['protocol'], '')]
            else:
                supported_protocol = [('rfio',rfioOpt)]
            #######################################    
        else:
            ## here we can add support for any kind of protocol,
            ## maybe some local schedulers need something dedicated
            pass

        # find out where we run and where we stageout
        siteCfg = loadSiteLocalConfig()
        localSeName = siteCfg.localStageOutSEName()
        remoteSeName = self.params['se_name']
        seIsLocal = localSeName == remoteSeName
        print "******************************************************"
        print "localSeName  = ",localSeName
        print "remoteSeName = ", remoteSeName
        print "******************************************************"

        # force lstore protocol for Vanderbilt, waiting for better
        # long term solution
        if seIsLocal and localSeName.endswith('vanderbilt.edu') :
            print "*** I am at, and writing-to, Vanderbilt. Try lstore first ***"
            supported_protocol.insert(0, ('lstore','') )
            
        return supported_protocol


    def checkCopy (self, copy_results, len_list_files, prot):
        """
        Checks the status of copy and update result dictionary
        """
        
        list_retry = []
        list_not_existing = []
        list_already_existing = []
        list_fallback = []
        list_ok = []
        
        if self.debug: 
            print 'in checkCopy() :\n'
        
        for file, dict in copy_results.iteritems():
            er_code = dict['erCode']
            if er_code == '0':
                list_ok.append(file)
                reason = 'Copy succedeed with %s utils'%prot
                dict['reason'] = reason
            elif er_code == '60308':
                list_fallback.append( file )
                reason = 'Copy succedeed with %s utils'%prot
                dict['reason'] = reason
            elif er_code == '60302': 
                list_not_existing.append( file )
            elif er_code == '60303':
                list_already_existing.append( file )
            else :    
                list_retry.append( file )
          
            if self.debug:
                print "\t file %s \n"%file
                print "\t dict['erCode'] %s \n"%dict['erCode']
                print "\t dict['reason'] %s \n"%dict['reason']
                
            upDict = self.updateReport(file, er_code, dict['reason'])

            copy_results.update(upDict)
        
        msg = ''
        if len(list_ok) != 0:
            msg += '\tCopy of %s succedeed with %s utils\n'%(str(list_ok),prot)
        if len(list_ok) != len_list_files :
            if len(list_fallback)!=0:
                msg += '\tCopy of %s succedeed with %s utils in the fallback SE\n'%(str(list_fallback),prot)
            if len(list_retry)!=0:
                msg += '\tCopy of %s failed using %s for files \n'%(str(list_retry),prot)
            if len(list_not_existing)!=0:
                msg += '\tCopy of %s failed using %s : files not found \n'%(str(list_not_existing),prot)
            if len(list_already_existing)!=0:
                msg += '\tCopy of %s failed using %s : files already existing\n'%(str(list_already_existing),prot)
        if self.debug : print msg
        return copy_results, list_ok, list_retry, list_fallback
        
    def check_for_retry_localSE (self, copy_results):
        """
        Checks the status of copy and create the list of file to copy to CloseSE
        """
        list_retry_localSE = []

        if self.debug: 
            print 'in check_for_retry_localSE() :\n'
            print "\t results in check local = ", copy_results
        for file, dict in copy_results.iteritems():
            er_code = dict['erCode']
            if er_code != '0' and  er_code != '60302' and er_code != '60308':
                list_retry_localSE.append( file )
                
            if self.debug:
                print "\t file %s \n"%file
                print "\t dict['erCode'] %s \n"%dict['erCode']
                print "\t dict['reason'] %s \n"%dict['reason']
                
        return list_retry_localSE

        
    def LocalCopy(self, list_retry, results):
        """
        Tries the stage out to the CloseSE
        """
        if self.debug: 
            print 'in LocalCopy() :\n'
            print '\t list_retry %s utils \n'%list_retry
            print '\t len(list_retry) %s \n'%len(list_retry)
                
        list_files = list_retry  
        self.params['inputFileList']=list_files

        ### copy backup
        from ProdCommon.FwkJobRep.SiteLocalConfig import loadSiteLocalConfig
        siteCfg = loadSiteLocalConfig()
        catalog = siteCfg.localStageOut.get("catalog", None)
        tfc = siteCfg.trivialFileCatalog()
        seName = siteCfg.localStageOutSEName()
        if seName is None:
            seName = ""
        option = siteCfg.localStageOutOption()
        if option is None:
            option = ""
        implName = siteCfg.localStageOutCommand()
        if implName is None:
            implName = ""

        if (implName == 'srm'):
           protocol = 'srmv2'
        elif (implName == 'srmv2-lcg'):
           protocol = 'srm-lcg'
           option = option + ' -b -D srmv2 '
        elif (implName == 'rfcp-CERN'):
           protocol = 'rfio'
        elif (implName == 'rfcp'):
           protocol = 'rfio'
        elif (implName == 'cp'):
           protocol = 'local'
        else: protocol = implName 
        
        self.params['protocol']=protocol
        self.params['option']=option

        if self.debug:
            print '\t siteCFG %s \n'%siteCfg
            print '\t catalog %s \n'%catalog 
            print '\t tfc %s '%tfc
            print '\t fallback seName %s \n'%seName 
            print "\t fallback protocol %s \n"%protocol            
            print "\t fallback option %s \n"%option
            print "\t self.params['inputFileList'] %s \n"%self.params['inputFileList']
                
        if (str(self.params['for_lfn']).find("/store/") == 0):
            temp = str(self.params['for_lfn']).replace("/store/","/store/temp/",1)
            self.params['for_lfn']= temp
        
        if ( self.params['for_lfn'][-1] != '/' ) : self.params['for_lfn'] = self.params['for_lfn'] + '/'
            
        file_backup=[]
        file_backup_surlgrid=[]
        for input in self.params['inputFileList']:
            file = self.params['for_lfn'] + os.path.basename(input)
            surl = tfc.matchLFN(tfc.preferredProtocol, file)
            
            ###### FEDE TEST_FOR_SURL_GRID
            surl_for_grid = tfc.matchLFN('srmv2', file)
            if (surl_for_grid == None):
                surl_for_grid = tfc.matchLFN('srmv2-lcg', file)
                if (surl_for_grid == None):
                    surl_for_grid = tfc.matchLFN('srm', file)
            if surl_for_grid:
                file_backup_surlgrid.append(surl_for_grid)
            ###### 

            file_backup.append(surl)
            if self.debug:
                print '\t for_lfn %s \n'%self.params['for_lfn']
                print '\t file %s \n'%file
                print '\t surl %s \n'%surl
                    
        destination=os.path.dirname(file_backup[0])
        if ( destination[-1] != '/' ) : destination = destination + '/'
        self.params['destination']=destination

        self.params['se_name']=seName

        ###### 
        if (len(file_backup_surlgrid)>0) :
            surl_for_grid=os.path.dirname(file_backup_surlgrid[0])
            if ( surl_for_grid[-1] != '/' ) : surl_for_grid = surl_for_grid + '/'
        else:
            surl_for_grid=''

        print "surl_for_grid = ", surl_for_grid    
        self.params['surl_for_grid']=surl_for_grid
        #####

        if self.debug:
            print '\tIn LocalCopy trying the stage out with: \n'
            print "\tself.params['destination'] %s \n"%self.params['destination']
            print "\tself.params['protocol'] %s \n"%self.params['protocol']
            print "\tself.params['option'] %s \n"%self.params['option']

        localCopy_results = self.copy( self.params['inputFileList'], self.params['protocol'], self.params['option'], backup='yes' )
           
        if localCopy_results.keys() == [''] or localCopy_results.keys() == '' :
            results.update(localCopy_results)
        else:
            localCopy_results, list_ok, list_retry, list_fallback = self.checkCopy(localCopy_results, len(list_files), self.params['protocol'])
                
            results.update(localCopy_results)
        if self.debug:
            print "\t localCopy_results = %s \n"%localCopy_results
        return results        

    def stager( self, middleware, list_files ):
        """
        Implement the logic for remote stage out
        """
 
        if self.debug: 
            print 'stager() :\n'
            print '\tmiddleware %s\n'%middleware
            print '\tlist_files %s\n'%list_files
        
        results={}
        for prot, opt in self.setProtocol( middleware ):
            if self.debug: 
                print '\tTrying the stage out with %s utils \n'%prot
                print '\tand options %s\n'%opt
                
            copy_results = self.copy( list_files, prot, opt )
            if copy_results.keys() == [''] or copy_results.keys() == '' :
                results.update(copy_results)
            else:
                copy_results, list_ok, list_retry, list_fallback = self.checkCopy(copy_results, len(list_files), prot)
                results.update(copy_results)
                if len(list_ok) == len(list_files) :
                    break
                if len(list_retry):
                    list_files = list_retry
                    #### FEDE added ramdom time before the retry copy with other protocol
                    sec =  240 * random.random()
                    sec = sec + 60
                    time.sleep(sec)
                else: break

        if self.local_stage:
            list_retry_localSE = self.check_for_retry_localSE(results)
            if len(list_retry_localSE):
                if self.debug: 
                    print "\t list_retry_localSE %s \n"%list_retry_localSE
                results = self.LocalCopy(list_retry_localSE, results)

        if self.debug:
            print "\t results %s \n"%results
        return results

    def initializeApi(self, protocol ):
        """
        Instantiate storage interface
        """
        if self.debug : print 'initializeApi() :\n'  
        self.source_prot = protocol
        self.dest_prot = protocol
        if not self.params['source'] : self.source_prot = 'local'
        Source_SE  = self.storageInterface( self.params['source'], self.source_prot )
        if not self.params['destination'] :
            self.dest_prot = 'local'
            Destination_SE = self.storageInterface( self.params['destinationDir'], self.dest_prot )
        else:     
            Destination_SE = self.storageInterface( self.params['destination'], self.dest_prot )

        if self.debug :
            msg  = '\t(source=%s,  protocol=%s)'%(self.params['source'], self.source_prot)
            msg += '\t(destination=%s,  protocol=%s)'%(self.params['destination'], self.dest_prot)
            msg += '\t(destinationDir=%s,  protocol=%s)'%(self.params['destinationDir'], self.dest_prot)
            print msg

        return Source_SE, Destination_SE

    def copy( self, list_file, protocol, options, backup='no' ):
        """
        Make the real file copy using SE API
        """
        msg = ""
        results = {}
        if self.debug :
            msg  = 'copy() :\n'
            msg += '\tusing %s protocol\n'%protocol
            msg += '\tusing %s options\n'%options
            msg += '\tlist_file %s\n'%list_file
            print msg
        try:
            Source_SE, Destination_SE = self.initializeApi( protocol )
        except Exception, ex:
            for filetocopy in list_file:
                results.update( self.updateReport(filetocopy, '-1', str(ex)))
            return results

        self.hostname = Destination_SE.hostname
        if Destination_SE.protocol in ['gridftp','rfio','srmv2','hadoop','lstore','local']:
            try:
                self.createDir( Destination_SE, Destination_SE.protocol )
            except OperationException, ex:
                for filetocopy in list_file:
                    results.update( self.updateReport(filetocopy, '60316', str(ex)))
                return results
            ## when the client commands are not found (wrong env or really missing)
            except MissingCommand, ex:
                msg = "ERROR %s %s" %(str(ex), str(ex.detail))
                for filetocopy in list_file:
                    results.update( self.updateReport(filetocopy, '10041', msg))
                return results
            except Exception, ex:
                msg = "ERROR %s" %(str(ex))
                for filetocopy in list_file:
                    results.update( self.updateReport(filetocopy, '-1', msg))
                return results
        ## prepare for real copy  ##
        try :
            sbi = SBinterface( Source_SE, Destination_SE )
            sbi_dest = SBinterface(Destination_SE)
            sbi_source = SBinterface(Source_SE)
        except ProtocolMismatch, ex:
            msg  = "ERROR : Unable to create SBinterface with %s protocol"%protocol
            msg += str(ex)
            for filetocopy in list_file:
                results.update( self.updateReport(filetocopy, '-1', msg))
            return results

        
        ## loop over the complete list of files
        for filetocopy in list_file:
            if self.debug : print '\tStart real copy for %s'%filetocopy
            try :
                ErCode, msg = self.checkFileExist( sbi_source, sbi_dest, filetocopy, options )
            except Exception, ex:
                ErCode = '60307'
                msg = str(ex)  
            if ErCode == '0':
                ErCode, msg = self.makeCopy( sbi, filetocopy , options, protocol,sbi_dest )
                if (ErCode == '0') and (backup == 'yes'):
                    ErCode = '60308'
            if self.debug : print '\tCopy results for %s is %s'%( os.path.basename(filetocopy), ErCode)
            results.update( self.updateReport(filetocopy, ErCode, msg))
        return results


    def storageInterface( self, endpoint, protocol ):
        """
        Create the storage interface.
        """
        if self.debug : print 'storageInterface():\n'
        try:
            interface = SElement( FullPath(endpoint), protocol )
        except ProtocolUnknown, ex:
            msg  = "ERROR : Unable to create interface with %s protocol"%protocol
            msg += str(ex)
            raise Exception(msg)

        return interface

    def createDir(self, Destination_SE, protocol):
        """
        Create remote dir for gsiftp REALLY TEMPORARY
        this should be transparent at SE API level.
        """
        if self.debug : print 'createDir():\n'
        msg = ''
        try:
            action = SBinterface( Destination_SE )
            action.createDir()
            if self.debug: print "\tThe directory has been created using protocol %s"%protocol
        except TransferException, ex:
            msg  = "ERROR: problem with the directory creation using %s protocol "%protocol
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            raise Exception(msg)
        except OperationException, ex:
            msg  = "ERROR: problem with the directory creation using %s protocol "%protocol
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        except MissingDestination, ex:
            msg  = "ERROR: problem with the directory creation using %s protocol "%protocol
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        except AlreadyExistsException, ex:
            if self.debug: print "\tThe directory already exist"
            pass
        except Exception, ex:    
            msg = "ERROR %s %s" %(str(ex), str(ex.detail))
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        return msg

    def checkFileExist( self, sbi_source, sbi_dest, filetocopy, option ):
        """
        Check both if source file exist AND 
        if destination file ALREADY exist. 
        """
        if self.debug : print 'checkFileExist():\n'
        ErCode = '0'
        msg = ''
        f_tocopy=filetocopy
        if self.source_prot != 'local':f_tocopy = os.path.basename(filetocopy)
        try:
            checkSource = sbi_source.checkExists( f_tocopy , opt=option, tout = self.subprocesstimeout['exists'] )
            if self.debug : print '\tCheck for local file %s existance executed \n'%f_tocopy  
        except OperationException, ex:
            msg  ='ERROR: problems checking source file %s existance'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            raise Exception(msg)
        except WrongOption, ex:
            msg  ='ERROR: problems checking source file %s existance'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            raise Exception(msg)
        except MissingDestination, ex:
            msg  ='ERROR: problems checking source file %s existance'%filetocopy
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        ## when the client commands are not found (wrong env or really missing)
        except MissingCommand, ex:
            ErCode = '10041'
            msg = "ERROR %s %s" %(str(ex), str(ex.detail))
            return ErCode, msg
        if not checkSource :
            ErCode = '60302'
            msg = "ERROR file %s does not exist"%os.path.basename(filetocopy)
            return ErCode, msg
        f_tocopy=os.path.basename(filetocopy)
        try:
            check = sbi_dest.checkExists( f_tocopy, opt=option, tout = self.subprocesstimeout['exists'] )
            if self.debug : print '\tCheck for remote file %s existance executed \n'%f_tocopy  
            if self.debug : print '\twith exit code = %s \n'%check  
        except OperationException, ex:
            msg  = 'ERROR: problems checking if file %s already exist'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
            raise Exception(msg)
        except WrongOption, ex:
            msg  = 'ERROR problems checking if file % already exists'%filetocopy
            msg += str(ex)
            if self.debug :
                msg += '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                msg += '\t'+str(ex.output)+'\n'
            raise Exception(msg)
        except MissingDestination, ex:
            msg  ='ERROR problems checking if destination file % exists'%filetocopy
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        ## when the client commands are not found (wrong env or really missing)
        except MissingCommand, ex:
            ErCode = '10041'
            msg = "ERROR %s %s" %(str(ex), str(ex.detail))
            return ErCode, msg
        if check :
            ErCode = '60303'
            msg = "file %s already exist"%os.path.basename(filetocopy)

        return ErCode, msg

    def makeCopy(self, sbi, filetocopy, option, protocol, sbi_dest ):
        """
        call the copy API.
        """
        if self.debug : print 'makeCopy():\n'
        path = os.path.dirname(filetocopy)
        file_name =  os.path.basename(filetocopy)
        source_file = filetocopy
        dest_file = file_name ## to be improved supporting changing file name  TODO
        if self.params['source'] == '' and path == '':
            source_file = os.path.abspath(filetocopy)
        elif self.params['destination'] =='':
            destDir = self.params.get('destinationDir',os.getcwd())
            dest_file = os.path.join(destDir,file_name)
        elif self.params['source'] != '' and self.params['destination'] != '' :
            source_file = file_name

        ErCode = '0'
        msg = ''

        copy_option = option
        if  self.params['option'].find('space_token')>=0: 
            space_token=self.params['option'].split('=')[1] 
            if protocol == 'srmv2': copy_option = '%s -space_token=%s'%(option,space_token)
            if protocol == 'srm-lcg': copy_option = '%s -S %s'%(option,space_token)
        try:
            sbi.copy( source_file , dest_file , opt = copy_option, tout = self.subprocesstimeout['copy'])
        except TransferException, ex:
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            ErCode = '60307'
        except WrongOption, ex:
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
            ### FEDE added for savannah 97460###     
            ErCode = '60307'
            ####################################
        except SizeZeroException, ex:
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            ErCode = '60307'
        ## when the client commands are not found (wrong env or really missing)
        except MissingCommand, ex:
            ErCode = '10041'
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
        except AuthorizationException, ex:
            ErCode = '60307'
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
        except SEAPITimeout, ex:
            ErCode = '60317'
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
 
        if ErCode == '0' and protocol.find('srmv') == 0:
            remote_file_size = -1 
            local_file_size = os.path.getsize( source_file ) 
            try:
                remote_file_size = sbi_dest.getSize( dest_file, opt=option, tout = self.subprocesstimeout['size'] )
                if self.debug : print '\t Check of remote size succeded for file %s\n'%dest_file
            except TransferException, ex:
                msg  = "Problem checking the size of %s file" % filetocopy
                msg += str(ex)
                if self.debug :
                    dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                    dbgmsg += '\t'+str(ex.output)+'\n'
                    print dbgmsg
                ErCode = '60307'
            except WrongOption, ex:
                msg  = "Problem checking the size of %s file" % filetocopy
                msg += str(ex)
                if self.debug :
                    dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                    dbgmsg += '\t'+str(ex.output)+'\n'
                    print dbgmsg
                ErCode = '60307'
            if local_file_size != remote_file_size:
                msg = "File size dosn't match: local size = %s ; remote size = %s " % (local_file_size, remote_file_size)
                ErCode = '60307'

        if ErCode != '0':
            try :
                self.removeFile( sbi_dest, dest_file, option )
            except Exception, ex:
                msg += '\n'+str(ex)  
        return ErCode, msg

    def removeFile( self, sbi_dest, filetocopy, option ):
        """  
        """  
        if self.debug : print 'removeFile():\n'
        f_tocopy=filetocopy
        if self.dest_prot != 'local':f_tocopy = os.path.basename(filetocopy)
        try:
            sbi_dest.delete( f_tocopy, opt=option, tout = self.subprocesstimeout['delete'] )
            if self.debug : '\t deletion of file %s succeeded\n'%str(filetocopy)
        except OperationException, ex:
            msg  ='ERROR: problems removing partially staged file %s'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
            raise Exception(msg)

        return 

    def updateReport(self, file, erCode, reason):
        """
        Update the final stage out infos
        """
        jobStageInfo={}
        jobStageInfo['erCode']=erCode
        jobStageInfo['reason']=reason
        if not self.params['for_lfn']: self.params['for_lfn']=''
        if not self.params['se_name']: self.params['se_name']=''
        if not self.hostname: self.hostname=''
        if (erCode != '0') and (erCode != '60308'):
           jobStageInfo['for_lfn']='/copy_problem/'
        else:   
            jobStageInfo['for_lfn']=self.params['for_lfn']
        jobStageInfo['se_name']=self.params['se_name']
        jobStageInfo['PNN'] = self.params['PNN']
        jobStageInfo['endpoint']=self.hostname
        ### ADDING SURLFORGRID FOR COPYDATA
        if not self.params['surl_for_grid']: self.params['surl_for_grid']=''
        jobStageInfo['surl_for_grid']=self.params['surl_for_grid']
        #####
        report = { file : jobStageInfo}
        return report

    def finalReport( self , results ):
        """
        It a list of LFNs for each SE where data are stored.
        allow "crab -copyLocal" or better "crab -copyOutput". TO_DO.
        """
        
        outFile = 'cmscpReport.sh'
        if os.getenv("RUNTIME_AREA"):
            #print "RUNTIME_AREA = ", os.getenv("RUNTIME_AREA")
            outFile = "%s/cmscpReport.sh"%os.getenv("RUNTIME_AREA")
            #print "--->>> outFile = ", outFile
        fp = open(outFile, "w")

        cmscp_exit_status = 0
        txt = '#!/bin/bash\n'
        for file, dict in results.iteritems():
            reason = str(dict['reason'])
            if str(reason).find("'") > -1:
                reason = " ".join(reason.split("'"))
            reason="'%s'"%reason
            if file:
                if dict['for_lfn']=='':
                    lfn = '${LFNBaseName}'+os.path.basename(file)
                    se  = '$SE'
                    LFNBaseName = '$LFNBaseName'
                else:
                    lfn = dict['for_lfn']+os.path.basename(file)
                    se = dict['se_name']
                    LFNBaseName = os.path.dirname(lfn)
                    if (LFNBaseName[-1] != '/'):
                        LFNBaseName = LFNBaseName + '/'

                
                txt += 'echo "Report for File: '+file+'"\n'
                txt += 'echo "LFN: '+lfn+'"\n'
                txt += 'echo "StorageElement: '+se+'"\n'
                txt += 'echo "StageOutExitStatusReason = %s" | tee -a $RUNTIME_AREA/$repo\n'%reason
                txt += 'echo "StageOutSE = '+se+'" >> $RUNTIME_AREA/$repo\n'

                
                if dict['erCode'] != '0':
                    cmscp_exit_status = dict['erCode']
            else:
                txt += 'echo "StageOutExitStatusReason = %s" | tee -a $RUNTIME_AREA/$repo\n'%reason
                cmscp_exit_status = dict['erCode']
        txt += '\n'
        txt += 'export StageOutExitStatus='+str(cmscp_exit_status)+'\n'
        txt += 'echo "StageOutExitStatus = '+str(cmscp_exit_status)+'" | tee -a $RUNTIME_AREA/$repo\n'
        fp.write(str(txt))
        fp.close()
        if self.debug: 
            print '--- reading cmscpReport.sh: \n'
            lp = open(outFile, "r")
            content = lp.read() 
            lp.close()
            print "    content = ", content
            print '--- end reading cmscpReport.sh'
        return


def usage():

    msg="""
    cmscp:
        safe copy of local file  to/from remote SE via lcg_cp/srmcp,
        including success checking  version also for CAF using rfcp command to copy the output to SE

    accepted parameters:
       source           =
       destination      =
       inputFileList    =
       outputFileList   =
       protocol         =
       option           =
       middleware       =  
       srm_version      =
       destinationDir   = 
       lfn=             = 
       local_stage      =  activate stage fall back  
       debug            =  activate verbose print out 
       help             =  print on line man and exit   
    
    mandatory:
       * "source" and/or "destination" must always be defined 
       * either "middleware" or "protocol" must always be defined 
       * "inputFileList" must always be defined
       * if "local_stage" = 1 also  "lfn" must be defined 
    """
    print msg

    return

def HelpOptions(opts=[]):
    """
    Check otps, print help if needed
    prepare dict = { opt : value }
    """
    dict_args = {}
    if len(opts):
        for opt, arg in opts:
            dict_args[opt.split('--')[1]] = arg
            if opt in ('-h','-help','--help') :
                usage()
                sys.exit(0)
        return dict_args
    else:
        usage()
        sys.exit(0)

if __name__ == '__main__' :

    import getopt

    allowedOpt = ["source=", "destination=", "inputFileList=", "outputFileList=", \
                  "protocol=","option=", "middleware=", "srm_version=", \
                  "destinationDir=", "for_lfn=", "local_stage", "debug", "help", "PNN=", "se_name="]
    try:
        opts, args = getopt.getopt( sys.argv[1:], "", allowedOpt )
    except getopt.GetoptError, err:
        print err
        HelpOptions()
        sys.exit(2)

    dictArgs = HelpOptions(opts)
    try:
        cmscp_ = cmscp(dictArgs)
        cmscp_.run()
    except Exception, ex :
        print str(ex)

