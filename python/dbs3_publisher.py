from xml.dom import minidom
import sys
import getopt
import uuid
import time
from dbs.apis.dbsClient import *
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

"""
#https://cmsweb-testbed.cern.ch/dbs/int/phys03/
#https://cmsweb-testbed.cern.ch/dbs/prod/global/
#https://cmsweb-testbed.cern.ch/dbs/int/global/
#https://cmsweb-testbed.cern.ch/dbs/dev/global/

### THE LAST
#url_local='https://cmsweb-testbed.cern.ch/dbs/int/global/'
url_local='https://cmsweb-testbed.cern.ch/dbs/int/phys03/'
########################

### local DBS3 where to publish the user dataset
url_local_writer=url_local + 'DBSWriter'
dbs3api_local = DbsApi(url=url_local_writer)

### global DBS3 from where read info about parents
url_global_reader='https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
dbs3api_global= DbsApi(url=url_global_reader)
### JUST FOR THE TEST ####
#url_global_reader='https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
###########################################

### migration from global to local
url_migrate=url_local + 'DBSMigrate'
dbs3api_migrate=DbsApi(url=url_migrate)
##########################################

print "##################################################"
print "### DBS istances:"
print "url_local_writer = ", url_local_writer
print "url_global_reader = ", url_global_reader
print "url_migrate = ", url_migrate
print "##################################################"
"""

### general dictionary with info about all the fjrs to publish ###
blockDump={}
### description ################
## from https://svnweb.cern.ch/trac/CMSDMWM/browser/DBS/trunk/Client/tests/dbsclient_t/unittests/blockdump.dict
blockDump['dataset_conf_list']=[]  # total (list)
blockDump['file_conf_list']=[]     # one foreach fjr (list)
blockDump['files']=[]              # one foreach fjr (list)
blockDump['processing_era']={}     # total  (dictionary)
blockDump['primds']={}             # total  (dictionary)
blockDump['dataset']={}            # total  (dictionary)
blockDump['acquisition_era']={}    # total  (dictionary)
blockDump['block']={}              # summary info about files (dictionary)
blockDump['file_parent_list']=[]   # summary list of parents lfn (list)
###############################################################

### general flow of script to publish fjrs in dbs3 ###
### input = arguments a list of fjr to parse ###
# check of fjr "sanity"
# parsing of "validated" fjr
# creation of list containing only valid fjrs
# create_dataset_common_info: create the part of blockDump related to the dataset.
#       These values are common for each frj in the task (so it is done reading only for the first valid fjr)
# create_file_info: parsing of each valid fjr to get info about the related output file. 
# create_block_info: info about the block, as total size, number of files. 
#       These values are obtained adding the info about each files in the block. 
# migration of parent (to implement)
# publication of block
# report of publication status

doc_list=[]
fjr_list=[]

### traslation's dictionaries for DBS3:  key is DBS3 name: value is the fjr tag 
### tag in <File>  
translation_File={"lfn":"LFN", "logical_file_name":"LFN", "check_sum":"Checksum", "event_count":"TotalEvents", "file_type":"FileType", "origin_site_name":"SEName", "file_size":"Size"}
#print "translation_File = ", translation_File

### tag in <File><Datasets><DatasetInfo> 
translation_DatasetInfo={"release_version":"ApplicationVersion", "pset_hash":"PSetHash", "app_name":"ApplicationName", "primary_ds_name":"PrimaryDataset", "data_tier_name":"DataTier", "processed_ds_name":"ProcessedDataset"}
#print "translation_DatasetInfo = ", translation_DatasetInfo

### tag in <File><Inputs><Input>
#translation_FileInputsInput={"parent_logical_file_name":"LFN"}
#print "translation_FileInputsInput = ", translation_FileInputsInput

### tag in <File><Runs> 
#translation_FileRuns={"lumi_section_num":"LumiSection","run_num":"Run"}
#print translation_FileRuns
###########

summary_already_published_file_name = 'fjr_already_published.txt'
summary_failed_file_name = 'fjr_failed_publication.txt'


def read_res_content(path):

  fjr_dir=path
  list_fjr_in_dir = []
  # find fjr files in the crab res dir 
  list_files = os.listdir(fjr_dir)
  for file in list_files:
      #print "file = ", file 
      if (str.find(file,".xml") != -1):
          list_fjr_in_dir.append(file)
  #print "list_fjr_in_dir = ", list_fjr_in_dir
  
  #####################
  # JUST A TEST
  ### creating the summary file in the crab res dir about already published fjrs
  #list_published_fjr = ["crab_fjr_1.xml", "crab_fjr_2.xml","crab_fjr_3.xml","crab_fjr_4.xml","crab_fjr_5.xml","crab_fjr_6.xml"]
  #already_published_txt=open(fjr_dir + "/fjr_already_published.txt", "w")
  #for file in list_published_fjr:
  #    already_published_txt.write(file + "\n")
  #already_published_txt.close()    
  ##################### 
  
  ### check if there is in the dir the summary file about previus publication steps ###
  list_A=[]
  summary_already_published_file=fjr_dir + summary_already_published_file_name
  if os.path.exists(summary_already_published_file):
      read_already_published_txt=open(summary_already_published_file, "r")
      list_A=read_already_published_txt.readlines()
      #print "list_A = ", list_A
      for ind, entry in enumerate(list_A):
          #print "ind = ", ind
          #print "entry = ", entry 
          entry = entry.rstrip("\n")
          list_A[ind]=entry
          #print "entry = ", entry 
      #print "list_A = ", list_A

  ### create the list with the fjr files to publish
  new_fjrs=[]
  for fjr in list_fjr_in_dir:
      if fjr not in list_A:
          new_fjrs.append(fjr)

  ### return the list with fjr files to publish
  return new_fjrs

def get_arg():
  ######
  ### parse command line options
  ### -c, --continue name of crab task ' looks inside the res dir to discover fjr files
  ### -f, --fjr specify the complete path of a fjr to publish
  ### -h, --help help of the script
  #############
  arg_fjrs=[]

  arg_value_dict={'fjr_dir':'','srcUrl':'', 'dstUrl':''}

  print "##################################################"
  print "### arguments:"
  try:
      opts, args = getopt.getopt(sys.argv[1:], "hc:f:s:d:", ["help", "continue=", "fjr=", "srcUrl=", "dstUrl"])
      print "opts = " , opts
      print "args = ", args
  except getopt.GetoptError, msg:
      print msg
      print "for help use --help"
      sys.exit(2)
  if len(opts) == 0:
     print "use --help for script usage"
     exit()
# process options
  for o, a in opts:
      #print "o = ", o
      #print "a = ", a
      if o in ("-c", "--continue"):
          ##### script tp use only with crab task
          if a[-1] != '/':
              a = a + '/'
          if os.path.isdir(a):
              if os.path.isdir(a + 'res/'):
                  fjr_dir = a + 'res/'
              else: fjr_dir = a
          else:
              print "fjr_dir does not exist"
              exit()
          print "fjr_dir = ", fjr_dir    
          arg_value_dict['fjr_dir']=fjr_dir
          arg_fjrs=read_res_content(fjr_dir)
          #print "in get_arg arg_fjrs = ", arg_fjrs
      if o in ("-f", "--fjr"):
          #print "complete file = a = ", a
          fjr_dir = os.path.dirname(a)
          filename = os.path.basename(a)
          #print "fjr_dir = ", fjr_dir 
          #print "filename = ", filename
          arg_value_dict['fjr_dir']=fjr_dir
          arg_fjrs.append(filename)
          #print "arg_fjrs = ", arg_fjrs

      if o in ("-s", "--srcUrl"):
          #print "url of input DBS = ", a        
          arg_value_dict['srcUrl']= a

      if o in ("-d", "--dstUrl"):
          #print "url of target DBS = ", a        
          arg_value_dict['dstUrl'] = a

      if o in ("-h", "--help"):
          print "usage: python parser_dbs3.py "
          print "option to use:"
          print "  -c, --continue name of crab task ' looks inside the res dir to discover fjr files"
          print "  -f, --fjr specify the complete path of a fjr to publish"
          print "  -h, --help help of the script"
          #print __doc__

  print "##################################################"
  return arg_fjrs, arg_value_dict        
  
def summary_block_publication(list, path, summary_file_name):
    #print "list = ", list
    #print "path = ", path
    #print "FINE"
    #exit()
    if path != '' and path[-1] != '/':
        path = path + '/'

    print "path = ", path
    if os.path.exists(path + summary_file_name):                  
        published_txt=open(path + summary_file_name, "a")
    else:
        published_txt=open(path + summary_file_name, "w")
    for file in list:
        published_txt.write(file + "\n")        
    published_txt.close()    
    print "##################################################"
    print "### publication summary file: ", path + summary_file_name
    print "##################################################"
  
def check_fjr(path, fjr, doc_list, fjr_list):
  #print "in check_fjr"
  #print  "fjr_list = fjr_list"

  if path != '':
      if path[-1]!='/':
          path = path + '/'
      #doc = minidom.parse(path + '/' + fjr)
      doc = minidom.parse(path + fjr)
  else:
      doc = minidom.parse(fjr)

  exe_exit_status=''
  wrapper_exit_status=''
  if doc.getElementsByTagName("FrameworkError"):
     for entry in doc.getElementsByTagName("FrameworkError"):
        #print "nel for"
        #print "entry = ", entry
        #print entry.toxml()
        if entry.getAttribute("Type"):
            type = entry.getAttribute("Type") 
            #print "type = ", type
            if type == "WrapperExitCode":
                wrapper_exit_status = str(entry.getAttribute("ExitStatus")).strip()
                #print "wrapper_exit_status = ", wrapper_exit_status
            elif type == "ExeExitCode":    
                exe_exit_status = str(entry.getAttribute("ExitStatus")).strip()
                #print "exe_exit_status = ", exe_exit_status
            else: 
                print "other exit_type in fjr", fjr    
        else:        
            print "no tag FrameworkError found --> skip fjr ", fjr
      
     if (wrapper_exit_status == "0" and exe_exit_status == "0"):
            #print "ok exit_codes ok"
            doc_list.append(doc)
            fjr_list.append(fjr)
            #print "doc_list = ", doc_list
            #print "fjr_list = ", fjr_list
     #else:
     #    print "exit_codes not zero --> skip fjr ", fjr

  return doc_list, fjr_list

def create_blockDump_commonpart(doc, blockDump):

  #print "in create_blockDump_commonpart"
  #print "doc = ", doc
  ### selected only the <File> part of fjr
  File = doc.getElementsByTagName("File")
  FileTag = File[0]
  #print "#######################"
  #print FileTag
  #print FileTag.toxml()
  #print "#######################"
  #print FileTag.childNodes

  ### taking info from tags <File><Datasets><DatasetInfo>
  entries={}

  for entry in FileTag.getElementsByTagName("Entry"):
      entries[entry.attributes["Name"].value] = entry 
  #print "entries = ", entries    

  ### vector containing the entries of tag  <File><Datasets><DatasetInfo>
  FileDatasetsDatasetInfo={}

  for key in entries.keys():
      #print "key = ", key
      #print "value = ",str(entries[key].firstChild.data).strip()
      FileDatasetsDatasetInfo[key]=str(entries[key].firstChild.data).strip()
  #print "FileDatasetsDatasetInfo = ",FileDatasetsDatasetInfo    

  ### creating dataset_conf_list_dictionary (this has to be created one time) 
  dataset_conf_list_dictionary={}
  dataset_conf_list_dictionary["release_version"]=FileDatasetsDatasetInfo[translation_DatasetInfo["release_version"]]
  dataset_conf_list_dictionary["pset_hash"]=FileDatasetsDatasetInfo[translation_DatasetInfo["pset_hash"]]
  dataset_conf_list_dictionary["app_name"]=FileDatasetsDatasetInfo[translation_DatasetInfo["app_name"]]
  dataset_conf_list_dictionary["output_module_label"]="crab2_mod_label"
  dataset_conf_list_dictionary["global_tag"]="crab2_tag"
  #print "dataset_conf_list_dictionary = " , dataset_conf_list_dictionary

  blockDump['dataset_conf_list'].append(dataset_conf_list_dictionary)
  #print "blockDump = ", blockDump

  ### creating processing_era_dictionary (this has to be created one time) 
  processing_era_dictionary={'create_by':'crab2', 'processing_version':'1', 'description':'crab2'}
  #print processing_era_dictionary

  blockDump['processing_era']=processing_era_dictionary
  #print "blockDump = ", blockDump

  ### creating primds_dictionary (this has to be created one time)
  primds_dictionary={}
  primds_dictionary['primary_ds_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["primary_ds_name"]]
  #print "in primds_dictionary, primds_ds_name = ", primds_dictionary['primary_ds_name']

  type = dbs3api_global.listPrimaryDatasets(primary_ds_name=primds_dictionary['primary_ds_name'])
  #print "type = ", type

  if not type:
      print "no info about the primds in the global dbs, using default parameters..."
      primds_dictionary['create_by']=''
      primds_dictionary['primary_ds_type']='mc'
      primds_dictionary['creation_date']=''
  else:    
      primds_dictionary['create_by']=type[0]['create_by']
      primds_dictionary['primary_ds_type']=type[0]['primary_ds_type']
      primds_dictionary['creation_date']=type[0]['creation_date']

  #print primds_dictionary

  blockDump['primds']=primds_dictionary

  ### creating dataset_dictionary (this has to be created one time) 
  dataset_dictionary={'physics_group_name':'', 'create_by':'', 'dataset_access_type':'VALID', 'last_modified_by':'', 'creation_date':'', 'xtcrosssection':'', 'last_modification_date':''}
  dataset_dictionary['data_tier_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["data_tier_name"]]
  ##### removing the -v1
  #dataset_dictionary['processed_ds_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["processed_ds_name"]] + '-v' + processing_era_dictionary['processing_version']
  dataset_dictionary['processed_ds_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["processed_ds_name"]]

  dataset_dictionary['dataset']='/'+FileDatasetsDatasetInfo[translation_DatasetInfo["primary_ds_name"]]+'/'+dataset_dictionary['processed_ds_name']+'/'+dataset_dictionary['data_tier_name']
  #print dataset_dictionary

  blockDump['dataset']=dataset_dictionary
  #print "blockDump['dataset'] = ", blockDump['dataset']


  ### creating acquisition_era_dictionary (this has to be created one time) 
  acquisition_era_dictionary={'acquisition_era_name':'CRAB', 'start_date':0}
  #print acquisition_era_dictionary

  blockDump['acquisition_era']=acquisition_era_dictionary
  #print "blockDump['acquisition_era'] = ", blockDump['acquisition_era']
  return blockDump 

def create_file_parent_list(doc, list):
### this has to be made foreach file to be published
### taking info from tags <File><Inputs>

  File = doc.getElementsByTagName("File")
  FileTag = File[0]
  FileLFN = File[0].getElementsByTagName("LFN")
  FileLFN_value = str(FileLFN[0].childNodes[0].data).strip()
  # print "FileLFN_value = ", FileLFN_value



  Inputs = FileTag.getElementsByTagName("Inputs")
  InputsTag=Inputs[0] 
  Lfn_tags = InputsTag.getElementsByTagName("LFN")
  for Lfn_tag in Lfn_tags:
      for child in Lfn_tag.childNodes:
          input_lfn = str(child.data).strip()
          #print "input_lfn = ", input_lfn
          list.append({'logical_file_name':FileLFN_value,'parent_logical_file_name':input_lfn})
  #print file_parent_list        
  return list        


def create_files_dictionary(doc):
### this has to be made foreach file to be published
### taking info from tags <File><Runs>

  File = doc.getElementsByTagName("File")
  FileTag = File[0]

  ### tag in <File> 
  ### creation of dictionary containing tag - value in <File>, warning: no subchild
  FileTag_dictionary={}

  for child in FileTag.childNodes:
      if int(child.nodeType) == 1: 
         if len(child.childNodes) == 1:
             #print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
             FileTag_name = str(child.tagName).strip()
             #print "FileTag_name = ", FileTag_name
             FileTag_value = str(child.childNodes[0].data).strip()
             #print "FileTag_value = ", FileTag_value
             FileTag_dictionary[FileTag_name]=FileTag_value
             #print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
  #print "FileTag_dictionary = ", FileTag_dictionary

  ### creation of dictionary containing run and lumi info of a file <File><Runs><Run>
  lumisRun_dictionary={}

  for run in FileTag.getElementsByTagName("Run"):
      lumiList=[]
      run_number = run.attributes["ID"].value
      #print "run_number = ", run_number
      for lumi in run.getElementsByTagName("LumiSection"):
          lumiNumber = lumi.attributes["ID"].value
          #print "lumiNumber = ", lumiNumber
          lumiList.append(lumiNumber)
      lumisRun_dictionary[run_number]=lumiList   

  #print lumisRun_dictionary  

  ### creating files_dictionary (this has to be created foreach fjr) 
  files_dictionary={}
  ### il valore e' una lista
  files_dictionary["file_lumi_list"]=[]

  for key in lumisRun_dictionary.keys():
      #print lumisRun_dictionary[key]
      for value in lumisRun_dictionary[key]:
          #print "key = ", key
          #print "value = ", value 
          files_dictionary["file_lumi_list"].append({'lumi_section_num':value, 'run_num':key})
  #print files_dictionary

  files_dictionary['check_sum']=FileTag_dictionary[translation_File['check_sum']]
  files_dictionary['event_count']=FileTag_dictionary[translation_File['event_count']]
  files_dictionary['file_type']=FileTag_dictionary[translation_File['file_type']]
  files_dictionary['logical_file_name']=FileTag_dictionary[translation_File['logical_file_name']]
  files_dictionary['file_size']=FileTag_dictionary[translation_File['file_size']]
  files_dictionary['adler32']=''
  files_dictionary['last_modified_by']=''
  files_dictionary['last_modification_date']=''
  files_dictionary['md5']=''
  files_dictionary['auto_cross_section']=''
  
  #print "files_dictionary in the function = ", files_dictionary
  return files_dictionary
  #blockDump['files'].append(files_dictionary)
  #print "blockDump = ", blockDump
  ##############################################################################################

def create_file_conf_list_dictionary(list,lfn):
   
    ### copy of dictionary in a new one ###
    file_conf_list_dictionary = list[0].copy()

    file_conf_list_dictionary['lfn']=lfn
    #print "file_conf_list_dictionary = ", file_conf_list_dictionary
    print ""
 
    return file_conf_list_dictionary

def create_block_dictionary(doc):
    block_dictionary={}
    block_dictionary['create_by']=''
    block_dictionary['creation_date']=''
    block_dictionary['open_for_writing']=0
    block_dictionary['block_name']=''


    File = doc.getElementsByTagName("File")
    FileTag = File[0]
    SEName_tag = FileTag.getElementsByTagName("SEName")[0]
    #print "SEName_tag =", SEName_tag
    SEName_value = str(SEName_tag.childNodes[0].data).strip()
    #print "SEName_value = ", SEName_value
    block_dictionary['origin_site_name']=SEName_value
    return block_dictionary
    
def check_and_migrate_block_parents(list):
    ### the argument list is the blockDump['file_parent_list'], i.e:
    ### blockDump['file_parent_list']=[{'parent_logical_file_name': '/store/user/fiori/D0_To_hh_v1/D0_To_hh_v1/413df7d897fc38d13a2bf8a964566587/D0_To_hh_1721_1_OD8.root', 'logical_file_name': '/store/user/fanzago/D0_To_hh_v1/testFedeDel/f30a6bb13f516198b2814e83414acca1/outfile_3_1_Rth.root'}]
     
    print "--------------------"
    print "in check_and_migrate, list = ", list
    print "--------------------"
    list_parent_lfn = []
    list_parent_block_name=[]

    ############################### JUST FOR TEST ###########################################
    ###query in the global https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/files?dataset=/jdetd/enszw-koimc-v4/RECO
    ### to have the lfn
    #list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/uvdqm.root')
    #list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/szarq.root')
    #list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/cefla.root')
    #list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/tfaaw.root')
    #list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/sebuv.root')
    #print "--> list_parent_lfn = ", list_parent_lfn

    ##dataset = '/RelValJpsiMM/CMSSW_7_0_0_pre11-START70_V4_HLTGRun-v1/GEN-SIM-DIGI-RAW-HLTDEBUG'
    #dataset = '/RelValSingleGammaPt35_UP15/CMSSW_7_0_0_pre12-POSTLS170_V1-v1/GEN-SIM-RECO'
    #print "########### GLOBAL ###################"
    #files_glo = dbs3api_global.listFiles(dataset=dataset)
    #print "files global = ", files_glo
    #print "########### LOCAL ###################"
    #files_loc = dbs3api_local.listFiles(dataset= dataset)
    #print "files loc = ", files_loc

    #for entry in files_glo:
    #    print "entry = ", entry
    #    print entry['logical_file_name']
    #    list_parent_lfn.append(entry['logical_file_name'])

    #print "list_parent_lfn = ", list_parent_lfn
    ##################################################################################
    
    #####################################################################
    ######## to use the real code, uncomment the following lines ########
    for entry in list:
        parent_lfn = entry['parent_logical_file_name']
        print "parent_lfn = ", parent_lfn
        list_parent_lfn.append(parent_lfn)
    #####################################################################

    ### from parent_lfn we obtain the list fo related blocks ###
    for parent_lfn in list_parent_lfn:
        #print "parent_lfn = ", parent_lfn
        ### reading from local dbs ###
        print "looking in the localDBS for parent_blocks associated with the parent_lfn ", parent_lfn
        parent_blocks=dbs3api_local.listBlocks(logical_file_name=parent_lfn)
        if not parent_blocks:
            print "no parent_blocks associated to the parent_lfn in the localDBS"
            print "finding blocks in the global one:"
            ### reading from global dbs ###
            ##################################################################################
            parent_blocks_to_migrate=dbs3api_global.listBlocks(logical_file_name=parent_lfn)
            if not parent_blocks_to_migrate:
                print "no parent_block associated to the parent_lfn in the globalDBS"
                print "may be there are problems with the parents of your dataset:"
                exit() 
            else:
                if len(parent_blocks_to_migrate)!=0:
                    for entry in parent_blocks_to_migrate:
                        parent_block_name = entry['block_name']
                        print "the parent_blocks to migrate is ", parent_block_name
                        ### list with unique block name ########
                        if parent_block_name not in list_parent_block_name:
                            list_parent_block_name.append(parent_block_name)
            ################################################################################# 
        else:
            print "parent_blocks associated with the parent_lfn already in the localDBS", parent_blocks

    print "list_parent_block_name to migrate from globalDBS to local one = ", list_parent_block_name
    
    if len(list_parent_block_name)!=0:
        #### FOR CHECK MIGRATION STATUS ####
        migrated_now = []
        tot_already_migrated_status = 0
        tot_now_migrated_status = 0
        for entry in list_parent_block_name:
            ##### checking if the migrations is already required: #####
            migration_status=dbs3api_migrate.statusMigration(block_name=entry)
            #print "migration_status = ", migration_status
            #print "bool(migration_status) = ", bool(migration_status)
            if (bool(migration_status) == False):
                ##### if not, migration request is submitted #####
                print "requiring migration"
                result_migration_request=dbs3api_migrate.submitMigration({'migration_url':url_global_reader, 'migration_input':entry})
                print "result_migration_request = ", result_migration_request
                migrated_now.append(entry)
            else:
                ##### check the status of previus request 
                if migration_status[0]['migration_status'] == 3:
                    print "status 3 migration failed, please contactdbs managers"
                    tot_already_migrated_status = tot_already_migrated_status + 1 
                    #exit()
                elif migration_status[0]['migration_status'] == 1:
                    print "status 1 migration in process, please wait around 15 minutes before retrying"
                    tot_already_migrated_status = tot_already_migrated_status + 1
                    #exit()
                elif migration_status[0]['migration_status'] == 0:
                    print "status 0 migration required, please wait around 15 minutes before retrying"
                    tot_already_migrated_status = tot_already_migrated_status + 1
                    #exit()
                elif migration_status[0]['migration_status'] == 2:
                    print "status 2 migration ok"

        if len(migrated_now) == 0 and tot_already_migrated_status > 0:
            print "migration from global to local dbs not completed"
            exit()

        if len(migrated_now) != 0:
            time.sleep(10)
            for entry in migrated_now:
                migration_status_now=dbs3api_migrate.statusMigration(block_name=entry)
                print "migration_status now = ", migration_status_now
                #print "bool(migration_status now) = ", bool(migration_status_now)

                if migration_status_now[0]['migration_status'] == 3:
                    print "status 3 migration failed, please contactdbs managers"
                    tot_now_migrated_status = tot_now_migrated_status + 1
                    #exit()
                elif migration_status_now[0]['migration_status'] == 1:
                    print "status 1 migration in process, please wait around 15 minutes before retrying"
                    tot_now_migrated_status = tot_now_migrated_status + 1
                    #exit()
                elif migration_status_now[0]['migration_status'] == 0:
                    print "status 0 migration required, please wait around 15 minutes before retrying"
                    tot_now_migrated_status = tot_now_migrated_status + 1
                    #exit()
                elif migration_status_now[0]['migration_status'] == 2:
                    print "status 2 migration ok"

        if len(migrated_now) == 0 and tot_now_migrated_status > 0:
            print "migration from global to local dbs not completed"
            exit()
             
        #### update primaryDsinfo if necessary:
        print "##############################################################################################"
        #print "FEDE UPDATE AFTER BLOCK MIGRATION"
        #print "blockDump['primds'] = ", blockDump['primds']
        #print  "blockDump['primds']['primary_ds_type'] = ", blockDump['primds']['primary_ds_type']

        if blockDump['primds']['primary_ds_type'] == 'mc':
            ##### JUST FOR TEST ##############################################################
            #type={}
            #type[0]={'primary_ds_type':'NEW_TYPE','create_by':'FEDE','creation_date':'NOW'}
            #print "type = ", type
            ##################################################################################

            type = dbs3api_global.listPrimaryDatasets(primary_ds_name=blockDump['primds']['primary_ds_name'])
            if not type or type[0]['primary_ds_type'] == 'mc':
                print "something was wrong during the migration of block"
                print "info about primary_ds not updated "
            else:
                blockDump['primds']['create_by']=type[0]['create_by']
                blockDump['primds']['primary_ds_type']=type[0]['primary_ds_type']
                blockDump['primds']['creation_date']=type[0]['creation_date']

        print "after migration the blockDump['primds'] = ", blockDump['primds']
        print "##############################################################################################"
        #######################################
    else:
        print "no parent blocks to migrate from global dbs to local one"
        #print "check the status of parents dataset is global dbs url ", url_global_reader
        #exit()

##### main #####
if __name__ == "__main__":
    arg_fjr_list, arg_value_dict= get_arg()
    print "------"
    print arg_fjr_list
    fjr_dir = arg_value_dict['fjr_dir']
    print "fjr_dir = ", fjr_dir
    print arg_value_dict
    print "------"
    if len(arg_fjr_list)==0:
        exit()

    
    print "##################################################"
    print "defining DBS instances to use"
    ### local DBS3 where to publish the user dataset
    if arg_value_dict['dstUrl']=='':
        ### THE LAST
        #url_local='https://cmsweb-testbed.cern.ch/dbs/int/global/'
        url_local = 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/'
    else:
        url_local = arg_value_dict['dstUrl']
        if url_local[-1] != '/':
            url_local = url_local + '/'

    ########################
    url_local_writer = url_local + 'DBSWriter'
    dbs3api_local = DbsApi(url=url_local_writer)

    ### global DBS3 from where read info about parents
    if arg_value_dict['srcUrl']=='':
        url_global = 'https://cmsweb.cern.ch/dbs/prod/global/'
    else:
        url_global = arg_value_dict['srcUrl']
        if url_global[-1] != '/':
            url_global = url_global + '/'

    url_global_reader = url_global + 'DBSReader'
    dbs3api_global = DbsApi(url=url_global_reader)
    ### JUST FOR THE TEST ####
    #url_global_reader='https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
    ###########################################

    ### migration from global to local
    url_migrate=url_local + 'DBSMigrate'
    dbs3api_migrate=DbsApi(url=url_migrate)
    ##########################################

    print "url_local_writer = ", url_local_writer
    print "url_global_reader = ", url_global_reader
    print "url_migrate = ", url_migrate
    print "##################################################"
    

    # fjr is the name of fjr to publish
    for fjr in arg_fjr_list:
        print "---> fjr = ", fjr 
        doc_list, fjr_list = check_fjr(fjr_dir, fjr, doc_list, fjr_list)

    ### list of files ok for publication
    print "##################################################"
    print "### fjr ok, they can be published:"
    #print "doc_list = ", doc_list
    print "fjr_list = ", fjr_list

    number_of_files=len(doc_list)
    print "number of fjrs to publish file_count = ", number_of_files
    print "##################################################"
    if len(doc_list)==0:
      print "exit"
      exit()

    #exit()
    
    #print "##############################################################"
    #print "creates common part of block"
    # creates the common part about dataset of blockDump
    blockDump = create_blockDump_commonpart(doc_list[0], blockDump)

    #print "blockDump = ", blockDump
    #print "##############################################################"

    # creates the "file" part of blockDump
    block_size=0

    file_parent_list=[]
    for doc in doc_list:
        file_parent_list = create_file_parent_list(doc, file_parent_list)
        #print "doc = ", doc
        files_dictionary = create_files_dictionary(doc)
        #print files_dictionary
        blockDump['files'].append(files_dictionary)
        file_conf_list_dictionary = create_file_conf_list_dictionary(blockDump['dataset_conf_list'], files_dictionary['logical_file_name'] )
        blockDump['file_conf_list'].append(file_conf_list_dictionary)

        #print "###### TEST SIZE ########"
        block_size = block_size + int(files_dictionary['file_size'])


    blockDump['file_parent_list'] = file_parent_list
    #print 'total_block_size = ', block_size

    block_dictionary = create_block_dictionary(doc_list[0])
    blockDump['block']=block_dictionary
    blockDump['block']['block_size']=block_size
    blockDump['block']['file_count']=number_of_files
    blockDump['block']['block_name']=blockDump['dataset']['dataset'] + '#' +str(uuid.uuid4())

    print "##################################################"
    print "### creating blockDump: "
    print "##################################################"
    print "blockDump = ", blockDump
    #print "blockDump['file_parent_list'] = ", blockDump['file_parent_list']
    #print "blockDump['file_conf_list'] = ", blockDump['file_conf_list']
    #print "blockDump['dataset_conf_list']= ", blockDump['dataset_conf_list']
    
    print "##################################################"
    print "### checking parents: "
    print "##################################################"
    check_and_migrate_block_parents(blockDump['file_parent_list'])
    print "##################################################"
    
    print "##################################################"
    print "### starting the publication of user dataset: "
    print "fjr_dir = ", fjr_dir
    print "fjr_list = ", fjr_list
    try:
        dbs3api_local.insertBulkBlock(blockDump)
        summary_block_publication(fjr_list, fjr_dir, summary_already_published_file_name) 
    except HTTPError as http_error:
        print "http_error = ", http_error
        summary_block_publication(fjr_list, fjr_dir, summary_failed_file_name) 
    
    print "############################ end ################"
