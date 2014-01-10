from xml.dom import minidom
import sys
import getopt
import uuid
import time
from dbs.apis.dbsClient import *
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

### local DBS3 where to publish the user dataset
#url_local_writer='https://cmsweb-testbed.cern.ch/dbs/prod/phys03/DBSWriter'
url_local_writer='https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
dbs3api_local = DbsApi(url=url_local_writer)
#### it was 
#url='https://cmsweb-testbed.cern.ch/dbs/prod/phys03/DBSWriter'
#api = DbsApi(url=url, proxy=proxy)
#########################################

### global DBS3 from where read info about parents
url_global_reader='https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
dbs3api_global= DbsApi(url=url_global_reader)
#dbs3api_global= DbsApi(url=url_local_writer)
### it was
#url_reader='https://cmsweb-testbed.cern.ch/dbs/prod/global/DBSReader'
#api_reader=DbsApi(url=url_reader, proxy=proxy)
###########################################

### migration from global to local
#url_migrate='https://cmsweb-testbed.cern.ch/dbs/prod/phys03/DBSMigrate'
url_migrate='https://cmsweb.cern.ch/dbs/prod/phys03/DBSMigrate'
dbs3api_migrate=DbsApi(url=url_migrate)
##########################################

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
  
  list_A=[]
  summary_already_published_file=fjr_dir + '/fjr_already_published.txt'
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

  new_fjrs=[]
  for fjr in list_fjr_in_dir:
      if fjr not in list_A:
          new_fjrs.append(fjr)
  #print "new_fjrs = ", new_fjrs        
  return new_fjrs
  # exit()    

def get_arg():
  ######
  ### parse command line options
  ### -c, --continue name of crab task ' looks inside the res dir to discover fjr files
  ### -f, --fjr specify the complete path of a fjr to publish
  ### -h, --help help of the script
  #############
  fjr_dir = ''
  arg_fjrs=[]
  try:
      opts, args = getopt.getopt(sys.argv[1:], "hc:f:", ["help", "continue=", "fjr="])
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
          if str.find(a,"crab_")!= -1:
              fjr_dir = a + '/res/'
          else: fjr_dir = a    
          print "fjr_dir = ", fjr_dir
          arg_fjrs=read_res_content(fjr_dir)
          #print "in get_arg arg_fjrs = ", arg_fjrs
      if o in ("-f", "--fjr"):
          print "file = a = ", a
          arg_fjrs.append(a)
          print "arg_fjrs = ", arg_fjrs
      if o in ("-h", "--help"):
          print "usage: python parser_dbs3.py "
          print "option to use:"
          print "  -c, --continue name of crab task ' looks inside the res dir to discover fjr files"
          print "  -f, --fjr specify the complete path of a fjr to publish"
          print "  -h, --help help of the script"

  return arg_fjrs, fjr_dir        
  
def summary_block_publication(list, pubbl_exit, summary_file):
    print "summary_file = ", summary_file
    if pubbl_exit == 'True':
        if os.path.exists(summary_file):                  
            published_txt=open(summary_file, "a")
        else:
            published_txt=open(summary_file, "w")
        for file in fjr_list:
            published_txt.write(file + "\n")        
        published_txt.close()    
    else:
        print 'block publication failed'
  
def check_fjr(path, fjr, doc_list, fjr_list):
  #print "in check_fjr"
  #print  "fjr_list = fjr_list"

  if path != '':
      doc = minidom.parse(path + '/' + fjr)
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
            print "ok exit_codes ok"
            doc_list.append(doc)
            fjr_list.append(fjr)
            print "doc_list = ", doc_list
            print "fjr_list = ", fjr_list
     else:
         print "exit_codes not zero --> skip fjr ", fjr

  return doc_list, fjr_list

def create_blockDump_commonpart(doc, blockDump):

  print "in create_blockDump_commonpart"
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
  #return blockDump 

  ### creating processing_era_dictionary (this has to be created one time) 
  processing_era_dictionary={'create_by':'crab2', 'processing_version':'1', 'description':'crab2'}
  #print processing_era_dictionary

  blockDump['processing_era']=processing_era_dictionary
  #print "blockDump = ", blockDump
  #return blockDump 

  ### creating primds_dictionary (this has to be created one time)
  primds_dictionary={}
  primds_dictionary['primary_ds_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["primary_ds_name"]]
  
  #print "##############################################################"
  #print "in primds_dictionary, primds_ds_name = ", primds_dictionary['primary_ds_name']
  type = dbs3api_global.listPrimaryDatasets(primary_ds_name=primds_dictionary['primary_ds_name'])

  #print "type = ", type

  if not type:
      print "no info about the primds in the global dbs, using default parameters..."
      primds_dictionary['create_by']=''
      primds_dictionary['primary_ds_type']='mc'
      primds_dictionary['creation_date']=''
  else:    
      #print "type[0]['create_by']= ", type[0]['create_by']
      #print "type[0]['creation_date']= ", type[0]['creation_date']
      #print "type[0]['primary_ds_type']= ", type[0]['primary_ds_type']
      primds_dictionary['create_by']=type[0]['create_by']
      primds_dictionary['primary_ds_type']=type[0]['primary_ds_type']
      primds_dictionary['creation_date']=type[0]['creation_date']
      #print "primds_dictionary = ", primds_dictionary
      #print "##############################################################"

  print primds_dictionary
  #exit()

  blockDump['primds']=primds_dictionary
  #print "blockDump['primds'] = ", blockDump['primds']
  #return blockDump 

  ### creating dataset_dictionary (this has to be created one time) 
  dataset_dictionary={'physics_group_name':'', 'create_by':'', 'dataset_access_type':'VALID', 'last_modified_by':'', 'creation_date':'', 'xtcrosssection':'', 'last_modification_date':''}
  dataset_dictionary['data_tier_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["data_tier_name"]]

  dataset_dictionary['processed_ds_name']=FileDatasetsDatasetInfo[translation_DatasetInfo["processed_ds_name"]] + '-v' + processing_era_dictionary['processing_version']

  dataset_dictionary['dataset']='/'+FileDatasetsDatasetInfo[translation_DatasetInfo["primary_ds_name"]]+'/'+dataset_dictionary['processed_ds_name']+'/'+dataset_dictionary['data_tier_name']
  #print dataset_dictionary

  blockDump['dataset']=dataset_dictionary
  #print "blockDump['dataset'] = ", blockDump['dataset']
  #return blockDump 


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
    #print ""
 
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

    ### JUST FOR TEST ###
    ###query in the global https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/files?dataset=/jdetd/enszw-koimc-v4/RECO
    ### to have the lfn
    list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/uvdqm.root')
    list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/szarq.root')
    list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/cefla.root')
    list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/tfaaw.root')
    list_parent_lfn.append('/store/data/enszw/jdetd/RECO/4/000000000/sebuv.root')

    print "--> list_parent_lfn = ", list_parent_lfn
    ######################
    ### to use the real code, uncomment the following lines
    #for entry in list:
    #    parent_lfn = entry['parent_logical_file_name']
    #    print "parent_lfn = ", parent_lfn
    #    list_parent_lfn.append(parent_lfn)
    #######################

    ### from parent_lfn we obtain the list fo related blocks ###
    for parent_lfn in list_parent_lfn:
        print "parent_lfn = ", parent_lfn
    
        ### reading from global dbs ###
        parent_blocks=dbs3api_global.listBlocks(logical_file_name=parent_lfn)
        print "parent_blocks = ", parent_blocks

        if len(parent_blocks)!=0:
            for entry in parent_blocks:
                parent_block_name = entry['block_name']
                ### list with unique block name ########
                if parent_block_name not in list_parent_block_name:
                    list_parent_block_name.append(parent_block_name)
    
    print "list_parent_block_name = ", list_parent_block_name
    
    ### check if blocks are already in the localDBS ###
    if len(list_parent_block_name)!=0:
        for entry in list_parent_block_name:
            print "--> parent_block_name = ", entry
            ### reading in the local dbs if the parent block already exists ###
            #dbs3api_local.listBlocks(block_name=entry)
            result_listBlock=bool(dbs3api_local.listBlocks(block_name=entry))
            #result = dbs3api_local.listBlocks(block_name='/jdetd/enszw-koimc-v4/RECO#35kg84l9-xbnm-ht5a-dj3q-nr6907ssq7i2')
            #result = dbs3api_local.listBlocks(block_name=entry)
            #print "result = ", result 
            print "Is the parent_block in dbs local? ",result_listBlock
            if not result_listBlock:
                print "the block has to be migrated"
                print "Trying the migration"
                #result_migration=dbs3api_migrate.submitMigration({'migration_url':url_global_reader, 'migration_input':entry})
                #print "result_migration=",result_migration
                #result_migration_request=bool(dbs3api_migrate.submitMigration({'migration_url':url_global_reader, 'migration_input':entry}))
                result_migration_request=dbs3api_migrate.submitMigration({'migration_url':url_global_reader, 'migration_input':entry})
                print "result_migration_request = ", result_migration_request
                #if not result_migration_request:
                #    print "the request of block has not been correctly migrated"
                #else:
                time.sleep(10)
                result_listBlock=bool(dbs3api_local.listBlocks(block_name=entry))
                print "Is now the parent_block in dbs local? ",result_listBlock
                if not result_listBlock:
                    print "problems migrating the block"
                else:
                    print "migration ok, parent block are now in the localDBS "
            else:
                print "parent block already in the localDBS"
    else:
        print "no parent blocks in global dbs associated to the parent_lfn"
        print "check the status of parents dataset is global dbs url ", url_global_reader
        exit()

##### main #####
if __name__ == "__main__":
   # print "sono nel main ..."
    arg_list, fjr_dir= get_arg()
    print "------"
    print arg_list
    print fjr_dir
    print "------"
    if len(arg_list)==0:
      #print "len(arg_list) = ", len(arg_list)
      #print "exit"
      exit()
    #exit()

    # fjr is the name of fjr to publish
    for fjr in arg_list:
        print "---> fjr = ", fjr 
        doc_list, fjr_list = check_fjr(fjr_dir, fjr, doc_list, fjr_list)

    ### list of files ok for publication
    print "doc_list = ", doc_list
    print "fjr_list = ", fjr_list

    number_of_files=len(doc_list)
    print "number of file to publish file_count = ", number_of_files
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
    print "blockDump = ", blockDump
    print "--------------------------------------------------"
    print "blockDump['file_parent_list'] = ", blockDump['file_parent_list']
    #print "blockDump['file_conf_list'] = ", blockDump['file_conf_list']
    #print "blockDump['dataset_conf_list']= ", blockDump['dataset_conf_list']
    print "##################################################"
    #print "##### inserting data in dbs #####################"

    ###############################################################################
    ######## to implement the migration of parent dataset #########################
    #### migration of parent files before inserting of block?
    #### we have the info about parent files in the blockDump['file_parent_list']
    check_and_migrate_block_parents(blockDump['file_parent_list'])
    
    ###############################################################################
    ###############################################################################

    ########################################
    ### to add the try-except ###
    ###try:
    ### uncomment the following line to publish the block
    #print "publication step"
    #dbs3api_local.insertBulkBlock(blockDump)
    ###pub_exit = 'True' 
    ###except:
    ###pub_exit = 'False' 
    ########################################

    ### just for test: 
    #pub_exit='True' 
    ###
    
    #
    #summary_block_publication(fjr_list, pub_exit, fjr_dir + '/fjr_already_published.txt')
