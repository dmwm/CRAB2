#
# collection of utilities for publishing in DBS3
#
# with minor modifications from
# https://github.com/bbockelm/AsyncStageout/blob/dbs3_playground/src/python/AsyncStageOut/PublisherWorker.py#L630
# https://github.com/dmwm/AsyncStageout/blob/master/src/python/AsyncStageOut/PublisherWorker.py#L733
#
#

import common
from crab_util import *
from crab_exceptions import *
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

#from dbs.apis.dbsClient import *


def format_file_3(file):

    nf = {'logical_file_name': file['lfn'],
          'file_type': 'EDM',
          'check_sum': unicode(file['cksum']),
          'event_count': file['inevents'],
          'file_size': file['filesize'],
          'adler32': file['adler32'],
          'file_parent_list': [{'file_parent_lfn': i} for i in file['parents']],
          }
    file_lumi_list = []
    for run, lumis in file['runlumi'].items():
        for lumi in lumis:
            file_lumi_list.append({'lumi_section_num': int(lumi), 'run_num': int(run)})
    nf['file_lumi_list'] = file_lumi_list
    if file.get("md5") != "asda" and file.get("md5") != "NOTSET": # asda is the silly value that MD5 defaults to
        nf['md5'] = file['md5']
    return nf

def createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files):

    file_conf_list = []
    file_parent_list = []
    for file in files:
        file_conf = output_config.copy()
        file_conf_list.append(file_conf)
        file_conf['lfn'] = file['logical_file_name']
        for parent_lfn in file.get('file_parent_list', []):
            file_parent_list.append({'logical_file_name': file['logical_file_name'], 'parent_logical_file_name': parent_lfn['file_parent_lfn']})
        del file['file_parent_list']
    blockDump = { \
         'dataset_conf_list': [output_config],
         'file_conf_list': file_conf_list,
         'files': files,
         'processing_era': processing_era_config,
         'primds': primds_config,
         'dataset': dataset_config,
         'acquisition_era': acquisition_era_config,
         'block': block_config,
         'file_parent_list': file_parent_list,
    }
    blockDump['block']['file_count'] = len(files)
    blockDump['block']['block_size'] = sum([int(file[u'file_size']) for file in files])
    return blockDump


def migrateDBS3(migrateApi, destReadApi, sourceApi, inputDataset):
    # Submit migration
    existing_datasets = destReadApi.listDatasets(dataset=inputDataset, detail=True,dataset_access_type='*')
    should_migrate = False
    if not existing_datasets or (existing_datasets[0]['dataset'] != inputDataset):
        should_migrate = True
        common.logger.info("Dataset %s must be migrated; not in the destination DBS." % inputDataset)
    if not should_migrate:
        # The dataset exists in the destination; make sure source and destination
        # have the same blocks.
        existing_blocks = set([i['block_name'] for i in destReadApi.listBlocks(dataset=inputDataset)])
        proxy = os.environ.get("SOCKS5_PROXY")
        source_blocks = set([i['block_name'] for i in sourceApi.listBlocks(dataset=inputDataset)])
        blocks_to_migrate = source_blocks - existing_blocks
        common.logger.info("Dataset %s in destination DBS with %d blocks; %d blocks in source." % (inputDataset, len(existing_blocks), len(source_blocks)))
        if blocks_to_migrate:
            common.logger.info("%d blocks (%s) must be migrated to destination dataset %s." % (len(existing_blocks), ", ".join(existing_blocks), inputDataset) )
            should_migrate = True
        else:
            common.logger.info("No migration needed")
    if should_migrate:
        sourceURL = sourceApi.url
        
        data = {'migration_url': sourceURL, 'migration_input': inputDataset}
        common.logger.debug("About to submit migrate request for %s" % str(data))
        try:
            result = migrateApi.submitMigration(data)
        except HTTPError, he:
            if he.msg.find("Requested dataset %s is already in destination" % inputDataset) >= 0:
                common.logger.info("Migration API believes this dataset has already been migrated.")
                return destReadApi.listDatasets(dataset=inputDataset, detail=True)
            common.logger.exception("Request to migrate %s failed." % inputDataset)
            return []
        common.logger.debug("Result of migration request %s" % str(result))
        id = result.get("migration_details", {}).get("migration_request_id")
        if id == None:
            common.logger.error("Migration request failed to submit.")
            common.logger.error("Migration request results: %s" % str(result))
            return []
        common.logger.debug("Migration ID: %s" % id)
        time.sleep(1)
        # Wait forever, then return to the main loop. Note we don't
        # fail or cancel anything. Just retry later if users Ctl-C crab
        # States:
        # 0=PENDING
        # 1=IN PROGRESS
        # 2=COMPLETED
        # 3=FAILED
        state=0
        wait=1
        while state==0 or state==1:
            common.logger.debug("About to get migration request for %s." % id)
            status = migrateApi.statusMigration(migration_rqst_id=id)
            state = status[0].get("migration_status")
            common.logger.debug("Migration status: %s" % state)
            if state == 0 or state == 1:
                time.sleep(wait)
                wait=max(wait*2,30)  # give it more time, but check every 30 sec at least

        if state == 0 or state == 1:
            common.logger.info("Migration of %s has taken too long - will delay publication." % inputDataset)
            return []
        if state == 3:
            common.logger.info("Migration of %s has failed. Full status: %s" % (inputDataset, str(status)))
            return []
        common.logger.info("Migration of %s is complete." % inputDataset)
        existing_datasets = destReadApi.listDatasets(dataset=inputDataset, detail=True,dataset_access_type='*')

    return existing_datasets


def publishInDBS3(sourceApi, inputDataset, toPublish, destApi, destReadApi, migrateApi, originSite):
    """
    Publish files into DBS3
    """

    publish_next_iteration = []
    failed = []
    published = []
    results = {}
    targetSE = originSite
    max_files_per_block = 500
    blockSize = max_files_per_block

    # Submit migration if needed
    if inputDataset.upper() != 'NONE':
        common.logger.info("Request migration of input dataset and its parents")
        existing_datasets = migrateDBS3(migrateApi, destReadApi, sourceApi, inputDataset)
        if not existing_datasets:
            common.logger.info("Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, sourceApi.url, migrateApi.url))
            return [], [], []

        # Get basic data about the parent dataset
        if not (existing_datasets and existing_datasets[0]['dataset'] == inputDataset):
            common.logger.error("Inconsistent state: %s migrated, but listDatasets didn't return any information")
            return [], [], []
        primary_ds_type = existing_datasets[0]['primary_ds_type']
        acquisition_era_name = existing_datasets[0]['acquisition_era_name']
    else :
        common.logger.info("user generate data. No migration needed")
        acquisition_era_name = "CRAB"
        primary_ds_type = 'mc'

    global_tag = 'crab2_tag'

    processing_era_config = {'processing_version': 1, 'description': 'crab2'}

    for datasetPath, files in toPublish.iteritems():
        results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0,}
        dbsDatasetPath = datasetPath

        if not files:
            continue

        appName = 'cmsRun'
        appVer = files[0]["swversion"]
        appFam = 'output'
        seName = targetSE
        # TODO: this is invalid:
        pset_hash = files[0]['publishname'].split("-")[-1]
        gtag = str(files[0]['globaltag'])
        if gtag == "None":
            gtag = global_tag
        acquisitionera = str(files[0]['acquisitionera'])
        if acquisitionera == "null":
            acquisitionera = acquisition_era_name
            
        empty, primName, procName, tier = dbsDatasetPath.split('/')
        #SB REDO consistently with Crab2
        
        # Change bbockelm-name-pset to bbockelm_name-pset
        procName = "_".join(procName.split("-")[:2]) + "-" + "-".join(procName.split("-")[2:])
        # NOTE: DBS3 currently chokes if we don't include a processing version / acquisition era
        procName = "%s-%s-v%d" % (acquisition_era_name, procName, processing_era_config['processing_version'])
        dbsDatasetPath = "/".join([empty, primName, procName, tier])
        #END SB REDO

        primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
        common.logger.debug("About to insert primary dataset: %s" % str(primds_config))
        destApi.insertPrimaryDataset(primds_config)
        common.logger.debug("Successfully inserting primary dataset %s" % primName)

        # Find any files already in the dataset so we can skip them
        try:
            existingDBSFiles = destApi.listFiles(dataset=dbsDatasetPath)
            existingFiles = [x['logical_file_name'] for x in existingDBSFiles]
            results[datasetPath]['existingFiles'] = len(existingFiles)
        except DbsException, ex:
            existingDBSFiles = []
            existingFiles = []
            msg = "Error when listing files in DBS"
            msg += str(ex)
            msg += str(traceback.format_exc())
            common.logger.error(msg)
        workToDo = False

        # Is there anything to do?
        for file in files:
            if not file['lfn'] in existingFiles:
                workToDo = True
                break
        if not workToDo:
            common.logger.info("Nothing uploaded, %s has these files already or not enough files" % datasetPath)
            for file in files:
                published.append(file['lfn'])
            continue

        acquisition_era_config = {'acquisition_era_name': acquisitionera, 'start_date': 0}

        output_config = {'release_version': appVer,
                         'pset_hash': pset_hash,
                         'app_name': appName,
                         'output_module_label': 'o', #TODO
                         'global_tag': global_tag,
                         }
        common.logger.debug("Published output config.")

        dataset_config = {'dataset': dbsDatasetPath,
                          'processed_ds_name': procName,
                          'data_tier_name': tier,
                          'acquisition_era_name': acquisitionera,
                          'dataset_access_type': 'PRODUCTION', # TODO
                          'physics_group_name': 'CRAB3',
                          'last_modification_date': int(time.time()),
                          }
        common.logger.debug("About to insert dataset: %s" % str(dataset_config))
        #destApi.insertDataset(dataset_config)
        del dataset_config['acquisition_era_name']
        #dataset_config['acquisition_era'] = acquisition_era_config

        dbsFiles = []
        for file in files:
            if not file['lfn'] in existingFiles:
                dbsFiles.append(format_file_3(file, output_config, dbsDatasetPath))
            published.append(file['lfn'])
        count = 0
        blockCount = 0
        if len(dbsFiles) < max_files_per_block:
            #common.logger.debug("WF is not expired %s and the list is %s" %(workflow, self.not_expired_wf))
            #if not self.not_expired_wf:
            if True:
                block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
                files_to_publish = dbsFiles[count:count+blockSize]
                try:
                    block_config = {'block_name': block_name, 'origin_site_name': seName, 'open_for_writing': 0}
                    common.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                    blockDump = createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                    destApi.insertBulkBlock(blockDump)
                    count += blockSize
                    blockCount += 1
                except Exception, ex:
                    failed += [i['logical_file_name'] for i in files_to_publish]
                    msg = "Error when publishing"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    common.logger.error(msg)
        else:
            while count < len(dbsFiles):
                block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
                files_to_publish = dbsFiles[count:count+blockSize]
                try:
                    if len(dbsFiles[count:len(dbsFiles)]) < max_files_per_block:
                        for file in dbsFiles[count:len(dbsFiles)]:
                            publish_next_iteration.append(file['logical_file_name'])
                        count += blockSize
                        continue
                    block_config = {'block_name': block_name, 'origin_site_name': seName, 'open_for_writing': 0}
                    #common.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                    blockDump = createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                    common.logger.debug("Block to insert: %s\n" % pprint.pformat(blockDump))
                    destApi.insertBulkBlock(blockDump)
                        
                    count += blockSize
                    blockCount += 1
                except Exception, ex:
                    failed += [i['logical_file_name'] for i in files_to_publish]
                    msg = "Error when publishing (%s) " % ", ".join(failed)
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    common.logger.error(msg)
                    count += blockSize
        results[datasetPath]['files'] = len(dbsFiles) - len(failed)
        results[datasetPath]['blocks'] = blockCount
    published = filter(lambda x: x not in failed + publish_next_iteration, published)
    common.logger.info("End of publication status: failed %s, published %s, publish_next_iteration %s, results %s" \
                       % (failed, published, publish_next_iteration, results))
    return failed, published, results



    
