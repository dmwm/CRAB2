#
# collection of utilities for publishing in DBS3
#
# with minor modifications from
# https://github.com/bbockelm/AsyncStageout/blob/dbs3_playground/src/python/AsyncStageOut/PublisherWorker.py#L630
# https://github.com/dmwm/AsyncStageout/blob/master/src/python/AsyncStageOut/PublisherWorker.py#L733
#
#

import uuid
import traceback
import pprint
import common
from crab_util import *
from crab_exceptions import *
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

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



def migrateByBlockDBS3(migrateApi, destReadApi, sourceApi, inputDataset):
    # Submit one migration request for each block that needs migrating

    # If the dataset exists in the destination; make sure source and destination
    # have the same blocks.
    existing_blocks = set([i['block_name'] for i in destReadApi.listBlocks(dataset=inputDataset)])
    source_blocks = set([i['block_name'] for i in sourceApi.listBlocks(dataset=inputDataset)])
    blocks_to_migrate = source_blocks - existing_blocks
    common.logger.info("Dataset %s in destination DBS with %d blocks; %d blocks in source." % (inputDataset, len(existing_blocks), len(source_blocks)))
    if blocks_to_migrate:
        common.logger.info("%d blocks (%s) must be migrated to destination dataset %s." % (len(blocks_to_migrate), ", ".join(blocks_to_migrate), inputDataset) )
        should_migrate = True
    else:
        common.logger.info("No migration needed")
        should_migrate = False
    if should_migrate:
        sourceURL = sourceApi.url
        migrationIds=[]
        todoMigrations=len(blocks_to_migrate)
        for block in blocks_to_migrate:
            data = {'migration_url': sourceURL, 'migration_input': block}
            common.logger.info("Submit migrate request for %s ..." % block)
            try:
                result = migrateApi.submitMigration(data)
            except HTTPError, he:
                if "is already at destination" in he.msg:
                    common.logger.info("...block is already at destination")
                    todoMigrations += -1
                    continue
                else:
                    common.logger.info("ERROR: Request to migrate %s failed." % block)
                    common.logger.info("ERROR: Request detail: %s" % data)
                    common.logger.info("DBS3 EXCEPTION %s\n" % he.msg)
                    return []
            common.logger.debug("Result of migration request %s" % str(result))
            id = result.get("migration_details", {}).get("migration_request_id")
            if id == None:
                common.logger.info("ERROR: Migration request failed to submit.")
                common.logger.info("ERROR: Migration request results: %s" % str(result))
                return []
            migrationIds.append(id)
        msg="%d block migration requests submitted" % todoMigrations
        common.logger.info(msg)

        # Wait forever, then return to the main loop. Note we don't
        # fail or cancel anything. Just retry later if users Ctl-C's crab
        # States:
        # 0=PENDING
        # 1=IN PROGRESS
        # 2=SUCCESS
        # 3=FAILED

        failMigrations=0
        okMigrations=0
        wait=1
        while len(migrationIds) > 0:
            if wait > 1:
                msg="Migration in progress. Next check in %d sec. " % wait
                msg+="You can Ctl_C at any time and redo crab -publish later"
                common.logger.info(msg)
            time.sleep(wait)
            for id in migrationIds:
                status = migrateApi.statusMigration(migration_rqst_id=id)
                try:
                    state = status[0].get("migration_status")
                except:
                    msg="Can't get status for mitration_id %d. Waiting" % id
                    common.logger.info(msg)
                    continue
                common.logger.debug("Migration status for id %s: %s" % (id,state))
                if state == 2:
                    migrationIds.remove(id)
                    okMigrations += 1
                if state ==3:
                    common.logger.info("Migration %d has failed. Full status: %s" % (id, str(status)))
                    migrationIds.remove(id)
                    failedMigrations += 1
                if state == 0 or state == 1:
                    pass
            wait=min(wait*2,30)  # give it more time, but check every 30 sec at least

        common.logger.info("Migration of %s is complete." % inputDataset)
        msg="blocks to migrate: %d. Success %d. Fail %d." % (todoMigrations, okMigrations, failMigrations)
        common.logger.info(msg)
        if failMigrations > 0:
            msg="some blocks failed to migrate, try again later after problem is resolved"
            common.logger.info(msg)
            return []
        else:
            common.logger.info("Migration was succesful")
        
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
    max_files_per_block = 500
    blockSize = max_files_per_block

    # Submit migration if needed
    if inputDataset.upper() != 'NONE':
        common.logger.info("Request migration of input dataset and its parents")
        existing_datasets = migrateByBlockDBS3(migrateApi, destReadApi, sourceApi, inputDataset)
        if not existing_datasets:
            common.logger.info("Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, sourceApi.url, migrateApi.url))
            return [], [], []

        # Get basic data about the parent dataset
        if not (existing_datasets and existing_datasets[0]['dataset'] == inputDataset):
            common.logger.info("ERROR: Inconsistent state: %s migrated, but listDatasets didn't return any information")
            return [], [], []
        primary_ds_type = existing_datasets[0]['primary_ds_type']
    else :
        common.logger.info("Input datset absent. No migration needed")
        primary_ds_type = 'mc'

    acquisition_era_name = 'CRAB'
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

        pset_hash = files[0]['publishname'].split("-")[-1]
        gtag = str(files[0]['globaltag'])
        if gtag == "None":
            gtag = global_tag
        acquisitionera = str(files[0]['acquisitionera'])
        if acquisitionera == "null":
            acquisitionera = acquisition_era_name
            
        empty, primName, procName, tier = dbsDatasetPath.split('/')

        primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
        common.logger.debug("About to insert primary dataset: %s" % str(primds_config))
        destApi.insertPrimaryDataset(primds_config)
        common.logger.debug("Successfully inserting primary dataset %s" % primName)

        # Find any files already in the dataset so we can skip them
        try:
            existingDBSFiles = destReadApi.listFiles(dataset=dbsDatasetPath)
            existingFiles = [x['logical_file_name'] for x in existingDBSFiles]
            results[datasetPath]['existingFiles'] = len(existingFiles)
        except DbsException, ex:
            existingDBSFiles = []
            existingFiles = []
            msg = "Error when listing files in DBS"
            msg += str(ex)
            msg += str(traceback.format_exc())
            common.logger.info(msg)
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
                          'dataset_access_type': 'VALID',
                          'physics_group_name': '',
                          }
        common.logger.debug("About to insert dataset: %s" % str(dataset_config))

        dbsFiles = []
        for file in files:
            if not file['lfn'] in existingFiles:
                dbsFiles.append(format_file_3(file))
            published.append(file['lfn'])
        count = 0
        blockCount = 0
        if len(dbsFiles) < max_files_per_block:
            #common.logger.debug("WF is not expired %s and the list is %s" %(workflow, self.not_expired_wf))
            #if not self.not_expired_wf:
            if True:  #exprired_wf concept does not exist for Crab2. bad place for this anyhow
                block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
                files_to_publish = dbsFiles[count:count+blockSize]
                try:
                    block_config = {'block_name': block_name, 'origin_site_name': originSite, 'open_for_writing': 0}
                    common.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                    blockDump = createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                    common.logger.info("Publishing block %s with %d files"%(block_name, len(files_to_publish)))
                    destApi.insertBulkBlock(blockDump)
                    count += blockSize
                    blockCount += 1
                except Exception, ex:
                    failed += [i['logical_file_name'] for i in files_to_publish]
                    msg = "ERROR when publishing block:\n"
                    msg += pprint.pformat(blockDump)+"\n\n"
                    msg += str(ex)+"\n"
                    msg += str(traceback.format_exc())
                    common.logger.info(msg)
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
                    block_config = {'block_name': block_name, 'origin_site_name': originSite, 'open_for_writing': 0}
                    #common.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                    blockDump = createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                    common.logger.debug("Block to insert: %s\n" % pprint.pformat(blockDump))
                    common.logger.info("Publishing block %s with %d files"%(block_name, len(files_to_publish)))
                    destApi.insertBulkBlock(blockDump)
                        
                    count += blockSize
                    blockCount += 1
                except Exception, ex:
                    failed += [i['logical_file_name'] for i in files_to_publish]
                    msg = "Error when publishing (%s) " % ", ".join(failed)
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    common.logger.info(msg)
                    count += blockSize
        results[datasetPath]['files'] = len(dbsFiles) - len(failed)
        results[datasetPath]['blocks'] = blockCount
    published = filter(lambda x: x not in failed + publish_next_iteration, published)
    common.logger.debug("Results of publication step: results = %s" % results)
    common.logger.info("Summary of file publication for this dataset: failed %d, published %d, publish_next_iteration %d" \
                       % (len(failed), len(published), len(publish_next_iteration)))
    return failed, published, results



    
