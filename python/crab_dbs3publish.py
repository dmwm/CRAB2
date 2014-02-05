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
#from dbs.apis.dbsClient import *


def format_file_3(file, output_config, dataset):
    pass

def createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files):

    pass

def migrateDBS3(migrateApi, destReadApi, sourceURL, inputDataset):
    pass


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
    if inputDataset != 'None':
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
        # user generate data
        acquisition_era_name = "CRAB"
        primary_ds_type = 'mc'

    # There's little chance this is correct, but it's our best guess for now.
    existing_output = destApi.listOutputConfigs(dataset=inputDataset)
    if not existing_output:
        common.logger.error("Unable to list output config for input dataset %s." % inputDataset)
    existing_output = existing_output[0]
    global_tag = existing_output['crab2_tag']

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
            common.logger.debug("WF is not expired %s and the list is %s" %(workflow, self.not_expired_wf))
            if not self.not_expired_wf:
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
                common.logger.debug("WF is not expired %s and the list is %s" %(workflow, self.not_expired_wf))
                return [], [], []
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



    
