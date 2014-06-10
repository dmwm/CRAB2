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

def requestBlockMigration(migrateApi, sourceApi, block):
    # submit migration request for one block checking output
    
    atDestination = False
    id = None
    
    common.logger.debug("Submit migrate request for %s ..." % block)
    sourceURL = sourceApi.url
    data = {'migration_url': sourceURL, 'migration_input': block}
    try:
        result = migrateApi.submitMigration(data)
    except HTTPError, he:
        if "is already at destination" in he.msg:
            common.logger.info("...block is already at destination")
            atDestination = True
            return (atDestination, id)
        else:
            common.logger.info("ERROR: Request to migrate %s failed." % block)
            common.logger.info("ERROR: Request detail: %s" % data)
            common.logger.info("DBS3 EXCEPTION %s\n" % he.msg)
            
    common.logger.debug("Result of migration request %s" % str(result))
    id = result.get("migration_details", {}).get("migration_request_id")
    report = result.get("migration_report")
    if id == None:
        msg =   "ERROR: Migration request failed to submit."
        msg +="\nERROR: Migration request results: %s" % str(result)
        raise CrabException(msg)
    if 'REQUEST ALREADY QUEUED' in report:
        # request could be queued in another thread, then there would be
        # no id here, so look by block and use the id of the queued request
        status = migrateApi.statusMigration(block_name=block)
        try:
            id = status[0].get("migration_request_id")
            state = status[0].get("migration_status")
            retry_count = status[0].get("retry_count")
        except:
            msg="ERROR: Can't get status for ALREADY QUEUED migration of block %s. Do crab -uploadLog and contact support" % block
            raise CrabException(msg)
        
    return (atDestination, id) 


def migrateByBlockDBS3(migrateApi, destReadApi, sourceApi, inputDataset, inputBlocks):

    # Submit one migration request for each block that needs migrating
    # if inputBlocks is missing, migrate full dataset

    if inputBlocks:
        blocks_to_migrate = set (inputBlocks)
    else :
        # If the dataset exists in the destination, make list of missing blocks
        existing_blocks = set([i['block_name'] for i in destReadApi.listBlocks(dataset=inputDataset)])
        source_blocks = set([i['block_name'] for i in sourceApi.listBlocks(dataset=inputDataset)])
        blocks_to_migrate = source_blocks - existing_blocks
        common.logger.info("Dataset %s in destination DBS with %d blocks; %d are desired." % (inputDataset, len(existing_blocks), len(source_blocks)))

    if blocks_to_migrate:
        nBlocksToMig=len(blocks_to_migrate)
        common.logger.info("%d blocks must be migrated to %s" % (nBlocksToMig,destReadApi.url) )
        common.logger.debug("list of blocks to migrate:\n%s." % ", ".join(blocks_to_migrate) )
        should_migrate = True
    else:
        common.logger.info("No migration needed")
        should_migrate = False
    if should_migrate:
        migrationIds=[]
        todoMigrations=nBlocksToMig
        msg="Submitting %d block migration requests to DBS3..." % nBlocksToMig
        common.logger.info(msg)
        for block in blocks_to_migrate:
            (atDestination,id) = requestBlockMigration(migrateApi, sourceApi, block)
            if atDestination:
                todoMigrations += -1
            else:
                migrationIds.append(id)

        msg="%d block migration requests submitted. Now wait for completion." % todoMigrations
        common.logger.info(msg)
        msg = " List of migration requests: %s" % migrationIds
        common.logger.info(msg)

        # Wait forever, then return to the main loop. Note we don't
        # fail or cancel anything. Just retry later if user Ctl-C's crab
        # States:
        # 0=PENDING
        # 1=IN PROGRESS
        # 2=SUCCESS
        # 3=FAILED (failed migrations are retried up to 3 times automatically)
        # 9=Terminally FAILED

        failedMigrations=0
        okMigrations=0
        wait=10
        while len(migrationIds) > 0:
            if wait > 1:
                msg=" %d Block migrations in progress. Next check in %d sec. " % (len(migrationIds), wait)
                if wait > 10:
                    msg+="\n     You can Ctl-C out at any time and re-issue crab -publish later,"
                    msg+="\n     migrations will continue in background"
                common.logger.info(msg)
            time.sleep(wait)
            idToCheck=migrationIds[:]  # copy, not reference
            for id in idToCheck:
                status = migrateApi.statusMigration(migration_rqst_id=id)
                try:
                    state = status[0].get("migration_status")
                    retry_count = status[0].get("retry_count")
                except:
                    msg="ERROR: Can't get status for migration_id %d. Do crab -uploadLog and contact support" % id
                    raise CrabException(msg)

                if state == 2:
                    common.logger.info("Migration id %d has succeeded" % id)
                    migrationIds.remove(id)
                    okMigrations += 1
                if state == 9:
                    common.logger.info("Migration id %d terminally FAILED. State = 9." % id)
                    common.logger.debug("Full status for migration id %d:\n%s" % (id, str(status)))
                    migrationIds.remove(id)
                    failedMigrations += 1
                if state == 3:
                    if retry_count == 3:
                        common.logger.info("Migration id %d has failed" % id)
                        common.logger.debug("Full status for migration id %d:\n%s" % (id, str(status)))
                        migrationIds.remove(id)
                        failedMigrations += 1
                    else:
                        common.logger.info("Migration id %d will be retried" % id)
                        pass
                if state == 0 or state == 1:
                    pass
            wait=min(wait+20,120)  # give it more time, but check every 2 minutes at least

        common.logger.info("Migration of %s is complete." % inputDataset)
        msg="blocks to migrate: %d. Success %d. Fail %d." % (todoMigrations, okMigrations, failedMigrations)
        common.logger.info(msg)
        if failedMigrations > 0:
            msg="some blocks failed to migrate, report to support and try again later after problem is resolved"
            common.logger.info(msg)
            return []
        else:
            common.logger.info("Migration was successful")
        
    existing_datasets = destReadApi.listDatasets(dataset=inputDataset, detail=True,dataset_access_type='*')

    return existing_datasets

def publishInDBS3(sourceApi, globalApi, inputDataset, toPublish, destApi, destReadApi, migrateApi, originSite):
    """
    Publish files into DBS3
    """

    publish_next_iteration = []
    failed = []
    published = []
    results = {}
    max_files_per_block = 500
    blockSize = max_files_per_block

    if inputDataset.upper() != 'NONE':
        existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
        primary_ds_type = existing_datasets[0]['primary_ds_type']
    else:
        common.logger.info("Input datset absent. No migration needed")
        primary_ds_type = 'mc'

    acquisition_era_name = 'CRAB'
    global_tag = 'crab2_tag'
    processing_era_config = {'processing_version': 1, 'description': 'crab2'}

    if len(toPublish) == 0:
        msg = "WARNING: nothing to  publish"
        common.logger.info(msg)

    for datasetPath, files in toPublish.iteritems():
        msg="%d files to publish in dataset %s" % (len(files),datasetPath)
        common.logger.info(msg)
        results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0,}
        dbsDatasetPath = datasetPath

        if not files:
            msg = "WARNING: no files to publish for dataset %s" % datasetPath
            common.logger.info(msg)
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
        # Also avoid files that only differ in resubmission counter or numeric hash
        # as they would be from resubmssion of same job in same task
        existingJobOutputs={}
        try:
            existingDBSFiles = destReadApi.listFiles(dataset=dbsDatasetPath)
            existingFiles = [x['logical_file_name'] for x in existingDBSFiles]
            msg = "This dataset already contains %d files" % len(existingFiles)
            common.logger.info(msg)
            for lfn in existingFiles:
                outputfile = lfn.rsplit('_',2)[0]   # use Crab2 PFN rules
                existingJobOutputs[outputfile] = lfn
            results[datasetPath]['existingFiles'] = len(existingFiles)
        except Exception, ex:
            existingDBSFiles = []
            existingFiles = []
            msg = "Error when listing files in DBS"
            msg += str(ex)
            msg += str(traceback.format_exc())
            common.logger.info(msg)

        # Is there anything to do?
        workToDo = False
        for file in files:
            if not file['lfn'] in existingFiles:
                workToDo = True
                break
        if not workToDo:
            common.logger.info("Nothing uploaded, %s has these files already or not enough new files" % datasetPath)
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
        parentFiles=set()
        parentsToSkip=set()
        #localParentFiles=set()
        localParentBlocks=set()
        #globalParentFiles=set()
        globalParentBlocks=set()

        for file in files:
            newLfn = file['lfn']
            if not newLfn in existingFiles:
                # CHECK HERE IF THIS JOB OUTPUT WAS ALREDY PUBLISHED
                jobOutputName = newLfn.rsplit('_',2)[0]
                if jobOutputName in existingJobOutputs.keys():
                    existingLfn = existingJobOutputs[jobOutputName]
                    existingFDict = destReadApi.listFiles(logical_file_name=existingLfn,detail=True)[0]
                    if existingFDict['is_file_valid'] :
                        jobId = int(existingLfn.rsplit('_',3)[1])
                        msg = "WARNING: a file was already published for Crab jobId %d in same task"%jobId
                        msg +="\n      Crab will ignore current request to publish file:\n%s"% file['lfn']
                        msg +="\n      If you want to publish that file, you must first invalidate the existing LFN:\n%s" % existingLfn
                        common.logger.info(msg)
                        continue
                    
                    
                # new file to publish, fill list of missing parent blocks
                for f in list(file['parents']) : # iterate on a copy, so can change original
                    if not f in parentFiles :
                        parentFiles.add(f)    # add to list of parents to be imported
                        # is this parent file already in destination DBS ?
                        bDict=destReadApi.listBlocks(logical_file_name=f)
                        if not bDict:
                            # parent file is not in destination DBS
                            # is it in same DBS instance as input dataset (source) ?  
                            bDict=sourceApi.listBlocks(logical_file_name=f)
                            if bDict:  # found in source, mark block to be inserted
                               localParentBlocks.add(bDict[0]['block_name'])
                            else:      # last chance: is file maybe in global DBS ?
                                bDict=globalApi.listBlocks(logical_file_name=f)
                                if bDict:  # found in global, mark block to be inserted
                                    globalParentBlocks.add(bDict[0]['block_name'])
                        if not bDict:
                            msg = "skipping parent file not known to DBS: %s" % f
                            common.logger.info(msg)
                            file['parents'].remove(f)
                            parentsToSkip.add(f)
                    if f in parentsToSkip:
                        msg = "skipping parent file not known to DBS: %s" % f
                        common.logger.info(msg)
                        if f in file['parents']: file['parents'].remove(f)
                # add to list of files to be published
                dbsFiles.append(format_file_3(file))
            published.append(file['lfn'])

        msg="Found %d files not already present in DBS which will be published" % len(dbsFiles)
        common.logger.info(msg)

        if len(dbsFiles) == 0:
            common.logger.info("Nothing to do for this dataset")
            continue

        if localParentBlocks:
            msg="list of parent blocks that need to be migrated from %s:\n%s" % \
                 (sourceApi.url, localParentBlocks)
            common.logger.info(msg)
        # migrate parent blocks before publishing
            existing_datasets = migrateByBlockDBS3(migrateApi, destReadApi, sourceApi, inputDataset, localParentBlocks)
            if not existing_datasets:
                common.logger.info("Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, sourceApi.url, migrateApi.url))
                return [], [], []
            if not existing_datasets[0]['dataset'] == inputDataset:
                common.logger.info("ERROR: Inconsistent state: %s migrated, but listDatasets didn't return any information")
                return [], [], []

        if globalParentBlocks:
            msg="list of parent blocks that need to be migrated from %s:\n%s" % \
                 (globalApi.url, globalParentBlocks)
            common.logger.info(msg)
        # migrate parent blocks before publishing
            existing_datasets = migrateByBlockDBS3(migrateApi, destReadApi, globalApi, inputDataset, globalParentBlocks)
            if not existing_datasets:
                common.logger.info("Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, sourceApi.url, migrateApi.url))
                return [], [], []
            if not existing_datasets[0]['dataset'] == inputDataset:
                common.logger.info("ERROR: Inconsistent state: %s migrated, but listDatasets didn't return any information")
                return [], [], []

        # all ready loop on files and publish blocks
        count = 0
        blockCount = 0
        if len(dbsFiles) < max_files_per_block:
            block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
            files_to_publish = dbsFiles[count:count+blockSize]
            try:
                block_config = {'block_name': block_name, 'origin_site_name': originSite, 'open_for_writing': 0}
                common.logger.debug("Inserting files %s into block %s" % ([i['logical_file_name'] for i in files_to_publish], block_name))
                blockDump = createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                common.logger.debug("Block to insert:\n%s\n" % pprint.pformat(blockDump))
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
                msg="Block Publication Successful"
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
                    common.logger.debug("Block to insert:\n%s\n" % pprint.pformat(blockDump))
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
                else:
                    msg="Block Publication Successful"
                    common.logger.info(msg)
        results[datasetPath]['files'] = len(dbsFiles) - len(failed)
        results[datasetPath]['blocks'] = blockCount
    published = filter(lambda x: x not in failed + publish_next_iteration, published)
    common.logger.debug("Results of publication step: results = %s" % results)
    # following msg is more misleading then useful, this has diverged enough
    # from logic in the version used in Crab3 that different summary is needed
    #common.logger.info("Summary of file publication :  published %d, failed %d, still_to_publish %d" \
    #                   % (len(published), len(failed), len(publish_next_iteration)))
    return failed, published, results



    
