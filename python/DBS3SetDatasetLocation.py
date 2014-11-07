#!/usr/bin/env python
"""
_setDatasetLocation_

Give the dataset path and the new location , it will
set origin_site_name for all blocks of that dataset. If no location is
given, it will print the current value.
Defaults to DBS3 instance phys03. Optoinally a different URL can be given

"""
from optparse import OptionParser
import logging
import sys
import os
import Lexicon
from dbs.apis.dbsClient import DbsApi

def get_command_line_options():
    parser = OptionParser(usage='%prog --dataset=</specify/dataset/path> --location=<newLocation> ')
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url", metavar="DBS_Instance_URL")
    parser.add_option("-d", "--dataset", dest="dataset", help="Dataset to change status", metavar="/specify/dataset/path")
    parser.add_option("-l", "--location", dest="new_location", help="New location of the dataset", metavar="Tx_cc_nnnn")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Increase verbosity")
    (options, args) = parser.parse_args()

    if not (options.dataset):
        parser.print_help()
        parser.error('Missing mandatory options --dataset')

    return options, args

if __name__ == "__main__":

    dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/'
    instance = 'phys03/'
    readUrl  = dbsUrl + instance + 'DBSReader'
    writeUrl = dbsUrl + instance + 'DBSWriter'
    new_location = None

    options, args = get_command_line_options()

    log_level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(format='%(message)s', level=log_level)

    if options.url:
        url = options.url
        for suffix in ['DBSReader', 'DBSWriter']:
            if url.endswith(suffix):
                url=url[0:-len(suffix)]
        readUrl  = url + 'DBSReader'
        writeUrl = url + 'DBSWriter'

    readApi  = DbsApi(url=readUrl)
    writeApi = DbsApi(url=writeUrl)

    dataset = options.dataset
    if options.new_location:
        new_location = options.new_location

    ###sanitize input
    # dataset name
    Lexicon.dataset(dataset)
    
    # PNN
    if new_location:
        Lexicon.cmsname(new_location)

    # process dataset by blocks

    blockDicts = readApi.listBlocks(dataset=dataset, detail=True)
    for block in blockDicts:
        blName = block['block_name']
        location = block['origin_site_name']
        logging.debug('block %s at location: %s' % (blName, location))
        if new_location:
            writeApi.updateBlockSiteName(block_name=blName, origin_site_name=new_location)
            logging.debug('location set to %s' % (new_location))
        

    logging.info("Done")
