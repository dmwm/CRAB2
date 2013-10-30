#!/usr/bin/env python
"""
_LFNBaseName_
"""

from crab_exceptions import *
from crab_util import runCommand, getUserName
import common
import os, string, time


def LFNBase(forced_path, PrimaryDataset='',ProcessedDataset='',merged=True,publish=False):
    """
    """
    if (PrimaryDataset == 'null'):
        PrimaryDataset = ProcessedDataset
    if PrimaryDataset != '':
        if ( PrimaryDataset[0] == '/' ):  PrimaryDataset=PrimaryDataset[1:]  
    if forced_path.find('store/group')>0:
        lfnbase = os.path.join(forced_path, PrimaryDataset, ProcessedDataset)
    else:
        lfnbase = os.path.join(forced_path, getUserName(), PrimaryDataset, ProcessedDataset)
    if (publish == True):
        checkSlash(ProcessedDataset)
        checkLength(lfnbase, forced_path, PrimaryDataset, ProcessedDataset)
    return lfnbase

def checkSlash(ProcessedDataset):
    """
    check if [USER].publish_data_name contains not allowed slash
    """
    import re
    
    #if (str(ProcessedDataset).find("/") > -1):
    #        raise CrabException("Warning: [USER] publication_data_name contains a '/' characters that is not allowed. Please change the publication_data_name value")
    m = re.findall("[^-\w_\.%]+", str(ProcessedDataset))
    if (m != []):
        common.logger.debug("check not allowed characters in the publication_data_name  = " + str(m))
        raise CrabException("Warning: [USER] publication_data_name contains not allowed characters %s . Please change the publication_data_name value" % str(m))

def checkLength(lfnbase, forced_path, PrimaryDataset, ProcessedDataset):
    """
    """
    
    len_primary = len(PrimaryDataset)
    
    common.logger.debug("CheckLength of LFN and User DatasetName")
    common.logger.debug("max length for complete LFN is 500 characters, max length for primary dataset is 100 characters")
    common.logger.debug("PrimaryDataset = " + PrimaryDataset)
    common.logger.debug("len_primary = " + str(len_primary))

    if (len_primary > 100):
       raise CrabException("Warning: you required user_data_publication. The PrimaryDatasetName has to be < 100 characters")
    
    if (PrimaryDataset != ProcessedDataset):
        common.logger.debug("ProcessedDataset = " + ProcessedDataset)
        common.logger.debug("len(ProcessedDataset) = " + str(len(ProcessedDataset)))

    common.logger.debug("forced_path = " + forced_path)
    common.logger.debug("len(forced_path) = " + str(len(forced_path)))    
    
    user = getUserName()
    len_user_name = len(user)
    common.logger.debug("user = " + user)
    common.logger.debug("len_user_name = " + str(len_user_name))
    
    common.logger.debug("lfnbase = " + lfnbase)
    common.logger.debug("len(lfnbase) = " + str(len(lfnbase)))
    
    ### we suppose a output_file_name of 50 characters ### 
    if len(lfnbase)>450:
        if (PrimaryDataset != ProcessedDataset):
            #500 - len_user_name - len_primary - len(forced) - len(PSETHASH = 32) - 4(/) - output(50)
            #~400 - len_user_name - len_primary - len(forced)
            if (len(ProcessedDataset) > (400 - len_user_name - len_primary - len(forced_path))):
                raise CrabException("Warning: publication name too long. USER.publish_data_name has to be < " + str(400 - len_user_name - len_primary - len(forced_path)) + " characters")
            else:
                raise CrabException("Warning: LFN > 500 characters")
        else:
            if (len(ProcessedDataset) > (400 - len_user_name - len(forced_path)) / 2):
                raise CrabException("Warning: publication name too long. USER.publish_data_name has to be < " + str((400 - len_user_name - len(forced_path)) / 2) + " characters")
            else:
                raise CrabException("Warning: LFN > 500 characters")
            

if __name__ == '__main__' :
    """
    """
    import logging 
    common.logger = logging

    print "xx %s xx"%getUserName()
    baselfn = LFNBase("datasetstring")
    print baselfn

    unmergedlfn = LFNBase("datasetstring",merged=False)
    print unmergedlfn
    print PFNportion("datasetstring")
