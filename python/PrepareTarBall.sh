#!/bin/bash

### $1 is the tag value CRAB_X_X_X ###
### the second argument is the BOSS version: 4_3_4-sl3-sl4 (for DBS2 publication)###
if [ $# -lt 1 ]; then
  echo "Usage: `basename $0` <CRAB_X_Y_Z> "
  exit 1
fi
CRABtag=$1
echo "CRABtag = $CRABtag"


DBSAPItag="DBS_2_0_9_patch_9"
DLSAPItag="DLS_1_1_3"
PRODCOMMONtag="PRODCOMMON_0_12_18_CRAB_68"
WMCOREtag="WMCORE_CRAB2_4"
WMCOREWMtag="0.9.94"
DBS3tag="DBS_3_2_6a"


## download CRAB from GITHUB
echo ">> downloading CRAB tag $CRABtag from GitHub"
git clone -b ${CRABtag} https://github.com/dmwm/CRAB2.git $CRABtag
status=$?
if [ $status != 0 ]
then
    echo ERROR
    echo "git clone -b ${CRABtag} FAILED"
    exit $status
fi

CRABdir=`pwd`/$CRABtag
echo "CRABdir = $CRABdir"

# verify that tag is the same in Git and code
pushd $CRABdir/python
ver=`grep "prog_version =" common.py`
vernums=`echo $ver|cut -d '(' -f2|tr -d ')'|tr -d ' '`
verN1=`echo $vernums|cut -d , -f1`
verN2=`echo $vernums|cut -d , -f2`
verN3=`echo $vernums|cut -d , -f3`
verTag=`grep "prog_tag =" common.py|cut -d"'" -f2`
CRABver=`echo "CRAB_"$verN1"_"$verN2"_"$verN3`
if [ X$verTag != X ]
then
    CRABver=`echo $CRABver"_"$verTag`
fi

if [ $CRABver != $CRABtag ]
then
    echo "ERROR"
    echo "requested tag $CRABtag is not the same as in common.py: $CRABver"
    exit 1
fi
popd


# cleanup a bit
pushd $CRABdir
echo "==================================="
pwd
chmod -x python/crab.py
rm -v python/crab.*sh
mv -v python/configure .
rm -v -rf .git

## put externals where Crab2 is used to find them
#mv -v externals external
cd external
pwd
echo "==================================="

## download DBS API
echo ">> downloading DBS API tag ${DBSAPItag} from GitHub/dmwm/DBSAPI"

git clone -b ${DBSAPItag} https://github.com/dmwm/DBSAPI.git DBSAPI
mv DBSAPI/Clients/Python/DBSAPI DBSAPI
rm -rf DBSAPI/Clients

## download DLS API
echo ">> downloading DLS PHEDeX API tag ${DLSAPItag} from CVS DLS/Client/LFCClient"
git clone -b ${DLSAPItag} https://github.com/dmwm/DLSAPI.git DLS

## create library in CRAB standard location for DLSAPI
cd DLS/Client/LFCClient
make PREFIX=../../../DLSAPI
cd -
rm -rf DLS

## download PRODCOMMON and reproduce
# the same directory structure we had when using CVS
#
echo ">> downloading PRODCOMMON tag ${PRODCOMMONtag} from CVS PRODCOMMON"
git clone -b ${PRODCOMMONtag} https://github.com/dmwm/ProdCommon.git FullProdCommon
mv FullProdCommon/src/python/ProdCommon ./ProdCommon
mv FullProdCommon/src/python/IMProv ./IMProv
rm -rf FullProdCommon

#
# download the needed pieces of WMCore and reproduce
# the same directory structure we had when using CVS
#

git clone -b ${WMCOREtag} https://github.com/dmwm/WMCore-legacy.git WMCore-legacy
mv WMCore-legacy/src/python/WMCore .
rm -rf WMCore-legacy

#
# download DBS3 client
#

git clone -b ${DBS3tag} https://github.com/dmwm/DBS.git DBS3

mkdir dbs3client
mv DBS3/PycurlClient/src/python/* ./dbs3client/
mv DBS3/Client/src/python/* ./dbs3client/
mv DBS3/Client/utils//DataOpsScripts/DBS3SetDatasetStatus.py ../python
mv DBS3/Client/utils//DataOpsScripts/DBS3SetFileStatus.py ../python
rm -rf DBS3

#
# download DBS Lexicon from recent WMCore version 
#

git clone -b ${WMCOREWMtag} https://github.com/dmwm/WMCore.git WMCore-current

mkdir WMCoreWM
mv WMCore-current/src/python/WMCore/Lexicon.py ./WMCoreWM/
rm -rf WMCore-current

## exit from external
popd

tar zcf ${CRABtag}.tgz ${CRABtag}
echo ""
echo " tarball prepared : ${CRABtag}.tgz "
