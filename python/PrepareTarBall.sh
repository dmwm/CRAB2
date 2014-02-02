#!/bin/sh

### $1 is the tag value CRAB_X_X_X ###
### the second argument is the BOSS version: 4_3_4-sl3-sl4 (for DBS2 publication)###
if [ $# -lt 1 ]; then
  echo "Usage: `basename $0` <CRAB_X_Y_Z> "
  exit 1
fi
tag=$1
echo "tag = $tag"

CRABdir=$tag
echo "CRABDIR = $CRABdir"
CRABtag=$tag
DBSAPItag="DBS_2_0_9_patch_9"
DLSAPItag="DLS_1_1_3"
PRODCOMMONtag="PRODCOMMON_0_12_18_CRAB_59"
WMCOREtag="WMCORE_CRAB2_3"
DBS3tag="DBS_3_1_9c"


## download CRAB from GITHUB and cleanup the code a bit
echo ">> downloading CRAB tag $CRABtag from GitHub"
git clone -b ${CRABtag} https://github.com/dmwm/CRAB2.git $CRABdir



cd $CRABdir

chmod -x python/crab.py
rm python/crab.*sh
mv python/configure .
rm -rf .git

# SB -- this is likely not needed
## create etc subdir for admin config file
#mkdir -p etc
## create basic config file
#cat > etc/crab.cfg <<EOF
#EOF
#SB ----------------------------------

## put externals where Crab2 is used to find them
mv externals external
cd external

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
rm -rf DBS3

## exit from external
cd ../..

tar zcf $CRABdir.tgz $CRABdir
echo ""
echo " tarball prepared : $CRABdir.tgz "
