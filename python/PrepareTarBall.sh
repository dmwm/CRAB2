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
PRODCOMMONtag="PRODCOMMON_0_12_18_CRAB_57"
WMCOREtag="WMCORE_CRAB2_3"

CVSrepo=":pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories"
#export CVSROOT=${CVSrepo}"/CMSSW"
#repo_url="https://cmsweb.cern.ch/crabconf"
repo_url='http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/Repository/'

## download CRAB from CVS and cleanup the code a bit
echo ">> downloading CRAB tag $CRABtag from CVS CRAB"
cvs co -r $CRABtag -d $CRABdir CRAB

#echo ">> downloading CRAB HEAD from CVS CRAB"
#echo ">> NOTE: Temporary Use of HEAD "
#cvs co -d $CRABdir CRAB

cd $CRABdir
cvs up -P python/BossScript
chmod -x python/crab.py
rm python/crab.*sh
rm python/tar*
rm python/zero
rm -rf CRABSERVER
rm -rf PsetCode
mv python/configure .

## create etc subdir for admin config file
mkdir -p etc
## create basic config file

cat > etc/crab.cfg <<EOF
EOF

## create external subdir  for dependeces
mkdir -p external
cd external

## download crablib TEMPORARY HACK
echo ">> downloading crablib from CRAB web page"
wget --user-agent="" --no-check-certificate $repo_url/crablib.tgz

echo ">> downloading py2-sqlalchemy from CRAB web page"
wget --user-agent="" --no-check-certificate $repo_url/py2-sqlalchemy.tgz

## download pbs_python
echo ">> downloading pbs_python from CRAB web page"
wget --user-agent="" --no-check-certificate $repo_url/pbs_python.tgz

## download sqlite
echo ">> downloading sqlite from CRAB web page"
wget --user-agent="" --no-check-certificate $repo_url/sqlite.tgz

## download py2-sqlite
echo ">> downloading py2-sqlite from CRAB web page"
wget --user-agent="" --no-check-certificate  $repo_url/py2-pysqlite.tgz

## download pyOpenSSL
echo ">> downloading pyOpenSSL CRAB web page"
wget --user-agent="" --no-check-certificate  $repo_url/pyOpenSSL-0.6-python2.4.tar.gz

## download simplejson
echo ">> downloading simplejson CRAB web page"
wget --user-agent="" --no-check-certificate  $repo_url/simplejson.tgz

## download DBS API
echo ">> downloading DBS API tag ${DBSAPItag} from CVS DBS/Clients/PythonAPI"
cvs co -r ${DBSAPItag} -d DBSAPI COMP/DBS/Clients/Python
# add this dirs to the PYTHONPATH

## download DLS API
echo ">> downloading DLS PHEDeX API tag ${DLSAPItag} from CVS DLS/Client/LFCClient"
cvs co -r ${DLSAPItag} DLS/Client/LFCClient
cd DLS/Client/LFCClient
## creating library
make PREFIX=../../../DLSAPI
cd -
## move to the CRAB standard location for DLSAPI
#mv DLS/Client/lib DLSAPI
rm -r DLS
# add this dir to PATH

## download PRODCOMMON
echo ">> downloading PRODCOMMON tag ${PRODCOMMONtag} from CVS PRODCOMMON"
#mkdir -p ProdCommon
#cd ProdCommon
cvs co -r ${PRODCOMMONtag} -d ProdCommon COMP/PRODCOMMON/src/python/ProdCommon
cvs co -r ${PRODCOMMONtag} -d IMProv COMP/PRODCOMMON/src/python/IMProv
## Use the Head
#cvs co -d ProdCommon COMP/PRODCOMMON/src/python/ProdCommon
#cvs co -d IMProv COMP/PRODCOMMON/src/python/IMProv

cvs co -r ${WMCOREtag} -d WMCore                   COMP/WMCORE/src/python/WMCore/__init__.py
cvs co -r ${WMCOREtag} -d WMCore/SiteScreening     COMP/WMCORE/src/python/WMCore/SiteScreening
cvs co -r ${WMCOREtag} -d WMCore/Services          COMP/WMCORE/src/python/WMCore/Services
cvs co -r ${WMCOREtag} -d WMCore/JobSplitting      COMP/WMCORE/src/python/WMCore/JobSplitting
cvs co -r ${WMCOREtag} -d WMCore/DataStructs       COMP/WMCORE/src/python/WMCore/DataStructs 
cvs co -r ${WMCOREtag} -d WMCore/                  COMP/WMCORE/src/python/WMCore/Configuration.py
cvs co -r ${WMCOREtag} -d WMCore/Algorithms        COMP/WMCORE/src/python/WMCore/Algorithms 
cvs co -r ${WMCOREtag} -d WMCore/                  COMP/WMCORE/src/python/WMCore/WMException.py
cvs co -r ${WMCOREtag} -d WMCore/Wrappers          COMP/WMCORE/src/python/WMCore/Wrappers
cvs co -r ${WMCOREtag} -d WMQuality                COMP/WMCORE/src/python/WMQuality
cvs co -r ${WMCOREtag} -d WMCore/                  COMP/WMCORE/src/python/WMCore/DAOFactory.py
cvs co -r ${WMCOREtag} -d WMCore/Database          COMP/WMCORE/src/python/WMCore/Database

## Use the Head
#cvs co  -d WMCore                   COMP/WMCORE/src/python/WMCore/__init__.py
#cvs co  -d WMCore/SiteScreening     COMP/WMCORE/src/python/WMCore/SiteScreening
#cvs co  -d WMCore/Services          COMP/WMCORE/src/python/WMCore/Services
#cvs co  -d WMCore/JobSplitting      COMP/WMCORE/src/python/WMCore/JobSplitting
#cvs co  -d WMCore/DataStructs       COMP/WMCORE/src/python/WMCore/DataStructs 
#cvs co  -d WMCore/                  COMP/WMCORE/src/python/WMCore/Configuration.py
#cvs co  -d WMCore/Algorithms        COMP/WMCORE/src/python/WMCore/Algorithms 
#cvs co  -d WMCore/                  COMP/WMCORE/src/python/WMCore/WMException.py
#cvs co  -d WMCore/Wrappers          COMP/WMCORE/src/python/WMCore/Wrappers
#cvs co  -d WMQuality                COMP/WMCORE/src/python/WMQuality
#cvs co  -d WMCore/                  COMP/WMCORE/src/python/WMCore/DAOFactory.py
#cvs co  -d WMCore/Database          COMP/WMCORE/src/python/WMCore/Database

#cd ..
## exit from external
cd ../..

tar zcvf $CRABdir.tgz $CRABdir
echo ""
echo " tarball prepared : $CRABdir.tgz "
