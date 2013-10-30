#! /bin/sh
# CRAB related Stuff
export CRABDIR=`\pwd`/..
export CRABSCRIPT=${CRABDIR}

CRABPATH=${CRABDIR}/python
CRABPYTHON=${CRABDIR}/python
export CRABDBSAPIPYTHON=${CRABDIR}/DBSAPI
export CRABDLSAPIPYTHON=${CRABDIR}/DLSAPI
export CRABPSETPYTHON=${CRABDIR}/PsetCode

if [ -z "$PATH" ]; then
export PATH=${CRABPATH}
else
export PATH=${CRABPATH}:${PATH}
fi
# if [ -z "$PYTHONPATH" ]; then
# export PYTHONPATH=${CRABPYTHON}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
# else
# export PYTHONPATH=${CRABPYTHON}:${PYTHONPATH}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
# fi

which boss 2>&1 > /dev/null
if [ $? -ne 0 ]; then
  echo "boss env not set"
fi
