#! /bin/csh
# CRAB related Stuff
setenv CRABDIR `\pwd`/..
setenv CRABSCRIPT ${CRABDIR}

set CRABPATH=${CRABDIR}/python
set CRABDLSAPIPATH=${CRABDIR}/DLSAPI
setenv CRABPYTHON ${CRABDIR}/python
setenv CRABDBSAPIPYTHON ${CRABDIR}/DBSAPI
setenv CRABDLSAPIPYTHON ${CRABDIR}/DLSAPI
setenv CRABPSETPYTHON ${CRABDIR}/PsetCode

if ( ! $?path ) then
set path=${CRABPATH}
else
set path=( ${CRABPATH} ${path} )
endif
# if ( ! $?PYTHONPATH ) then
# setenv PYTHONPATH ${CRABPYTHON}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
# else
# setenv PYTHONPATH ${CRABPYTHON}:${PYTHONPATH}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
# endif

which boss >& /dev/null
if ( $? ) then
  echo "boss env not set"
endif
