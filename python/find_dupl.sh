#!/bin/bash
set -o nounset

PROGNAME=$(basename $0)

function usage
{
  cat <<EOF
Find a list of duplicate root files for a dataset at the SE that 
should be removed.

Usage: $PROGNAME -c <crab_dir> [-q | --quiet]
where options are:
  -c              Mandatory argument, crab project directory
  -v|--verbose    Turn on debug statements (D=false)
  -q|--quiet      Do not print anything (D=false)
  -h|--help       This message

  example: $PROGNAME -c <crab_dir> -q

  This script creates two files in the present directory:

    allfiles.list  - all the root files for the dataset present at the SE
    goodfiles.list - root files for successful jobs as found in the crab_fjr_n.xml files  

  and finds the duplicate files from the difference. Note that, at times jobs may finish
  and root files tranferred to the SE successfully, but crab may not immediately know about 
  job completion. Those 'most recent' root files, in such cases, will be tagged as duplicate, 
  but they are not. check id of the running job in order to avoid such issues.
EOF

  exit 1
}

[ $# -gt 0 ] || usage

crab_dir=""
let "verbose = 0"
let "quiet = 0"
while [ $# -gt 0 ]; do
  case $1 in
    -c)                     shift
                            crab_dir=$1
                            ;;
    -v | --verbose )        let "verbose = 1"
                            ;;
    -q | --quiet )          let "quiet = 1"
                            ;;
    -h | --help )           usage
                            ;;
     * )                    usage
                            ;;
  esac
  shift
done

[ "$crab_dir" != "" ] || usage
[ -e $crab_dir ] || { echo ERROR. $crab_dir not found!; exit 2; }

gflist=goodfiles.list
aflist=allfiles.list

[ $quiet -gt 0 ] || echo ">>> Find list of good files from fjr files..."
python $CRABDIR/python/find_goodfiles.py -c $crab_dir -q > $gflist

# Now find the remote directory name
rdir=$(dirname $(head -1 $gflist))

# is storage local?
srmp=""
echo $rdir | grep 'srm://' > /dev/null
if [ $? -eq 0 ]; then
  srmp=$(echo $rdir | awk -F= '{print $1}')
fi

# Get list of all files for the project
[ $quiet -gt 0 ] || echo ">>> Find list of all root files at $rdir ..."
if [ "$srmp" != "" ]; then
  srmls $rdir 2> /dev/null | grep '.root$' | awk '{if (NF==2) print $NF}' > $aflist
else
  ls -1 $rdir/*.root > $aflist
fi

# Now compare
[ $quiet -gt 0 ] || echo ">>> Following is the list of duplicate files at $rdir ..."
prefix=""
[ "$srmp" != "" ] && prefix="$srmp""="

for file in $(cat $aflist)
do
  grep $file $gflist > /dev/null
  [ $? -eq 0 ] && continue

  bname=$(basename $file)
  grep $bname $gflist > /dev/null
  [ $? -eq 0 ] && continue

  echo "$prefix""$file"
done

exit 0
