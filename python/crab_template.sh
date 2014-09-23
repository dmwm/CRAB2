#!/bin/sh

# put STDERR to STDOUT 
exec 2>&1

#CRAB title

#
# HEAD
#
#
echo "Running $0 with $# positional parameters: $*"

getRandSeed() {
     den=(0 1 2 3 4 5 6 7 8 9 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z a b c d e f g h i j k l m n o p q r s t u v w x y z)
    nd=${#den[*]}
    randj=${den[$RANDOM % $nd]}${den[$RANDOM % $nd]}${den[$RANDOM % $nd]}
    echo $randj
}

dumpStatus() {
    echo ">>> info for dashboard:"
    echo "***** Cat $1 *****"
    cat $1
    echo "***** End Cat jobreport *****"
    chmod a+x $RUNTIME_AREA/report.py

    $RUNTIME_AREA/report.py $(cat $1)
    rm -f $1
    echo "MonitorJobID=`echo $MonitorJobID`" > $1
    echo "MonitorID=`echo $MonitorID`" >> $1
}

amIanOverflowJob() {
# check if this is a job which glideinWMS scheduled to a site not among the submission list
OverflowFlag=0
echo \$_CONDOR_JOB_AD =${_CONDOR_JOB_AD}
if [ "X$_CONDOR_JOB_AD" != "X" ]; then
    DESIRED_Sites=`grep '^DESIRED_Sites =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'|tr ',' ' '`
    JOB_CMSSite=`grep '^JOB_CMSSite =' $_CONDOR_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    OverflowFlag=1
    for site in $DESIRED_Sites; do
      if [ "$site" = "$JOB_CMSSite" ]; then
	  OverflowFlag=0
      fi
    done
fi
}


### REMOVE THE WORKING_DIR IN OSG SITES ###
remove_working_dir() {
    cd $RUNTIME_AREA
    echo ">>> working dir = $WORKING_DIR"
    echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"
    echo ">>> Remove working directory: $WORKING_DIR"
    /bin/rm -rf $WORKING_DIR
    if [ -d $WORKING_DIR ] ;then
        echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname`"
        job_exit_code=10017
    fi
}

### DUMP ORIGINAL ENVIRONMENT BEFORE CMSSW CUSTOMIZATIOn
dumpEnv(){
echo export PATH=$PATH >> CacheEnv.sh
echo export LD_LIBRARY_PATH=$LD_LIBRARY_PATH >> CacheEnv.sh
echo export PYTHONPATH=$PYTHONPATH >> CacheEnv.sh
}

outOfBound()
{
echo "********** CRAB WRAPPER CMSSW.sh TERMINATED BY A KILL"
if  [ ! -f ${RUNTIME_AREA}/WATCHDOG-SAYS-EXCEEDED-RESOURCE ]
then
  echo "********** KILL WAS NOT ISSUED BY CRAB WATCHDOG. SET GENERIC EXIT CODE"
  job_exit_code=50669
else
  echo "********** KILL signal was issued by Crab Watchdog"
  exceededResource=`cat  ${RUNTIME_AREA}/WATCHDOG-SAYS-EXCEEDED-RESOURCE`
  echo "********** because usage of resource ${exceededResource} was excessive"
  case ${exceededResource} in 
    "RSS"        ) job_exit_code=50660 ;;
    "VSIZE"      ) job_exit_code=50661 ;;
    "DISK"       ) job_exit_code=50662 ;;
    "CPU TIME"   ) job_exit_code=50663 ;;
    "WALL TIME"  ) job_exit_code=50664 ;;
    * ) echo "watchdog kill reason not given, set generic exit code"; job_exit_code=50669 ;;
  esac
  echo "************** JOB EXIT CODE set to: ${job_exit_code}"
fi
func_exit
}

exitBySignal()
{
echo "********** ${externalSignal} detected at `date`  -  `date -u`"
outOfBound
}

detectSIGINT()
{
externalSignal='SIGINT'; exitBySignal
}
detectSIGUSR1()
{
externalSignal='SIGUSR1'; exitBySignal
}
detectSIGUSR2()
{
externalSignal='SIGUSR2'; exitBySignal
}
detectSIGXCPU()
{
externalSignal='SIGXCPU'; exitBySignal
}
detectSIGXFSZ()
{
externalSignal='SIGXFSZ'; exitBySignal
}
detectSIGTERM()
{
externalSignal='SIGTERM'; exitBySignal
}


#CRAB func_exit


#CRAB initial_environment

RUNTIME_AREA=`pwd`
export RUNTIME_AREA

echo "Today is `date` - `date -u`"
echo "Job submitted on host `hostname`"
uname -a
lsb_release -id
echo ">>> current directory (RUNTIME_AREA): `pwd`"
#echo ">>> current directory content:"
#ls -Al
echo ">>> current user: `id`"
chmod 0700 -R .
echo ">>> directory permission set to 0700"
# proxy file needs special setting
proxyFile=`voms-proxy-info -path`
chmod 0600 ${proxyFile}

umask 077
echo ">>> umask set to: " `umask -S`

chmod 0704 /proc/$$/fd/1
chmod 0704 /proc/$$/fd/2
echo ">>> stdout stderr permission set to 0704"

echo ">>> voms proxy information:"
voms-proxy-info -all

repo=jobreport.txt
echo "WNHostName=`hostname`" | tee -a $RUNTIME_AREA/$repo

amIanOverflowJob
echo "OverflowFlag=${OverflowFlag}" | tee -a  $RUNTIME_AREA/$repo

#CRAB untar_software

#
# SETUP ENVIRONMENT
#

#CRAB setup_scheduler_environment

dumpEnv

#CRAB setup_jobtype_environment

dumpStatus $RUNTIME_AREA/$repo

#
# END OF SETUP ENVIRONMENT
#

#
# fork watchdog
#
${RUNTIME_AREA}/crabWatchdog.sh &
export WatchdogPID=$!
echo "crabWatchdog started as process $WatchdogPID"
trap detectSIGINT  SIGINT
trap detectSIGUSR1 SIGUSR1
trap detectSIGUSR2 SIGUSR2
trap detectSIGXCPU SIGXCPU
trap detectSIGXFSZ SIGXFSZ
trap detectSIGTERM SIGTERM

#
# PREPARE AND RUN EXECUTABLE
#

#CRAB build_executable


#
# END OF PREPARE AND RUN EXECUTABLE
#

#
# COPY INPUT
#

#CRAB copy_input

#
# Rewrite cfg or cfgpy file
#

#CRAB rewrite_cmssw_cfg

echo ">>> Executable $executable"
which $executable
res=$?
if [ $res -ne 0 ];then
  echo "ERROR ==> executable not found on WN `hostname`"
  job_exit_code=10035
  func_exit
else
  echo "ok executable found"
fi

echo "ExeStart=$executable" >>  $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo

echo ">>> $executable started at `date -u`"
start_exe_time=`date +%s`
#CRAB run_executable
executable_exit_status=$?
CPU_INFOS=`tail -n 1 cpu_timing.txt`
stop_exe_time=`date +%s`
echo ">>> $executable ended at `date -u`"

chmod 0704 $RUNTIME_AREA/crab_fjr_*.xml
echo ">>> crab_fjr_*.xml  permission set to 0704"


#### dashboard add timestamp!
echo "ExeEnd=$executable" >> $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo

let "TIME_EXE = stop_exe_time - start_exe_time"
echo "TIME_EXE = $TIME_EXE sec"
echo "ExeTime=$TIME_EXE" >> $RUNTIME_AREA/$repo

#
# limit executable stdout size to 2K lines
#
exeOutLines=`wc -l executable.out | awk '{print $1'}`
echo ">>> $executable wrote $exeOutLines lines of stdout+stderr"
echo ">>> START OF printout of $exeOutLines lines from stdout+stderr"
if [ $exeOutLines -gt 4000 ]
then
  echo ">>> print out only first 1000 and last 3000 lines:"
  head -1000 executable.out; echo ""; echo ">>>[...BIG SNIP...]";echo "";tail -3000 executable.out
else
  cat executable.out
fi
echo ">>> END OF printout of $exeOutLines lines from stdout+stderr"

grep -q 'Fatal Exception' executable.out
fatal=$?
if [ ${fatal} == "0" ]
then
    echo ">>> ERROR: Fatal Exception from CMSSW:"
    awk '/Begin .* Exception/ { printing =1 } /End .* Exception/ { print $0; printing = 0 } printing { print $0 } ' executable.out 
fi

egrep -i 'segmentation.*fault|segmentation.*violation|segfault' executable.out
segFault=$?
if [ ${segFault} == "0" ]
then
  echo "ERROR ==> user executable segfaulted"
  job_exit_code=50800
  func_exit
fi

#
# if Watchdog killed executable, make sure we
# do not go on before Watchdog completes the cleanup
#
if [ -f ${RUNTIME_AREA}/WATCHDOG-SAYS-EXCEEDED-RESOURCE ]; then wait ${WatchdogPID}; fi

#CRAB parse_report

#
# PROCESS THE PRODUCED RESULTS
#

#CRAB rename_output

#CRAB copy_output

echo ">>> current dir: `pwd`"
echo ">>> current dir content:"
ls -Al

#CRAB modify_report

chmod 0704 $RUNTIME_AREA/crab_fjr_*.xml
echo ">>> crab_fjr_*.xml  permission set to 0704"

#
# END OF PROCESS THE PRODUCED RESULTS
#

#CRAB clean_env

func_exit
