#!/bin/bash
#
# watchdog for crab jobs
# needs to be forked out by crab job wrapper at
# the beginning of the job
# this script does:
# 1. log resource usage to a file
# 2. kill crab wrapper childs and signal the wrapper if a child goes out of bound
# 
# if all goes well, crab wrapper will get resource
# usage from the file at the end and report them
# if the wrapper is signaled before, it will be
# wrapper's care to close down, cleanup, report etc.
#

#debug=1  # comment this line, i.e. unset $debug to avoid debug printouts

#
wdLogFile=${RUNTIME_AREA}/Watchdog_${NJob}.log

# default limits. see https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsPoliciesNodeProtection
# and also tighten defaults to fit realistic average worker nodes
let rssLimit=23*100*1000    #   2.3GB (unit = KB)
let vszLimit=100*1000*1000  # 100GB (unit = KB) = no limit
let diskLimit=19*1000       #  19GB (unit = MB)
let cpuLimit=30*24*60*60    #  30d i.e. no limit  (unit = sec)
let wallLimit=21*3600+50*60 #  21:50h  (unit = sec)

# allow limits to be changed via ad hoc files for tests
# or when indicated via crab.cfg

if [ -f ${RUNTIME_AREA}/rssLimit ] ; then
  rssLimitUser=`cat ${RUNTIME_AREA}/rssLimit`
  rssLimit=${rssLimitUser}
fi
if [ -f ${RUNTIME_AREA}/vszLimit ] ; then
  vszLimitUser=`cat ${RUNTIME_AREA}/vszLimit`
  vszLimit=${vszLimitUser}
fi
if [ -f ${RUNTIME_AREA}/diskLimit ] ; then
  diskLimitUser=`cat ${RUNTIME_AREA}/diskLimit`
  diskLimit=${diskLimitUser}
fi
if [ -f ${RUNTIME_AREA}/cpuLimit ] ; then
  cpuLimitUser=`cat ${RUNTIME_AREA}/cpuLimit`
  cpuLimit=${cpuLimitUser}
fi
if [ -f ${RUNTIME_AREA}/wallLimit ] ; then
  wallLimitUser=`cat ${RUNTIME_AREA}/wallLimit`
  wallLimit=${wallLimitUser}
fi

cpuLimitDHMS=`printf "%dh:%dm:%ds" $(($cpuLimit/3600)) $(($cpuLimit%3600/60)) $(($cpuLimit%60))`
wallLimitDHMS=`printf "%dh:%dm:%ds" $(($wallLimit/3600)) $(($wallLimit%3600/60)) $(($wallLimit%60))`

CrabJobID=`ps -o ppid -p $$|tail -1` # id of the parent process

echo "# RESOURCE USAGE SUMMARY FOR PROCESS ID ${CrabJobID}" >  ${wdLogFile}

ps u -ww -p ${CrabJobID} >> ${wdLogFile}

echo " "  | tee -a ${wdLogFile}

echo "# LIMITS USED BY CRAB WATCHDOG:"  | tee -a ${wdLogFile}
echo "# RSS  (KBytes)   : ${rssLimit}"  | tee -a ${wdLogFile}
echo "# VSZ  (KBytes)   : ${vszLimit}"  | tee -a ${wdLogFile}
echo "# DISK (MBytes)   : ${diskLimit}" | tee -a ${wdLogFile}
echo "# CPU TIME        : ${cpuLimitDHMS}"  | tee -a ${wdLogFile}
echo "# WALL CLOCK TIME : ${wallLimitDHMS}" | tee -a ${wdLogFile}

echo " "  | tee -a ${wdLogFile}

echo "# FOLLOWING PRINTOUT TRACKS MAXIMUM VALUES OF RESOURCES USE" >> ${wdLogFile}
echo "# ONE LINE IS PRINTED EACH TIME A MAXIMUM CHANGES"  >> ${wdLogFile}
echo "# IF CHANGE WAS IN RSS OR VSZ ALSO PROCID AND COMMAND ARE PRINTED"  >> ${wdLogFile}
echo "# THERE IS NO PRINTOUT FOR CPU/WALL TIME INCREASE"  >> ${wdLogFile}
echo " "  >> ${wdLogFile}

#
# find all processes in the group of the CrabWrapper
# and for each process do ps and track the maximum
# usage of resources. hen  max goes over limit i.e...
# when any of the processes in the tree goes out of bound
# send a TERM to all child of the wrapper and
# then signal the wrapper itself so it an close up
# with proper error code

maxRss=0
maxVsz=0
maxDisk=0
maxCpu=0
maxTime=0
maxWall=0

iter=0
nLogLines=0

startTime=`date +%s`

# start infinite loop of watching what job is doing
while true
do

 let residue=${nLogLines}%30    # print a header line every 30 lines
 if [ ${residue} = 0 ]; then
  echo -e "# TIME\t\t\tPID\tRSS(KB)\tVSZ(KB)\tDsk(MB)\ttCPU(s)\ttWALL(s)\tCOMMAND" >>  ${wdLogFile}
 fi
 now=`date +'%b %d %T %Z'`
 processGroupID=`ps -o pgrp -p ${CrabJobID}|tail -1|tr -d ' '`
 processes=`pgrep -g ${processGroupID}`

 for pid in ${processes}
 do
   maxChanged=0
   metrics=`ps --no-headers -o pid,cputime,rss,vsize,args  -ww -p ${pid}`
   if [ $? -ne 0 ] ; then continue ; fi # make sure process is still alive

   [ $debug ] && echo metrics = ${metrics}

   let wallTime=`date +%s`-${startTime}

   [ $debug ] && echo wallTime = ${wallTime}

   cpu=`echo $metrics|awk '{print $2}'`  # in the form [dd-]hh:mm:ss
   #convert to seconds
   [[ $cpu =~ "-" ]] && cpuDays=`echo $cpu|cut -d- -f1`*86400
   [[ ! $cpu =~ "-" ]] && cpuDays=0
   cpuHMS=`echo $cpu|cut -d- -f2` # works even if there's no -
   cpuSeconds=10\#`echo ${cpuHMS}|cut -d: -f3`
   cpuMinutes=10\#`echo ${cpuHMS}|cut -d: -f2`*60
   cpuHours=10\#`echo ${cpuHMS}|cut -d: -f1`*3600
   let cpuTime=$cpuSeconds+$cpuMinutes+$cpuHours+$cpuDays
   
   rss=`echo $metrics|awk '{print $3}'`

   vsize=`echo $metrics|awk '{print $4}'`

   cmd=`echo $metrics|awk '{print $5" "$6" "$7" "$8" "$9" "$10" "$11" "$12" "$13" "$14" "$15}'`

   # track max for the metrics
   if [ $rss -gt $maxRss ]; then maxChanged=1; maxRss=$rss; fi
   if [ $vsize -gt $maxVsz ]; then maxChanged=1; maxVsz=$vsize; fi
   if [ $cpuTime -gt $maxCpu ]; then maxCpu=$cpuTime; fi
   if [ $wallTime -gt $maxWall ]; then maxWall=$wallTime; fi

# only add a line to log when max increases

   if [ ${maxChanged} = 1 ]  ; then 
     echo -e " $now\t$pid\t$maxRss\t$maxVsz\t$maxDisk\t$cpuTime\t$wallTime\t$cmd" >>  ${wdLogFile}
     let nLogLines=${nLogLines}+1
   fi
 done  # end loop on processes in the tree

# now check disk

 disk=`du -sm ${RUNTIME_AREA}|awk '{print $1}'`
 if [ $OSG_GRID ]; then
   disk=`du -sm ${WORKING_DIR}|awk '{print $1}'`
 fi

 if [ $disk -gt $maxDisk ]; then
     maxDisk=$disk
     echo -e " $now\t---\t$maxRss\t$maxVsz\t$maxDisk\t----\t----\t----" >>  ${wdLogFile}
     let nLogLines=${nLogLines}+1
 fi

# if we hit a limit, make a note and exit the infinite loop
 if [ $maxRss -gt $rssLimit ] ; then
     exceededResource=RSS
     resVal=$maxRss
     resLim=$rssLimit
     break
 fi
 if [ $maxVsz -gt $vszLimit ] ; then
     exceededResource=VSIZE
     resVal=$maxVsz
     resLim=$vszLimit
     break
 fi
 if [ $maxDisk -gt $diskLimit ] ; then
     exceededResource=DISK
     resVal=$maxDisk
     resLim=$diskLimit
     break
 fi
 if [ $cpuTime -gt $cpuLimit ] ; then
     exceededResource="CPU TIME"
     resVal=$maxCpu
     resLim=$cpuLimit
     break
 fi
 if [ $wallTime -gt $wallLimit ] ; then
     exceededResource="WALL TIME"
     resVal=$maxWall
     resLim=$wallLimit
     break
 fi

 let iter=${iter}+1
 sleep 60
done # infinite loop watching processes

# reach here if something went out of limit


cat >> ${wdLogFile} <<EOF
 ********************************************************
 * JOB HIT PREDEFINED RESOURCE LIMIT ! PROCESSING HALTED
 *   ${exceededResource} value is ${resVal} while limit is ${resLim}
 ********************************************************
EOF

#
# write a file to communicate CrabWrapper that
# we are killing cmsRun, so it does not process
# fjr etc. before being signaled

echo $exceededResource > ${RUNTIME_AREA}/WATCHDOG-SAYS-EXCEEDED-RESOURCE

# send TERM to all childs of crab wrapper but myself

# traverse the process group in reverse tree order
# and stop when we reach CrabWrapper

processes=`pgrep -g ${processGroupID}`
echo "send pkill TERM  to all CrabWrapper childs in this Process Tree" >> ${wdLogFile}
ps --forest -p ${processes} >>  ${wdLogFile}
procTree=`ps --no-header --forest -o pid -p ${processes}`
revProcTree=`echo ${procTree}|tac -s" "`
for pid in ${revProcTree}
do
  if [ $pid -eq ${CrabJobID} ] ; then break; fi # do not go above crab wrapper
  if [ $pid -eq $$ ] ; then continue; fi # do not kill myself
  procCmd=`ps --no-headers -o args -ww -p ${pid}`
  if [ $? -ne 0 ] ; then
    echo " process PID ${pid} already ended" >> ${wdLogFile}
    continue
  fi
  kill -TERM $pid
  echo " Sent TERM to: PID ${pid} executing: ${procCmd}" >> ${wdLogFile}
done
echo " Wait 30 sec to let processes close up ..." >> ${wdLogFile}
sleep 30
for pid in ${revProcTree}
do
  if [ $pid -eq ${CrabJobID} ] ; then break; fi # do not go above crab wrapper
  if [ $pid -eq $$ ] ; then continue; fi # do not kill myself
  ps ${pid} > /dev/null
  if [ $? -ne 0 ] ; then
    echo " OK. Process ${pid} is gone" >> ${wdLogFile}
  else
    echo -n " Process ${pid} is still there. Send KILL" >> ${wdLogFile}
    kill -KILL $pid
    sleep 10
    echo " Did it die ? do a ps ${pid}" >> ${wdLogFile}
    ps  ${pid}  >> ${wdLogFile}
    if [ $? -ne 0 ] ; then
	echo " Process ${pid} gone at last" >> ${wdLogFile}
    else
	echo -n " Process ${pid} is still there. One last KILL and move on" >> ${wdLogFile}
	kill -KILL $pid
    fi
  fi
done

echo "Finally gently signal CrabWrapper with USR2" >> ${wdLogFile}
kill -USR2 ${CrabJobID}

echo "Process cleanup completed, crabWatchdog.sh exiting" >> ${wdLogFile}

