##########################################################################
#
#   C O N V E N I E N C E    F U N C T I O N S
#
###########################################################################

import string, sys, os, time, signal
import ConfigParser, re, select, fcntl
import statvfs
from subprocess import Popen, PIPE, STDOUT

import common
from crab_exceptions import CrabException
from ServerConfig import *

###########################################################################
def parseOptions(argv):
    """
    Parses command-line options.
    Returns a dictionary with specified options as keys:
    -opt1             --> 'opt1' : None
    -opt2 val         --> 'opt2' : 'val'
    -opt3=val         --> 'opt3' : 'val'
    Usually called as
    options = parseOptions(sys.argv[1:])
    """
    options = {}
    argc = len(argv)
    i = 0
    while ( i < argc ):
        if argv[i][0] != '-':
            i = i + 1
            continue
        eq = string.find(argv[i], '=')
        if eq > 0 :
            opt = argv[i][:eq]
            val = argv[i][eq+1:]
            pass
        else:
            opt = argv[i]
            val = None
            if ( i+1 < argc and argv[i+1][0] != '-' ):
                i = i + 1
                val = argv[i]
                pass
            pass
        options[opt] = val
        i = i + 1
        pass
    return options

def loadConfig(file, config):
    """
    returns a dictionary with keys of the form
    <section>.<option> and the corresponding values
    """
    #config={}
    cp = ConfigParser.ConfigParser()
    cp.read(file)
    for sec in cp.sections():
        for opt in cp.options(sec):
            ## temporary check. Allow compatibility
            new_sec = sec
            if sec == 'EDG':
                print ('\tWARNING: The [EDG] section is now deprecated.\n\tPlease remove it and use [GRID] instead.\n')
                new_sec = 'GRID'
            config[new_sec+'.'+opt] = string.strip(cp.get(sec,opt))
    return config

###########################################################################
def isInt(str):
    """ Is the given string an integer ?"""
    try: int(str)
    except ValueError: return 0
    return 1

###########################################################################
def isBool(str):
    """ Is the given string 0 or 1 ?"""
    if (str in ('0','1')): return 1
    return 0

###########################################################################
def parseRange(range):
    """
    Takes as the input a string with two integers separated by
    the minus sign and returns the tuple with these numbers:
    'n1-n2' -> (n1, n2)
    'n1'    -> (n1, n1)
    """
    start = None
    end   = None
    minus = string.find(range, '-')
    if ( minus < 0 ):
        if isInt(range):
            start = int(range)
            end   = start
            pass
        pass
    else:
        if isInt(range[:minus]) and isInt(range[minus+1:]):
            start = int(range[:minus])
            end   = int(range[minus+1:])
            pass
        pass
    return (start, end)

###########################################################################
def parseRange2(range):
    """
    Takes as the input a string in the form of a comma-separated
    numbers and ranges
    and returns a list with all specified numbers:
    'n1'          -> [n1]
    'n1-n2'       -> [n1, n1+1, ..., n2]
    'n1,n2-n3,n4' -> [n1, n2, n2+1, ..., n3, n4]
    """
    result = []
    if not range: return result

    comma = string.find(range, ',')
    if comma == -1: left = range
    else:           left = range[:comma]

    (n1, n2) = parseRange(left)
    while ( n1 <= n2 ):
        try:
            result.append(n1)
            n1 += 1
            pass
        except:
            msg = 'Syntax error in range <'+range+'>'
            raise CrabException(msg)

    if comma != -1:
        try:
            result.extend(parseRange2(range[comma+1:]))
            pass
        except:
            msg = 'Syntax error in range <'+range+'>'
            raise CrabException(msg)

    return result

###########################################################################
def findLastWorkDir(dir_prefix, where = None):

    if not where: where = os.getcwd() + '/'
    # dir_prefix usually has the form 'crab_0_'
    pattern = re.compile(dir_prefix)

    file_list = [f for f in os.listdir(where) if os.path.isdir(f) and pattern.match(f)]

    if len(file_list) == 0: return None

    file_list.sort()

    wdir = where + file_list[-1]
    return wdir

###########################################################################
def checkCRABVersion(current, url = "http://cmsdoc.cern.ch/cms/LCG/crab/config/", fileName = "allowed_releases.conf"):
    """
    _checkCRABVersion_

    compare current release with allowed releases
    format of allowed release:  ['2.6.5','2.6.6','2.7.*']
    """
    result=[]
    match_result = False
    from Downloader import Downloader
    blacklist = Downloader(url)
    try: 
        result =eval(blacklist.config(fileName))
    except:
        common.logger.info("ERROR: Problem reading allowed releases file...") 
    current_dot = current.split('.')
    for version in result:
        if version.find('.') != -1:
            version_dot = version.split('.')
            temp = False
            for compare in map(None, current_dot, version_dot):
                if compare[1].find('*') != -1:
                    return True
                elif int(compare[0]) != int(compare[1]):
                    temp = False
                    break
                else:
                    temp = True
            if temp:
                return temp
        elif version == '*':
            return True
    return False

###########################################################################
def getCentralConfigLink(linkname, url = "http://cmsdoc.cern.ch/cms/LCG/crab/config/", fileName = "URLs.conf"):
    """
    _getCentralConfigLink_

    This retrieves the remote URLs file containing a dictionary of a central manager URLs
    {
       'reportLogURL': 'http://gangamon.cern.ch/django/cmserrorreports/',
       'dashbTaskMon': 'http://dashb-cms-job-task.cern.ch/taskmon.html#task=',
       'servTaskMon' : 'http://glidein-mon.t2.ucsd.edu:8080/dashboard/ajaxproxy.jsp?p='
    }
    """
    result = {}
    from Downloader import Downloader
    links = Downloader(url)
    try:
        result = eval(links.config(fileName))
        common.logger.debug(str(result))
    except:
        common.logger.info("ERROR: Problem reading URLs releases file...")
    if result.has_key(linkname):
        return result[linkname]
    common.logger.info("ERROR: Problem reading URLs releases file: no %s present!" % linkname)
    return ''       

###########################################################################
def importName(module_name, name):
    """
    Import a named object from a Python module,
    i.e., it is an equivalent of 'from module_name import name'.
    """
    module = __import__(module_name, globals(), locals(), [name])
    return vars(module)[name]

###########################################################################
def readable(fd):
    return bool(select.select([fd], [], [], 0))

###########################################################################
def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.FNDELAY)

###########################################################################
def setPgid():
    """
    preexec_fn for Popen to set subprocess pgid

    """

    os.setpgid( os.getpid(), 0 )

def runCommand(command, printout=0, timeout=30.,errorCode=False):
    """
    _executeCommand_

    Util it execute the command provided in a popen object with a timeout
    """

    start = time.time()
    p = Popen( command, shell=True, \
               stdin=PIPE, stdout=PIPE, stderr=STDOUT, \
               close_fds=True, preexec_fn=setPgid )

    # playing with fd
    fd = p.stdout.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    # return values
    timedOut = False
    outc = []

    while 1:
        (r, w, e) = select.select([fd], [], [], timeout)

        if fd not in r :
            timedOut = True
            break

        read = p.stdout.read()
        if read != '' :
            outc.append( read )
        else :
            break

    if timedOut :
        common.logger.info('Command %s timed out after %d sec' % (command, int(timeout)))
        stop = time.time()
        try:
            os.killpg( os.getpgid(p.pid), signal.SIGTERM)
            os.kill( p.pid, signal.SIGKILL)
            p.wait()
            p.stdout.close()
        except OSError, err :
            common.logger.info(
                'Warning: an error occurred killing subprocess [%s]' \
                % str(err) )

        raise CrabException("Timeout")

    try:
        p.wait()
        p.stdout.close()
    except OSError, err:
        common.logger.info( 'Warning: an error occurred closing subprocess [%s] %s  %s' \
                         % (str(err), ''.join(outc), p.returncode ))

    returncode = p.returncode
    if returncode != 0 :
        msg = 'Command: %s \n failed with exit code %s \n'%(command,returncode)
        msg += str(''.join(outc))
        if not errorCode:
            common.logger.info( msg )
            return None
    if errorCode:
        if returncode is None :returncode=-66666
        return returncode,''.join(outc) 

    return ''.join(outc)


####################################
def makeCksum(filename) :
    """
    make check sum using filename and content of file
    """

    from zlib import crc32
    hashString = filename

    inFile = open(filename, 'r')
    hashString += inFile.read()
    inFile.close()

    cksum = str(crc32(hashString))
    return cksum


def spanRanges(jobArray):
    """
    take array of job numbers and concatenate 1,2,3 to 1-3
    return string
    """

    output = ""
    jobArray.sort()

    previous = jobArray[0]-1
    for job in jobArray:
        if previous+1 == job:
            previous = job
            if len(output) > 0 :
                if output[-1] != "-":
                    output += "-"
            else :
                output += str(previous)
        else:
            output += str(previous) + "," + str(job)
            #output += "," + str(job)
            previous = job
    if len(jobArray) > 1 :
        output += str(previous)

    return output

def displayReport(self, header, lines, xml=''):

    horizontalRuler=''
    for i in range(80):
        horizontalRuler+='-'
    horizontalRuler+='\n'
    counter = 0
    printline = ''
    printline+= header
    msg = '\n%s'%printline

    for i in range(len(lines)):
        if counter != 0 and counter%10 == 0 :
            msg += horizontalRuler
        msg+=  '%s\n'%lines[i]
        counter += 1
    if xml != '' :
        fileName = common.work_space.shareDir() + xml
        task = common._db.getTask()
        taskXML = common._db.serializeTask(task)
        common.logger.log(10-1, taskXML)
        f = open(fileName, 'w')
        f.write(taskXML)
        f.close()
        pass
    common.logger.info(msg)

def CliServerParams(self):
    """
    Init client-server interactions
    """
    self.srvCfg = {}
    ## First I have to check if the decision has been already taken...
    task = common._db.getTask()
    if task['serverName']!=None and task['serverName']!="":
        self.cfg_params['CRAB.server_name']=task['serverName']

    if self.cfg_params.has_key('CRAB.server_name'):
        self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()
    elif self.cfg_params.has_key('CRAB.use_server'):
        serverName=self.cfg_params.get('CRAB.server_name','default')
        if self.cfg_params.has_key('CRAB.server_name'):
            serverName=self.cfg_params['CRAB.server_name']
        else:
            serverName='default'
        self.srvCfg = ServerConfig(serverName).config()
    else:
        msg = 'No server selected or port specified.\n'
        msg += 'Please specify a server in the crab cfg file'
        raise CrabException(msg)
        return
    # save the serverName for future use
    opsToBeSaved={}
    opsToBeSaved['serverName']=self.srvCfg['serverGenericName']
    common._db.updateTask_(opsToBeSaved)

    self.server_admin = str(self.srvCfg['serverAdmin'])
    self.server_dn = str(self.srvCfg['serverDN'])

    self.server_name = str(self.srvCfg['serverName'])
    self.server_port = int(self.srvCfg['serverPort'])

    self.storage_name = str(self.srvCfg['storageName'])
    self.storage_path = str(self.srvCfg['storagePath'])

    if self.srvCfg.has_key('proxyPath'):
        self.proxy_path = str(self.srvCfg['proxyPath'])
    else:
        self.proxy_path = os.path.dirname(str(self.srvCfg['storagePath'])) + '/proxyCache'

    self.storage_proto = str(self.srvCfg['storageProtocol'])
    if self.cfg_params.has_key('USER.client'):
        self.storage_proto = self.cfg_params['USER.client'].lower()

    self.storage_port = str(self.srvCfg['storagePort'])

def bulkControl(self,list):
    """
    Check the BULK size and  reduce collection ...if needed
    """
    max_size = 400
    sub_bulk = []
    if len(list) > int(max_size):
        n_sub_bulk = int( int(len(list) ) / int(max_size) )
        for n in xrange(n_sub_bulk):
            first =n*int(max_size)
            last = (n+1)*int(max_size)
            sub_bulk.append(list[first:last])
        if len(list[last:]) < 50:
            for pp in list[last:]:
                sub_bulk[n_sub_bulk-1].append(pp)
        else:
            sub_bulk.append(list[last:])
    else:
        sub_bulk.append(list)

    return sub_bulk


def getUserName():
    """
    extract user name from either SiteDB or Unix
    """
    if common.scheduler.name().upper() in ['LSF', 'CAF', 'PBS','PBSV2', 'SLURM']:
        common.logger.log(10-1, "Using as username the Unix user name")
        userName = unixUserName()
    else :
        userName = gethnUserNameFromSiteDB()

    return userName


def unixUserName():
    """
    extract username from whoami
    """
    try:
        userName = runCommand("whoami")
        userName = string.strip(userName)
    except:
        msg = "Error. Problem with whoami command"
        raise CrabException(msg)
    return userName


def getDN():
    """
    extract DN from user proxy's identity
    """
    try:
        userdn = runCommand("eval `scram unsetenv -sh`; voms-proxy-info -identity")
        userdn = string.strip(userdn)
        #remove /CN=proxy that could cause problems with siteDB check at server-side
        userdn = userdn.replace('/CN=proxy','') 
        #search for a / to avoid picking up warning messages
        userdn = userdn[userdn.find('/'):]
    except:
        msg = "Error. Problem with voms-proxy-info -identity command"
        raise CrabException(msg)
    return userdn.split('\n')[0]


def gethnUserNameFromSiteDB():
    """
    extract user name from SiteDB
    """
    from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
    hnUserName = None
    userdn = getDN()
    params = { 'cacheduration' : 24,
               'logger' : common.logger() }
    mySiteDB = SiteDBJSON(params)
    msg = "Error extracting user name from SiteDB:\n"
    msg += " Issue crab -cleanCache and try again.\n If problem persists"
    msg += " check that you are registered in SiteDB, see https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB\n"
    msg += " and follow the diagnostics steps indicated there at"
    msg += " https://twiki.cern.ch/twiki/bin/viewauth/CMS/SiteDBForCRAB#Check_username_extraction_from_s"
    try:
        hnUserName = mySiteDB.dnUserName(dn=userdn)
        # cast to a string, for odd reasons new
        # WMCore/Services/SiteDB/SiteDB.py returns unicode
        # even if cached file seems to have plain strings
        # unicode in user name has bad effects on some old
        # code e.g. in crab -uploadLog
        hnUserName = str(hnUserName)
    except Exception, text:
        raise CrabException(msg)
    if not hnUserName:
        raise CrabException(msg)
    return hnUserName


def numberFile(file, txt):
    """
    append _'txt' before last extension of a file
    """
    txt=str(txt)
    p = string.split(file,".")
    # take away last extension
    name = p[0]
    for x in p[1:-1]:
        name=name+"."+x
    # add "_txt"
    if len(p)>1:
        ext = p[len(p)-1]
        result = name + '_' + txt + "." + ext
    else:
        result = name + '_' + txt

    return result

def readTXTfile(self,inFileName):
    """
    read file and return a list with the content
    """
    out_list=[]
    if os.path.exists(inFileName):
        f = open(inFileName, 'r')
        for line in  f.readlines():
            out_list.append(string.strip(line))
        f.close()
    else:
        msg = ' file '+str(inFileName)+' not found.'
        raise CrabException(msg)
    return out_list

def writeTXTfile(self, outFileName, args):
    """
    write a file with the given content ( args )
    """
    outFile = open(outFileName,"a")
    outFile.write(str(args))
    outFile.close()
    return

def readableList(self,rawList):
    """
    Turn a list of numbers into a string like 1-5,7,9,12-20
    """
    if not rawList:
        return ''

    listString = str(rawList[0])
    endRange = ''
    for i in range(1,len(rawList)):
        if rawList[i] == rawList[i-1]+1:
            endRange = str(rawList[i])
        else:
            if endRange:
                listString += '-' + endRange + ',' + str(rawList[i])
                endRange = ''
            else:
                listString += ',' + str(rawList[i])
    if endRange:
        listString += '-' + endRange
        endRange = ''

    return listString

def getLocalDomain(self):
    """
    Get local domain name
    """
    import socket
    tmp=socket.getfqdn()
    dot=string.find(tmp,'.')
    if (dot==-1):
        msg='Unkown domain name. Cannot use local scheduler'
        raise CrabException(msg)
    localDomainName = string.split(tmp,'.',1)[-1]
    return localDomainName

#######################################################
# Brian Bockelman bbockelm@cse.unl.edu
# Module to check the avaialble disk space on a specified directory.
#

def has_freespace(dir_name, needed_space_kilobytes):
    
     enough_unix_quota = False
     enough_quota = False
     enough_partition = False
     enough_mount = False
     try:
         enough_mount = check_mount(dir_name, needed_space_kilobytes)
     except Exception,e :
         common.logger.debug(str(e)+" while checking mount. Treat as check OK")
         common.logger.log(10-1,e) 
         enough_mount = True
     try:
         enough_quota = check_quota(dir_name, needed_space_kilobytes)
     except Exception, e:
         common.logger.debug(str(e)+" while checking AFS quota. Treat as check OK")
         common.logger.log(10-1,e) 
         enough_quota = True
     try:
         enough_partition = check_partition(dir_name,
             needed_space_kilobytes)
     except Exception, e:
         common.logger.debug(str(e)+" while checking partition. Treat as check OK")
         common.logger.log(10-1,e) 
         enough_partition = True
     try:
         enough_unix_quota = check_unix_quota(dir_name,
             needed_space_kilobytes)
     except Exception, e:
         common.logger.debug(str(e)+" while checking unix_quota. Treat as check OK")
         common.logger.log(10-1,e) 
         enough_unix_quota = True
         
     return enough_mount and enough_quota and enough_partition \
         and enough_unix_quota

def check_mount(dir_name, needed_space_kilobytes):
     try:
         vfs = os.statvfs(dir_name)
     except:
         raise Exception("Unable to query VFS for %s." % dir_name)
     dev_free = vfs[statvfs.F_FRSIZE] * vfs[statvfs.F_BAVAIL]
     return dev_free/1024 > needed_space_kilobytes

def check_quota(dir_name, needed_space_kilobytes):
     err,results = runCommand("/usr/bin/fs lq %s" % dir_name,errorCode=True)
     if results and err == 0 :
         try:
             results = results.split('\n')[1].split()
             quota, used = results[1:3]
             avail = int(quota) - int(used)
             return avail > needed_space_kilobytes
         except:
             raise Exception("Unable to parse AFS output.")
     elif results and err !=0:
         raise Exception(results)

def check_partition(dir_name, needed_space_kilobytes):
     err,results = runCommand("/usr/bin/fs diskfree %s" % dir_name,errorCode=True)
     if results and err==0:
         try:
             results = results.split('\n')[1].split()
             avail = results[3]
             return int(avail) > needed_space_kilobytes
         except:
             raise Exception("Unable to parse AFS output.")
     elif results and err !=0:
         raise Exception(results)

def check_unix_quota(dir_name, needed_space_kilobytes):
     err0, results0 = runCommand("df %s" % dir_name,errorCode=True)
     if results0 and err0==0:
         fs = results0.split('\n')[1].split()[0]
         err,results = runCommand("quota -Q -u -g",errorCode=True)
         if err != 0:
             raise Exception(results)
         has_info = False
         for line in results.splitlines():
             info = line.split()
             if info[0] in ['Filesystem', 'Disk']:
                 continue
             if len(info) == 1:
                 filesystem = info[0]
                 has_info = False
             if len(info) == 6:
                 used, limit = info[0], max(info[1], info[2])
                 has_info = True
             if len(info) == 7:
                 filesystem, used, limit = info[0], info[1], max(info[2], info[3])
                 has_info = True
             if has_info:
                if filesystem != fs:
                    continue
                avail = int(limit) - int(used)
                if avail < needed_space_kilobytes:
                    return False
     elif results0 and err0 !=0:
         raise Exception(results0)
     return True

def getGZSize(gzipfile):
    # return the uncompressed size of a gzipped file
    import struct
    f = open(gzipfile, "rb")
    if f.read(2) != "\x1f\x8b":
        raise IOError("not a gzip file")
    f.seek(-4, 2)
    return struct.unpack("<i", f.read())[0]

def showWebMon(server_name):
    taskName = common._db.queryTask('name')
    msg = ''
    msg +='You can also follow the status of this task on :\n'
    msg +='\tCMS Dashboard: http://dashb-cms-job-task.cern.ch/taskmon.html#task=%s\n'%(taskName)
    if server_name != '' :
        msg += '\tServer page: http://%s:8888/logginfo\n'%server_name
    msg += '\tYour task name is: %s \n'%taskName
    return msg

def SE2CMS(dests):
    """
    Trasnsform a list of SE grid name into a list SE according to CMS naming convention
    input: array of SE grid names
    output: array of SE CMS names
    """
    from ProdCommon.SiteDB.CmsSiteMapper import SECmsMap
    se_cms = SECmsMap()
    SEDestination = [se_cms[d] for d in dests]
    return SEDestination

def CE2CMS(dests):
    """
    Trasnsform a list of CE grid name into a list SE according to CMS naming convention
    input: array of CE grid names
    output: array of CE CMS names
    """
    from ProdCommon.SiteDB.CmsSiteMapper import CECmsMap
    ce_cms = CECmsMap()
    CEDestination = [ce_cms[d] for d in dests]
    return CEDestination

def checkLcgUtils( ):
    """
    _checkLcgUtils_
    check the lcg-utils version and report
    """
    import commands
    cmd = "lcg-cp --version | grep lcg_util"
    status, output = commands.getstatusoutput( cmd )
    num_ver = -1
    if output.find("not found") == -1 or status == 0:
        temp = output.split("-")
        version = ""
        if len(temp) >= 2:
            version = output.split("-")[1]
            temp = version.split(".")
            if len(temp) >= 1:
                num_ver = int(temp[0])*10
                num_ver += int(temp[1])
    return num_ver

def setLcgTimeout( ):
    """
    """
    opt = ' -t 600 ' 
    if checkLcgUtils() >= 17: opt=' --connect-timeout 600 '
    return opt

def schedulerGlite():

    scheduler = None
    err, out = runCommand('glite-version',errorCode=True)
    if err==0:
        if out.strip().startswith('3.1'):
            scheduler = 'SchedulerGLiteAPI'
        else:
            scheduler = 'SchedulerGLite'

    return scheduler

class Color():
    def __init__(self, docolor=False):
        self.red= ''
        self.blue= ''
        self.green= ''
        self.yellow= ''
        self.magenta= ''
        self.cyan= ''
        self.bold= ''
        self.end= ''
        if (docolor & self.has_colours(sys.stdout)):
            self.red= '\033[1;31m'
            self.blue= '\033[1;34m'
            self.green= '\033[1;32m'
            self.yellow= '\033[93m'
            self.magenta= '\033[1;35m'
            self.cyan= '\033[1;36m'
            self.bold= "\033[1m"
            self.end= '\033[0m'
        pass

    def has_colours(self,stream):
        if not hasattr(stream, "isatty"):
            return False
        if not stream.isatty():
            return False # auto color only on TTYs
        try:
            import curses
            curses.setupterm()
            return curses.tigetnum("colors") > 2
        except:
            # guess false in case of error
            return False

def verify_dbs_url(self) :
# parse dbs_url from crab.cfg, turn into a standard
# one, decide if DBS2 or DBS3 and compute the corresponding
# one in the other DBS if possible
# also forcefully map to DBS3 is crab.cfg has use_dbs3=1
# take no input argument and returns an ntuple
# (isDbs2, isDbs3, dbs2_url, dbs3_url) first two are boolean, others strings
#
    DBS2HOST = 'cmsdbsprod.cern.ch'
    DBS3HOST = 'cmsweb.cern.ch'
# knwon DBS end-points
    known_dbs2_urls = []
    known_dbs3_urls = []
    global_dbs2 = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    global_dbs3 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    caf_dbs2_01 = "http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet"
    local_dbs2_01 = "http://cmsdbsprod.cern.ch/cms_dbs_ph_analysis_01/servlet/DBSServlet"
    local_dbs2_02 = "http://cmsdbsprod.cern.ch/cms_dbs_ph_analysis_02/servlet/DBSServlet"
    caf_dbs3_01   = "https://cmsweb.cern.ch/dbs/prod/caf/DBSReader"
    local_dbs3_01 = "https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader"
    local_dbs3_02 = "https://cmsweb.cern.ch/dbs/prod/phys02/DBSReader"
    local_dbs3_03 = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader"
    known_dbs2_urls = [ \
        global_dbs2, caf_dbs2_01, local_dbs2_01, local_dbs2_02,
        ]
    known_dbs3_urls = [ \
        global_dbs3, caf_dbs3_01, local_dbs3_01, local_dbs3_02, local_dbs3_03,
        ]
    known_dbs_urls = known_dbs2_urls + known_dbs3_urls

    ## correspondence maps of DBS2/3 isntances
    dbs2to3={}
    dbs3to2={}
    dbs2to3[global_dbs2] = global_dbs3
    dbs2to3[global_dbs3] = global_dbs3
    dbs2to3[caf_dbs2_01]   = caf_dbs3_01
    dbs2to3[local_dbs2_01] = local_dbs3_01
    dbs2to3[local_dbs2_02] = local_dbs3_02
    dbs2to3[local_dbs3_01] = local_dbs3_01
    dbs2to3[local_dbs3_02] = local_dbs3_02
    dbs2to3[local_dbs3_03] = local_dbs3_03
    # reverse map:
    dbs3to2[global_dbs2] = global_dbs2
    dbs3to2[global_dbs3] = global_dbs2
    dbs3to2[caf_dbs3_01]   = caf_dbs2_01
    dbs3to2[local_dbs3_01] = local_dbs2_01
    dbs3to2[local_dbs3_02] = local_dbs2_02
    dbs3to2[local_dbs3_03] = None
    dbs3to2[local_dbs2_01] = local_dbs2_01
    dbs3to2[local_dbs2_02] = local_dbs2_02

    ## get DBS URL specified by user (default to global DBS3)
    dbs_url = self.cfg_params.get('CMSSW.dbs_url', global_dbs3)

    # support shortcuts for local scope DBS's
    if dbs_url == "dbs2_caf_01" :  dbs_url=caf_dbs2_01
    if dbs_url == "analysis_01" :  dbs_url=local_dbs2_01
    if dbs_url == "analysis_02" :  dbs_url=local_dbs2_02
    if dbs_url == "caf01"  :       dbs_url=caf_dbs3_01
    if dbs_url == "phys01" :       dbs_url=local_dbs3_01
    if dbs_url == "phys02" :       dbs_url=local_dbs3_02
    if dbs_url == "phys03" :       dbs_url=local_dbs3_03

    # someone uses Writer for reading, make sure we still identify known instances
    if dbs_url == "https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_01_writer/servlet/DBSServlet" :
        dbs_url = local_dbs2_01
    if dbs_url == "https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet" :
        dbs_url = local_dbs2_02
    if dbs_url == "https://cmsweb.cern.ch/dbs/prod/phys01/DBSWriter" :
        dbs_url = local_dbs3_01
    if dbs_url == "https://cmsweb.cern.ch/dbs/prod/phys02/DBSWriter" :
        dbs_url = local_dbs3_02
    if dbs_url == "https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter" :
        dbs_url = local_dbs3_03

    # if user asked for DBS3, remap DBS url if needed and possible
    useDBS3 = self.cfg_params.get('CMSSW.use_dbs3','1')=='1'
    if useDBS3  and dbs_url in known_dbs2_urls:
        dbs_url = dbs2to3 [dbs_url]
    # make sure all crab functions use this new DBS url:
    self.cfg_params['CMSSW.dbs_url'] = dbs_url

# now that we have settled on the input dbs_url from user
# check if it is DBS2 or 3 and find corresponding url in the other
# for possible verification

    if  DBS3HOST in dbs_url:
        isDbs2=False
        isDbs3=True
        dbs3_url=dbs_url
        if dbs3_url in known_dbs3_urls and not dbs_url == local_dbs3_03:
            dbs2_url=dbs3to2[dbs3_url]
        else:
            msg="No mapping to a DBS2 instance possible for dbs_url=%s"%dbs_url
            common.logger.debug(msg)
            dbs2_url=None
    elif DBS2HOST in dbs_url:
        isDbs2=True
        isDbs3=False
        dbs2_url=dbs_url
        if dbs2_url in known_dbs2_urls:
            dbs3_url=dbs2to3[dbs2_url]
        else:
            msg="No mapping to a DBS3 instance possible for dbs_url=%s"%dbs_url
            common.logger.debug(msg)
            dbs3_url=None
    else:
        msg="WARNING, unknwon DBS url: %s. Assume it is some DBS3 test instance"%dbs_url
        common.logger.info(msg)
        isDbs2=False
        isDbs3=True
        dbs3_url = dbs_url
        dbs2_url = None

    # if local scope DBS is selected, make sure dataset tier is one which
    # is expected to be born there !
    tiers_for_local_scope_dbs = ['USER']
    if dbs3_url in [local_dbs3_01, local_dbs3_02, local_dbs3_03] :
        datasetPath = self.cfg_params['CMSSW.datasetpath']
        tier = datasetPath.split('/')[-1]
        if not tier in tiers_for_local_scope_dbs :
            msg = "ERROR: local scope DBS instance %s" % dbs3_url
            msg += "\n      specified while dataset is"
            msg += "\n      %s" % datasetPath
            msg += "\n    When using local scope DBS, dataset must have one of the following tiers:"
            msg += "\n      %s" %  tiers_for_local_scope_dbs
            raise CrabException(msg)

    return (isDbs2, isDbs3, dbs2_url, dbs3_url)


####################################
if __name__ == '__main__':
    print 'sys.argv[1] =',sys.argv[1]
    list = parseRange2(sys.argv[1])
    print list
    cksum = makeCksum("crab_util.py")
    print cksum

