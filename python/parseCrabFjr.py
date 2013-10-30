#!/usr/bin/env python

import sys, getopt, string, os

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from DashboardAPI import apmonSend, apmonFree

class parseFjr:
    def __init__(self, argv):
        """
        parseCrabFjr

        - parse CRAB FrameworkJobReport on WN: { 'protocol' : { 'action' : [attempted,succedeed,total-size,total-time,min-time,max-time] , ... } , ... }
        - report parameters to DashBoard using DashBoardApi.py: for all 'read' actions of all protocols, report MBPS
        - return ExitStatus and dump of DashBoard report separated by semi-colon to WN wrapper script
        """
        # defaults
        self.input = ''
        self.MonitorID = ''
        self.MonitorJobID = ''
        self.info2dash = False
        self.exitCode = False
        self.lfnList = False
        self.debug = 0
        try:
            opts, args = getopt.getopt(argv, "", ["input=", "dashboard=", "exitcode", "lfn" , "debug", "popularity=", "help"])
        except getopt.GetoptError:
            print self.usage()
            sys.exit(2)
        self.check(opts)

        return

    def check(self,opts):
        # check command line parameter
        for opt, arg in opts :
            if opt  == "--help" :
                print self.usage()
                sys.exit()
            elif opt == "--input" :
                self.input = arg
            elif opt == "--exitcode":
                self.exitCode = True
            elif opt == "--lfn":
                self.lfnList = True
            elif opt == "--popularity":
                self.popularity = True
                try:
                   self.MonitorID = arg.split(",")[0]
                   self.MonitorJobID = arg.split(",")[1]
                   self.inputInfos = arg.split(",")[2]
                except:
                   self.MonitorID = ''
                   self.MonitorJobID = ''
                   self.inputInfos = ''
            elif opt == "--dashboard":
                self.info2dash = True
                try:
                   self.MonitorID = arg.split(",")[0]
                   self.MonitorJobID = arg.split(",")[1]
                except:
                   self.MonitorID = ''
                   self.MonitorJobID = ''
            elif opt == "--debug" :
                self.debug = 1

        if self.input == '' or (not self.info2dash and not self.lfnList and not self.exitCode and not self.popularity)  :
            print self.usage()
            sys.exit()

        if self.info2dash:
            if self.MonitorID == '' or self.MonitorJobID == '':
                print self.usage()
                sys.exit()
        return

    def run(self):

        # load FwkJobRep
        try:
            jobReport = readJobReport(self.input)[0]
        except:
            print '50115'
            sys.exit()

        if self.exitCode :
            self.exitCodes(jobReport)
        if self.lfnList :
           self.lfn_List(jobReport)
        if self.info2dash :
           self.reportDash(jobReport)
        if self.popularity:
           self.popularityInfos(jobReport)
        return

    def exitCodes(self, jobReport):

        exit_status = ''
        ##### temporary fix for FJR incomplete ####
        fjr = open (self.input)
        len_fjr = len(fjr.readlines())
        if (len_fjr <= 6):
           ### 50115 - cmsRun did not produce a valid/readable job report at runtime
           exit_status = str(50115)
        else:
            # get ExitStatus of last error
            if len(jobReport.errors) != 0 :
                exit_status = str(jobReport.errors[-1]['ExitStatus'])
            else :
                exit_status = str(0)
        #check exit code
        if string.strip(exit_status) == '': exit_status = -999
        print exit_status

        return

    def lfn_List(self,jobReport):
        '''
        get list of analyzed files
        '''
        lfnlist = [x['LFN'] for x in jobReport.inputFiles]
        for file in lfnlist: print file
        return

    def storageStat(self,jobReport):
        '''
        get i/o statistics
        '''
        storageStatistics = str(jobReport.storageStatistics)
        storage_report = {}

        # check if storageStatistics is valid
        if storageStatistics.find('Storage statistics:') != -1 :
            # report form: { 'protocol' : { 'action' : [attempted,succedeed,total-size,total-time,min-time,max-time] , ... } , ... }
            data = storageStatistics.split('Storage statistics:')[1]
            data_fields = data.split(';')
            for data_field in data_fields:
                # parse: format protocol/action = attepted/succedeed/total-size/total-time/min-time/max-time
                if data_field == ' ' or not data_field or data_field == '':
                   continue
                key = data_field.split('=')[0].strip()
                item = data_field.split('=')[1].strip()
                protocol = str(key.split('/')[0].strip())
                action = str(key.split('/')[1].strip())
                item_array = item.split('/')
                attempted = str(item_array[0].strip())
                succeeded = str(item_array[1].strip())
                total_size = str(item_array[2].strip().split('MB')[0])
                total_time = str(item_array[3].strip().split('ms')[0])
                min_time = str(item_array[4].strip().split('ms')[0])
                max_time = str(item_array[5].strip().split('ms')[0])
                # add to report
                if protocol in storage_report.keys() :
                    if action in storage_report[protocol].keys() :
                        print 'protocol/action:',protocol,'/',action,'listed twice in report, taking the first'
                    else :
                        storage_report[protocol][action] = [attempted,succeeded,total_size,total_time,min_time,max_time]
                else :
                    storage_report[protocol] = {action : [attempted,succeeded,total_size,total_time,min_time,max_time] }

            if self.debug :
                for protocol in storage_report.keys() :
                    print 'protocol:',protocol
                    for action in storage_report[protocol].keys() :
                        print 'action:',action,'measurement:',storage_report[protocol][action]

        #####
        # throughput measurements # Fabio
        throughput_report = { 'rSize':0.0, 'rTime':0.0, 'wSize':0.0, 'wTime':0.0 }
        for protocol in storage_report.keys() :
            for action in storage_report[protocol].keys():
                # not interesting
                if 'read' not in action and 'write' not in action and 'seek' not in action:
                    continue

                # convert values
                try:
                    sizeValue = float(storage_report[protocol][action][2])
                    timeValue = float(storage_report[protocol][action][3])
                except Exception,e:
                    continue

                # aggregate data
                if 'read' in action:
                    throughput_report['rSize'] += sizeValue
                    throughput_report['rTime'] += timeValue
                elif 'write' in action:
                    throughput_report['wSize'] += sizeValue
                    throughput_report['wTime'] += timeValue
                elif 'seek' in action:
                    throughput_report['rTime'] += timeValue
                else:
                   continue

        # calculate global throughput
        throughput_report['readThr'] = 'NULL'
        if throughput_report['rTime'] > 0.0:
            throughput_report['rTime'] /= 1000.0 # scale ms to s
            throughput_report['readThr'] = float(throughput_report['rSize']/throughput_report['rTime'])

        throughput_report['avgNetThr'] = 'NULL'
        try:
            throughput_report['avgNetThr'] = throughput_report['rSize'] / float(jobReport.performance.summaries['ExeTime'])
        except:
            pass

        throughput_report['writeThr'] = 'NULL'
        if throughput_report['wTime'] > 0.0:
            throughput_report['wTime'] /= 1000.0
            throughput_report['writeThr'] = float(throughput_report['wSize']/throughput_report['wTime'])
        #####

        if self.debug == 1 :
            print storage_report
            print throughput_report
        return storage_report, throughput_report

    def popularityInfos(self, jobReport):
        report_dict = {}
        inputList = []
        inputParentList = []
        report_dict['inputBlocks'] = ''
        if (os.path.exists(self.inputInfos)):
            file=open(self.inputInfos,'r')
            lines = file.readlines()
            for line in lines:
                if line.find("inputBlocks")>=0:
                    report_dict['inputBlocks']= line.split("=")[1].strip()
                if line.find("inputFiles")>=0:
                    inputList = line.split("=")[1].strip().split(";")
                if line.find("parentFiles")>=0:
                    inputParentList = line.split("=")[1].strip().split(";")
            file.close()
        if len(inputList) == 1 and inputList[0] == '':
            inputList=[]
        if len(inputParentList) == 1 and inputParentList[0] == '':
            inputParentList=[]
        basename = ''
        if len(inputList) > 1:
            basename = os.path.commonprefix(inputList)
        elif len(inputList) == 1:
            basename =  "%s/"%os.path.dirname(inputList[0])
        basenameParent = ''
        if len(inputParentList) > 1:
            basenameParent = os.path.commonprefix(inputParentList)
        elif len(inputParentList) == 1:
            basenameParent = "%s/"%os.path.dirname(inputParentList[0])

        readFile = {}

        readFileParent = {}
        fileAttr = []
        fileParentAttr = []
        for inputFile in  jobReport.inputFiles:
            fileAccess = 'Local'
            if inputFile.get("PFN").find('xrootd') >= 0 : fileAccess = 'Remote'
            if inputFile['LFN'].find(basename) >=0:
                fileAttr = (inputFile.get("FileType"), fileAccess, inputFile.get("Runs"))
                readFile[inputFile.get("LFN").split(basename)[1]] = fileAttr
            else:
                fileParentAttr = (inputFile.get("FileType"), fileAccess, inputFile.get("Runs"))
                readParentFile[inputFile.get("LFN").split(basenameParent)[1]] = fileParentAttr
        cleanedinputList = []
        for file in inputList:
            cleanedinputList.append(file.split(basename)[1])
        cleanedParentList = []
        for file in inputParentList:
            cleanedParentList.append(file.split(basenameParent)[1])

        inputString = ''
        LumisString = ''
        countFile = 1
        for f,t in readFile.items():
            cleanedinputList.remove(f)
            inputString += '%s::%d::%s::%s::%d;'%(f,1,t[0],t[1],countFile)
            LumisString += '%s::%s::%d;'%(t[2].keys()[0],self.makeRanges(t[2].values()[0]),countFile)
            countFile += 1

        inputParentString = ''
        LumisParentString  = ''
        countParentFile = 1
        for fp,tp in readFileParent.items():
            cleanedParentList.remove(fp)
            inputParentString += '%s::%d::%s::%s::%d;'%(fp,1,tp[0],tp[1],countParentFile)
            LumisParentString += '%s::%s::%d;'%(tp[2].keys()[0],self.makeRanges(tp[2].values()[0]),countParentFile)
            countParentFile += 1

        if len(cleanedinputList):
           for file in cleanedinputList :
               if len(jobReport.errors):
                   if jobReport.errors[0]["Description"].find(file) >= 0:
                       fileAccess = 'Local'
                       if jobReport.errors[0]["Description"].find('xrootd') >= 0: fileAccess = 'Remote'
                       inputString += '%s::%d::%s::%s::%s;'%(file,0,'Unknown',fileAccess,'Unknown')
                   else:
                       inputString += '%s::%d::%s::%s::%s;'%(file,2,'Unknown','Unknown','Unknown')
               else:
                   inputString += '%s::%d::%s::%s::%s;'%(file,2,'Unknown','Unknown','Unknown')

        if len(cleanedParentList):
           for file in cleanedParentList :
               if len(jobReport.errors):
                   if jobReport.errors[0]["Description"].find(file) >= 0:
                       inputString += '%s::%d::%s::%s::%s;'%(file,0,'Unknown','Local','Unknown')
                   else:
                       inputString += '%s::%d::%s::%s::%s;'%(file,2,'Unknown','Unknown','Unknown')
               else:
                   inputParentString += '%s::%d::%s::%s::%s;'%(file,2,'Unknown','Unknown','Unknown')

        report_dict['inputFiles']= inputString
        report_dict['parentFiles']= inputParentString
        report_dict['lumisRange']= LumisString
        report_dict['lumisParentRange']= LumisParentString
        report_dict['Basename']= basename
        report_dict['BasenameParent']= basenameParent

         # send to DashBoard
        apmonSend(self.MonitorID, self.MonitorJobID, report_dict)
        apmonFree()

       # if self.debug == 1 :
        print "Popularity start"
        for k,v in report_dict.items():
            print "%s : %s"%(k,v)
        print "Popularity stop"
        return

    def n_of_events(self,jobReport):
        '''
        #Brian's patch to sent number of events procedded to the Dashboard
        # Add NoEventsPerRun to the Dashboard report
        '''
        event_report = {}
        eventsPerRun = 0
        for inputFile in jobReport.inputFiles:
            try:
                eventsRead = str(inputFile.get('EventsRead', 0))
                eventsRead = int(eventsRead.strip())
            except:
                continue
            eventsPerRun += eventsRead
        event_report['NoEventsPerRun'] = eventsPerRun
        event_report['NbEvPerRun'] = eventsPerRun
        event_report['NEventsProcessed'] = eventsPerRun

        if self.debug == 1 : print event_report

        return event_report

    def reportDash(self,jobReport):
        '''
        dashboard report dictionary
        '''
        event_report = self.n_of_events(jobReport)
        storage_report, throughput_report = self.storageStat(jobReport)
        dashboard_report = {}
        #
        for k,v in event_report.iteritems() :
            dashboard_report[k]=v

        # extract information to be sent to DashBoard
        # per protocol and for action=read, calculate MBPS
        # dashboard key is io_action
        dashboard_report['MonitorID'] = self.MonitorID
        dashboard_report['MonitorJobID'] = self.MonitorJobID
        for protocol in storage_report.keys() :
            for action in storage_report[protocol].keys() :
                try: size = float(storage_report[protocol][action][2])
                except: size = 'NULL'
                try: time = float(storage_report[protocol][action][3])/1000
                except: time = 'NULL'
                dashboard_report['io_'+protocol+'_'+action] = str(size)+'_'+str(time)
        if self.debug :
            ordered = dashboard_report.keys()
            ordered.sort()
            for key in ordered:
                print key,'=',dashboard_report[key]

        # IO throughput information
        dashboard_report['io_read_throughput'] = throughput_report['readThr']
        dashboard_report['io_write_throughput'] = throughput_report['writeThr']
        dashboard_report['io_netAvg_throughput'] = throughput_report['avgNetThr']

        # send to DashBoard
        apmonSend(self.MonitorID, self.MonitorJobID, dashboard_report)
        apmonFree()

        if self.debug == 1 : print dashboard_report

        return

    def makeRanges(self,lumilist):
        """ convert list to range """

        counter = lumilist[0]
        lumilist.remove(counter)
        tempRange=[]
        tempRange.append(counter)
        string = ''
        for i in lumilist:
            if i == counter+1:
                tempRange.append(i)
                counter +=1
            else:
                if len(tempRange)==1:
                    string += "%s,"%tempRange[0]
                else:
                    string += "%s-%s,"%(tempRange[:1][0],tempRange[-1:][0])
                counter = i
                tempRange=[]
                tempRange.append(counter)
            if i == lumilist[-1:][0]   :
                if len(tempRange)==1:
                    string += "%s"%tempRange[0]
                else:
                    string += "%s-%s"%(tempRange[:1][0],tempRange[-1:][0])
        return string

    def usage(self):

        msg="""
        required parameters:
        --input            :       input FJR xml file

        optional parameters:
        --dashboard        :       send info to the dashboard. require following args: "MonitorID,MonitorJobID"
            MonitorID        :       DashBoard MonitorID
            MonitorJobID     :       DashBoard MonitorJobID
        --exitcode         :       print executable exit code
        --lfn              :       report list of files really analyzed
        --help             :       help
        --debug            :       debug statements
        """
        return msg


if __name__ == '__main__' :
    try:
        parseFjr_ = parseFjr(sys.argv[1:])
        parseFjr_.run()
    except:
        pass
