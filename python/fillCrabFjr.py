#!/usr/bin/env python
"""
_fillCrabFjr.py

Adds to the FJR the WrapperExitCode and the ExeExitCode


"""
import os, string
import sys

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport

from ProdCommon.FwkJobRep.PerformanceReport import PerformanceReport

class fjrParser:
    def __init__(self, argv):
        if (len(argv))<2:
            print "it is necessary to specify the fjr name"
            sys.exit(2)
        self.reportFileName = argv[1]
        self.directive = argv[2]

        if self.directive=='--errorcode':
            self.wrapperExitCode = argv[3]
            self.exeExitCode=''
            if (len(argv))>4: self.exeExitCode = argv[4] 

        elif self.directive=='--timing':
            self.wrapperTime = 'NULL'
            self.exeTime = 'NULL'
            self.stageoutTime = 'NULL'
            self.cpuTime = 'NULL'
            try:
                self.wrapperTime = argv[3]
                self.exeTime = argv[4]
                self.stageoutTime = argv[5]
                # pay attenition that the input env var is actually a string of 3 attrutes # Fabio
                self.cpuTime = "%s %s %s"%(argv[6], argv[7], argv[8])
                self.cpuTime.replace('"','').replace('&quot;','')
            except:
                pass
        else: 
            print "bad directive specified"
            sys.exit(2)
        return

    def run(self): 

        if not os.path.exists(self.reportFileName):
            self.writeFJR()
        else:
            self.fillFJR()
        if self.directive=='--errorcode':
            self.setStatus() 

        return

    def setStatus(self):
        """
        """    
        if (self.wrapperExitCode == '0') and (self.exeExitCode == '0'):
           status = 'Success'
        else:
           status = 'Failed'

        jobReport = readJobReport(self.reportFileName)[0]
        jobReport.status = status
        jobReport.write(self.reportFileName)

        return 

    def writeFJR(self):
        """
        """                                         
        fwjr = FwkJobReport()
        fwjr.addError(self.wrapperExitCode, "WrapperExitCode")
        if (self.exeExitCode != ""):
            fwjr.addError(self.exeExitCode, "ExeExitCode")
        fwjr.write(self.reportFileName)

        return

    def checkValidFJR(self): 
        """
        """ 
        valid = 0
        fjr=readJobReport(self.reportFileName)
        if len(fjr) > 0 : valid = 1

        return valid

    def fillFJR(self): 
        """
        """
        valid = self.checkValidFJR()
        if valid == 1 and self.directive=='--errorcode':
            jobReport = readJobReport(self.reportFileName)[0]
            if (len(jobReport.errors) > 0):
                error = 0
                for err in jobReport.errors:
                    if err['Type'] == "WrapperExitCode" :
                        err['ExitStatus'] = self.wrapperExitCode 
                        jobReport.write(self.reportFileName)
                        error = 1
                    if (self.exeExitCode != ""):
                        if err['Type'] == "ExeExitCode" :
                            err['ExitStatus'] = self.exeExitCode 
                            jobReport.write(self.reportFileName)
                            error = 1
                if (error == 0):
                    jobReport.addError(self.wrapperExitCode, "WrapperExitCode")
                    if (self.exeExitCode != ""):
                        jobReport.addError(self.exeExitCode, "ExeExitCode")
                    jobReport.write(self.reportFileName) 
            else:
                jobReport.addError(self.wrapperExitCode, "WrapperExitCode")
                if (self.exeExitCode != ""):
                    jobReport.addError(self.exeExitCode, "ExeExitCode")
                jobReport.write(self.reportFileName)

        elif valid == 1 and self.directive=='--timing':
            jobReport = readJobReport(self.reportFileName)[0]
            # add here timing settings
            perf = jobReport.performance
            perf.addSummary("CrabTiming",  WrapperTime = self.wrapperTime, ExeTime = self.exeTime,\
                StageoutTime = self.stageoutTime, CpuTime = self.cpuTime)
            jobReport.write(self.reportFileName)
            pass
        else: 
            self.writeFJR()

if __name__ == '__main__':

        FjrParser_ = fjrParser(sys.argv) 
        FjrParser_.run()  

