#!/usr/bin/env python

import sys

class CondorGLoggingInfo:
    def __init__(self) :
        self._categories = ['Grid error'
                            'Resource unavailable',
                            'Grid error before job started',
                            'Grid error after job started',
                            'Aborted by user',
                            'Application error',
                            'Success']
        self.category = ''
        self.reason = ''

    def parseFile(self,filename) :

        # open file
        try:
            file = open(filename)
        except IOError:
            print ''
            print 'Could not open file: ',filename
            return ''

        return self.decodeReason(file.readlines())

    def decodeReason(self, input) :
        """
        extract meaningful message from condor_q -l
        """

        msg = ''
        for line in input.splitlines() :
            if line.find('HoldReason') != -1 :
                msg = 'HoldReason=\n'+ line.split('\"')[-2]
                break
            if line.find('RemoveReason') != -1 :
                msg = 'RemoveReason=\n'+line.split('\"')[-2]
                break

        if msg.find('authentication with the remote server failed')>=0 :
            self.category = self._categories[2]
        else :
            self.category = self._categories[0]

        self.reason = msg
            
        return msg

    def getCategory(self) :
        return self.category

    def getReason(self) :
        return self.reason
    

if __name__ == '__main__' :

    info = CondorGLoggingInfo()
    print info.parseFile(sys.argv[1])
