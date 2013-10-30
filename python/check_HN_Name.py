class check_HN_name:
    def init(self): 
        pass
    
    def stdaloneCheck(self):  
 
        import urllib
        from commands import getstatusoutput
        print 'start standalone check ...\n'
        status, dn = getstatusoutput('voms-proxy-info -identity')
        if status == 0:
           print "my DN is: %s \n"%dn
        dn = dn.split('\n')[-1]
        dn  = urllib.urlencode({'dn':dn})
        print 'Using urlencoded DN: \n\t %s '%dn
        try:
            f = urllib.urlopen("https://cmsweb.cern.ch/sitedb/json/index/dnUserName?%s" % dn)
            username = str(f.read())
            f.close()
        except:
            print "failed to get username via SiteDB V1 API"
            username="None"

        cmd = "curl -s  --capath $X509_CERT_DIR --cert $X509_USER_PROXY --key $X509_USER_PROXY 'https://cmsweb.cern.ch/sitedb/data/prod/whoami'|tr ':,' '\n'|grep -A1 login|tail -1"
        uname = subprocess.check_output(cmd,shell=True)
        
        
        print 'v1: my HN user name is: %s' % username
        print 'v2: my HN user name is: %s' % uname
        print '\nend check.....................'

    def crabCheck(self):
        from CrabLogger import CrabLogger
        from WorkSpace import WorkSpace, common
        import tempfile, urllib, os, string
      
        dname = tempfile.mkdtemp( "", "crab_", '/tmp' )
        os.system("mkdir %s/log"%dname )
        os.system("touch %s/crab.log"%dname )
        
        cfg_params={'USER.logdir' : dname }
        common.work_space = WorkSpace(dname, cfg_params)
        args = string.join(sys.argv,' ')
        common.debugLevel = 0
        common.logger = CrabLogger(args)
        
        from crab_util import getDN,gethnUserNameFromSiteDB
        print 'start using CRAB utils ...\n'
        print "my DN is: %s \n"%getDN()
        try:
            print 'my HN user name is: %s \n'%gethnUserNameFromSiteDB()
        except:
            print '\nWARNING native crab_utils failed! ' 
            dn=urllib.urlencode({'dn':getDN()})
            print 'trying now using urlencoded DN: \n\t %s '%dn
            status,hnName = self.gethnName_urlenc(dn)
            if status == 1: 
                print '\nWARNING: failed also using urlencoded DN '
            else: 
                print 'my HN user name is: %s \n'%name
                print 'problems with crab_utils'   
        print '\nend check.....................'
        
        os.system("rm -rf %s"%dname )
         
    def gethnName_urlenc(self,dn):
        from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
        hnUserName = None
        userdn = dn
        mySiteDB = SiteDBJSON()
        status = 0 
        try:
            hnUserName = mySiteDB.dnUserName(dn=userdn)
        except:
            status = 1 
        return status,hnUserName


if __name__ == '__main__' :
    import sys
    import subprocess
    args = sys.argv[1:]
    check = check_HN_name()

#    print "verify if user DN is mapped in CERN's SSO"
#    cmd='curl -s  --capath $X509_CERT_DIR --cert $X509_USER_PROXY --key $X509_USER_PROXY "https://cmsweb.cern.ch/sitedb/data/dev/people" | grep "`voms-proxy-info -identity`"'
#    DocUrl = "\nhttps://hypernews.cern.ch/HyperNews/CMS/get/cernCompAnnounce/820.html"
#    DocUrl += "\nhttps://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB"

#    try:
#        subprocess.check_call(cmd,shell=True)
#        print "OK. user ready for SiteDB switchover on March 12, 2013\n\n"
#    except:
#        print "********** ATTENTION !!!!  **************"
#        print "DN not registered in CERN's SSO"
#        print "need to follow instructions in %s" % DocUrl
#        print "or Crab will stop working for you on March 12, 2013"
#        print "*****************************************"
#        dummy=raw_input("\nHit Return to acknowlege that you read this message")
    
    if 'crab' in args:
        check.crabCheck()  
    else:
        check.stdaloneCheck()  

