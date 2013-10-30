#!/usr/bin/env python
        
##
## MAIN PROGRAM
##

import getopt,sys
from DashboardAPI import report

if __name__ == '__main__' :
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:")
    except getopt.GetoptError:
        # print help information and exit:
        print "Unknown option"
        sys.exit(1)
    if len( opts)==1 :
        copt=opts[0]
        filename=copt[1]
        try:
            rfile=open(filename)
        except IOError, ex:
            print "Can not open input file"
            sys.exit(1)
        lines=rfile.readlines()
        for line in lines :
           args.append(line.strip())
#        print args
#        print "********************"
    if len(args)>0 :
         argstring=' '.join(args)
         mytmp=argstring.split('=')
         mystring=''
         newkey=''
         if len(mytmp)>0 :
          for i in range(0, len(mytmp)-1):
              mytmp[i]=mytmp[i].strip()
              mytmp[i+1]=mytmp[i+1].strip()
              if  newkey=='':
                  ckey=mytmp[i]
              else :
                  ckey=newkey
              if i< len(mytmp)-2 :  
                  cvalue=' '.join(mytmp[i+1].split(' ')[:-1])+","
              else:
                  cvalue= mytmp[-1]    
              newkey=mytmp[i+1].split(' ')[-1]
              mystring=mystring+ckey.replace(" ",'')+"="+cvalue
          args=mystring.split(',')  
    report(args)
#    print "***"
#    print opts
#    print "###"
#    print args
    sys.exit(0)

