#!/usr/bin/env python

# issues:
# 
# - error checking on subprocesses
# - where to get remote stageout PFN
# - authentication?
# - tool for copying
# - correct ExitCode in rewritten fjr

import re, os
import getopt, sys 
import FjrParser as fp

verbosity = 0
quiet = False
dryrun = False
remove_after_copy = False
copy_to_ui = False

def usage (prog_name):
    print ">>> usage: " + prog_name + " -c <crab directory> [--help | -h] [--dry-run | -n]" \
                      " [--copy-to-ui | -l] [--quiet | -q] [--verbose | -v | -vv | -vvv]"

def help (prog_name):
    usage(prog_name)
    print """
    -c\t\t\t (Mandatory) CRAB project directory to parse
    --help, -h\t\t Print this message
    --dry--run, -n\t Do not copy anything, only print a list of local PFN's that need to be copied
    --copy-to-ui, -l\t Copy output to local UI in the res dir of crab directory
    --quiet, -q\t\t Print only error messages or the list of PFN's produced by -n
    --verbose, -v, -vv, -vvv\t Be verbose.
      . first level of verbosity prints what the program is doing and whether external commands succeeded
      . second level also prints the output of external commands
      . third level runs the external commands in verbose mode, if available
    """
    sys.exit(1)
    
def copy_local_to_remote_pfn (fjr, directory):
    (local_lfn, local_pfn, local_surl) = fp.get_filenames(fjr)
    remote_filename = os.path.split(local_lfn)[1]

    remote_pfn = ''
    if copy_to_ui:
        path, endpoint = fp.cp_ui_target(directory)
        remote_pfn = endpoint + remote_filename
        remote_path = path + remote_filename 
    else:
        remote_pfn = fp.cp_target(directory) + remote_filename
    if remote_pfn == '':
        print ">>> Failed to find remote_pfn: ", remote_pfn

    list_stageout.append(local_surl)
    if not quiet:
        print ">>> copying from local:", local_surl, "to remote:", remote_pfn
    if dryrun:
        return

    # copy
    lcg_cp = "lcg-cp -v " + "-D srmv2 " + local_surl + " " + remote_pfn
    print ">>> ", lcg_cp
    (lcg_cp_stat, lcg_cp_output) = fp.getstatusoutput(lcg_cp)
    if lcg_cp_stat != 0:
       print ">>> aborting retrieval, copy error:", lcg_cp_output
       raise RuntimeError('lcg-cp failed!')

    # check copied file (source)
    (lcg_ls_source_stat, lcg_ls_source) = fp.getstatusoutput("lcg-ls -l -D srmv2 " + local_surl)
    if lcg_ls_source_stat != 0:
        print ">>> aborting retrieval, size check error:", lcg_ls_source
        raise RuntimeError('lcg-ls source failed!')

    # destination
    if copy_to_ui:
        (lcg_ls_dest_stat, lcg_ls_dest) = fp.getstatusoutput("ls -l " + remote_path)
    else:
        (lcg_ls_dest_stat, lcg_ls_dest) = fp.getstatusoutput("lcg-ls -l -D srmv2 " + remote_pfn)
    if lcg_ls_dest_stat != 0:
        print ">>> aborting retrieval, size check error:", lcg_ls_dest
        raise RuntimeError('lcg-ls destination failed!')

    # compare file size
    source_size = lcg_ls_source.split()[4]
    dest_size = lcg_ls_dest.split()[4]
    if source_size != dest_size:
        print ">>> aborting retrieval, copy error: source size", source_size, "dest size", dest_size
        raise RuntimeError('source/dest filesize mis-match!')

    # remove original
    if remove_after_copy:
        if not quiet:
            print ">>> removing lcoal copy", local_surl
        (lcg_del_output_stat, lcg_del_output) = fp.getstatusoutput("lcg-del -D srmv2 " + local_surl)
        if lcg_del_output_stat != 0:
            print ">>> warning: could not remove local copy:", lcg_del_output

    # update local_pfn to the remote surl
    for node in doc.getElementsByTagName("PFN"):
        if node.parentNode.tagName == "AnalysisFile":
            node.attributes["Value"].value = remote_pfn

if __name__ == '__main__':
  (prog_path, prog_name) = os.path.split(sys.argv[0])
  if len(sys.argv) < 2:
      help(prog_name)

  try:
      opts, argv = getopt.getopt(sys.argv[1:], "hc:vqnl", ["help", "verbose", "quiet", "dry-run", "copy-to-ui"])
  except getopt.GetoptError, err:
      print ">>> " + str(err) # will print ">>> ", something like "option -a not recognized"
      help(prog_name)

  directory = ""
  for o, a in opts:
    if o in ("-v", "--verbose"):
      verbosity += 1
    elif o in ("-n", "--dry-run"):
      dryrun = True
    elif o in ("-l", "--copy-to-ui"):
      copy_to_ui = True
    elif o in ("-q", "--quiet"):
      quiet = True
    elif o in ("-h", "--help"):
      help(prog_name)
    elif o == "-c":
      directory = a
    else:
      print ">>> unhandled option", o
      help(prog_name)

  if directory == "" or not os.path.exists(directory):
    print ">>> path <" + directory + "> may not exist"
    help(prog_name)

  list_stageout = []
  list_not_stageout = []
    
  fjrs = fp.get_fjrs(directory)
  for j in fjrs:
    if not quiet:
      print ">>> processing fjr", j

    try:
      doc = fp.parse_xml(j) # read xml file to see if the job failed
    except Exception, err:
      print ">>> skipping fjr " + j + '; error: ' + str(err)
      continue

    try:
      if fp.has_local_stageout(doc) : 
        if not quiet:
          print ">>> fjr <" + j + "> indicates remote stageout failure with local copy"
        copy_local_to_remote_pfn(doc, directory)
        if not dryrun and not copy_to_ui:
          fp.rewrite_fjr(j, doc)
      else:
        list_not_stageout.append(j)
        if not quiet:
          print ">>> fjr <" + j + ">, no need for local-to-remote copy"
    except Exception, err:
      print ">>> skipping fjr" + j + '; error: ' + str(err)

  if not quiet:
    print ">>> all fjrs processed, exiting"
  if dryrun:
    print ">>> files that need to be copied:"
    if len(list_stageout) == 0:
      print "\tnone"
    for i in list_stageout:
      print "\t", i
