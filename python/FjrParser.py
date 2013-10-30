import os, commands, re
import xml.dom.minidom
from xml.dom.minidom import Node

# we use this function for popen calls so that we can control verbosity
def getstatusoutput (cmd):
    (stat, output) = commands.getstatusoutput(cmd)
    return stat, output

def get_fjrs (directory):
    cmd = '/bin/ls ' + directory + '/res/*.xml'
    (stat, fjrs) = getstatusoutput(cmd)
    if stat != 0:
        print ">>> aborting retrieval, error:",  fjrs
        return []
    return fjrs.split('\n')

def get_nodes ():
    cmd = "wget --no-check-certificate -O- -q https://cmsweb.cern.ch/phedex/datasvc/xml/prod/nodes"
    (stat, nodes) = getstatusoutput(cmd)
    if stat != 0:
        print ">>> aborting retrieval, error:", nodes
        raise RuntimeError('command ' + cmd + ' execution error')
    return nodes

def parse_xml(file):
    return xml.dom.minidom.parse(file)

def parse_nodes():
  datasvc_nodes = get_nodes()
  return xml.dom.minidom.parseString(datasvc_nodes)

def is_goodfile (doc):
    ns = 0 
    for node in doc.getElementsByTagName("FrameworkJobReport"):
        key = node.attributes.keys()[0].encode('ascii')
        value = node.attributes[key].value
        if value == "Success":
            for node2 in doc.getElementsByTagName("FrameworkError"):
                exitStatus = node2.attributes["ExitStatus"].value
                type = node2.attributes["Type"].value
                if exitStatus == "0" and (type == "WrapperExitCode" or type == "ExeExitCode"):
                    ns = ns + 1
    if (ns > 1): return True
    return False

def has_local_stageout (doc):
    for node in doc.getElementsByTagName("FrameworkJobReport"):
        key =  node.attributes.keys()[0].encode('ascii')
        value = node.attributes[key].value
        if value == "Failed":
            for node2 in doc.getElementsByTagName("FrameworkError"):
                exitStatus = node2.attributes["ExitStatus"].value
                type = node2.attributes["Type"].value
                if exitStatus == "60308" and type == "WrapperExitCode":
                    node.attributes[key].value = "Success"
                    node2.attributes["ExitStatus"].value = "0"
                    return True
    return False

def get_filenames (doc):
    lfn = ""
    for node in doc.getElementsByTagName("LFN"):
        if node.parentNode.tagName == "AnalysisFile":
            lfn = node.attributes["Value"].value.strip()

    pfn = ""
    for node in doc.getElementsByTagName("PFN"):
        if node.parentNode.tagName == "AnalysisFile":
            pfn = node.attributes["Value"].value.strip()

    surl = ""
    for node in doc.getElementsByTagName("SurlForGrid"):
        if node.parentNode.tagName == "AnalysisFile":
            surl = node.attributes["Value"].value.strip()

    return (lfn, pfn, surl)

def local_stageout_filenames_from_datasvc (doc, nodes):
    # convert SEName into node
    seName = ""
    for node in doc.getElementsByTagName("SEName"):
        if node.parentNode.tagName == "File":
            seName = node.firstChild.nodeValue.strip()
    if seName == "":
        print ">>> could not find SEName in fjr, aborting retrieval"
        raise RuntimeError('Failed to find SE name in fjr')
    nodeName = ""
    for node in nodes.getElementsByTagName("node"):
        se = ""
        name = ""
        for key in node.attributes.keys():
            if key.encode("ascii") == "se":
                se = node.attributes[key].value
            if key.encode("ascii") == "name":
                name = node.attributes[key].value
        if se == seName:
            nodeName = name
            break
    if verbosity > 0:
        print ">>> local stageout nodeName =", nodeName
    lfn = ""
    for node in doc.getElementsByTagName("LFN"):
        if node.parentNode.tagName == "File":
            lfn = node.firstChild.nodeValue.strip()
    cmd = "wget --no-check-certificate -O- -q \"https://cmsweb.cern.ch/phedex/datasvc/xml/prod/lfn2pfn?node=" + nodeName + "&lfn=" + lfn + "&protocol=srmv2\""
    (stat, pfnXml) = getstatusoutput(cmd)
    if stat != 0:
        print ">>> aborting retrieval, error:", pfnXml
        raise RuntimeError('command ' + cmd + ' execution error')
    try:
        pfnDoc = xml.dom.minidom.parseString(pfnXml)
    except:
        print ">>> aborting retrieval, could not parse pfn xml for node/lfn:", nodeName, lfn
        raise RuntimeError('xml parsing error')
    pfn = ""
    for node in pfnDoc.getElementsByTagName("mapping"):
        for key in node.attributes.keys():
            if key.encode("ascii") == "pfn":
                pfn = node.attributes[key].value.encode("ascii")
    return lfn, pfn

def cp_target (directory):
    # this is a bit trickier; we need to parse CMSSW.sh to get $endpoint
    cmd = "grep 'export endpoint=' " + directory + "/job/CMSSW.sh"
    (stat, grep_output) = getstatusoutput(cmd)
    if stat != 0:
        print ">>> aborting retrieval, error:", grep_output
        raise RuntimeError('Command ' + cmd + ' execution error')
    return grep_output.replace("export endpoint=", "")

def cp_ui_target(directory):
    path =  os.getcwd() + '/' + directory + '/res/'
    endpoint = 'file:/' + path
    return path, endpoint

def rewrite_fjr (file, doc, quiet=True):
    if not quiet:
        print ">>> rewriting fjr to indicate remote stageout success"
    (bkup_path, bkup_file) = os.path.split(file)
    bkup_path += "/retry_backup"
    if not quiet:
        print ">>> backup path is", bkup_path
    try: 
        stat_result = os.stat(bkup_path)
    except OSError as err: 
        if err.errno == os.errno.ENOENT:
            if not quiet:
                print ">>> backup directory does not exist, creating ..."
            os.mkdir(bkup_path)
        else:
            raise RuntimeError('Error: ' + err.errno)
    bkup_file = os.path.join(bkup_path, bkup_file)
    if not quiet:
        print ">>> \told fjr will be backed up to", bkup_file
    (bkup_cp_output_stat, bkup_cp_output) = getstatusoutput("mv " + file + " " + bkup_file)
    if bkup_cp_output_stat != 0:
        print ">>> could not back up fjr, error:", bkup_cp_output, "(fjr not rewritten)"
        raise RuntimeError('failed to backup fjr')

    out = open(file, "w")
    doc.writexml(out)
