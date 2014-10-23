#
#  This class is a collection of unrelated objects which should be
#  accessible from almost any place of the program.
#  The possible design alternative is to implement these objects
#  as singletons.
#

###########################################################################
#
#   General information about the program
#
###########################################################################

prog_name = 'crab'
prog_version = (2, 11, 1)
prog_tag = 'patch1_pre1'

prog_version_str=`prog_version[0]`+'.'+`prog_version[1]`+'.'+`prog_version[2]`
if prog_tag and len(prog_tag)>0:
    prog_version_str+= "_%s" % prog_tag
prog_authors = [
    ['Stefano Lacaprara', 'Stefano.Lacaprara@pd.infn.it', 'INFN/LNL'],
    ['Daniele Spiga', 'Daniele.Spiga@pg.infn.it', 'CERN'],
    ['Mattia Cinquilli', 'Mattia.Cinquilli@cern.ch', 'INFN/Perugia'],
    ['Alessandra Fanfani', 'Alessandra.Fanfani@bo.infn.it', 'INFN/Bologna'],
    ['Federica Fanzago', 'Federica.Fanzago@cern.ch' , 'INFN/Padova'],
    ['Fabio Farina', 'fabio.farina@cern.ch', 'INFN/Milano Bicocca'],
    ['Eric Vaandering', 'ewv@fnal.gov', 'FNAL'],
    ['Filippo Spiga', 'filippo.spiga@disco.unimib.it', 'INFN/Milano Bicocca'],
    ['Hassen Riahi', 'hassen.riahi@pg.infn.it', 'INFN/Perugia']
    ]

###########################################################################
#
#   Objects accessible from almost any place of the program.
#
###########################################################################

logger     = None
work_space = None
scheduler  = None
job_list   = []
jobDB      = None
taskDB     = None
apmon      = None
