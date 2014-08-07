
###########################################################################
#
#   H E L P   F U N C T I O N S
#
###########################################################################

import common

import sys, os, string

import tempfile

###########################################################################
def usage():
    print 'in usage()'
    usa_string = common.prog_name + """ [options]

The most useful general options (use '-h' to get complete help):

  -create                                 -- Create all the jobs.
  -submit n                               -- Submit the first n available jobs. Default is all.
  -status                                 -- check status of all jobs.
  -getoutput|-get [range]                 -- get back the output of all jobs: if range is defined, only of selected jobs.
  -publish                                -- after the getouput, publish the data user in a local DBS instance.
  -kill [range]                           -- kill submitted jobs.
  -resubmit range or all                  -- resubmit killed/aborted/retrieved jobs.
  -forceResubmit range or all             -- resubmit jobs regardless to their status.
  -copyData [range [dest_se or dest_endpoint]] -- copy locally (in crab_working_dir/res dir) or on a remote SE your produced output,
                                                  already stored on remote SE.
  -renewCredential                        -- renew credential on the server.
  -clean                                  -- gracefully cleanup the directory of a task.
  -match|-testJdl [range]                 -- check if resources exist which are compatible with jdl.
  -report                                 -- print a short report about the task
  -list [range]                           -- show technical job details.
  -postMortem [range]                     -- provide a file with information useful for post-mortem analysis of the jobs.
  -printId                                -- print the SID for all jobs in task 
  -createJdl [range]                      -- provide files with a complete Job Description (JDL).
  -validateCfg [fname]                    -- parse the ParameterSet using the framework's Python API.
  -cleanCache                             -- clean SiteDB and CRAB caches.
  -uploadLog [jobid]                      -- upload main log files to a central repository
  -continue|-c [dir]                      -- Apply command to task stored in [dir].
  -h [format]                             -- Detailed help. Formats: man (default), tex, html, txt.
  -cfg fname                              -- Configuration file name. Default is 'crab.cfg'.
  -debug N                                -- set the verbosity level to N.
  -v                                      -- Print version and exit.

  "range" has syntax "n,m,l-p" which correspond to [n,m,l,l+1,...,p-1,p] and all possible combination

Example:
  crab -create -submit 1
"""
    print usa_string
    sys.exit(2)

###########################################################################
def help(option='man'):
    help_string = """
=pod

=head1 NAME

B<CRAB>:  B<C>ms B<R>emote B<A>nalysis B<B>uilder

"""+common.prog_name+""" version: """+common.prog_version_str+"""

This tool B<must> be used from an User Interface and the user is supposed to
have a valid Grid certificate.

=head1 SYNOPSIS

B<"""+common.prog_name+"""> [I<options>] [I<command>]

=head1 DESCRIPTION

CRAB is a Python program intended to simplify the process of creation and submission of CMS analysis jobs to the Grid environment .

Parameters for CRAB usage and configuration are provided by the user changing the configuration file B<crab.cfg>.

CRAB generates scripts and additional data files for each job. The produced scripts are submitted directly to the Grid. CRAB makes use of BossLite to interface to the Grid scheduler, as well as for logging and bookkeeping.

CRAB supports any CMSSW based executable, with any modules/libraries, including user provided ones, and deals with the output produced by the executable. CRAB provides an interface to CMS data discovery services (DBS and DLS), which are completely hidden to the final user. It also splits a task (such as analyzing a whole dataset) into smaller jobs, according to user requirements.

CRAB can be used in two ways: StandAlone and with a Server.
The StandAlone mode is suited for small task, of the order of O(100) jobs: it submits the jobs directly to the scheduler, and these jobs are under user responsibility.
In the Server mode, suited for larger tasks, the jobs are prepared locally and then passed to a dedicated CRAB server, which then interacts with the scheduler on behalf of the user, including additional services, such as automatic resubmission, status caching, output retrieval, and more.
The CRAB commands are exactly the same in both cases.

CRAB web page is available at

I<https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrab>

=head1 HOW TO RUN CRAB FOR THE IMPATIENT USER

Please, read all the way through in any case!

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.

Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list. A template and commented B<crab.cfg> can be found on B<$CRABDIR/python/full_crab.cfg> (detailed cfg) and B<$CRABDIR/python/minimal_crab.cfg> (only basic parameters) 

~>crab -create
  create all jobs (no submission!)

~>crab -submit 2 -continue [ui_working_dir]
  submit 2 jobs, the ones already created (-continue)

~>crab -create -submit 2
  create _and_ submit 2 jobs

~>crab -status
  check the status of all jobs

~>crab -getoutput
  get back the output of all jobs

~>crab -publish
  publish all user outputs in the DBS specified in the crab.cfg (dbs_url_for_publication) or written as argument of this option

=head1 RUNNING CMSSW WITH CRAB

=over 4

=item B<A)>

Develop your code in your CMSSW working area.  Do anything which is needed to run interactively your executable, including the setup of run time environment (I<cmsenv>), a suitable I<ParameterSet>, etc. It seems silly, but B<be extra sure that you actually did compile your code> I<scram b>.

=item B<B)>

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.  Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list.

The most important parameters are the following (see below for complete description of each parameter):

=item B<Mandatory!>

=over 6

=item B<[CMSSW]> section: datasetpath, pset, splitting parameters, output_file

=item B<[USER]> section: output handling parameters, such as return_data, copy_data etc...

=back

=item B<Run it!>

You must have a valid voms-enabled Grid proxy. See CRAB web page for details.

=back

=head1 RUNNING MULTICRAB

MultiCRAB is a CRAB extension to submit the same job to multiple datasets in one go.

The use case for multicrab is when you have your analysis code that you want to run on several datasets, typically some signals plus some backgrounds (for MC studies)
or on different streams/configuration/runs for real data taking. You want to run exactly the same code, and also the crab.cfg are different only for few keys:
for sure datasetpath but also other keys, such as eg total_number_of_events, in case you want to run on all signals but only a fraction of background, or anything else.
So far, you would have to create a set of crab.cfg, one for each dataset you want to access, and submit several instances of CRAB, saving the output to different locations.
Multicrab is meant to automatize this procedure.
In addition to the usual crab.cfg, there is a new configuration file called multicrab.cfg. The syntax is very similar to that of crab.cfg, namely
[SECTION]   <crab.cfg Section>.Key=Value

Please note that it is mandatory to add explicitly the crab.cfg [SECTION] in front of [KEY].
The role of multicrab.cfg is to apply modification to the template crab.cfg, some which are common to all tasks, and some which are task specific.

=head2  So there are two sections:

=over 2

=item B<[COMMON]>

section: which applies to all task, and which is fully equivalent to modify directly the template crab.cfg

=item B<[DATASET]>

section: there could be an arbitrary number of sections, one for each dataset you want to run. The names are free (but COMMON and MULTICRAB), and they will be used as ui_working_dir for the task as well as an appendix to the user_remote_dir in case of output copy to remote SE. So, the task corresponding to section, say [SIGNAL] will be placed in directory SIGNAL, and the output will be put on /SIGNAL/, so SIGNAL will be added as last subdir in the user_remote_dir.

=back

For further details please visit

I<https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideMultiCrab>

=head1 HOW TO RUN ON CONDOR-G

The B<Condor-G> mode for B<CRAB> is a special submission mode next to the standard Resource Broker submission. It is designed to submit jobs directly to a site and not using the Resource Broker.

Due to the nature of B<Condor-G> submission, the B<Condor-G> mode is restricted to OSG sites within the CMS Grid, currently the 7 US T2: Florida(ufl.edu), Nebraska(unl.edu), San Diego(ucsd.edu), Purdue(purdue.edu), Wisconsin(wisc.edu), Caltech(ultralight.org), MIT(mit.edu).

=head2 B<Requirements:>

=over 2

=item installed and running local Condor scheduler

(either installed by the local Sysadmin or self-installed using the VDT user interface: http://www.uscms.org/SoftwareComputing/UserComputing/Tutorials/vdt.html)

=item locally available LCG or OSG UI installation

for authentication via Grid certificate proxies ("voms-proxy-init -voms cms" should result in valid proxy)

=item set the environment variable GRID_WL_LOCATION to the edg directory of the local LCG or OSG UI installation

=back

=head2 B<What the Condor-G mode can do:>

=over 2

=item submission directly to multiple OSG sites,

the requested dataset must be published correctly by the site in the local and global services.
Previous restrictions on submitting only to a single site have been removed. SE and CE whitelisting
and blacklisting work as in the other modes.

=back

=head2 B<What the Condor-G mode cannot do:>

=over 2

=item submit jobs if no condor scheduler is running on the submission machine

=item submit jobs if the local condor installation does not provide Condor-G capabilities

=item submit jobs to an LCG site

=item support Grid certificate proxy renewal via the myproxy service

=back

=head2 B<CRAB configuration for Condor-G mode:>

The CRAB configuration for the Condor-G mode only requires one change in crab.cfg:

=over 2

=item select condor_g Scheduler:

scheduler = condor_g

=back


=head1 HOW TO RUN ON NORDUGRID ARC

The ARC scheduler can be used to submit jobs to sites running the NorduGrid
ARC grid middleware. To use it you need to have the ARC client
installed.

=head2 B<CRAB configuration for ARC mode:>

The ARC scheduler requires some changes to crab.cfg:

=over 2

=item B<scheduler:>

Select the ARC scheduler:
scheduler = arc

=item B<requirements>, B<additional_jdl_parameters:>

Use xrsl code instead of jdl for these parameters.

=item B<max_cpu_time>, B<max_wall_clock_time:>

When using ARC scheduler, for parameters max_cpu_time and max_wall_clock_time,
you can use units, e.g.  "72 hours" or "3 days", just like with the xrsl attributes
cpuTime and wallTime. If no unit is given, minutes is assumed by default.

=back

=head2 B<CRAB Commands:>

Most CRAB commands behave approximately the same with the ARC scheduler, with only some minor differences:

=over 2

=item B<*> B<-printJdl|-createJdl> will print xrsl code instead of jdl.

=back




=head1 COMMANDS

=head2 B<-create>

Create the jobs: from version 1_3_0 it is only possible to create all jobs.
The maximum number of jobs depends on dataset and splitting directives. This set of identical jobs accessing the same dataset are defined as a task.
This command create a directory with default name is I<crab_0_date_time> (can be changed via ui_working_dir parameter, see below). Inside this directory it is placed whatever is needed to submit your jobs. Also the output of your jobs (once finished) will be place there (see after). Do not cancel by hand this directory: rather use -clean (see).
See also I<-continue>.

=head2 B<-submit [range]>

Submit n jobs: 'n' is either a positive integer or 'all' or a [range]. The default is all.
If 'n' is passed as an argument, the first 'n' suitable jobs will be submitted. Please note that this is behaviour is different from other commands, where -command N means act the command to the job N, and not to the first N jobs. If a [range] is passed, the selected jobs will be submitted. In order to only submit job number M use this syntax (note the trailing comma): I<crab -submit M,>

This option may be used in conjunction with -create (to create and submit immediately) or with -continue (which is assumed by default) to submit previously created jobs. Failure to do so will stop CRAB and generate an error message.  See also I<-continue>.

=head2 B<-continue [dir] | -c [dir]>

Apply the action on the task stored in directory [dir]. If the task directory is the standard one (crab_0_date_time), the most recent in time is assumed. Any other directory must be specified.
Basically all commands (except -create) need -continue, so it is automatically assumed. Of course, the standard task directory is used in this case.

=head2 B<-status [options]>

Check the status of all jobs. With the server, the full status, including  application and wrapper exit codes, is available as soon as a job end. In StandAlone mode it is necessary to retrieve (crab -get) the job output first to obtain the exit codes. The status is printed on the console as a table with 7 columns: ID (identifier in the task), END (job completed or not. Crab server resubmit failed jobs, therefore: N=server is still working on this job, Y=server has done and status will not change anymore), STATUS (the job status), ACTION (some additional status info useful for experts), ExeExitCode (exit code from cmsRun, if not zero it means cmsRun failed), JobExitCode (the exit code assigned by Crab and reported by dashboard), E_HOST (the CE where the job executed). A list of comma separated options can be passed to -status (which do not accept a range). The option implmented are: I<-status short> which skip the detailed job-per-job status, printing only the summary; I<-status color> which add some coloring to the summary status. The color code is the following: Green for successfully finished jobs, Red for jobs which ended unsuccessfully, Blue for jobs done but not retireved, yellow for jobs still to be submitted, default color for all other jobs, namely those running or pending on the grid. The color will be used only if the output stream is capable of accepting it. The two options can coexist I<-status short,color>.

=head2 B<-getoutput|-get [range]>

Retrieve the output declared by the user via the output sandbox. By default the output will be put in task working dir under I<res> subdirectory. This can be changed via config parameters. B<Be extra sure that you have enough free space>. From version 2_3_x, the available free space is checked in advance. See I<range> below for syntax.

=head2 B<-publish>

Publish user output in a local DBS instance after the retrieval of output. By default publish uses the dbs_url_for_publication specified in the crab.cfg file, otherwise you can supply it as an argument of this option.
Warnings about publication:

CRAB publishes only EDM files (in the FJR they are written in the tag <File>)

CRAB publishes in the same USER dataset more EDM files if they are produced by a job and written in the tag <File> of FJR.

It is not possible for the user to select only one file to publish, nor to publish two files in two different USER datasets.


=head2 B<-checkPublication [-USER.dbs_url_for_publication=dbs_url -USER.dataset_to_check=datasetpath -debug]>

Check if a dataset is published in a DBS. This option is automaticaly called at the end of the publication step, but it can be also used as a standalone command. By default it reads the parameters (USER.dbs_url_for_publication and USER.dataset_to_check) in your crab.cfg. You can overwrite the defaults in crab.cfg by passing these parameters as option. Using the -debug option, you will get detailed info about the files of published blocks.

=head2 B<-publishNoInp>

To be used only if you know why and you are of sure what you are doing, or if crab support persons told you to use it.It is meant for situations where crab -publish fails because framework job report xml file contains input files not present in DBS. It will publish the dataset anyhow, while marking it as Unknown Provenace to indicate that parentage information is partial. Those dataset will not be accepted for promotion to Global Scope DBS. In all other respects this works as crab -publish

=head2 B<-resubmit range or all>

Resubmit jobs which have been previously submitted and have been either I<killed> or are I<aborted>. See I<range> below for syntax. Also possible with key I<bad>, which will resubmit all jobs in I<killed> or I<aborted> or I<failed submission> or I<retrieved> but with exit status not 0 (with the exception for wrapper exit status equal 60307).

=head2 B<-forceResubmit range or all>

iSame as -resubmit but without any check about the actual status of the job: please use with caution, you can have problem if both the original job and the resubmitted ones actually run and tries to write the output ona a SE. This command is meant to be used if the killing is not possible or not working but you know that the job failed or will. See I<range> below for syntax.

=head2 B<-kill [range]>

Kill (cancel) jobs which have been submitted to the scheduler. A range B<must> be used in all cases, no default value is set.

=head2 B<-copyData [range -dest_se=the official SE name or -dest_endpoint=the complete endpoint of the remote SE]>

Option that can be used only if your output have been previously copied by CRAB on a remote SE.
By default the copyData copies your output from the remote SE locally on the current CRAB working directory (under res). Otherwise you can copy the output from the remote SE to another one, specifying either -dest_se=<the remote SE official name> or -dest_endpoint=<the complete endpoint of remote SE>. If dest_se is used, CRAB finds the correct path where the output can be stored.

Example: crab -copyData  --> output copied to crab_working_dir/res directory
         crab -copyData -dest_se=T2_IT_Legnaro -->  output copied to the legnaro SE, directory discovered by CRAB
         crab -copyData -dest_endpoint=srm://<se_name>:8443/xxx/yyyy/zzzz --> output copied to the se <se_name> under
         /xxx/yyyy/zzzz directory.

=head2 B<-renewCredential >

If using the server modality, this command allows to delegate a valid credential (proxy/token) to the server associated with the task.

=head2 B<-match|-testJdl [range]>

Check if the job can find compatible resources. It is equivalent of doing I<glite-wms-job-list-match> on edg.

=head2 B<-printId>

Just print the Scheduler Job Identifierb (Grid job identifier e.g.) of the jobs in the task.

=head2 B<-createJdl [range]>

Collect the full Job Description in a file located under share directory. The file base name is File- .

=head2 B<-postMortem [range]>

Try to collect more information of the job from the scheduler point of view.
And this is the only way to obtain info about failure reason of aborted jobs.

=head2 B<-list [range]>

Dump technical information about jobs: for developers only.

=head2 B<-report>

Print a short report about the task, namely the total number of events and files processed/requested/available, the name of the dataset path, a summary of the status of the jobs, and so on. A summary file of the runs and luminosity sections processed is written to res subdirecttory as lumiSummary.json and can be used as input to tools that compute the luminosity like lumiCalc.py. In the same subdirectory also a file containing all the input runs and lumis, called InputLumiSummaryOfTask.json and the file containing the missing runs and lumis due to failed jobs, called missingLumiSummary.json are produced. The missingLumiSummary.json can be use as lumimask file to create a new task in order to analyse the missing data (instead of failure jobs resubmission).      

=head2 B<-clean [dir]>

Clean up (i.e. erase) the task working directory after a check whether there are still running jobs. In case, you are notified and asked to kill them or retrieve their output. B<Warning> this will possibly delete also the output produced by the task (if any)!

=head2 B<-cleanCache>

Clean up (i.e. erase) the SiteDb and CRAB cache content.

=head2 B<-uploadLog [jobid]>

Upload main log files to a central repository. It prints a link to be forwared to supporting people (eg: crab feedback hypernews).

It can optionally take a job id as input. It does not allow job ranges/lists.

Uploaded files are: crab.log, crab.cfg, job logging info, summary file and a metadata file.
If you specify the jobid, also the job standard output and fjr will be uploaded. Warning: in this case you need to run the getoutput before!!
In the case of aborted jobs you have to upload the postMortem file too, creating it with crab -postMortem jobid and then uploading files specifying the jobid number. 

=head2 B<-validateCfg [fname]>

Parse the ParameterSet using the framework\'s Python API in order to perform a sanity check of the CMSSW configuration file.
You have to create your task with crab -create and then to validate the config file with crab -validateCfg.

=head2 B<-help [format] | -h [format]>

This help. It can be produced in three different I<format>: I<man> (default), I<tex> and I<html>.

=head2 B<-v>

Print the version and exit.

=head2 B<range>

The range to be used in many of the above commands has the following syntax. It is a comma separated list of jobs ranges, each of which may be a job number, or a job range of the form first-last.
Example: 1,3-5,8 = {1,3,4,5,8}

=head1 OPTIONS

=head2 B<-cfg [file]>

Configuration file name. Default is B<crab.cfg>.

=head2 B<-debug [level]>

Set the debug level: high number for high verbosity.

=head1 CONFIGURATION PARAMETERS

All the parameter describe in this section can be defined in the CRAB configuration file. The configuration file has different sections: [CRAB], [USER], etc. Each parameter must be defined in its proper section. An alternative way to pass a config parameter to CRAB is via command line interface; the syntax is: crab -SECTION.key value . For example I<crab -USER.outputdir MyDirWithFullPath> .
The parameters passed to CRAB at the creation step are stored, so they cannot be changed by changing the original crab.cfg . On the other hand the task is protected from any accidental change. If you want to change any parameters, this require the creation of a new task.
Mandatory parameters are flagged with a *.

=head2 B<[CRAB]>

=head3 B<jobtype *>

The type of the job to be executed: I<cmssw> jobtypes are supported. No default value.

=head3 B<scheduler *>

The scheduler to be used: <remoteglidein> is the schedulers to be used for grid submission. Some local scheduler are supported by the community. I<LSF> is the standard CERN local scheduler or I<CAF> which is LSF dedicated to CERN Analysis Facilities. I<condor> is the scheduler to submit jobs to US LPC CAF, or I<arc> scheduler to be used with the NorduGrid ARC middleware. No default value.

=head3 B<use_server>

To use the server for job handling. Only 0=no (default) is supported. This parameter should not be used.


=head2 B<[CMSSW]>

=head3 B<datasetpath *>

The path of the processed or analysis dataset as defined in DBS. It comes with the format I</PrimaryDataset/DataTier/Process[/OptionalADS]>. If no input is needed I<None> must be specified. When running on an analysis dataset, the job splitting must be specified by luminosity block rather than event. Analysis datasets are only treated accurately on a lumi-by-lumi level with CMSSW 3_1_x and later. No default value.

=head3 B<runselection *>

Within a dataset you can restrict to run on a specific run number or run number range. For example runselection=XYZ or runselection=XYZ1-XYZ2 . Run number range will include both run XYZ1 and XYZ2. Combining runselection with a lumi_mask runs on the intersection of the two lists. No default value

=head3 B<use_parent>

Within a dataset you can ask to run over the related parent files too. E.g., this will give you access to the RAW data while running over a RECO sample. Setting use_parent=1 CRAB determines the parent files from DBS and will add secondaryFileNames = cms.untracked.vstring( <LIST of parent FIles> ) to the pool source section of your parameter set.
This setting is supposed to works both with Splitting by Lumis and Splitting by Events. Default value = 0. 

=head3 B<pset *>

The python ParameterSet to be used. No default value.

=head3 B<pycfg_params *>

These parameters are passed to the python config file, as explained in https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideAboutPythonConfigFile#Passing_Command_Line_Arguments_T

=head3 B<lumi_mask>

The filename of a JSON file that describes which runs and lumis to process. CRAB will skip luminosity blocks not listed in the file. When using this setting, you must also use the split by lumi settings rather than split by event as described below. Combining runselection with a lumi_mask runs on the intersection of the two lists. No default value.

=head3 B<Splitting jobs by Lumi>

=over 4

=item B<NOTE: Exactly two of these three parameters must be used: total_number_of_lumis, lumis_per_job, number_of_jobs.> Split by lumi (or by run, explained below) is required for real data. Because jobs in split by lumi mode process entire rather than partial files, you will often end up with fewer jobs processing more lumis than you are expecting. Additionally, a single job cannot analyze files from multiple blocks in DBS. All job splitting parameters in split by lumi mode are "advice" to CRAB rather than determinative.

=back

=head4 B<total_number_of_lumis *>

The number of luminosity blocks to be processed. -1 for processing a whole dataset. Your task will process this many lumis regardless of how the jobs are actually split up. If you do not specify this, the total number of lumis processed will be number_of_jobs x lumis_per_job. No default value

=head4 B<lumis_per_job *>

The number of luminosity blocks to be accessed by each job. Since a job cannot access less than a whole file, it may be that the actual number of lumis per job is more than you asked for. NO default value

=head4 B<number_of_jobs *>

Define the number of jobs to be run for the task. This parameter is common between split by lumi and split by event modes. In split by lumi mode, the number of jobs will only approximately match this value. No default value

=head3 B<Splitting jobs by Event>

=over 4

=item B<NOTE: Exactly two of these three parameters must be used: total_number_of_events, events_per_job, number_of_jobs.> Otherwise CRAB will complain. Only MC data can be split by event. No default value

=back

=head4 B<total_number_of_events *>

The number of events to be processed. To access all available events, use I<-1>. Of course, the latter option is not viable in case of no input. In this case, the total number of events will be used to split the task in jobs, together with I<events_per_job>. No default value.

=head4 B<events_per_job*>

The number of events to be accessed by each job. Since a job cannot cross the boundary of a fileblock it might be that the actual number of events per job is not exactly what you asked for. It can be used also with no input. No default value.

=head4 B<number_of_jobs *>

Define the number of jobs to be run for the task. The number of events for each job is computed taking into account the total number of events required as well as the granularity of EventCollections. Can be used also with No input. No default value.

=head4 B<split_by_event *>

This setting is for experts only. If you don't know why you want to use it, you don't want to use it.  Set the value to 1 to enabe split by event on data. CRAB then behaves like old versions of CRAB which did not enforce split by lumi for data. Default value = 0.

=head3 B<split_by_run>

To activate the split run based (each job will access a different run) use I<split_by_run>=1. You can also define I<number_of_jobs>  and/or I<runselection>. NOTE: the Run Based combined with Event Based split is not available. Default value = 0.

=head3 B<output_file *>

The output files produced by your application (comma separated list). From CRAB 2_2_2 onward, if TFileService is defined in user Pset, the corresponding output file is automatically added to the list of output files. User can avoid this by setting B<skip_TFileService_output> = 1 (default is 0 == file included). The Edm output produced via PoolOutputModule can be automatically added by setting B<get_edm_output> = 1 (default is 0 == no). B<warning> it is not allowed to have a PoolOutputSource and not save it somewhere, since it is a waste of resource on the WN. In case you really want to do that, and if you really know what you are doing (hint: you dont!) you can user I<ignore_edm_output=1>. No default value.

=head3 B<skip_TFileService_output>

Force CRAB to skip the inclusion of file produced by TFileService to list of output files. Default value = 0, namely the file is included.

=head3 B<get_edm_output>

Force CRAB to add the EDM output file, as defined in PSET in PoolOutputModule (if any) to be added to the list of output files. Default value = 0 (== no inclusion)

=head3 B<increment_seeds>

Specifies a comma separated list of seeds to increment from job to job. The initial value is taken
from the CMSSW config file. I<increment_seeds=sourceSeed,g4SimHits> will set sourceSeed=11,12,13 and g4SimHits=21,22,23 on
subsequent jobs if the values of the two seeds are 10 and 20 in the CMSSW config file.

See also I<preserve_seeds>. Seeds not listed in I<increment_seeds> or I<preserve_seeds> are randomly set for each job.

=head3 B<preserve_seeds>

Specifies a comma separated list of seeds to which CRAB will not change from their values in the user
CMSSW config file. I<preserve_seeds=sourceSeed,g4SimHits> will leave the Pythia and GEANT seeds the same for every job.

See also I<increment_seeds>. Seeds not listed in I<increment_seeds> or I<preserve_seeds> are randomly set for each job.

=head3 B<first_lumi>

Relevant only for Monte Carlo production for which it defaults to 1. The first job will generate events with this lumi block number, subsequent jobs will
increment the lumi block number. Setting this number to 0 (not recommend) means CMSSW will not be able to read multiple such files as they
will all have the same run, lumi and event numbers. This check in CMSSW can be bypassed by setting
I<process.source.duplicateCheckMode = cms.untracked.string('noDuplicateCheck')> in the input source, should you need to
read files produced without setting first_run (in old versions of CRAB) or first_lumi. Default value = 1.

=head3 B<generator>

Name of the generator your MC job is using. Some generators require CRAB to skip events, others do not.
Possible values are pythia (default), comphep, lhe, and madgraph. This will skip events in your generator input file.

=head3 B<executable>

The name of the executable to be run on remote WN. The default is cmsrun. The executable is either to be found on the release area of the WN, or has been built on user working area on the UI and is (automatically) shipped to WN. If you want to run a script (which might internally call I<cmsrun>, use B<USER.script_exe> instead. Default value = cmsRun. 

=head3 I<DBS and DLS parameters:>

=head3 B<dbs_url>

The URL of the DBS query page. For expert only. Default value the global DBS http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet

=head3 B<show_prod>

To enable CRAB to show data hosted on Tier1s sites specify I<show_prod> = 1. By default those data are masked.

=head3 B<subscribed>

By setting the flag I<subscribed> = 1 only the replicas that are subscribed to its site are considered.The default is to return all replicas. The intended use of this flag is to avoid sending jobs to sites based on data that is being moved or deleted (and thus not subscribed).

=head3 B<no_block_boundary>

To remove fileblock boundaries in job splitting specify I<no_block_boundary> = 1. Default value = 0.

=head3 B<use_dbs3>

To use DBS3 for data discovery: I<use_dbs3> = 1. Default value =1. DBS2 local scope analysis_01/02 will be automatically mapped to DBS3 phys_01/02.
Alternatively use I<dbs_url> to indicate an explicit DBS3 endpoint

=head3 B<verify_dbs23>

To use verify data discovery information retrieved from DBS2 and DBS3:  I<verify_dbs23> = 1. Will not affect which DBS is used for task creation, but will read from the other as well and compare retrieved informations.

=head2 B<[USER]>

=head3 B<additional_input_files>

Any additional input file you want to ship to WN: comma separated list. IMPORTANT NOTE: they will be placed in the WN working dir, and not in ${CMS_SEARCH_PATH}. Specific files required by CMSSW application must be placed in the local scram directory tree in a location that will be automatically added to ${CMS_SEARCH_PATH} (e.g. $CMSSW_BASE/src), which will be automatically shipped by CRAB itself, without specifying them as additional_input_files. You do not need to specify the I<ParameterSet> you are using, which will be included automatically. Wildcards are allowed. No default value. 

=head3 B<script_exe>

A user script that will be run on WN (instead of default cmsRun). It is up to the user to setup properly the script itself to run on WN enviroment. CRAB guarantees that the CMSSW environment is setup (e.g. scram is in the path) and that the modified pset.py will be placed in the working directory, with name pset.py . The user must ensure that a properly name job report will be written, this can be done e.g. by calling cmsRun within the script as "cmsRun -j $RUNTIME_AREA/crab_fjr_$NJob.xml -p pset.py". The script itself will be added automatically to the input sandbox so user MUST NOT add it within the B<USER.additional_input_files>.
Arguments: CRAB does automatically pass the job index as the first argument of script_exe.
The MaxEvents number is set by CRAB in the environment variable "$MaxEvents". So the script can reads this value directly from there. No default value.

=head3 B<script_arguments>

Any arguments you want to pass to the B<USER.script_exe>:  comma separated list.
CRAB does automatically pass the job index as the first argument of script_exe.
The MaxEvents number is set by CRAB in the environment variable "$MaxEvents". So the script can read this value directly from there. No default value. 

=head3 B<ui_working_dir>

Name of the working directory for the current task. By default, a name I<crab_0_(date)_(time)> will be used. If this card is set, any CRAB command which require I<-continue> need to specify also the name of the working directory. A special syntax is also possible, to reuse the name of the dataset provided before: I<ui_working_dir : %(dataset)s> . In this case, if e.g. the dataset is SingleMuon, the ui_working_dir will be set to SingleMuon as well. Default value = crab_0_(date)_(time).

=head3 B<thresholdLevel>

This has to be a value between 0 and 100, that indicates the percentage of task completeness (jobs in a ended state are complete, even if failed). The server will notify the user by e-mail (look at the field: B<eMail>) when the task will reach the specified threshold. Works just when using the server. Default value = 100.

=head3 B<eMail>

The server will notify the specified e-mail when the task will reaches the specified B<thresholdLevel>. A notification is also sent when the task will reach the 100\% of completeness. This field can also be a list of e-mail: "B<eMail = user1@cern.ch, user2@cern.ch>". Works just when using the server. No default value.

=head3 B<client>

Specify the client storage protocol that can be used to interact with the server in B<CRAB.server_name>. The default is the value in the server configuration.

=head3 B<return_data *>

The output produced by the executable on WN is returned (via output sandbox) to the UI, by issuing the I<-getoutput> command. B<Warning>: this option should be used only for I<small> output, say less than 10MB, since the sandbox cannot accommodate big files. Depending on Resource Broker used, a size limit on output sandbox can be applied: bigger files will be truncated. To be used in alternative to I<copy_data>. Default value = 0. 

=head3 B<outputdir>

To be used together with I<return_data>. Directory on user interface where to store the output. Full path is mandatory, "~/" is not allowed: the default location of returned output is ui_working_dir/res . BEWARE: does not work with scheduler=CAF

=head3 B<logdir>

To be used together with I<return_data>. Directory on user interface where to store the standard output and error. Full path is mandatory, "~/" is not allowed: the default location of returned output is ui_working_dir/res .

=head3 B<copy_data *>

The output (only the file produced by the analysis executable, not the std-out and err) is copied to a Storage Element of your choice (see below). To be used as an alternative to I<return_data> and recommended in case of large output. Default value = 0.

=head3 B<storage_element>

To be used with <copy_data>=1
If you want to copy the output of your analysis in a official CMS Tier2 or Tier3, you have to write the CMS Site Name of the site, e.g. as written in SiteDB at https://cmsweb.cern.ch/sitedb/prod/sites (i.e T2_IT_legnaro). You have also to specify the <remote_dir>(see below)

If you want to copy the output in a not_official_CMS remote site you have to specify the complete storage element name (i.e se.xxx.infn.it).You have also to specify the <storage_path> and the <storage_port> if you do not use the default one(see below). No default value.

=head3 B<user_remote_dir>

To be used with <copy_data>=1 and <storage_element> official CMS sites.
This is the directory or tree of directories where your output will be stored. This directory will be created under the mountpoint ( which will be discover by CRAB if an official CMS storage Element has been used, or taken from the crab.cfg as specified by the user). B<NOTE> This part of the path will be used as logical file name of your files in the case of publication without using an official CMS storage Element. Generally it should start with "/store".

=head3 B<storage_path>

To be used with <copy_data>=1 and <storage_element> not official CMS sites.
This is the full path of the Storage Element writeable by all, the mountpoint of SE (i.e /srm/managerv2?SFN=/pnfs/se.xxx.infn.it/yyy/zzz/)
No default value.

=head3 B<storage_port>

To choose the storage port specify I<storage_port> = N. Default value = 8443.

=head3 B<caf_lfn>
Running at CAF, you can decide in which mountpoint to copy your output, by selecting the first part of LFN.
The default value is /store/caf/user.
To test eos area you can use caf_lfn = /store/eos/user

=head3 B<local_stage_out *>

This option enables the local stage out of produced output to the "close storage element" where the job is running, in case of failure of the remote copy to the Storage element decided by the user in che crab.cfg. It has to be used with the copy_data option. In the case of backup copy, the publication of data is forbidden. Set I<local_stage_out> = 1. Default value = 0.

=head3 B<publish_data*>

To be used with <copy_data>=1
To publish your produced output in a local istance of DBS set publish_data = 1
All the details about how to use this functionality are written in https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabForPublication
N.B 1) if you are using an official CMS site to stored data, the remote dir will be not considered. The directory where data will be stored is decided by CRAB, following the CMS policy in order to be able to re-read published data.
2) if you are using a not official CMS site to store data, you have to check the <lfn>, that will be part of the logical file name of you published files, in order to be able to re-read the data.
Default value = 0.

=head3 B<publish_data_name>

You produced output will be published in your local DBS with dataset name <primarydataset>/<publish_data_name>/USER. No default value.

=head3 B<dbs_url_for_publication [obsolete]>

Specify the URL of your local DBS istance where CRAB has to publish the output files. No default value.


=head3 B<xml_report>

To be used to switch off the screen report during the status query, enabling the db serialization in a file. Specifying I<xml_report> = FileName CRAB will serialize the DB into CRAB_WORKING_DIR/share/FileName. No default value.

=head3 B<usenamespace>

To use the automate namespace definition (perfomed by CRAB) it is possible to set I<usenamespace>=1. The same policy used for the stage out in case of data publication will be applied. Default value = 0.

=head3 B<debug_wrapper>

To enable the higer verbose level on wrapper specify I<debug_wrapper> = 1. The Pset contents before and after the CRAB maipulation will be written together with other useful infos. Default value = 0.

=head3 B<deep_debug>

To be used in case of unexpected job crash when the sdtout and stderr files are lost. Submitting again the same jobs specifying I<deep_debug> = 1 these files will be reported back. NOTE: it works only on standalone mode for debugging purpose.

=head3 B<dontCheckSpaceLeft>

Set it to 1 to skip the check of free space left on your working directory before attempting to get the output back. Default is 0 (=False)

=head3 B<check_user_remote_dir>

To avoid stage out failures CRAB checks the remote location content at the creation time. By setting I<check_user_remote_dir>=0  crab will skip the check. Default value = 0.

=head3 B<tasktype>

Expert only parameter. Not to be used. Default value = analysis.

=head3 B<ssh_control_persist>

Expert only parameter. Not to be used. Default value = 3600. Behaves like ControlPersist in ssh_config but time is only supported in seconds.

=head2 B<[GRID]>

in square brackets the name of the schedulers this parameter applies to in case it does not apply to all

=head3 B<RB [glite]>

Which WMS you want to use instead of the default one, as defined in the configuration file automatically downloaded by CRAB from CMSDOC web page. You can use any other WMS which is available, if you provide the proper configuration files. E.g., for gLite WMS XYZ, you should provide I< 0_GET_glite_wms_XXX.conf> where XXX is the RB value. These files are searched for in the cache dir (~/.cms_crab), and, if not found, on cmsdoc web page. So, if you put your private configuration files in the cache dir, they will be used, even if they are not available on crab web page. 
Please get in contact with crab team if you wish to provide your WMS as a service to the CMS community.

=head3 B<role [glite]>

The role to be set in the VOMS. Beware that simultaneus use of I<role> and I<group> is not supported. See VOMS documentation for more info. No default value.

=head3 B<group [glite]>

The group to be set in the VOMS. Beware that simultaneus use of I<role> and I<group> is not supported. See VOMS documentation for more info. No default value.

=head3 B<dont_check_proxy>

If you do not want CRAB to check your proxy. The creation of the proxy (with proper length), its delegation to a myproxyserver is your responsibility.

=head3 B<dont_check_myproxy>

If you want to to switch off only the proxy renewal set I<dont_check_myproxy>=1. The proxy delegation to a myproxyserver is your responsibility. Default value = 0.

=head3 B<requirements [glite]>

Any other requirements to be add to JDL. Must be written in compliance with JDL syntax (see LCG user manual for further info). No requirement on Computing element must be set. No default value.

=head3 B<additional_jdl_parameters [glite, remoteGlidein]>

Any other parameters you want to add to jdl file:semicolon separated list, each
item in the list must, including the closing ";". No default value.
  Works both for gLite and remoteGlidein

=head3 B<wms_service [glite]>

With this field it is also possible to specify which WMS you want to use (https://hostname:port/pathcode) where "hostname" is WMS name, the "port" generally is 7443 and the "pathcode" should be something like "glite_wms_wmproxy_server". No default value.

=head3 B<max_wall_clock_time>

Maximum wall clock time needed to finish one job.It will be used to select a suitable place to run the job. Short running jobs have a higher chance to start sooner if this is set to a proper value (less then the default). Job will be terminated by Crab if it runs over the limit and log will be returned to user. Time in minutes. Default value is 21 hours and 50 minutes. Only works if used in crab configuration file. Can not be overridden via command line in later crab commands.

=head3 B<max_cpu_time>

Maximum CPU time needed to finish one job. It is not recommended, better use only max_wall_clock_time, in which case max_cpu_time is automatically set to the same value as max_wall_clock_time. Only works if used in crab confifuration file. Can not be overridden via command line in later crab commands. 

=head3 B<max_rss [remoteGlidein]>

Maximum Resident Set Size (memory)  needed for one job. It will be used to select a suitable queue on the CE and to adjust the crab watchdog. Memory need in Mbytes. Default value = 2300. Only works if used in crab confifuration file. Can not be overridden via command line in later crab commands. 

=head3 B<ce_black_list [glite]>

All the CE (Computing Element) whose name contains the following strings (comma separated list) will not be considered for submission.  Use the dns domain (e.g. fnal, cern, ifae, fzk, cnaf, lnl,....). You may use hostnames or CMS Site names (T2_DE_DESY) or substrings.
By default T0 and T1s site are in blacklist. 

=head3 B<ce_white_list[glite]>

Only the CE (Computing Element) whose name contains the following strings (comma separated list) will be considered for submission.  Use the dns domain (e.g. fnal, cern, ifae, fzk, cnaf, lnl,....). You may use hostnames or CMS Site names (T2_DE_DESY) or substrings. Please note that if the selected CE(s) does not contain the data you want to access, no submission can take place.

=head3 B<se_black_list [glite,glidein,remoteGlidein]>

Sites whose SE (Storage Element) whose name contains the following strings (comma separated list) will not be considered for submission. You may use hostnames or CMS Site names (T2_DE_DESY) or substrings. With glite scheduler it works only if a datasetpath is specified.

=head3 B<se_white_list [glite,glidein,remoteGlidein]>

Only sites whose SE (Storage Element) whose name contains the following strings (comma separated list) will be considered for submission. If the selected CE(s) does not contain the data you want to access, no submission can take place. You may use hostnames or CMS Site names (T2_DE_DESY) or substrings.  With glite scheduler it works only if a datasetpath is specified.

=head3 B<remove_default_blacklist [glite]>

CRAB enforce the T1s Computing Eelements Black List. By default it is appended to the user defined I<CE_black_list>. To remove the enforced T1 black lists set I<remove_default_blacklist>=1. Default value = 0.

=head3 B<data_location_override [remoteGlidein]>

Overrides the data location list obtained from DLS/PhEDEx with the list of sites indicated. Same syntax as se_white_list. Up to the user to make sure that needed data can be read nevertheless. Note: ONLY WORKS INSIDE crab.cfg at crab -create time, not when issued in the command line as crab -submit -GRID.data_location_override=...

=head3 B<allow_overflow [remoteGlidein]>

Tells glidein wether it can overlow this job, i.e. run at another site and access data via xrootds if the sites were data are located are full. Set to 0 to disallow overflow. Default value = 1.

=head2 B<[LSF]> or B<[CAF]> or B<[PBS]> or B<[SGE]>

=head3 B<queue>

The LSF/PBS queue you want to use: if none, the default one will be used. For CAF, the proper queue will be automatically selected.

=head3 B<resource>

The resources to be used within a LSF/PBS queue. Again, for CAF, the right one is selected.

=head3 B<group>

The physics GROUP which the user belong to ( it is for example PHYS_SUSY etc...). By specifying that the LSF accounting and fair share per sub-group is done properly.

=head1 FILES

I<crab> uses a configuration file I<crab.cfg> which contains configuration parameters. This file is written in the INI-style.  The default filename can be changed by the I<-cfg> option.

I<crab> creates by default a working directory 'crab_0_E<lt>dateE<gt>_E<lt>timeE<gt>'

I<crab> saves all command lines in the file I<crab.history>.

I<crab> downloads some configuration files from internet and keeps cached copies in ~/.cms_crab and ~/.cms_sitedbcache directories. The location of those caches can be redirected using the enviromental variables CMS_SITEDB_CACHE_DIR and  CMS_CRAB_CACHE_DIR

=head1 HISTORY

B<CRAB> is a tool for the CMS analysis on the Grid environment. It is based on the ideas from CMSprod, a production tool originally implemented by Nikolai Smirnov.

=head1 AUTHORS

"""
    author_string = '\n'
    for auth in common.prog_authors:
        #author = auth[0] + ' (' + auth[2] + ')' + ' E<lt>'+auth[1]+'E<gt>,\n'
        author = auth[0] + ' E<lt>' + auth[1] +'E<gt>,\n'
        author_string = author_string + author
        pass
    help_string = help_string + author_string[:-2] + '.'\
"""

=cut
"""

    pod = tempfile.mktemp()+'.pod'
    pod_file = open(pod, 'w')
    pod_file.write(help_string)
    pod_file.close()

    if option == 'man':
        man = tempfile.mktemp()
        pod2man = 'pod2man --center=" " --release=" " '+pod+' >'+man
        os.system(pod2man)
        os.system('man '+man)
        pass
    elif option == 'tex':
        fname = common.prog_name+'-v'+common.prog_version_str
        tex0 = tempfile.mktemp()+'.tex'
        pod2tex = 'pod2latex -full -out '+tex0+' '+pod
        os.system(pod2tex)
        tex = fname+'.tex'
        tex_old = open(tex0, 'r')
        tex_new = open(tex,  'w')
        for s in tex_old.readlines():
            if string.find(s, '\\begin{document}') >= 0:
                tex_new.write('\\title{'+common.prog_name+'\\\\'+
                              '(Version '+common.prog_version_str+')}\n')
                tex_new.write('\\author{\n')
                for auth in common.prog_authors:
                    tex_new.write('   '+auth[0]+
                                  '\\thanks{'+auth[1]+'} \\\\\n')
                tex_new.write('}\n')
                tex_new.write('\\date{}\n')
            elif string.find(s, '\\tableofcontents') >= 0:
                tex_new.write('\\maketitle\n')
                continue
            elif string.find(s, '\\clearpage') >= 0:
                continue
            tex_new.write(s)
        tex_old.close()
        tex_new.close()
        print 'See '+tex
        pass
    elif option == 'html':
        fname = common.prog_name+'-v'+common.prog_version_str+'.html'
        pod2html = 'pod2html --title='+common.prog_name+\
                   ' --infile='+pod+' --outfile='+fname
        os.system(pod2html)
        print 'See '+fname
        pass
    elif option == 'txt':
        fname = common.prog_name+'-v'+common.prog_version_str+'.txt'
        pod2text = 'pod2text '+pod+' '+fname
        os.system(pod2text)
        print 'See '+fname
        pass

    sys.exit(0)
