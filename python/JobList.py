from NameSpace import NameSpace
from WorkSpace import WorkSpace
import common

import os

class JobList:
    """
    Container for jobs (class Job).
    Accessed via indexing, e.g.,
    jn = jobs[nj].number()
    """
    def __init__(self, njobs=1, jtype=None):

        self._job_type = jtype
        self._job_list = []

        if jtype : base = self._job_type.name()
        else     : base = ''

        for nj in range(njobs):
            job = NameSpace.Job(jtype, nj)
            self._job_list.append(job)
            self._job_list[nj].setStdout(base + '_' + job.number() + '.stdout')
            self._job_list[nj].setStderr(base + '_' + job.number() + '.stderr')
            pass

    def __getitem__(self, nj):
        return self._job_list[nj]

    def __setitem__(self, nj, value):
        self._job_list[nj] = value
        return

    def __len__(self):
        return len(self._job_list)

    def __str__(self):
        txt = ''
        for nj in range(len(self)):
            txt = txt + 'Job ' + `nj` + ':\n'
            txt = txt + `self._job_list[nj]`+'\n'
            pass
        return txt

    def type(self):
        """Returns job_type_object."""
        return self._job_type

    def setCfgNames(self,pattern):
        job_dir = common.work_space.jobDir()
        for nj in range(len(self._job_list)) :
            fname = job_dir + pattern
            self._job_list[nj].setConfigFilename(fname)

        return

    def setScriptNames(self, pattern):
        job_dir = common.work_space.jobDir()
        for nj in range(len(self._job_list)) :
            fname = job_dir + pattern
            self._job_list[nj].setScriptFilename(fname)
            pass
        return

    def setJDLNames(self, pattern):
        (path, ext) = os.path.splitext(pattern)
        job_dir = common.work_space.jobDir()
        for nj in range(len(self._job_list)) :
            num = self._job_list[nj].number()
            fname = job_dir + path + '_' + num + ext
            self._job_list[nj].setJdlFilename(fname)
            pass
        return
