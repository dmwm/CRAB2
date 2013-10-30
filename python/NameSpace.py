#!/usr/bin/env python

class NameSpace:
    def __init__(self): 
        raise RuntimeError("This Class is a namespace, dont instantiate it")
        
    class Job: 
        """
        Describes individual job production parameters.
        """
        def __init__(self, jtype, nj):
            self._job_type = jtype              # job type object
            self._job_number = nj               # job number
            self._jn_str = '%06d' % nj          # job number string, eg '000014'

            self._stdout = ''                   # filename for stdout
            self._stderr = ''                   # filename for stderr
            
            self._cfg_fname      = ''           # cfg file
            self._jdl_fname      = ''           # JDL filename
            self._script_fname   = ''           # script filename
            return

        def __str__(self):
            txt = ''
            for k in self.__dict__.keys():
                if self.__dict__[k] == None:
                    txt = txt + k + ' = None\n'
                    pass
                else:
                    txt = txt + k + ' = ' + self.__dict__[k] + '\n'
                    pass
                pass
            return txt

        def type(self):
            """Returns job_type_object."""
            return self._job_type

        def number(self):
            """
            Returns 6-digit job number as a string with leading zeros,
            e.g. '000123'.
            """
            return self._jn_str

        def setStdout(self, fname):
            self._stdout = fname
            return

        def stdout(self):
            return self._stdout

        def setStderr(self, fname):
            self._stderr = fname
            return

        def stderr(self):
            return self._stderr

        def setConfigFilename(self, fname):
            self._cfg_fname = fname
            return

        def configFilename(self):
            """
            Returns cards filename for the current job.
            """
            return self._cfg_fname

        def setScriptFilename(self, fname):
            self._script_fname = fname
            return

        def scriptFilename(self):
            """Returns job's script filename."""
            return self._script_fname

        def setJdlFilename(self, fname):
            self._jdl_fname = fname
            return

        def jdlFilename(self):
            """Returns job's JDL filename."""
            return self._jdl_fname
