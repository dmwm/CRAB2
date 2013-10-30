from DashboardAPI import apmonSend, apmonFree    

class ApmonIf:
    """
    Provides an interface to the Monalisa Apmon python module
    """
    def __init__(self, taskid=None, jobid=None) :
        self.taskId = taskid
        self.jobId = jobid
        self.fName = 'mlCommonInfo'

    #def fillDict(self, parr):
    #    """
    #    Obsolete
    #    """
    #    pass
    
    def sendToML(self, params, jobid=None, taskid=None):
        # Figure out taskId and jobId
        taskId = 'unknown'
        jobId = 'unknown'
        # taskId
        if self.taskId is not None :
            taskId = self.taskId
        if params.has_key('taskId') :
            taskId = params['taskId']
        if taskid is not None :
            taskId = taskid
        # jobId
        if self.jobId is not None :
            jobId = self.jobId
        if params.has_key('jobId') :
            jobId = params['jobId']
        if jobid is not None :
            jobId = jobid
        # Send to Monalisa
        apmonSend(taskId, jobId, params)
            
    def free(self):
        apmonFree()

if __name__ == '__main__' :
    apmonIf = ApmonIf()
    apmonIf.sendToML({'taskId' : 'Test-ApmonIf', 'jobId': 'jobid', 'param-to-send':'value-to-send'})
    apmonIf.free()
    apmonIf = ApmonIf('Test-ApmonIf-Constuctor-Argument')
    apmonIf.sendToML({'jobId': 'jobid', 'param-to-send':'value-to-send'})
    apmonIf.sendToML({'param-to-send':'value-to-send'}, 'jobid-method-argument')
    apmonIf.sendToML({'param-to-send':'value-to-send'}, 'jobid-method-argument', 'Test-ApmonIf-Override-Argument')
    apmonIf.sendToML({'taskId': 'Test-ApmonIf-Override-Params','param-to-send':'value-to-send'}, 'jobid-method-argument')
    apmonIf.free()
    
