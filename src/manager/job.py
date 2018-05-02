#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.dataobject import assignment

class Job(assignment.Assignment):
    
    def __init__(self,job = None):
        self.retrytimes = 0
        self.job = job
        self.set_ID(self.job['job-ID'])

    def get_job(self):
        return self.job
    
    def set_retrytimes(self, times):
        self.retrytimes = times

    def get_retrytimes(self):
        return self.retrytimes
    
    def to_string(self):
        return ('the job, jobID:'+self.ID)
    