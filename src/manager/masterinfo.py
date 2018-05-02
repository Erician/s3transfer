#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.enum import status
from common.const import port
from common.dataobject import info
import SimpleXMLRPCServer
from collections import deque
import threading
import logging
log = logging.getLogger('root')

class MasterInfo(info.Info):
    
    def set_job(self, job):
        self.job = job
    
    def get_job(self):
        try:
            return self.job
        except AttributeError:
            return None

if __name__ == '__main__':

    a = MasterInfo(12,2)
    print a.get_job()
    