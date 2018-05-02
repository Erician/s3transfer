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

class WorkerInfo(info.Info):
    
    def set_task(self, task):
        self.task=task
    
    def get_task(self):
        try:
            return self.task
        except AttributeError:
            return None
    