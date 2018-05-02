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

class WorkerManagerInfo(info.Info):
    __taskqueue = deque()
    def appendright_task(self, task):
        self.__taskqueue.append(task)
    
    def popleft_task(self):
        return self.__taskqueue.popleft()
    
    def get_taskqueue(self):
        return self.__taskqueue

    def set_taskqueue(self, taskqueue):
        self.__taskqueue = taskqueue
    
    def get_task(self, taskID):
        for task in self.__taskqueue:
            if task.get_ID() == taskID:
                return task
        return None

    def rm_task(self, task):
        self.__taskqueue.remove(task)
    