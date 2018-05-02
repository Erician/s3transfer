#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.dataobject import message
import logging
log = logging.getLogger('root')
'''
Message 记录一个任务(task )的完成情况，失败还是成功, 以及是否需要重新执行
还有完成的结果（result)，如消耗的时间等
'''

class WorkerMessage(message.Message):
    def __init__(self, ip='', ID='', port='', isSuccess=True,  result=None):
        message.Message.__init__(self,ip,ID,port,isSuccess,result)

    def with_attribute(self, attributeDict):
        message.Message.with_attribute(self, attributeDict)
        if attributeDict.has_key('result') and attributeDict['result']:
            self.result = TaskResult().with_attribute(attributeDict['result'])
        if attributeDict.has_key('type'):
            self.type = attributeDict['type']
        return self

    def set_type(self, taskType):
        self.type = taskType

    def get_type(self):
        return self.type
    
    def to_string(self):
        return (self.type + ', ID:'+self.ID)

'''
记录一个任务(task)的完成的结果，如耗时，平均速度等
'''
class TaskResult(message.Result):

    #long int exceeds XML-RPC limits, so we save attribute as string

    def __init__(self, fileNumber='', taskSize='', succeeded='', failed='', uploadSize='', costTime='', speed=''):
        self.fileNumber = str(fileNumber)
        self.taskSize = str(taskSize)
        self.succeeded = str(succeeded)
        self.failed = str(failed)
        self.uploadSize = str(uploadSize)
        self.costTime = str(costTime)
        self.speed = str(speed)
    
    def with_attribute(self, attributeDict):
        if attributeDict.has_key('fileNumber'):
            self.fileNumber = attributeDict['fileNumber']
        if attributeDict.has_key('taskSize'):
            self.taskSize = attributeDict['taskSize']
        if attributeDict.has_key('succeeded'):
            self.succeeded = attributeDict['succeeded']
        if attributeDict.has_key('failed'):
            self.failed = attributeDict['failed']
        if attributeDict.has_key('uploadSize'):
            self.uploadSize = attributeDict['uploadSize']
        if attributeDict.has_key('costTime'):
            self.costTime = attributeDict['costTime']
        if attributeDict.has_key('speed'):
            self.speed = attributeDict['speed']
        return self

    def get_fileNumber(self):
        return long(self.fileNumber)
    def get_taskSize(self):
        return long(self.taskSize)
    def get_succeeded(self):
        return long(self.succeeded)
    def get_failed(self):
        return long(self.failed)
    def get_uploadSize(self):
        return long(self.uploadSize)
    def get_costTime(self):
        return float(self.costTime)
    def get_speed(self):
        return float(self.speed)

    
class A:
    def __init__(self):
        print 'a'
class B(A):
    pass

if __name__ == '__main__':
    b = B()