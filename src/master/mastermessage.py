#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.dataobject import message
'''
Message 记录一个任务(Job )的完成情况，失败还是成功, 以及是否需要重新执行
还有完成的结果（result)，如消耗的时间等
'''
class MasterMessage(message.Message):
    def __init__(self, ip='', ID='', port='', isSuccess=True,  result=None):
        message.Message.__init__(self,ip,ID,port,isSuccess,result)
'''
记录一个任务(Job)的完成的结果，如耗时，平均速度等
'''
class JobResult(message.Result):
    pass

if __name__ == '__main__':
    mess = MasterMessage()
    print mess
    