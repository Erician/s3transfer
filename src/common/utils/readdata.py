#encoding=utf-8
import os, sys
from botocore.exceptions import IncompleteReadError
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
absdir = absdir[0:absdir.rfind('/',0,len(absdir))]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
reload(sys)
sys.setdefaultencoding('utf8')
from common.myexceptions import ossexception
import logging
log = logging.getLogger('root')

class LinesObject:
    def __init__(self, linelist):
        self.linelist = linelist
        self.currentPos = 0
        self.length = len(self.linelist)
    def readline(self):
        if self.currentPos == self.length:
            return None
        else:
            self.currentPos += 1
            return self.linelist[self.currentPos-1]

def readlines(client, bucketName, keyName):
    #写这个函数的目的是为了防止读超时，导致的读取错误
    #if success return a list, or return None
    linelist = list()
    while True:
        try:
            linelist[:] = []
            isSuccess,body = client.get_object_with_StreamingBody(bucketName, keyName)
            tmpFile = open(keyName.replace('/','.'),'w')
            tmpFile.write(body.read())
            tmpFile.close()
            tmpFile = open(keyName.replace('/','.'),'r')
            line = tmpFile.readline()
            while line:
                linelist.append(line)
                line = tmpFile.readline()
            tmpFile.close()
            os.remove(keyName.replace('/','.'))
            return LinesObject(linelist)

        except ossexception.GetObjectFailed, e:
            raise ossexception.GetObjectFailed(e)
        except Exception, e:
            log.error(e)
            log.error('read lines of '+bucketName+'failed, and we will try again')

    
    