#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

import xmlrpclib
import threading
from common.sdk import s3client,qiniuclient,aliyunclient,tencentclient,clientfactory
from common.enum import status
from common.dataobject import assignment
from collections import deque
from common.utils import clean, readdata
from utils import generateID
from common.const import type
import time
import logging
    
class TransferOrCheckTask(assignment.Assignment):
    def __init__(self, synctaskID='', jobType='', source='', destination=None, keyForTaskPath=None, 
                master_ip_port='', checkmode='', sync = ''):
        self.synctaskID = synctaskID
        self.jobType = jobType
        self.source = source
        self.destination = destination
        self.keyForTaskPath = keyForTaskPath
        self.master_ip_port = master_ip_port
        self.checkmode = checkmode
        self.sync = sync
        self.type = type.Task.TransferOrCheck
        #string is more safe than int or long for xmlrpc
        #以byte为单位
        self.taskFileSize = "1"
        self.taskFileNumbers = "1"
    
    def with_attribute(self, attrDict):
        if attrDict.has_key('ID'):
            self.ID = attrDict['ID']
        if attrDict.has_key('synctaskID'):
            self.synctaskID = attrDict['synctaskID']
        if attrDict.has_key('jobType'):
            self.jobType = attrDict['jobType']
        if attrDict.has_key('source'):
            self.source = attrDict['source']
        if attrDict.has_key('destination'):
            self.destination = attrDict['destination']
        if attrDict.has_key('keyForTaskPath'):
            self.keyForTaskPath = attrDict['keyForTaskPath']
        if attrDict.has_key('master_ip_port'):
            self.master_ip_port = attrDict['master_ip_port']
        if attrDict.has_key('checkmode'):
            self.checkmode = attrDict['checkmode']
        if attrDict.has_key('sync'):
            self.sync = attrDict['sync']
        return self

    def get_synctaskID(self):
        return self.synctaskID
    
    def get_jobType(self):
        return self.jobType
    
    def get_source(self):
        return self.source
    
    def get_destination(self):
        return self.destination
    
    def get_keyForTaskPath(self):
        return self.keyForTaskPath
    
    def get_master_ip_port(self):
        return self.master_ip_port
    
    def get_checkmode(self):
        return self.checkmode
    
    def get_sync(self):
        return self.sync
    
    def get_type(self):
        return self.type

    def set_taskFileSize(self, size):
        self.taskFileSize = str(size)

    def get_taskFileSize(self):
        return long(self.taskFileSize)

    def set_setTaskFileNumbers(self, numbers):
        self.taskFileNumbers = str(numbers)

    def get_taskFileNumbers(self):
        return long(self.taskFileNumbers)
    
    def to_string(self):
        return (self.jobType + ' task, synctaskID:'+self.synctaskID+
                ', taskID:'+self.ID+
                ', bucketname:'+self.destination['bucketName']+
                ', keyForTaskPath:'+self.keyForTaskPath)


class GenerateDiskFileListTask(assignment.Assignment):
    def __init__(self, absolutepath='', accessKey='', secretKey='', endpoint='',
                    bucketName='', fileNamePrefix='', sync='', master_ip_port=''):
        self.absolutepath = absolutepath
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.endpoint = endpoint
        self.bucketName = bucketName
        self.fileNamePrefix = fileNamePrefix
        self.sync = sync
        self.master_ip_port = master_ip_port
        self.type = type.Task.GenerateFileList
    
    def with_attribute(self, attrDict):
        if attrDict.has_key('ID'):
            self.ID = attrDict['ID']
        if attrDict.has_key('absolutepath'):
            self.absolutepath = attrDict['absolutepath']
        if attrDict.has_key('accessKey'):
            self.accessKey = attrDict['accessKey']
        if attrDict.has_key('secretKey'):
            self.secretKey = attrDict['secretKey']
        if attrDict.has_key('endpoint'):
            self.endpoint = attrDict['endpoint']
        if attrDict.has_key('bucketName'):
            self.bucketName = attrDict['bucketName']
        if attrDict.has_key('fileNamePrefix'):
            self.fileNamePrefix = attrDict['fileNamePrefix']
        if attrDict.has_key('sync'):
            self.sync = attrDict['sync']
        if attrDict.has_key('master_ip_port'):
            self.master_ip_port = attrDict['master_ip_port']
        return self
        
    def get_absolutepath(self):
        return self.absolutepath
    
    def get_accessKey(self):
        return self.accessKey
    
    def get_secretKey(self):
        return self.secretKey
    
    def get_endpoint(self):
        return self.endpoint
    
    def get_bucketName(self):
        return self.bucketName
    
    def get_fileNamePrefix(self):
        return self.fileNamePrefix
    
    def get_sync(self):
        return self.sync

    def get_master_ip_port(self):
        return self.master_ip_port

    def get_type(self):
        return self.type
    
    def to_string(self):
        return ('generate disk filelist, taskID:'+self.ID+
                ', bucketname:'+self.bucketName+
                ', absolutepath:'+self.absolutepath+
                ', filenameprefix:'+self.fileNamePrefix)

    
if __name__ == '__main__':
    pass
    
    