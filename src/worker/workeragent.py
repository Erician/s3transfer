#coding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

import random,time
import threading
from common.const import type
from common.myexceptions import taskexception
from common.utils import readdata,timeoperation,urltools
from common.sdk import s3client,qiniuclient,aliyunclient,tencentclient,clientfactory
from multiprocessing import cpu_count,Queue
import hashlib
import logging
import workermessage
log = logging.getLogger('root')
           
#do more complex task for worker
class WorkerAgent:
    lock = threading.Lock()
    FileQueueDefaultCapacity = 16
    FileQueueSleepTime = 0
    OssServiceTimeOut = 3
    CheckPartSize = 8 * 1024 * 1024
    EachRequestAndResponseTime = 0.04
    queue = None
    fileNumbersToCountDown = 0
    errorFileList = None
    md5List = None
    objectstatusFile = None
    isUploadAll = False
    #record
    succeeded = 0
    failed = 0
    failedSize = 0
    uploadSize = 0
    MaxThreadNum = 3
    def __init__(self, task):
        self.taskID = task.get_ID()
        self.sync = task.get_sync()
        self.source = task.get_source()
        self.destination = task.get_destination()
        self.fileType = self.source['filetype']
        self.jobType = task.get_jobType()
        self.keyForTaskPath = task.get_keyForTaskPath()
        self.checkMode = task.get_checkmode()
        if self.fileType == type.FileType.DiskFile:
            self.absolutePath = self.source['absolutepath']
        [self.srcClient, self.desClient] = self.create_clients()
        self.lines = readdata.readlines(self.desClient, self.destination['bucketName'], task.get_keyForTaskPath()+"filelist")
        line = self.lines.readline()
        pair = line.split('\t')
        self.fileNumbers = long(pair[0].strip())
        self.taskSize = long(pair[1].strip())
        self.initialClassVar()

    def initialClassVar(self):
        self.queue = Queue(WorkerAgent.FileQueueDefaultCapacity)
        self.fileNumbersToCountDown = long(self.fileNumbers)
        self.errorFileList = list()
        self.md5List = list()
        self.isUploadAll = False
        #record
        self.succeeded = 0
        self.failed = 0
        self.failedSize = 0
        self.uploadSize = 0
    
    def create_clients(self):
        if self.fileType != type.FileType.DiskFile and self.fileType != type.FileType.UrlFile:
            srcClient = clientfactory.create_client(self.fileType,self.source['accessKey'],
                                                self.source['secretKey'],self.source['endpoint'])
        else:
            srcClient = None
        desClient = clientfactory.create_client(type.FileType.S3File,self.destination['accessKey'],
                                            self.destination['secretKey'],self.destination['endpoint'])
        return [srcClient, desClient]

    def transfer(self):
        [srcClient, desClient] = self.create_clients()
        while True:
            lineNum,line = self.get_from_queue()
            line = line.strip('\n').strip()
            pair = line.split('\t')
            key = pair[0].strip()
            size = pair[1].strip()
            isUploadOk = False
            recordErrorReason = ''
            try:
                [isRightTime, recordErrorReason] = self.isRightTimeToOpeareteThisFile(srcClient, key)
                if not isRightTime:
                    raise Exception
                keyRemovedSlash = key[1:] if key.startswith('/') else key
                srcPrefixRemovedSlash = self.source['prefix'][1:] if self.source['prefix'].startswith('/') else self.source['prefix']
                desKey = self.destination['prefix'] + keyRemovedSlash.decode('utf8')[len(srcPrefixRemovedSlash.decode('utf-8')):].encode('utf-8')
                if self.fileType == type.FileType.DiskFile:
                    log.info(desKey)
                    [isUploadOk,recordErrorReason] = self.upload_diskfile(desClient, key, desKey)
                elif self.fileType == type.FileType.UrlFile:
                    [isUploadOk,recordErrorReason] = self.upload_urlfile(desClient, key, desKey)
                else:
                    isOK,streamingBody = srcClient.get_object_with_StreamingBody(self.source['bucketName'],key)
                    if not isOK:
                        recordErrorReason = streamingBody
                        raise Exception
                    if self.fileType == type.FileType.QiniuFile:
                        [isUploadOk,recordErrorReason] = self.upload_qiufile(desClient, desKey, streamingBody)
                    elif (self.fileType == type.FileType.S3File 
                          or self.fileType == type.FileType.AliyunFile 
                          or self.fileType == type.FileType.TencentFile
                          or self.fileType == type.FileType.BaiduFile):
                        [isUploadOk,recordErrorReason] = self.upload_s3file(desClient, desKey, streamingBody)
            except Exception,e:
                log.error(e)
                isUploadOk = False
            finally:
                self.save_objectstatus(key, lineNum, size, isUploadOk, recordErrorReason)
                
    def isRightTimeToOpeareteThisFile(self, srcClient, key):
        if self.fileType == type.FileType.DiskFile:
            lastModify = os.path.getmtime(key)
        elif self.fileType == type.FileType.UrlFile:
            return [True, '']
        else:
            lastModify = srcClient.get_object_lastmodify(self.source['bucketName'], key)
        if lastModify == -1:
            return [False, key + ' not exists']
        else:
            if float(lastModify) >= float(self.sync['since']) and float(lastModify) <= float(self.sync['starttime']):
                return [True, '']
            else:
                return [False, timeoperation.fmt_time(lastModify)+' is not between '+
                        timeoperation.fmt_time(self.sync['since'])+' and '+
                        timeoperation.fmt_time(self.sync['starttime'])]
                
    def upload_diskfile(self,desClient, key, desKey):
        try:
            myfile = open(key,'r')
            mybool,res = desClient.upload_fileobj( self.destination['bucketName'], desKey, myfile)
            if not mybool:
                return [False,res]
            myfile.close()
            return [True,None]
        except IOError,e:
            log.error(e)
            return [False,e]
        except Exception,e:
            log.error(e)
            return [False,e]

    def upload_urlfile(self, desClient, url, desKey):
        try:
            return desClient.put_object_with_url(self.destination['bucketName'], desKey, url)
        except Exception, e:
            return [False, e]
        
    def upload_qiufile(self,desClient, desKey, url):
        return self.upload_urlfile(desClient, url, desKey)
    
    def upload_s3file(self,desClient, desKey,streamingBody):
        return desClient.upload_fileobj(self.destination['bucketName'], desKey, streamingBody)
    
    def check(self):
        [srcClient, desClient] = self.create_clients()
        while True:
            lineNum,line = self.get_from_queue()
            line = line.strip('\n').strip()
            pair = line.split('\t')
            key = pair[0].strip()
            size = pair[1].strip()
            isCheckOk = False
            recordErrorReason = ''
            try:
                [isRightTime, recordErrorReason] = self.isRightTimeToOpeareteThisFile(srcClient, key)
                if not isRightTime:
                    raise Exception
                keyRemovedSlash = key[1:] if key.startswith('/') else key
                srcPrefixRemovedSlash = self.source['prefix'][1:] if self.source['prefix'].startswith('/') else self.source['prefix']
                desKey = self.destination['prefix'] + keyRemovedSlash.decode('utf8')[len(srcPrefixRemovedSlash.decode('utf-8')):].encode('utf-8')
                if self.checkMode == 'md5':
                    [isCheckOk,recordErrorReason] = self.check_with_md5(srcClient, desClient, key, desKey)
                elif self.checkMode == 'head':
                    [isCheckOk,recordErrorReason] = self.check_with_head(desClient, desKey, size)
            except Exception,e:
                log.error(e)
                isCheckOk = False
            finally:
                self.save_objectstatus(key, lineNum, size, isCheckOk, recordErrorReason)
    
    def check_with_head(self,desClient, desKey, size):
        if desClient.does_object_exists(self.destination['bucketName'], desKey) is False:
            return [False, desKey + ' does not exist']
        if long(desClient.get_object_size(self.destination['bucketName'], desKey)) != long(size):
            return [False, 'the length of ' + desKey + ' is not ' + str(size)]
        return [True, '']
    
    def check_with_md5(self,srcClient, desClient, key, desKey):
        if self.fileType == type.FileType.DiskFile:
            srcMd5 = self.compute_diskfile_md5(key)
        elif self.fileType == type.FileType.UrlFile:
            srcMd5 = self.compute_urlfile_md5(key)
        elif self.fileType == type.FileType.QiniuFile:
            srcMd5 = self.compute_qiniufile_md5(srcClient,self.source['bucketName'],key)
        elif (self.fileType == type.FileType.S3File 
              or self.fileType == type.FileType.AliyunFile 
              or self.fileType == type.FileType.TencentFile 
              or self.fileType == type.FileType.BaiduFile):
            srcMd5 = self.compute_s3file_md5(srcClient,self.source['bucketName'],key)
        if not srcMd5:
            if self.fileType == type.FileType.DiskFile:
                return [False,key+' doesn\'t exist in the source ']
            else:
                return [False,key+' doesn\'t exist in the source ']
        desMd5 = self.compute_s3file_md5(desClient, self.destination['bucketName'], desKey)
        if not desMd5:
            return [False,self.destination['prefix']+key+' doesn\'t exist in the destination']
        if srcMd5 == desMd5:
            return [True,desMd5]
        else:
            return [False,'md5 wrong,src:'+srcMd5+' des:'+desMd5+' ']
        
    def compute_s3file_md5(self,client,bucketName,key):
        try:
            stream = client.get_object_with_stream(bucketName, key)
            if not stream:
                return False
            myhash = hashlib.md5()
            mybytes = stream.read(WorkerAgent.CheckPartSize)
            while mybytes:
                myhash.update(mybytes)
                mybytes = stream.read(WorkerAgent.CheckPartSize)
            return myhash.hexdigest().lower()
        except Exception,e:
            log.error(e)
            return None
        
    def compute_qiniufile_md5(self, client, bucketName, key):
        isOk, url = client.get_object_with_StreamingBody(bucketName, key)
        if isOk:
            return self.compute_urlfile_md5(url)
        else:
            return None

    def compute_urlfile_md5(self, url):
        try:
            myhash = hashlib.md5()
            size = urltools.get_info(url)
            if size == -1:
                raise Exception
            elif size != 0:
                for partNumber in range(1, size // WorkerAgent.CheckPartSize + 2):
                    content = urltools.download_by_rang(url, (partNumber - 1) * WorkerAgent.CheckPartSize, partNumber * WorkerAgent.CheckPartSize - 1)
                    if content:
                        myhash.update(content)
            return myhash.hexdigest().lower()
        except Exception,e:
            log.error(e)
            return None
        
    def compute_diskfile_md5(self,abspath):
        try:
            myhash = hashlib.md5()
            myfile = open(abspath,'r')
            mybytes = myfile.read(WorkerAgent.CheckPartSize)
            while mybytes:
                myhash.update(mybytes)
                mybytes = myfile.read(WorkerAgent.CheckPartSize)
            myfile.close()
            return myhash.hexdigest().lower()
        except Exception,e:
            log.error(e)
            return None
        
    def save_objectstatus(self,key,lineNum,size,isObjectOperationOk,errorReason=''):
        self.lock.acquire()
        if isObjectOperationOk:
            self.succeeded = self.succeeded + 1
            self.uploadSize = self.uploadSize+long(size)
            if self.jobType == type.JobType.Transfer:
                log.info('uploaded ok:'+key + '\t'+ str(self.succeeded)+'/'+ str(self.fileNumbers))
            elif self.jobType == type.JobType.Check:
                self.md5List.append(key+'\t'+errorReason)
                log.info('check ok:'+key +'\t'+errorReason+ '\t'+ str(self.succeeded)+'/'+ str(self.fileNumbers))
        else:
            self.failed = self.failed + 1
            self.failedSize = self.failedSize+long(size)
            self.errorFileList.append(key+'\t'+str(size)+'\t'+str(errorReason))
            if self.jobType == type.JobType.Transfer:
                log.info('upload failed:'+key + '\t'+ str(self.succeeded)+'/'+ str(self.fileNumbers))
            elif self.jobType == type.JobType.Check:
                log.info('check failed:'+key +'\t'+errorReason + '\t'+ str(self.succeeded)+'/'+ str(self.fileNumbers))
        self.fileNumbersToCountDown -= 1
        self.lock.release()
                
    def get_from_queue(self):
        while True:
            try:
                lineNum,line = self.queue.get(True,5)
                return [lineNum,line]
            except Exception,e:
                if self.isUploadAll:
                    exit(0)
                else:
                    continue

    def execute(self):
        startTime = time.time()
        subThreads= self.generate_threads()
        line = self.lines.readline()
        cntLine = 0 
        while (line):
            time.sleep(self.FileQueueSleepTime)     #for flow control
            self.queue.put([cntLine,line])
            cntLine+=1
            line = self.lines.readline()
        while self.fileNumbersToCountDown != 0:
            time.sleep(random.random()*2)
        self.isUploadAll = True
        for i in range(WorkerAgent.MaxThreadNum):
            subThreads[i].join()
        
        self.upload_list()
        log.info('the cost time is '+ str(round(time.time()-startTime))+'s'+ ' and the average rate is '\
                  + str(round(float(self.uploadSize)/(time.time()-startTime)/1024,2))+'KB/s')
        #minus the time of request and response time, EachRequestAndResponseTime is not very accurate
        return workermessage.TaskResult(self.fileNumbers,self.taskSize,self.succeeded,self.failed,
                                self.uploadSize,round(time.time()-startTime),
                                round(float(self.uploadSize)/(time.time()-startTime-self.fileNumbers*WorkerAgent.EachRequestAndResponseTime)/1024,2))
        
    def generate_threads(self):
        subThreads = list()
        for i in range(WorkerAgent.MaxThreadNum):
            if self.jobType == type.JobType.Transfer:
                th = threading.Thread(target=self.transfer, args=())
            else:
                th = threading.Thread(target=self.check, args=())
            subThreads.append(th)
            th.setDaemon(True)
            th.start()

        th = threading.Thread(target=self.flowcontrol_thread, args=())
        subThreads.append(th)
        th.setDaemon(True)
        th.start()
        return subThreads
                
    def upload_list(self):
        lines = readdata.readlines(self.desClient, self.destination['bucketName'],self.keyForTaskPath+'taskstatus')
        lines.readline()
        taskID = lines.readline()
        if taskID != self.taskID:
            raise taskexception.TaskIDIsNotTheSame(taskID + ' and ' + self.taskID + ' are not the same!')
        if self.errorFileList:
            self.errorFileList.insert(0, str(self.failed)+'\t'+str(self.failedSize))
            self.desClient.put_object(self.destination['bucketName'], self.keyForTaskPath+'errorlist','\n'.join(self.errorFileList))
                       
        if self.md5List and self.jobType == type.JobType.Check:
            self.desClient.put_object(self.destination['bucketName'], self.keyForTaskPath+'md5list','\n'.join(self.md5List))

    def flowcontrol_thread(self):
        cntTimeOut = 0
        while True:
            try:
                if self.isUploadAll is True:
                    return

                if self.does_oss_service_timeout():
                    cntTimeOut += 1
                else:
                    if cntTimeOut == 0:
                        self.OssServiceTimeOut = self.OssServiceTimeOut -1 if self.OssServiceTimeOut != 0 else 0
                    cntTimeOut = 0

                if cntTimeOut >= 4:
                    self.OssServiceTimeOut = self.OssServiceTimeOut * 2 if self.OssServiceTimeOut != 0 else 1
                    cntTimeOut = 0
            except Exception, e:
                log.info(e)
            time.sleep(5)


    def does_oss_service_timeout(self):
        try:
            startTime = time.time()
            self.desClient.list_objects_without_delimiter(self.destination['bucketName'], '', '', 1)
            return time.time() - startTime > WorkerAgent.OssServiceTimeOut
        except Exception, e:
            log.info(e)
            return False

if __name__ == '__main__':
    myhash = hashlib.md5()
    print  myhash.hexdigest().lower()




