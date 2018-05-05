#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

import threading
from common.sdk import s3client,qiniuclient,aliyunclient,tencentclient,clientfactory
from common.enum import status
from collections import deque
from common.myexceptions import ossexception,taskexception
from common.utils import clean, readdata, configoperations, mail, urltools
from utils import generateID
from common.const import type
import time
from task import GenerateDiskFileListTask, TransferOrCheckTask
from time import sleep
import logging

log = logging.getLogger('root')

class TaskPool:
    MaxTimeOneTaskShouldSpend = 1000
    lockTasklist = threading.Lock()

    def __init__(self,master, master_ip_port):
        self.master = master
        self.master_ip_port = master_ip_port

    def init(self):
        #初始化
        self.taskNumbers = 0
        self.tasklistStartPosToFind = 0
        self.lockTasklist.acquire()
        self.tasklist = deque()
        self.lockTasklist.release()
        #打印使用,all指的是一个job
        self.allFiles = 0
        self.allSize = 0
        #设置一个正在生成tasks的标志，一轮完成的判断条件
        self.isGeneratingTasks = False

        
    def put(self,task):
        self.lockTasklist.acquire()
        self.tasklist.append(task)
        self.taskNumbers_pp()
        self.lockTasklist.release()

    def set_task_status(self,pos,status,oldstatus=-1):
        '''
        该函数用来设置tasklist中某个task的状态
        :param pos: 要设置的task在tasklist中的位置
        :param status:将该task设置为status状态
        :param oldstatus:如果oldstatus==-1,可直接设置task的状态为status;
                         否则，只有当task的状态是oldstatus时，才可以设置
               设置状态的函数的逻辑和这个函数基本类似
        '''
        try:
            self.lockTasklist.acquire()
            if oldstatus != -1:
                if self.tasklist[pos].get_status()!=oldstatus:
                    return False
            self.tasklist[pos].set_status(status)
            return True
        except Exception, e:
            log.info(e)
        finally:
            self.lockTasklist.release()

    def get_task_status(self,pos):
        '''
        获取tasklist中位于pos位置task的状态
        '''
        try:
            self.lockTasklist.acquire()
            if pos>=0 and pos < len(self.tasklist):
                return self.tasklist[pos].get_status()
            return -1
        except Exception, e:
            log.info(e)
        finally:
            self.lockTasklist.release()

    def get(self):
        try:
            while self.tasklistStartPosToFind<len(self.tasklist):
                if self.tasklist[self.tasklistStartPosToFind].get_status() == status.TASK.ready:
                    if self.set_task_status(self.tasklistStartPosToFind, status.TASK.dispatched, status.TASK.ready):
                        task = (self.tasklist[self.tasklistStartPosToFind])
                        self.tasklistStartPosToFind += 1
                        taskID = generateID.taskID(str(time.time())+' '+self.destination['bucketName']+' '+str(self.tasklistStartPosToFind-1))
                        task.set_ID(taskID)
                        task.set_startTime(time.time())
                        return task
                self.tasklistStartPosToFind += 1
            return None
        except Exception, e:
            log.info(e)
            return None
    
    def is_all_tasks_finished(self):
        try:
            self.lockTasklist.acquire()
            for task in self.tasklist:
                if task.get_status() != status.TASK.done:
                    return False
            return True
        except Exception, e:
            log.info(e)
        finally:
            self.lockTasklist.release()
        
    def is_generating_tasks(self):
        return self.isGeneratingTasks

    def taskNumbers_pp(self):
        self.taskNumbers+=1
    
    def get_taskNumbers(self):
        try:
            self.lockTasklist.acquire()
            return len(self.tasklist)
        except Exception, e:
            log.info(e)
        finally:
            self.lockTasklist.release()
    
    def get_round_info(self):
        '''
        :return : 返回值是个list，分别是：config-master中配置的轮数、开始的轮数、当前正在运行的轮数
        '''
        return [self.requireRoundNum,self.startRound,self.currentRound]

    def set_current_round(self,currentRound):
        self.currentRound = currentRound
        
    def init_job(self, job):
        self.jobID = job['job-ID']
        #src
        self.source = dict()
        self.source['file-list'] = job['src-file-list']
        self.source['filetype'] = job['src-filetype']
        filetype = job['src-filetype']
        if filetype == type.FileType.DiskFile:
            self.source['absolutepath'] = job['src-absolutepath']
            #对文件系统,src-absolutepath也就是absolutepath
            self.source['prefix'] = job['src-absolutepath']
        elif filetype == type.FileType.UrlFile:
            self.source['prefix'] = job['src-prefix']
        else:
            self.source['accessKey'] = job['src-accesskey']
            self.source['secretKey'] = job['src-secretkey']
            self.source['endpoint'] = job['src-endpoint']
            self.source['bucketName'] = job['src-bucketName']
            self.source['prefix'] = job['src-prefix']
        #des
        self.destination = dict()
        self.destination['accessKey'] = job['des-accesskey']
        self.destination['secretKey'] = job['des-secretkey']
        self.destination['endpoint'] = job['des-endpoint']
        self.destination['bucketName'] = job['des-bucketName']
        self.destination['prefix'] = job['des-prefix']
        
        #可以设置下面三个配置，但是我们不建议设置
        self.taskSize = 10*1024*1024 if (not job.has_key('task-size')) else long(job['task-size'])*1024*1024
        self.taskFileNumbers = 50000 if (not job.has_key('task-filenumbers')) else long(job['task-filenumbers'])
        self.requireRoundNum = 5 if (not job.has_key('round')) else long(job['round'])
        #sync
        self.sync = dict()
        self.sync['since'] = job['sync-since']
        self.sync['starttime'] = job['sync-starttime']
        self.nextmarker = job['nextmarker']
        self.synctask = job['synctask']
        #check
        self.check = dict()
        self.check['mode'] = job['check-mode']
        #
        if job.has_key('task-size') and job.has_key('task-filenumbers'):
            self.is_user_set_task_attributes = True
        else:
            self.is_user_set_task_attributes = False
    
    def waitfor_task0_finished(self):
        startTime = time.time()
        while True:
            if self.is_all_tasks_finished() == True:
                spentTime = time.time() - startTime
                self.taskSize = long(TaskPool.MaxTimeOneTaskShouldSpend*self.allSize*1.0/spentTime)
                break
            sleep(5)
    
    def generate_task_from_job(self,jobType,desClient):
        self.init()
        self.jobType = jobType
        self.isGeneratingTasks = True
        self.desClient = desClient
        self.startRound = 0
        self.currentRound = 0
        self.th = threading.Thread(target=self.generate_task_from_job_thread, args=())
        self.th.setDaemon(True)
        self.th.start()
        
    def generate_task_from_job_thread(self):
        try:
            desBucketName = self.destination['bucketName']
            if self.is_this_job_has_finished(self.jobType):
                log.info('the '+self.jobType+' of '+ self.synctask+' is already finished')
                self.currentRound = self.startRound+self.requireRoundNum-1
                self.isGeneratingTasks = False
                return
            
            if self.source['filetype'] != type.FileType.DiskFile and self.source['filetype'] != type.FileType.UrlFile:
                self.srcClient = clientfactory.create_client(self.source['filetype'],self.source['accessKey'],
                                                             self.source['secretKey'],self.source['endpoint'])
            cntRound = 0
            while True: 
                roundStatus = self.get_this_round_status(desBucketName,self.synctask+'/'+self.jobType+'/round'+str(cntRound)+'/roundstatus')
                if roundStatus and roundStatus == 'done':
                    cntRound += 1
                else:
                    break
            if roundStatus and roundStatus == 'ready':
                self.recover_this_round_task(cntRound)
            else:
                if cntRound == 0:
                    self.generate_round0_task()
                else:
                    self.startRound = cntRound
                    self.generate_nextround_task(cntRound)
            put_roundstatus(self.desClient,desBucketName,self.synctask+'/'+self.jobType+'/round'+str(self.startRound)+'/roundstatus', 'ready')
            self.isGeneratingTasks = False
        except (ossexception.GetObjectFailed, ossexception.PutObjectFailed, ossexception.ListObjectFailed, taskexception.GenerateTaskFailed, Exception), e:
            log.warn(e)
            mail.send_mail('[error]generating task for job, jobID:'+self.jobID+' failed, you should check why and restart the job')
        finally:
            pass
        
    def is_this_job_has_finished(self,jobType):
        try:
            lines = readdata.readlines(self.desClient, self.destination['bucketName'],self.synctask+'/'+jobType+'/'+jobType+'status')
            line = lines.readline()
            if line == 'done':
                return True
        except (ossexception.GetObjectFailed, Exception),e:
            log.info(e)
            log.info('the upper means the '+jobType+' is not finished, we will do it')
            return False
        
    def get_this_round_status(self,bucketName,keyName):
        try:
            lines = readdata.readlines(self.desClient, bucketName, keyName)
            line = (lines.readline()).strip('\n').strip()
            return line
        except (ossexception.GetObjectFailed, Exception),e:
            log.info(e)
            log.info('the upper means that round is not finished, we will do it')
            return None
        
    def recover_this_round_task(self,cntRound):
        self.currentRound = cntRound
        self.startRound = cntRound
        #从本轮中获取已完成的task和未完成的task
        log.info( 'start to recover the round'+str(self.currentRound)+' for '+ self.jobType)
        taskDirs = self.desClient.get_alldir(self.destination['bucketName'], self.synctask+'/'+self.jobType+'/round'+str(self.startRound)+'/','')
        for taskdir in taskDirs:
            pair = taskdir.split('/')
            taskNumber = pair[4].strip()
            lines = readdata.readlines(self.desClient, self.destination['bucketName'], taskdir+'taskstatus')
            line = (lines.readline()).strip('\n').strip()
            if line == 'done':
                log.info(taskNumber + ',finished already')
            else:
                reset_taskinfo(self.desClient, self.destination['bucketName'], taskdir)
                lines = readdata.readlines(self.desClient, self.destination['bucketName'], taskdir+'filelist')
                line = (lines.readline()).strip().strip()
                pair = line.split('\t')
                self.allFiles+=long(pair[0].strip())
                self.allSize+=long(pair[1].strip())
                task = TransferOrCheckTask(self.synctask,self.jobType,self.source,self.destination,
                                        taskdir,self.master_ip_port,self.check['mode'],self.sync)
                task.set_status(status.TASK.ready)
                self.put(task)
                log.info( str(taskNumber)+',fileNumbers:'+str(pair[0].strip())+',taskSize:'+str(pair[1].strip()))
        log.info( 'recover the round'+str(self.currentRound)+' successfully')
        return True

    def generate_nextround_task_with_thread(self, cntRound):
        try:

            self.init()
            self.isGeneratingTasks = True
            self.th = threading.Thread(target=self.generate_nextround_task_thread, args=(cntRound,))
            self.th.setDaemon(True)
            self.th.start()
        except Exception, e:
            log.info(e)

    def generate_nextround_task_thread(self, cntRound):
        try:
            self.generate_nextround_task(cntRound)
            self.isGeneratingTasks = False
        except Exception, e:
            log.info(e)
            mail.send_mail('[error]generating task for job, jobID:' + self.jobID + ' failed, you should check why and restart the job')
        finally:
            pass

    def generate_nextround_task(self,cntRound):
        self.currentRound = cntRound
        roundPrefix = self.synctask + '/' + self.jobType + '/round' + str(self.currentRound)
        log.info('clean ' + roundPrefix + ' first')
        clean.clean_with_prefix(self.desClient, self.destination['bucketName'], roundPrefix)
        log.info('start to generate tasks(round ' + str(self.currentRound) + ') for ' + self.jobType)
        taskNumber = 0
        taskPrefix = self.synctask+'/'+self.jobType+'/round'+str(self.currentRound)+'/task'
        currentSize = 0
        currentFileNumbers = 0
        filelist = list()
        oldTaskDirs = self.desClient.get_alldir(self.destination['bucketName'], self.synctask+'/'+self.jobType+'/round'+str(self.currentRound-1)+'/','')
        for oldTaskDir in oldTaskDirs:
            lines = readdata.readlines(self.desClient, self.destination['bucketName'], oldTaskDir + 'taskstatus')
            line = (lines.readline()).strip('\n').strip()
            if line == 'done':
                fileName = 'errorlist'
            else:
                fileName = 'filelist'
            lines = readdata.readlines(self.desClient, self.destination['bucketName'],oldTaskDir + fileName)
            line = lines.readline()
            line = lines.readline()
            while line:
                line = line.strip('\n')
                objectSize = line.split('\t')[1].strip()
                filelist.append(line)
                currentSize += long(objectSize)
                currentFileNumbers += 1
                if currentSize >= self.taskSize or currentFileNumbers>=self.taskFileNumbers:
                    self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
                    if taskNumber == 0 and self.is_user_set_task_attributes == False:
                        self.waitfor_task0_finished()
                    taskNumber += 1
                    currentFileNumbers = 0
                    currentSize = 0
                    filelist[:] = []
                line = lines.readline()
        if currentSize != 0 or currentFileNumbers != 0:
            self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
        log.info( 'generate tasks successfully')
        return True
        
    def generate_round0_task(self):
        self.currentRound = 0
        self.startRound = 0
        if self.source['file-list']:
            return self.generate_round0_task_from_file()
        if self.jobType == type.JobType.Check:
            if self.generate_checktask_from_transfer_round0():
                return True
        if self.source['filetype'] == type.FileType.DiskFile:
            return self.generate_round0_task_diskfile()
        else:
            return self.generate_round0_task_s3file()
    #从用户指定的文件生成task(s)
    def generate_round0_task_from_file(self):
        log.info( 'start to generate tasks(round '+str(self.currentRound)+') for '+self.jobType+' from an appointed file')
        taskNumber = 0
        taskPrefix = self.synctask+'/'+self.jobType+'/round0/task'
        currentFileNumbers = 0
        currentSize = 0
        filelist = list()
        myfile = open(self.source['file-list'],'r')
        for line in myfile:
            line = line.strip('\n').strip()
            if line.startswith('#') or not line:
                continue
            if self.source['filetype'] == type.FileType.UrlFile and len(line.split('\t')) == 1:
                objectSize = 1 if urltools.get_info(line)==-1 else urltools.get_info(line)
                filelist.append(line+'\t'+str(objectSize))
            else:
                objectSize = line.split('\t')[1].strip()
                filelist.append(line)

            currentSize += long(objectSize)
            currentFileNumbers += 1
            if currentSize >= self.taskSize or currentFileNumbers>=self.taskFileNumbers:
                self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
                if taskNumber == 0 and self.is_user_set_task_attributes == False:
                    self.waitfor_task0_finished()
                taskNumber += 1
                currentFileNumbers = 0
                currentSize = 0
                filelist[:] = []
        if currentSize != 0 or currentFileNumbers != 0:
            self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
        log.info( 'generate tasks successfully')
        return True
 
    #like generate_next_round
    def generate_checktask_from_transfer_round0(self):
        log.info('generate checktask from transfer round0')
        log.info( 'start to generate tasks(round '+str(self.currentRound)+') for '+self.jobType)
        taskNumber = 0
        taskPrefix = self.synctask+'/'+self.jobType+'/round0/task'
        currentSize = 0
        currentFileNumbers = 0
        filelist = list()
        transferTaskDirs = self.desClient.get_alldir(self.destination['bucketName'], self.synctask+'/'+type.JobType.Transfer+'/round0/','')
        try:
            for transferTaskDir in transferTaskDirs:
                lines = readdata.readlines(self.desClient, self.destination['bucketName'],transferTaskDir + 'filelist')
                line = lines.readline()
                line = lines.readline()
                while line:
                    line = line.strip('\n')
                    objectSize = line.split('\t')[1].strip()
                    filelist.append(line)
                    currentSize += long(objectSize)
                    currentFileNumbers += 1
                    if currentSize >= self.taskSize  or currentFileNumbers>=self.taskFileNumbers:
                        self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
                        taskNumber += 1
                        currentFileNumbers = 0
                        currentSize = 0
                        filelist[:] = []
                    line = lines.readline()
            if currentSize != 0 or currentFileNumbers != 0:
                self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
            if self.allFiles == 0:
                return False
            return True
        except ossexception.GetObjectFailed, e:
            log.info(e)
            return False

    def recover_generating_round0_task_info(self,taskPrefix):
        taskNumber = 0
        self.allFiles = 0
        self.allSize = 0
        while self.desClient.does_object_exists(self.destination['bucketName'], taskPrefix+str(taskNumber)+'/taskstatus'):
            lines = readdata.readlines(self.desClient, self.destination['bucketName'], taskPrefix+str(taskNumber)+'/filelist')
            fileNumbers, size = lines.readline().strip('\n').strip().split('\t')
            self.taskFileNumbers = long(fileNumbers)
            self.taskSize = long(size)
            self.allFiles += long(fileNumbers)
            self.allSize += long(size)
            taskNumber += 1
        # 放弃恢复的内容
        if taskNumber == 1:
            self.nextmarker = ''
            taskNumber = 0
            self.allFiles = 0
            self.allSize = 0
        log.info('recover generating round0 task info successfully, taskNumber:'+str(taskNumber)+
                 ' allfiles:'+str(self.allFiles)+' allsize:'+str(self.allSize) + ' nextmarker:'+self.nextmarker)
        return taskNumber

    def generate_round0_task_s3file(self):
        taskPrefix = self.synctask+'/'+self.jobType+'/round0/task'
        taskNumber = self.recover_generating_round0_task_info(taskPrefix)
        currentSize = 0
        currentFileNumbers = 0
        filelist = list()
        #in first request of list obejects,marker is ''
        marker = self.nextmarker
        log.info( 'start to generate '+self.jobType+'-tasks(round 0) for '+self.synctask)
        while True:
            returnVal = self.srcClient.list_objects_without_delimiter(self.source['bucketName'],self.source['prefix'],marker)
            [objects,isTruncated,marker] = returnVal
            for i in range(0,len(objects)):
                objectSize,objectLastModify = self.srcClient.get_object_size_and_lastmodify(self.source['bucketName'], objects[i])
                if self.isRightTimeToOpeareteThisFile(objectLastModify):
                    filelist.append(objects[i]+'\t'+str(objectSize))
                    currentSize+=long(objectSize)
                    currentFileNumbers += 1
                if currentSize >=self.taskSize or currentFileNumbers>=self.taskFileNumbers:
                    self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
                    put_nextmarker(self.desClient, self.destination['bucketName'], self.synctask + '/nextmarker',str(filelist[len(filelist) - 1].split('\t')[0]))
                    if taskNumber == 0 and self.is_user_set_task_attributes == False :
                        self.waitfor_task0_finished()
                    taskNumber += 1
                    currentSize = 0
                    currentFileNumbers = 0
                    filelist[:] = []
            if isTruncated == False:
                break
        if currentSize != 0 or currentFileNumbers != 0:
            self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
        log.info( 'generate tasks for '+self.synctask+' successfully')
        return True
    
    def isRightTimeToOpeareteThisFile(self,lastModify):
        if lastModify == -1:
            return False
        else:
            if float(lastModify) >= float(self.sync['since']) and float(lastModify) <= float(self.sync['starttime']):
                return True
            else:
                return False
            
    def generate_round0_task_diskfile(self):
        fileNamePrefix = self.synctask+'/diskfilelist/'
        self.generate_diskfile_list(fileNamePrefix)
        taskPrefix = self.synctask+'/'+self.jobType+'/round0/task'
        currentFileNumbers = 0
        currentSize = 0
        filelist = list()
        log.info( 'start to generate '+self.jobType+'-tasks(round 0) for '+self.synctask+' nextmarker is:' + self.nextmarker)
        pairs = self.nextmarker.split('\t')
        filelistCnt = long(pairs[0])
        taskNumber = long(pairs[1])
        while True:
            filelistName = fileNamePrefix+'filelist'+str(filelistCnt)
            lines = self.get_filelist(self.desClient, self.destination['bucketName'], filelistName, fileNamePrefix+'status')
            if not lines:
                break
            else:
                filelistCnt+=1
                line = lines.readline()
                while(line):
                    line = line.strip().strip('\n')
                    pair = line.split('\t')
                    for i in range(0,len(pair)):
                        pair[i] = pair[i].strip()
                    filelist.append(line)
                    currentSize+=long(pair[1])
                    currentFileNumbers += 1
                    if (currentSize >= self.taskSize) or (currentFileNumbers >= self.taskFileNumbers):
                        self.generate_a_task(filelist,currentFileNumbers,currentSize,taskNumber,taskPrefix)
                        if taskNumber == 0:
                            self.waitfor_task0_finished()
                        taskNumber += 1
                        currentSize = 0
                        currentFileNumbers = 0
                        filelist[:] = []
                    line = lines.readline()
                if currentSize != 0 or currentFileNumbers != 0:
                    self.generate_a_task(filelist, currentFileNumbers, currentSize, taskNumber, taskPrefix)
                    taskNumber += 1
                    currentSize = 0
                    currentFileNumbers = 0
                    filelist[:] = []
                put_nextmarker(self.desClient, self.destination['bucketName'], self.synctask + '/nextmarker', str(filelistCnt)+'\t'+str(taskNumber))
        log.info( 'generate tasks for '+self.synctask+' successfully')
        return True
    
    def generate_diskfile_list(self,fileNamePrefix):
        log.info('start to generate disk file list')
        task = GenerateDiskFileListTask(self.source['absolutepath'],self.destination['accessKey'],
                                        self.destination['secretKey'],self.destination['endpoint'],
                                        self.destination['bucketName'],fileNamePrefix,self.sync, self.master_ip_port)
        taskID = generateID.taskID(str(time.time())+' '+self.destination['bucketName']+' '+self.source['absolutepath'])
        task.set_ID(taskID)
        self.master.do_task(task)
    
    def get_filelist(self, client, bucketname, filelistName, filelistStatusName):
        while True:
            try:
                lines = readdata.readlines(client, bucketname,filelistName)
                return lines
            except ossexception.GetObjectFailed,e:
                if client.does_object_exists(bucketname,filelistStatusName)==True:
                    log.info(e)
                    log.info('no more filelist can be read, the last is the last one')
                    return None
                else:
                    log.info(e)
                    log.info('the upper means generating disk-filelist is not finished')
                    sleep(10)
    
    def generate_a_task(self,filelist,currentFileNumbers,currentSize,taskNumber,taskPrefix):
        filelist.insert(0, str(currentFileNumbers)+'\t'+str(currentSize))
        put_task(self.desClient, self.destination['bucketName'],taskPrefix+str(taskNumber)+'/',filelist)
        self.allFiles += currentFileNumbers
        self.allSize += currentSize
        keyForTaskPath = taskPrefix+str(taskNumber)+'/'
        task = TransferOrCheckTask(self.synctask,self.jobType,self.source,self.destination,
                                        keyForTaskPath,self.master_ip_port,self.check['mode'],self.sync)

        task.set_taskFileSize(currentSize)
        task.set_setTaskFileNumbers(currentFileNumbers)
        task.set_status(status.TASK.ready)
        self.put(task)
        log.info( 'task'+str(taskNumber) +' '+ self.synctask +' '+ self.jobType 
                             + ' fileNumbers:'+str(currentFileNumbers)+' taskSize:'+str(currentSize))
            
    def get_all_files_size(self):
        return [self.allFiles,self.allSize]

def put_task(client, bucketName,prefix,fileList):
    client.put_object( bucketName, prefix+'filelist','\n'.join(fileList))
    client.put_object( bucketName, prefix+'errorlist','')
    client.put_object(bucketName, prefix + 'md5list', '')
    client.put_object( bucketName, prefix+'taskstatus','ready')
    
def put_roundstatus(client, bucketName, keyname, status):
    client.put_object(bucketName, keyname, status)

def put_nextmarker(client, bucketName, keyName, nextmarker):
    client.put_object(bucketName, keyName, nextmarker)

def reset_taskinfo(client,bucketName,keyForTaskPath):
    client.put_object(bucketName,keyForTaskPath+'taskstatus','ready')
    client.put_object(bucketName,keyForTaskPath+'errorlist','')
    client.put_object( bucketName, keyForTaskPath+'md5list','')
    
    #如果下面的workerstatus也设置了，那么当master退出再重启时，会导致worker无法读取workerstatus
    """
    if client.does_object_exists(bucketName,keyForTaskPath+'workerstatus'):
        if not (client.delete_one_object(bucketName,keyForTaskPath+'workerstatus')):
            exit(1)
    """
    return True
    

if __name__ == '__main__':
    pass
    
    
    
    
    
    
