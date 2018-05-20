#coding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.utils import getpathinfo, setlogging
masterport = sys.argv[1] if len(sys.argv) == 2 else '4244'
logsdir = getpathinfo.get_logs_dir(os.path.abspath(__file__).replace('\\', '/'))
s3transferPath = logsdir[0:logsdir.rfind('/',0,len(logsdir)-1)]

masterslogdir = logsdir+'log-masters/'
if os.path.exists(masterslogdir) == False:
    os.mkdir(masterslogdir)
logFileName = 'log-master-' + str(masterport) + '.txt'
setlogging.set_logging(masterslogdir+logFileName)

reload(sys)
sys.setdefaultencoding('utf8')

from common.const import type, port
from common.enum import status
from common.myexceptions import ossexception,jobexception
from common.utils import clean,timeoperation,configoperations,mail
from utils import collectinfo,decodeID
import taskpool
from time import sleep
from common.sdk import s3client, tencentclient, aliyunclient, clientfactory
from SimpleXMLRPCServer import SimpleXMLRPCServer
import workermanagerinfo
from mastermessage import MasterMessage
from worker.workermessage import WorkerMessage
from collections import deque
from SocketServer import ThreadingMixIn
import threading
import xmlrpclib
import time
import logging
log = logging.getLogger('root')


"""异步的server类"""
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class Master:
    EachRequestAndResponseTime = 0.04
    __bigGranularityLock = threading.Lock()
    __workermanagerdict = dict()
    __job = None
    __masterLock = threading.Lock()     #保证一个master同一个时间仅仅处理一个job
    __IsDeleteThisJob = False
    __workerPriorityqueue = deque()
    __workerSpeed = dict()

    def __init__(self, port):
        self.port = port
        self.masterstatus = status.MASTER.ready
        self.__IsDeleteThisJob = False
        self.taskPool = None
        self.__init()
        self.__doesHaveToken = False

    def __init(self):
        self.finishedFiles = 0
        self.finishedSize = 0
        self.failedFiles = 0
        self.FinishedTasks = 0
        self.roundStartTime = time.time()

    def set_token(self):
        self.__doesHaveToken = True
        sleep(3)
        self.__doesHaveToken = False

    def get_status(self):
        return self.masterstatus

    def check_workermanagers(self):
        try:
            self.__bigGranularityLock.acquire()
            for ip, workermanagerinfo in self.__workermanagerdict.items():
                workermanagerstatus = get_workermanagerstatus(ip)
                if workermanagerstatus == status.WORKER.shutdown:
                    log.warning('workermanager, ip:'+str(ip)+' is shutdown, we will remove all tasks on it:')
                    log.warning(workermanagerinfo.get_taskqueue())
                    mail.send_mail('[warning] workermanager, ip:' + str(ip) + ' is shutdown, please check it')
                    for task in workermanagerinfo.get_taskqueue():
                        pos = decodeID.taskID(task.get_ID())
                        self.taskPool.set_task_status(pos, status.TASK.done, status.TASK.dispatched)
                    self.__workermanagerdict.pop(ip)
                    break
                else:
                    if self.__workerSpeed[ip] != 0:
                        for task in workermanagerinfo.get_taskqueue():
                            if (time.time() - float(task.get_startTime())) >\
                                (float(task.get_taskFileSize())/1024/self.__workerSpeed[ip] + task.get_taskFileNumbers()*Master.EachRequestAndResponseTime) * 3:
                                self.taskPool.set_task_status(decodeID.taskID(task.get_ID()), status.TASK.done, status.TASK.dispatched)
        except Exception, e:
            log.info(e)
        finally:
            self.__bigGranularityLock.release()

    def send_message(self, messageAttr):
        try:
            self.__bigGranularityLock.acquire()
            message = WorkerMessage().with_attribute(messageAttr)
            log.info('receive message from workermanager:'+str(message.get_ip()))
            log.info(messageAttr)
            workerip = message.get_ip()
            if not self.__workermanagerdict.has_key(workerip):
                log.info('we didn\'t have any record of '+str(workerip))
            else:
                task = self.__workermanagerdict[workerip].get_task(message.get_ID())
                if not task:
                    log.info('we didn\'t have any record of '+str(message.get_ID())+' in '+str(workerip))
                self.__workermanagerdict[workerip].rm_task(task)
                if  message.get_type()== type.Task.TransferOrCheck:
                    self.__dealwith_TransferOrCheckTask_message(message, task)
                elif message.get_type()== type.Task.GenerateFileList:
                    self.__dealwith_GenerateDiskFileListTask_message(message, task)
            return True
        except Exception, e:
            log.warning(e)
            return False
        finally:
            self.__bigGranularityLock.release()

    def __dealwith_TransferOrCheckTask_message(self, message, task):
        if message.get_isSuccess() == True:
            result = message.get_result()
            self.finishedFiles += result.get_succeeded()
            self.finishedSize += result.get_uploadSize()
            self.failedFiles += result.get_failed()
            self.FinishedTasks += 1
            log.info(task.to_string()+' finished successfully')
            log.info('for this task, succeed:'+str(result.get_succeeded())+'/'+str(result.get_fileNumber())+
                    ', failed:'+str(result.get_failed())+'/'+str(result.get_fileNumber())+
                    ', size:'+str(result.get_uploadSize())+'/'+str(result.get_taskSize())+
                    ', costtime:'+str(result.get_costTime())+'s, speed:'+str(result.get_speed())+"KB/s")
            allfiles, allsize = self.taskPool.get_all_files_size()
            infoForLog = ('for this job, succeed:'+str(self.finishedFiles)+'/'+str(allfiles)+
                            ', failed:'+str(self.failedFiles)+'/'+str(allfiles)+
                            ', size:'+str(self.finishedSize)+'/'+str(allsize)+' ')
            if long(allsize) != 0:
                infoForLog += str(round((self.finishedSize*1.0/allsize)*100,2)) + '%'
            log.info(infoForLog)
            self.__workerSpeed[message.get_ip()] = float(result.get_speed())
            self.__update_priorityqueue()
        else:
            log.info(task.to_string()+' failed')
        pos = decodeID.taskID(message.get_ID())
        self.taskPool.set_task_status(pos, status.TASK.done, status.TASK.dispatched)

    def __update_priorityqueue(self):
        for i in range(len(self.__workerPriorityqueue)):
            for j in range(i, len(self.__workerPriorityqueue) - 1):
                if self.__workerSpeed[self.__workerPriorityqueue[j]] < self.__workerSpeed[self.__workerPriorityqueue[j + 1]]:
                    tmp = self.__workerPriorityqueue[j]
                    self.__workerPriorityqueue[j] = self.__workerPriorityqueue[j + 1]
                    self.__workerPriorityqueue[j + 1] = tmp

    def __dealwith_GenerateDiskFileListTask_message(self, message, task):
        if message.get_isSuccess() == True:
            log.info(task.to_string()+' finished successfully') 
        else:
            log.info(task.to_string()+' failed')
            message = MasterMessage(configoperations.get_master_ip(), self.__job['job-ID'], masterport, False)
            send_message_to_mastermanager(message)
            
    def do_job(self,job):
        log.info('receive job:'+job['job-ID'])
        log.info(job)
        th = threading.Thread(target=self.__do_job_thread, args=(job,))
        th.setDaemon(True)
        th.start()
        return True
    
    def __do_job_thread(self, job):
        try:
            self.__masterLock.acquire()
            message = MasterMessage(configoperations.get_master_ip(), job['job-ID'],masterport,True,None)
            self.masterstatus = status.MASTER.busy
            self.__bigGranularityLock.acquire()
            self.__workermanagerdict = dict()
            self.__bigGranularityLock.release()
            self.__job = job
            self.desClient = s3client.S3Client(self.__job['des-accesskey'],self.__job['des-secretkey'],self.__job['des-endpoint'])
            self.__build_synctask_context()
            self.currentJobType = self.__job['job-type']
            self.taskPool = taskpool.TaskPool(self, str(configoperations.get_master_ip())+':'+str(masterport))
            self.__do_synctask()
            send_message_to_mastermanager(message)
            log.info('job:' + self.__job['job-ID'] + ' finished successfully')
        except (ossexception.GetObjectFailed,ossexception.PutObjectFailed, jobexception.GenerateSynctaskFailed,Exception), e:
            log.warning(e)
            message.set_isSuccess(False)
            send_message_to_mastermanager(message)
            log.info('job:' + self.__job['job-ID'] + ' failed')
        finally:
            self.__init()
            self.taskPool = None
            self.masterstatus = status.MASTER.ready
            self.__masterLock.release()
    
    def deletejob(self, jobID):
        try:
            self.__bigGranularityLock.acquire()
            log.info('start to delete job:' + jobID)
            self.__IsDeleteThisJob = True
            for ip, workermanagerinfo in self.__workermanagerdict.items():
                for task in workermanagerinfo.get_taskqueue():
                    delete_task_from_workermanager(ip, task.get_ID())
            log.info('deleted job:' + jobID + ' successfully')
            return True
        except Exception, e:
            log.info(e)
        finally:
            self.__bigGranularityLock.release()

    def get_job_status(self):
        if not self.taskPool:
            return None
        allfiles, allsize = self.taskPool.get_all_files_size()
        return [self.currentJobType, str(self.finishedFiles), str(self.finishedSize), str(allfiles), str(allsize), self.roundStartTime ]

    def __do_synctask(self):
        self.taskPool.init_job(self.__job)
        if self.__job['job-type'] == type.JobType.Transfer:
            self.currentJobType = type.JobType.Transfer
            self.__dispatcher(type.JobType.Transfer)
            self.__generate_transfer_errorlist()
            self.__write_synctask_status(type.JobType.Transfer, 'done')
            
        if self.__job['job-type'] == type.JobType.Check or self.__job['check-time'] == 'now':
            self.currentJobType = type.JobType.Check
            self.__dispatcher(type.JobType.Check)
            self.__generate_check_errorlist()
            self.__generate_md5_list()
            self.__write_synctask_status(type.JobType.Check, 'done')
            
        self.__clean_syntask()
        if self.__job['sync-enable-increment']=='True':
            self.__write_synctask_status('', 'done')
            
    def __dispatcher(self,jobType):
        self.__init()
        self.taskPool.generate_task_from_job(jobType, self.desClient)
        while True:
            if self.__IsDeleteThisJob == True:
                sleep(100)
                continue
            if self.is_this_round_finished():
                [requireRoundNum,startRound,currentRound] = self.taskPool.get_round_info()
                self.currentRound = currentRound
                taskpool.put_roundstatus(self.desClient, self.__job['des-bucketName'],self.__job['synctask']+'/'+jobType+'/round'+str(currentRound)+'/roundstatus', 'done')
                if (currentRound+1-startRound == requireRoundNum):
                    log.info('all round finished')
                    break
                else:
                    log.info('round '+str(currentRound)+' of '+self.__job['synctask']+' has finished')
                    self.__init()
                    self.taskPool.generate_nextround_task_with_thread(currentRound+1)

            task = self.taskPool.get()
            if task:
                self.do_task(task)
            else:
                sleep(10)
    
    def do_task(self, task):
        while True:
            try:
                workerip = self.__get_ready_worker()
                self.__bigGranularityLock.acquire()
                if not self.__workermanagerdict.has_key(workerip):
                    self.__workermanagerdict[workerip] = workermanagerinfo.WorkerManagerInfo(port.ManagerPort.WorkerManager, status.WORKER.ready)
                s = xmlrpclib.ServerProxy('http://' + str(workerip) + ':' + str(port.ManagerPort.WorkerManager))
                log.info('workermanager:'+workerip+' is ready')
                if s.addtask(task) == True:
                    s.set_ip(workerip)
                    self.__workermanagerdict[workerip].appendright_task(task)
                    log.info('send the following task to workermanager:'+str(workerip)+' successfully')
                    log.info(task.to_string())
                    return True
            except Exception,e:
                log.warning(e)
                pos = decodeID.taskID(task.get_ID())
                self.taskPool.set_task_status(pos, status.TASK.done, status.TASK.dispatched)
                return False
            finally:
                self.__bigGranularityLock.release()
                
    def __get_ready_worker(self):
        try:
            while True:
                while self.__doesHaveToken == False:
                    sleep(1)
                machinesIpList = configoperations.read_machines_ip()
                if not machinesIpList:
                    if self.__get_workerstatus(configoperations.get_master_ip()) == status.WORKER.ready:
                        return configoperations.get_master_ip()
                for workerip in machinesIpList:
                    if self.__workerSpeed.has_key(workerip) == False:
                        self.__workerSpeed[workerip] = 0
                        self.__workerPriorityqueue.append(workerip)
                for workerip in self.__workerPriorityqueue:
                    if self.__get_workerstatus(workerip) == status.WORKER.ready:
                        return workerip
        except Exception, e:
            log.info(e)
        finally:
            self.__doesHaveToken = False
            
    def __get_workerstatus(self, workerip):
        try:
            s = xmlrpclib.ServerProxy('http://' + str(workerip) + ':' + str(port.ManagerPort.WorkerManager))
            return s.get_status()
        except Exception,e:
            log.warning(e)
            return status.WORKER.shutdown

    def __write_synctask_status(self, jobType,status):
        bucketName = self.__job['des-bucketName']
        if jobType == '':
            keyName = self.__job['synctask'] + '/syncstatus'
        else: 
            keyName = self.__job['synctask'] + '/' + jobType+'/'+jobType+'status'
        self.desClient.put_object(bucketName, keyName, status)
        
    def is_this_round_finished(self):
        return ((self.taskPool.is_all_tasks_finished() and self.taskPool.is_generating_tasks()==False))
    
    def __generate_transfer_errorlist(self):
        collectinfo.generate_errorlist(self.desClient, self.__job['des-bucketName'],type.JobType.Transfer,self.__job['synctask'],\
                                             self.currentRound,s3transferPath+'/jobs/'+self.__job['job-ID']+'/'+self.__job['transfer-error-output'])
           
    def __generate_check_errorlist(self):
        collectinfo.generate_errorlist(self.desClient, self.__job['des-bucketName'],type.JobType.Check,self.__job['synctask'],\
                                             self.currentRound,s3transferPath+'/jobs/'+self.__job['job-ID']+'/'+self.__job['check-error-output'])
    def __generate_md5_list(self):
        if self.__job['check-mode'] == 'md5':
            return collectinfo.generate_md5list(self.desClient, self.__job['des-bucketName'],self.__job['synctask'],
                                            s3transferPath+'/jobs/'+self.__job['job-ID']+'/'+self.__job['check-md5-output'])
        else:
            return -1
        
    def __clean_syntask(self):
        if self.__job['sync-enable-increment']=='True' or (self.__job['job-type'] == type.JobType.Transfer and self.__job['check-time'] == 'future'):
            pass
        else:
            clean.clean_with_prefix(self.desClient,self.__job['des-bucketName'], self.__job['synctask'])
          
    def __build_synctask_context(self):
        log.info('start to build context for job:'+self.__job['job-ID'])
        if self.__job['sync-enable-increment'] == 'False':
            self.__build_context_with_disable_increment_sync()
        else:
            self.__build_context_with_enable_increment_sync()
            
    def __build_context_with_disable_increment_sync(self):
        try:
            if self.desClient.does_object_exists(self.__job['des-bucketName'],self.__job['job-ID']+'/synctask0/syncstatus') == True:
                self.__create_synctask(0, True)
            else:
                self.__create_synctask(0, False)
        except ossexception.GetObjectFailed, e:
            log.info(e)
            log.info('build context failed with disable increment sync')
    
    def __build_context_with_enable_increment_sync(self):
        synctaskCnt = 0
        while self.desClient.does_object_exists(self.__job['des-bucketName'],
                                                self.__job['job-ID']+'/synctask'+str(synctaskCnt)+'/syncstatus'):
            synctaskCnt+=1
        if synctaskCnt == 0:
            self.__create_synctask(synctaskCnt, False)
            log.info('create synctask0 successfully!')
        else:
            lines = self.desClient.get_object_with_stream(self.__job['des-bucketName'],
                                                self.__job['job-ID']+'/synctask'+str(synctaskCnt-1)+'/syncstatus')
            line = lines.readline()
            if line == 'done':
                self.__create_synctask(synctaskCnt, False)
                log.info('create synctask'+str(synctaskCnt)+' successfully!')
            else:
                self.__create_synctask(synctaskCnt-1, True)
                log.info('recover synctask'+str(synctaskCnt-1)+' successfully!')
        return True
    
    def __create_synctask(self, synctaskCnt, isRecoverThisSyncTask):
        self.__job['synctask'] = self.__job['job-ID']+'/synctask'+str(synctaskCnt)
        if synctaskCnt == 0:
            if isRecoverThisSyncTask == False:
                self.__create_new_synctask()
            else:
                self.__recover_old_synctask()
        else:
            lines = self.desClient.get_object_with_stream(self.__job['des-bucketName'],
                                                self.__job['job-ID']+'/synctask'+str(synctaskCnt-1)+'/starttime')
            self.__job['sync-since'] = str(float(lines.readline())-300)
            if isRecoverThisSyncTask ==False:
                self.__create_new_synctask()
            else:
                self.__recover_old_synctask()
    
    def __create_new_synctask(self):
        clean.clean_with_prefix(self.desClient, self.__job['des-bucketName'], self.__job['synctask'])
        if self.__job['sync-enable-increment']=='False':
            serverTime = 1.7e+308
        else:
            serverTime = self.__get_source_time()
        if serverTime == -1:
            log.error('get server time failed')
            raise jobexception.GenerateSynctaskFailed
        if self.__job['src-filetype'] == type.FileType.DiskFile:
            nextmarker = '0\t0'
        else:
            nextmarker = ''
        self.desClient.put_object(self.__job['des-bucketName'],self.__job['synctask']+'/starttime',str(serverTime))
        self.desClient.put_object(self.__job['des-bucketName'],self.__job['synctask']+'/nextmarker', nextmarker)
        self.desClient.put_object(self.__job['des-bucketName'],self.__job['synctask']+'/syncstatus','running')
        self.__job['nextmarker'] = nextmarker
        self.__job['sync-starttime'] = serverTime
        log.info('create synctask:'+self.__job['synctask']+' successfully')

    def __recover_old_synctask(self):
        lines = self.desClient.get_object_with_stream(self.__job['des-bucketName'],self.__job['synctask']+'/starttime')
        self.__job['sync-starttime'] = lines.readline()
        lines = self.desClient.get_object_with_stream(self.__job['des-bucketName'],self.__job['synctask']+'/nextmarker')
        self.__job['nextmarker'] = lines.readline()
        log.info('recove synctask:'+self.__job['synctask']+' successfully')
        
    def __get_source_time(self):
        if self.__job['src-filetype'] == type.FileType.DiskFile:
            return timeoperation.get_filesystem_time(self.__job['src-absolutepath'],self.__job['sync-enable-increment'])
        else:
            client = clientfactory.create_client(self.__job['src-filetype'],self.__job['src-accesskey'],self.__job['src-secretkey'],self.__job['src-endpoint'])
            return timeoperation.get_server_time(client,self.__job['src-bucketName'], self.__job['synctask'])

def send_message_to_mastermanager(message):
    #when send message failed, then loop
    while True:
        try:
            s = xmlrpclib.ServerProxy('http://' + configoperations.get_master_ip() + ':' + str(port.ManagerPort.MasterManager), allow_none=True)
            s.send_message(message)
            log.info('send message to mastermanager successfully')
            return True
        except Exception, e:
            log.warning(e)
            log.warning('send message to mastermanager failed, will retry')
            sleep(10)

def start_master(port):
    log.info('master is starting, and listening on port:'+str(port))
    server = ThreadXMLRPCServer(("0.0.0.0", int(port)),allow_none=True,logRequests=False)  
    server.register_instance(Master(port))
    server.serve_forever()

def monitor_thread():
    while True:
        sleep(10)
        try:
            s = xmlrpclib.ServerProxy('http://'+configoperations.get_master_ip()+':'+str(masterport))
            s.check_workermanagers()
        except Exception,e:
            log.warning(e)

def get_workermanagerstatus(workermanagerip):
    retrytimes = 3
    while retrytimes:
        try:
            s = xmlrpclib.ServerProxy('http://'+workermanagerip+':'+str(port.ManagerPort.WorkerManager))
            return s.get_status()
        except Exception,e:
            log.warning(e)
            retrytimes = retrytimes - 1
        sleep(1)
    return status.WORKER.shutdown

def delete_task_from_workermanager(ip, taskID):
    try:
        s = xmlrpclib.ServerProxy('http://'+ip+':'+str(port.ManagerPort.WorkerManager))
        isSuccess = s.deletetask(taskID)
        if isSuccess==True:
            log.info('delete task:'+taskID+' from workermanager successfully')
        else:
            log.warning('delete task:'+taskID+' from workermanager failed')      
        return True
    except Exception,e:
        log.warning(e)
        log.warning('delete task:'+taskID+' from workermanager failed')
        return False

def main(port):
    dispatcher_thread = threading.Thread(target=monitor_thread, args=())
    dispatcher_thread.setDaemon(True)
    dispatcher_thread.start()
    start_master(port)
    
def catch_main_except():
    try:
        main(masterport)
    except Exception,e:
        log.error(e)
    finally:
        log.info('the master:' + str(masterport) + ' is closed!')


if __name__ == '__main__':
    catch_main_except()


    
        






