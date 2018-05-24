#encoding=utf-8
import os,sys
from time import sleep
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

from common.utils import getpathinfo,setlogging
logsdir = getpathinfo.get_logs_dir(os.path.abspath(__file__).replace('\\', '/'))
logFileName = 'log-workermanager.txt'
setlogging.set_logging(logsdir+logFileName)

reload(sys)
sys.setdefaultencoding('utf8')
from common.enum import status
from SimpleXMLRPCServer import SimpleXMLRPCServer
from common.const import port
from common.utils import process, mail
from common.dataobject.message import Message
from collections import deque
from worker.workermessage import WorkerMessage
from multiprocessing import cpu_count
import threading
import xmlrpclib
import workerinfo
from SocketServer import ThreadingMixIn
import time

import logging
log = logging.getLogger('root')

"""异步的server类"""
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class WorkerManager:
    MaxWorkerNum = cpu_count()
    bigGranularityLock = threading.Lock()
    workers_port_startafter = port.ManagerPort.WorkerManager
    taskqueue = deque()
    workerqueue = deque()
    ip = ''
    def __init__(self):
        pass

    def stop(self):
        WorkerManager.bigGranularityLock.acquire()
        for worker in WorkerManager.workerqueue:
            log.info('start to kill workers')
            process.stop_process(worker.get_port())
        WorkerManager.workerqueue = deque()
        WorkerManager.workerqueue = deque()
        WorkerManager.bigGranularityLock.release()
    
    def set_ip(self, ip):
        WorkerManager.ip = ip
    
    def get_status(self):
        #有可能遍历WorkerManager.workerqueue会失败，原因是dequeue对这种遍历不是线程安全的，这种情况下直接返回busy
        try:
            if len(WorkerManager.taskqueue) >= WorkerManager.MaxWorkerNum:
                return status.WORKER.busy
            cnt = 0
            for worker in WorkerManager.workerqueue:
                if worker.get_status() == status.WORKER.ready:
                    return status.WORKER.ready
                cnt += 1
                if cnt >= WorkerManager.MaxWorkerNum:
                    break

            if len(WorkerManager.workerqueue) < WorkerManager.MaxWorkerNum:
                return status.WORKER.ready
            return status.WORKER.busy
        except Exception, e:
            log.info(e)
            return status.WORKER.busy

    def addtask(self, task):
        WorkerManager.bigGranularityLock.acquire()
        log.info('receive the following task:')
        log.info(task)
        WorkerManager.taskqueue.append(task)
        WorkerManager.bigGranularityLock.release()
        return True
    
    def deletetask(self, taskID):
        try:
            WorkerManager.bigGranularityLock.acquire()
            log.info('start to delete task:'+taskID)
            for worker in WorkerManager.workerqueue:
                if worker.get_task() and taskID == worker.get_task()['ID']:
                    process.stop_process(worker.get_port())
                    worker.get_task().set_isDeleted(True)
                    break
            log.info('deleted task:' + taskID)
            return True
        except Exception, e:
            log.info(e)
            return False
        finally:
            WorkerManager.bigGranularityLock.release()

    def send_message(self, messageAttr):
        try:
            WorkerManager.bigGranularityLock.acquire()
            message = WorkerMessage().with_attribute(messageAttr)
            log.info('receive message from worker:'+message.get_port())
            log.info(messageAttr)
            isFindFlag = 0
            for worker in WorkerManager.workerqueue:
                if (str(worker.get_port()) == str(message.get_port()) and worker.get_task()
                    and str(message.get_ID()) == str(worker.get_task()['ID'])):
                    isFindFlag = 1
                    worker.set_status(status.WORKER.ready)
                    master_ip_port = worker.get_task()['master_ip_port']
                    worker.set_task(None)
                    message.set_ip(WorkerManager.ip)
                    break
            if isFindFlag == 0:
                log.info('we didn\'t have any record of '+message.get_ID())
        except Exception, e:
            log.info(e)
        finally:
            WorkerManager.bigGranularityLock.release()
        #放在这个是为了解决死锁
        if isFindFlag == 1:
            send_message_to_master(master_ip_port, message)

    def set_max_worker_num(self, maxNum):
        WorkerManager.MaxWorkerNum = maxNum
        return True

    def get_worker_num_info(self):
        WorkerManager.bigGranularityLock.acquire()
        cntBusy = 0
        for worker in WorkerManager.workerqueue:
            if worker.get_status() == status.WORKER.busy:
                cntBusy += 1
        WorkerManager.bigGranularityLock.release()
        return [WorkerManager.MaxWorkerNum, len(WorkerManager.workerqueue), cntBusy]

def start_workermanager():
    try:
        server = ThreadXMLRPCServer(('0.0.0.0', port.ManagerPort.WorkerManager), allow_none=True, logRequests=False)
        server.register_instance(WorkerManager())
        log.info('create workermanager successfully, its port is '+str(port.ManagerPort.WorkerManager))
        server.serve_forever()
    except Exception,e:
        log.error('create workermanager failed, its port is '+str(port.ManagerPort.WorkerManager))
        log.error(e)
        
def manager_thread():
    while True:
        try:
            WorkerManager.bigGranularityLock.acquire()
            if len(WorkerManager.taskqueue) != 0:
                task = WorkerManager.taskqueue.popleft()
                worker = findReadyWorker()
                if not worker:
                    raise Exception('there are no workers available')
                s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(worker.get_port()))
                worker.set_task(task)
                worker.set_status(status.WORKER.busy)
                s.set_ip(WorkerManager.ip)
                s.do_task(task)
                log.info('send task:' + task['ID'] + ' to worker:' + str(worker.get_port()))
        except Exception, e:
            log.info(e)
            WorkerManager.taskqueue.append(task)
        finally:
            WorkerManager.bigGranularityLock.release()
        time.sleep(10)
    
def findReadyWorker():
    for worker in WorkerManager.workerqueue:
        if worker.get_status() == status.WORKER.ready:
            workerstatus  = get_workerstatus(worker.get_port())
            if workerstatus == status.WORKER.ready:
                return worker
    if len(WorkerManager.workerqueue) < WorkerManager.MaxWorkerNum:
        return start_worker()
    return None

def start_worker():
    try:
        while True:
            WorkerManager.workers_port_startafter += 1
            if WorkerManager.workers_port_startafter > port.ManagerPort.WorkerManager+1000:
                WorkerManager.workers_port_startafter = port.ManagerPort.WorkerManager + 1
            if process.get_processID_return_when_failed(WorkerManager.workers_port_startafter) != -1:
                log.info(str(WorkerManager.workers_port_startafter) + ' is already in use')
                continue
            path = os.path.abspath(__file__).replace('\\', '/')
            absdir = path[0:path.rfind('/')]
            workerpath = absdir[0:absdir.rfind('/',0,len(absdir))]
            os.system('nohup python '+workerpath+'/worker/worker.py '+
                        str(WorkerManager.workers_port_startafter)+' >.tmp 2>.tmp &')
            workerProcessID = process.get_processID_loop_when_failed(WorkerManager.workers_port_startafter)
            if workerProcessID != -1:
                worker = workerinfo.WorkerInfo(WorkerManager.workers_port_startafter, status.WORKER.ready)
                WorkerManager.workerqueue.append(worker)
                log.info('start worker:'+str(worker.get_port())+' successfully, the process id is '+str(workerProcessID))
                return worker
            else:
                log.warning('start worker:'+str(WorkerManager.workers_port_startafter)+' failed, and we will try another port')
    except Exception, e:
        log.info(e)
        return None

def get_busy_worker_number(workerqueue):
    cnt = 0
    for worker in workerqueue:
        if worker.get_status() == status.WORKER.busy:
            cnt += 1
    return cnt

def get_currentrate():
    currentRate = 0
    for worker in WorkerManager.workerqueue:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(worker.get_port()))
            currentRate += s.getrate()
        except Exception,e:
            log.info(e)
            pass
    return currentRate

def send_message_to_master(master_ip_port, message):
    retryTimes = 0
    while True:
        try:
            s = xmlrpclib.ServerProxy('http://' + str(master_ip_port), allow_none=True)
            if s.send_message(message):
                log.info('send message:' + message.get_ID() + ' to master successfully, master_ip_port:' + str(master_ip_port))
                return True

        except Exception, e:
            log.warning(e)
            log.warning('send message to master failed, master_ip_port:' + str(master_ip_port)+ ', will retry')
            retryTimes += 1
            sleep(10)
        if retryTimes > 5:
            mail.send_mail('[error] send message to master failed, master_ip_port:' + str(master_ip_port) + ', you should check why')

def monitor_thread():
    while True:
        try:
            flag = 0
            WorkerManager.bigGranularityLock.acquire()
            for worker in WorkerManager.workerqueue:
                workerstatus = get_workerstatus(worker.get_port())
                if workerstatus == status.WORKER.shutdown:
                    if worker.get_status() == status.WORKER.busy:
                        flag = 1
                        task = worker.get_task()
                        WorkerManager.workerqueue.remove(worker)
                        message = WorkerMessage(WorkerManager.ip, task['ID'],worker.get_port(),False)
                        message.set_type(task['type'])
                        log.warning('[warning] '+'worker, port:'+str(worker.get_port())+' ip:'+str(WorkerManager.ip)+' is shutdown, we will remove task, taskID:'+str(task['ID'])+' on it')
                        if task.get_isDeleted() == False:
                            mail.send_mail('[warning] '+'worker, port:'+str(worker.get_port())+' ip:'+str(WorkerManager.ip)+' is shutdown, we will remove task, taskID:'+str(task['ID'])+' on it')
                    else:
                        WorkerManager.workerqueue.remove(worker)
                        log.warning('[warning] ' + 'worker, port:' + str(worker.get_port()) + ' ip:' + str(WorkerManager.ip) + ' is shutdown, no tasks on it')
                        mail.send_mail('[warning] ' + 'worker, port:' + str(worker.get_port()) + ' ip:' + str(WorkerManager.ip) + ' is shutdown, no tasks on it')
                    break
        except Exception, e:
            log.warning(e)
        finally:
            WorkerManager.bigGranularityLock.release()
        if flag == 1:
            send_message_to_master(task['master_ip_port'], message)

        sleep(10)

def get_workerstatus(port):
    retrytimes = 3
    while retrytimes:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port))
            return s.get_status()
        except Exception,e:
            log.warning(e)
            log.info('getting status of worker:' + str(port) + ' failed')
            retrytimes = retrytimes - 1
        sleep(1)
    return status.WORKER.shutdown

def get_current_taskID(port):
    retrytimes = 3
    while retrytimes:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port))
            return s.get_current_taskID()
        except Exception, e:
            log.warning(e)
            retrytimes = retrytimes - 1
        sleep(1)
    return status.WORKER.shutdown

def start():
    dispatcherThread = threading.Thread(target=manager_thread, args=())
    dispatcherThread.setDaemon(True)
    dispatcherThread.start()
    
    monitorThread = threading.Thread(target=monitor_thread, args=())
    monitorThread.setDaemon(True)
    monitorThread.start()
    
    start_workermanager()

if __name__ == '__main__':
    start()

    
    
        
        
        
