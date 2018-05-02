#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

from common.utils import getpathinfo,setlogging
logsdir = getpathinfo.get_logs_dir(os.path.abspath(__file__).replace('\\', '/'))
logFileName = 'log-mastermanager.txt'
setlogging.set_logging(logsdir+logFileName)

reload(sys)
sys.setdefaultencoding('utf8')
from common.enum import status
from SimpleXMLRPCServer import SimpleXMLRPCServer
from common.const import port
from common.utils import process, mail
from common.dataobject.message import Message
from collections import deque
from multiprocessing import cpu_count
import threading
import xmlrpclib
import masterinfo
from time import sleep
from SocketServer import ThreadingMixIn
import logging
log = logging.getLogger('root')

"""异步的server类"""
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class MasterManager:
    masters_port_startafter = port.ManagerPort.MasterManager
    MaxMasterNum = 1 if cpu_count()//2 == 0 else cpu_count()//2
    masterqueue = deque()
    bigGranularityLock = threading.Lock()

    def __init__(self):
        pass

    def stop(self):
        MasterManager.bigGranularityLock.acquire()
        for master in MasterManager.masterqueue:
            log.info('start to kill masters')
            process.stop_process(master.get_port())
        MasterManager.masterqueue = deque()
        MasterManager.bigGranularityLock.release()
    
    def get_status(self):
        try:
            cnt = 0
            for master in MasterManager.masterqueue:
                if master.get_status() == status.MASTER.ready:
                    return status.MASTER.ready
                cnt += 1
                if cnt >= MasterManager.MaxMasterNum:
                    break
            if len(MasterManager.masterqueue) < MasterManager.MaxMasterNum:
                return status.MASTER.ready
            return status.MASTER.busy
        except Exception, e:
            log.info(e)
            return status.MASTER.busy
    
    def addjob(self, job):
        log.info('received job:'+job['job-ID'])
        return job_dispatcher(job)
        
    def deletejob(self, jobID):
        MasterManager.bigGranularityLock.acquire()
        log.info('start to delete job:'+jobID)
        for master in MasterManager.masterqueue:
            job = master.get_job()
            if job and jobID == job['job-ID']:
                if delete_job_from_master(master.get_port(), jobID)==True:
                    process.stop_process(master.get_port())
                    if start_master_with_port(master.get_port()):
                        master.set_status(status.MASTER.ready)
                    else:
                        MasterManager.masterqueue.remove(master)
                break

        log.info('deleted job:'+jobID)
        MasterManager.bigGranularityLock.release()
        return True

    def send_message(self, messageAttr):
        try:
            MasterManager.bigGranularityLock.acquire()
            message = Message().with_attribute(messageAttr)
            port = message.get_port()
            log.info('receive message from master:' + str(port) + ', ' + 'job:' + str(message.get_ID()) + '')
            log.info(messageAttr)
            isFindFlag = 0
            for master in MasterManager.masterqueue:
                if str(master.get_port()) == str(port) and message.get_ID() == master.get_job()['job-ID']:
                    isFindFlag = 1
                    if message.get_isSuccess() == False:
                        process.stop_process(master.get_port())
                        MasterManager.masterqueue.remove(master)
                    else:
                        master.set_status(status.MASTER.ready)
                        master.set_job(None)
                    break
            if isFindFlag == 0:
                log.info('we didn\'t have any record of ' + message.get_ID())
        except Exception, e:
            log.info(e)
        finally:
            MasterManager.bigGranularityLock.release()
        if isFindFlag == 1:
            send_message_to_jobmanager(message)

    def set_max_master_num(self, maxNum):
        MasterManager.MaxMasterNum = maxNum
        return True

    def get_master_num_info(self):
        MasterManager.bigGranularityLock.acquire()
        cntBusy = 0
        for master in MasterManager.masterqueue:
            if master.get_status() == status.MASTER.busy:
                cntBusy += 1
        MasterManager.bigGranularityLock.release()
        return [MasterManager.MaxMasterNum, len(MasterManager.masterqueue), cntBusy]

    def where_is_the_job(self, jobID):
        try:
            MasterManager.bigGranularityLock.acquire()
            for master in MasterManager.masterqueue:
                job = master.get_job()
                if job and jobID == job['job-ID']:
                    return  master.get_port()
            return -1
        except Exception, e:
            log.info(e)
        finally:
            MasterManager.bigGranularityLock.release()

def start_mastermanager():
    try:
        server = ThreadXMLRPCServer(('0.0.0.0', port.ManagerPort.MasterManager),allow_none=True,logRequests=False)
        server.register_instance(MasterManager())
        log.info('create MasterManager successfully, its port is '+str(port.ManagerPort.MasterManager) )
        server.serve_forever()
    except Exception,e:
        log.error('create MasterManager failed, its port is '+str(port.ManagerPort.MasterManager))
        log.error(e)
        
def job_dispatcher(job):
    try:
        MasterManager.bigGranularityLock.acquire()
        master = findReadyMaster()
        if not master:
            raise Exception('did not find a master to do this job')
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(master.get_port()))
        master.set_job(job)
        master.set_status(status.MASTER.busy)
        s.do_job(job)
        log.info('send job:'+job['job-ID']+' to master:'+str(master.get_port())+' successfully')
        return True
    except Exception, e:
        log.warning(e)
        if master in MasterManager.masterqueue:
            MasterManager.masterqueue.remove(master)
            log.info('master:' + str(master.get_port()) + ' is shutdown and we remove this master from masterqueue')
        return False
    finally:
        MasterManager.bigGranularityLock.release()
    
def findReadyMaster():
    for master in MasterManager.masterqueue:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(master.get_port()))
            masterstatus = s.get_status()
            if masterstatus == status.MASTER.ready and master.get_status()==status.MASTER.ready:
                return master
        except Exception, e:
            log.info(e)
            if master in MasterManager.masterqueue:
                MasterManager.masterqueue.remove(master)
                log.info('master:' + str(master.get_port()) + ' is shutdown and we remove this master from masterqueue')
    return start_master()

def start_master():
    try:
        while True:
            MasterManager.masters_port_startafter += 1
            if MasterManager.masters_port_startafter > port.ManagerPort.MasterManager+1000:
                MasterManager.masters_port_startafter = port.ManagerPort.MasterManager + 1
            if process.get_processID_return_when_failed(MasterManager.masters_port_startafter) != -1:
                log.info(str(MasterManager.masters_port_startafter) + ' is already in use')
                continue
            path = os.path.abspath(__file__).replace('\\', '/')
            absdir = path[0:path.rfind('/')]
            masterpath = absdir[0:absdir.rfind('/',0,len(absdir))]
            os.system('nohup python '+masterpath+'/master/master.py '+
                        str(MasterManager.masters_port_startafter)+' >.tmp 2>.tmp &')
            workerProcessID = process.get_processID_loop_when_failed(MasterManager.masters_port_startafter)
            if workerProcessID != -1:
                master = masterinfo.MasterInfo(MasterManager.masters_port_startafter, status.MASTER.ready)
                MasterManager.masterqueue.append(master)
                log.info('start '+str(master.get_port())+' successfully, the process id is '+str(workerProcessID))
                return master
            else:
                log.warning('start '+str(MasterManager.masters_port_startafter)+' failed, and we will try another port')
    except Exception, e:
        log.warning(e)
        return None

def start_master_with_port(masterport = 0):
    try:
        if process.get_processID_return_when_failed(masterport) != -1:
            raise Exception(str(MasterManager.masters_port_startafter) + ' is already in use')
        path = os.path.abspath(__file__).replace('\\', '/')
        absdir = path[0:path.rfind('/')]
        masterpath = absdir[0:absdir.rfind('/', 0, len(absdir))]
        os.system('nohup python ' + masterpath + '/master/master.py ' +
                  str(masterport) + ' >.tmp 2>.tmp &')
        workerProcessID = process.get_processID_loop_when_failed(masterport)
        if workerProcessID != -1:
            log.info('start ' + str(masterport) + ' successfully, the process id is ' + str(workerProcessID))
            return True
        else:
            raise Exception('start ' + str(masterport) + ' failed')
    except Exception, e:
        log.warning(e)
        return None

def send_message_to_jobmanager(message):
    #loop, when fail
    while True:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port.ManagerPort.JobManager), allow_none=True)
            s.send_message(message)
            log.info('send message to jobmanager successfully')
            return True
        except Exception, e:
            log.warning(e)
            log.warning('send message to jobmanager failed, will retry')
            sleep(10)

def monitor_thread():
    while True:
        try:
            MasterManager.bigGranularityLock.acquire()
            flag = 0
            message = None
            for master in MasterManager.masterqueue:
                masterstatus = get_masterstatus(master.get_port())
                if masterstatus == status.MASTER.shutdown:
                    if master.get_status() == status.MASTER.busy:
                        flag = 1
                        job = master.get_job()
                        message = Message('127.0.0.1', job['job-ID'], master.get_port(), False)
                        MasterManager.masterqueue.remove(master)
                        log.warning('master, port:' + str(master.get_port()) + ' is shutdown, we will remove job, jobID:' + job['job-ID'] + ' on it')
                        mail.send_mail('[warning] master, port:' + str(master.get_port()) + ' is shutdown, we will remove job, jobID:' + job['job-ID'] + ' on it')
                        break
                    else:
                        MasterManager.masterqueue.remove(master)
                        log.warning('master, port:' + str(master.get_port()) + ' is shutdown, no jobs on it')
                        mail.send_mail('master, port:' + str(master.get_port()) + ' is shutdown, no jobs on it')
                        break
                elif masterstatus == status.MASTER.busy:
                    set_master_token(master.get_port())

        except Exception, e:
            log.info(e)
        finally:
            MasterManager.bigGranularityLock.release()
        if flag == 1:
            send_message_to_jobmanager(message)

        sleep(1)

def get_masterstatus(port):
    retrytimes = 3
    while retrytimes:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port))
            return s.get_status()
        except Exception,e:
            log.warning(e)
            retrytimes = retrytimes - 1
        sleep(1)
    return status.MASTER.shutdown

def set_master_token(port):
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port))
        s.set_token()
    except Exception, e:
        log.warning(e)
        log.info('give token to master failed, port:' + str(port))

def delete_job_from_master(masterport, jobID):
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(masterport))
        isSuccess = s.deletejob(jobID)
        if isSuccess==True:
            log.info('delete the job of '+jobID+' from master successfully')
        else:
            log.warning('delete the job of '+jobID+' from master failed') 
        return True
    except Exception,e:
        log.warning(e)
        log.warning('delete the job of '+jobID+' from master failed')
        return True

def start():
    dispatcher_thread = threading.Thread(target=monitor_thread, args=())
    dispatcher_thread.setDaemon(True)
    dispatcher_thread.start()
    start_mastermanager()

if __name__ == '__main__':
    start()

    
    
    
        
        
        
