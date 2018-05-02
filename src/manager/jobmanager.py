#encoding=utf-8
import os,sys
from time import sleep
import time
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

managerPath = path[0:path.rfind('/')]
srcPath = managerPath[0:managerPath.rfind('/',0,len(managerPath))]
s3transferPath = srcPath[0:srcPath.rfind('/',0,len(srcPath))]

from common.utils import getpathinfo,setlogging,configoperations
logsdir = getpathinfo.get_logs_dir(os.path.abspath(__file__).replace('\\', '/'))
logFileName = 'log-jobmanager.txt'
setlogging.set_logging(logsdir+logFileName)

reload(sys)
sys.setdefaultencoding('utf8')
from common.enum import status
from common.const import port
from common.dataobject.message import Message
from common.utils import mail
from collections import deque
import threading
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging
from job import Job
import time
import commands
log = logging.getLogger('root')

class JobManager:
    MAXJOBRETRYTIMES = 3
    bigGranularityLock = threading.Lock()
    jobqueue = deque()
    
    def addjob(self, purejob):
        JobManager.bigGranularityLock.acquire()
        job = Job(purejob)
        job.set_status(status.JOB.ready)
        JobManager.jobqueue.append(job)
        JobManager.bigGranularityLock.release()
        log.info('received job:'+job.get_ID())
        return True
        
    def deletejob(self, jobID):
        JobManager.bigGranularityLock.acquire()
        log.info('start to delete job:'+jobID)
        delete_job_from_mastermanager(jobID)
        for job in JobManager.jobqueue:
            if jobID == job.get_ID():
                JobManager.jobqueue.remove(job)
                break
        JobManager.bigGranularityLock.release()
        log.info('deleted job:'+jobID)
        return True

    def get_status(self):
        return True
    
    def stop(self):
        JobManager.bigGranularityLock.acquire()
        returnStringForPrint = ''
        for job in JobManager.jobqueue:
            if job.get_status() == status.JOB.running:
                returnStringForPrint += '\tjob:'+str(job.get_ID())+' is running, set it paused\n'
                commands.save_job_status_to_local(job.get_ID(), 'paused\n')
        JobManager.bigGranularityLock.release()
        return returnStringForPrint

    def send_message(self, messageAttr):
        try:
            JobManager.bigGranularityLock.acquire()
            message = Message().with_attribute(messageAttr)
            log.info('receive message from mastermanager, job:' + str(message.get_ID()))
            log.info(messageAttr)
            isFindFlag = 0
            for job in JobManager.jobqueue:
                if str(job.get_ID()) == str(message.get_ID()):
                    isFindFlag = 1
                    if message.get_isSuccess() == True:
                        commands.save_job_status_to_local(job.get_ID(), 'done\n')
                        if job.get_job()['sync-enable-increment'] == 'True':
                            save_job_lastdonetime(job.get_ID())
                            syncinfo_times_pp(job.get_ID())
                        JobManager.jobqueue.remove(job)
                        log.info('[info] '+job.to_string() + ' finished successfully')
                        mail.send_mail('[info] '+job.to_string() + ' finished successfully')
                    else:
                        if job.get_retrytimes() < JobManager.MAXJOBRETRYTIMES:
                            job.set_retrytimes(job.get_retrytimes() + 1)
                            job.set_status(status.JOB.ready)
                            log.info('[warning] '+job.to_string() + ' failed, we will retry, retrytimes:' + str(job.get_retrytimes()))
                            mail.send_mail('[warning] '+job.to_string() + ' failed, we will retry, retrytimes:' + str(job.get_retrytimes()))
                        else:
                            commands.save_job_status_to_local(job.get_ID(), 'failed\n')
                            if job.get_job()['sync-enable-increment'] == 'True':
                                save_job_lastdonetime(job.get_ID())
                            JobManager.jobqueue.remove(job)
                            log.warning('[error] '+job.to_string() + ' failed, please check it')
                            mail.send_mail('[error] '+job.to_string() + ' failed, please check it')
                    break
            if isFindFlag == 0:
                log.info('we didn\'t have any record of ' + message.get_ID())
            return True
        except Exception, e:
            log.info(e)
            return True
        finally:
            JobManager.bigGranularityLock.release()

    def get_job_status(self, jobID):
        try:
            JobManager.bigGranularityLock.acquire()
            startTime = 0
            for job in JobManager.jobqueue:
                if job.get_status() == status.JOB.running and job.get_ID() == jobID :
                    startTime = job.get_startTime()
                    break
            if startTime == 0:
                return None
            s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port.ManagerPort.MasterManager))
            desPort = s.where_is_the_job(jobID)
            if desPort == -1:
                return [False, 'the job is not dispatched, please wait...']
            s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(desPort))
            response = s.get_job_status()
            if not response:
                return  [False, 'the job is just to start to execute']
            response.append(startTime)
            response.append(desPort)
            return [True, response]
        except Exception, e:
            log.info(e)
            return [False, '']
        finally:
            JobManager.bigGranularityLock.release()

def start_jobmanager():
    try:
        server = SimpleXMLRPCServer(('0.0.0.0', port.ManagerPort.JobManager),allow_none=True,logRequests=False)  
        server.register_instance(JobManager())
        log.info('create jobmanager successfully, its port is '+str(port.ManagerPort.JobManager))
        server.serve_forever()
    except Exception,e:
        print e
        log.error('create jobmanager failed, its port is '+str(port.ManagerPort.JobManager))
        log.error(e)

def manager_thread():
    while True:
        try:
            JobManager.bigGranularityLock.acquire()
            s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.MasterManager))
            masterstatus = s.get_status()
            if masterstatus == status.MASTER.ready:
                for job in JobManager.jobqueue:
                    if job.get_status() == status.JOB.ready:
                        if s.addjob(job.get_job()) and commands.save_job_status_to_local(job.get_ID(), 'running\n'):
                            job.set_status(status.JOB.running)
                            job.set_startTime(time.time())
                            log.info('add job:'+job.get_ID()+' to mastermanager successfully')
                            break
                        else:
                            log.info('add job:' + job.get_ID() + ' to mastermanager failed')

        except Exception, e:
            log.info(e)
        finally:
            JobManager.bigGranularityLock.release()
        time.sleep(5)
        

def save_job_syncinfo_to_local(jobID, syncinfo):
    try:
        statusFile = open(s3transferPath+'/jobs/'+str(jobID)+'/sync', 'w')
        statusFile.write(str(syncinfo))
        statusFile.close()
        log.info('save job:'+jobID+', sync:'+syncinfo)
        return True
    except Exception,e:
        log.warning(e)
        return False

def get_job_status(jobID):
    try:
        statusFile = open(s3transferPath+'/jobs/'+str(jobID)+'/status', 'r')
        jobstatus = statusFile.readline().strip('\n').strip()
        statusFile.close()
        return jobstatus
    except Exception,e:
        log.warning(e)
        return None

def get_job_syncinfo(jobID):
    try:
        syncFile = open(s3transferPath+'/jobs/'+str(jobID)+'/sync', 'r')
        return syncFile.readline().strip('\n').strip().split('\t')
    except Exception,e:
        log.warning(e)
        return None

def is_righttime_to_start_sync(jobID):
    jobstatus = get_job_status(jobID)
    if jobstatus and jobstatus == 'done':
        syncinfo = get_job_syncinfo(jobID)
        if syncinfo:
            times,interval,lastdonetime = syncinfo
            if (long(times) != 0) and (time.time()-float(lastdonetime) >= float(interval)):
                return True
    return False

def syncinfo_times_pp(jobID):
    syncinfo = get_job_syncinfo(jobID)
    if syncinfo:
        times,interval,lastdonetime = syncinfo
        log.info('sync time plus plus')
        times = long(times)+1
        return save_job_syncinfo_to_local(jobID, str(times)+'\t'+str(interval)+'\t'+str(lastdonetime))
    return False

def save_job_lastdonetime(jobID):
    syncinfo = get_job_syncinfo(jobID)
    if syncinfo:
        times,interval,lastdonetime = syncinfo
        lastdonetime = time.time()
        log.info('save lastdone time')
        return save_job_syncinfo_to_local(jobID, str(times)+'\t'+str(interval)+'\t'+str(lastdonetime))
    return False

def delete_job_from_mastermanager(jobID):
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.MasterManager))
        isSuccess = s.deletejob(jobID)
        if isSuccess==True:
            log.info('delete the job of '+jobID+' from mastermanager successfully')
        else:
            log.warning('delete the job of '+jobID+' from mastermanager failed')
        return True
    except Exception,e:
        log.warning(e)
        log.warning('delete the job of '+jobID+' from mastermanager failed')
        return False

def mycrontab_thread():
    while True:
        try:
            isInJobQueue = 0
            for jobID in os.listdir(s3transferPath+'/jobs'):
                for job in JobManager.jobqueue:
                    if job.get_ID() == jobID:
                        isInJobQueue = 1
                        break
                if (isInJobQueue == 0
                    and os.path.exists(s3transferPath+'/jobs/'+jobID+'/sync')
                    and is_righttime_to_start_sync(jobID) == True
                    and get_job_status(jobID) == 'done'):
                    commands.add_job_to_jobmaster(configoperations.read_job(s3transferPath+'/jobs/'+jobID+'/job.cfg', True))
        except Exception, e:
            log.warning(e)
        sleep(2)

                    
def start():
    managerThread = threading.Thread(target=manager_thread, args=())
    managerThread.setDaemon(True)
    managerThread.start()
    
    mycrontabThread = threading.Thread(target=mycrontab_thread, args=())
    mycrontabThread.setDaemon(True)
    mycrontabThread.start()
    
    start_jobmanager()
    
if __name__ == '__main__':
    start()
    
    
    
        
        
        
