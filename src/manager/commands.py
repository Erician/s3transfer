#encoding=utf-8
import os,sys
import xmlrpclib
from time import sleep
#https://github.com/boto/boto3/issues/1399

path = os.path.abspath(__file__).replace('\\', '/')
managerPath = path[0:path.rfind('/')]
srcPath = managerPath[0:managerPath.rfind('/',0,len(managerPath))]
s3transferPath = srcPath[0:srcPath.rfind('/',0,len(srcPath))]

path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

from common.enum import status
from common.const import port, type
from common.utils import process, configoperations, clean, timeoperation
from common.sdk import clientfactory
import re
import time
import logging
log = logging.getLogger('root')

def print_command_error(argv):
    
    print ('the command:\''+list_to_string(argv)+'\' is not right\n'+
            'please see the document with s3transfer --help')

def list_to_string(mylist):
    returnString = ''
    for item in mylist:
        returnString += str(item)+' '
    return returnString.strip()

def show_help():
    print ('this is a simple document of s3transfer\n'+
           '    --help                  show this document\n'+
           '    start                   start s3transfer\n'+
           '    stop                    stop s3transfer\n'+
           '    add                     add a job\n'+
           '    rm                      remove a job\n'+
           '    pause                   pause a job\n'+
           '    continue                continue execute a job that you paused\n'+
           '    redo                    clean all history about the job, and redo it\n'+
           '    look                    look the configuration of the spefic job\n' +
           '    edit                    edit the configuration of the spefic job\n'
           '    set                     set somethings\n'+
           '         --max-master-num=  set the max number of master\n'+
           '         --max-worker-num=  set the max number of master\n'+
           '         --msg_to=          set mail sender, the format is, mailaddr:passwd\n' +
           '         --msg_from=        set mail receiver, the format is, mailaddr\n' +
           '    status                  show all jobs\' status\n'+
           '         --job              show all jobs\' status\n'+
           '         --jobID=           show the specific job status\n'+
           '         --machine          show all machines\' status\n' +
           '         --ip=              show the specific machine status\n'+
           '         --mail             show mail infomation'
        )
    
def start_s3transfer():
    if get_local_s3transfer_status() == status.S3TRANSFER.ok:
        print 'the s3transfer is running already, please don\'t use the start command'
        return
    stop_s3transfer(False)
    os.system('nohup python '+managerPath+'/jobmanager.py >.tmp 2>.tmp &')
    os.system('nohup python '+managerPath+'/mastermanager.py >.tmp 2>.tmp &')
    os.system('nohup python '+managerPath+'/workermanager.py >.tmp 2>.tmp &')
    if get_local_s3transfer_status() == status.S3TRANSFER.ok:
        print 's3transfer is ok'
        print '  ts-configure:'
        set_ts_cfg()
        if os.path.exists(s3transferPath+'/jobs'):
            print '  jobs:'
            for jobID in os.listdir(s3transferPath + '/jobs'):
                if os.path.isdir(s3transferPath + '/jobs/' + jobID):
                    jobstatus = get_job_status(jobID)
                    if jobstatus == 'running':
                        save_job_status_to_local(jobID, 'paused\n')
                    print_job_simple_info(jobID)
                jobstatus = get_job_status(jobID)
                if jobstatus == 'ready':
                    job = configoperations.read_job(s3transferPath + '/jobs/' + jobID + '/job.cfg', True)
                    if job:
                        add_job_to_jobmaster(job)
    else:
        stop_s3transfer(False)
        print 'start s3transfer failed, now is closed'

def set_ts_cfg():
    tsCfg = configoperations.read_ts_cfg()
    if tsCfg:
        if tsCfg.has_key('max-master-num') and set_max_master_num(long(tsCfg['max-master-num']), False):
            print '\tmax-master-num:' + tsCfg['max-master-num']
        if tsCfg.has_key('max-worker-num') and set_max_worker_num(long(tsCfg['max-worker-num']), False):
            print '\tmax-worker-num:' + tsCfg['max-worker-num']

def stop_s3transfer(isUserCommand=True):
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.JobManager))
        s.get_status()
        stringForPrint = s.stop()
        if stringForPrint:
            print stringForPrint.strip('\n')
        log.info('start to kill jobmanager')
        process.stop_process(port.ManagerPort.JobManager)
    except Exception,e:
        pass
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.MasterManager))
        s.stop()
        log.info('start to kill mastermanager')
        process.stop_process(port.ManagerPort.MasterManager)
    except Exception, e:
        pass
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.WorkerManager))
        s.stop()
        log.info('start to kill workermanager')
        process.stop_process(port.ManagerPort.WorkerManager)
    except Exception, e:
        pass
    
    if isUserCommand == True:
        print 's3transfer is closed'

def restart_s3transfer():
    stop_s3transfer()
    start_s3transfer()

def add_job(jobpath):
    job = configoperations.read_job(jobpath, True)
    if job:
        jobID = job['job-ID']
        if not os.path.exists(s3transferPath+'/jobs'):
            os.mkdir(s3transferPath+'/jobs')
        if os.path.exists(s3transferPath+'/jobs/'+str(jobID)):
            print 'job:' + str(jobID) + ' exists already, ' + 'maybe you can change the job-ID'
            return False
        if does_has_object_with_prefix_jobID(job) is True:
            isDeleteObjectsWithPrefixJobID = raw_input('This is very important, and please read it carefully!!!\n'
                +'We detect some objects with the same prefix of job-ID. Maybe they are important data or temporary files of the former transfer.\n'
                +'If you want to continue, we will delete them y/n ')
            if isDeleteObjectsWithPrefixJobID == "Y" or isDeleteObjectsWithPrefixJobID == "y":
                delete_tmpfiles_in_oss(jobID, job)
            else:
                return False
        os.mkdir(s3transferPath+'/jobs/'+str(jobID))
        if save_job_to_local(jobpath, job['job-ID'], job['sync-enable-increment'], job['sync-increment-interval']) and add_job_to_jobmaster(job):
            print 'add job:'+str(jobID)+' successfully, status:ready'
        else:
            rm_job(jobID)
            print 'add job:'+str(jobID)+' failed, you can look log/log-commands.txt to see why'

def does_has_object_with_prefix_jobID(job):
    try:
        s3Client = clientfactory.create_client(type.FileType.S3File, job['des-accesskey'], job['des-secretkey'],job['des-endpoint'])
        objects, isTruncated, nextmarker = s3Client.list_objects_without_delimiter(job['des-bucketName'], job['job-ID'], '', 1)
        if objects:
            return True
        else:
            return False
    except Exception, e:
        log.info(e)
        return False

def save_job_to_local(jobpath, jobID, isEnableIncrementSync, interval):
    try:
        srcJobFile = open(jobpath, 'r')
        jobFile = open(s3transferPath+'/jobs/'+str(jobID)+'/job.cfg', 'w')
        for line in srcJobFile:
            jobFile.write(line)
        save_job_status_to_local(jobID, 'ready\n')
        if isEnableIncrementSync == 'True':
            #格式为：第几次同步，同步间隔，上次同步的结束时间
            save_job_syncinfo_to_local(jobID, '0\t'+str(interval)+'\t0'+'\n')
        return True
    except Exception,e:
        log.error(e)
        return False

def save_job_status_to_local(jobID, jobstatus):
    try:
        statusFile = open(s3transferPath+'/jobs/'+str(jobID)+'/status', 'w')
        statusFile.write(str(jobstatus))
        statusFile.close()
        log.info('save job:'+jobID+' status, '+jobstatus.strip('\n'))
        return True
    except Exception,e:
        log.warning(e)
        return False
    
def save_job_syncinfo_to_local(jobID, syncinfo):
    try:
        statusFile = open(s3transferPath+'/jobs/'+str(jobID)+'/sync', 'w')
        statusFile.write(str(syncinfo))
        statusFile.close()
        log.info('save job:'+jobID+' syncinfo, '+syncinfo.strip('\n'))
        return True
    except Exception,e:
        log.warning(e)
        return False
        
def add_job_to_jobmaster(job):
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.JobManager))
        isSuccess = s.addjob(job)
        if isSuccess==True:
            log.info('add job:'+job['job-ID']+' to jobmanager successfully')
        else:
            log.warning('add job:'+job['job-ID']+' to jobmanager failed')
        return True
    except Exception,e:
        log.warning(e)
        log.warning('add job:'+job['job-ID']+' to jobmanager failed')
        return False

def rm_job(jobIDNeededRM, ):
    for jobID in os.listdir(s3transferPath+'/jobs'):
        if jobID == jobIDNeededRM:
            delete_job_from_jobmanager(jobIDNeededRM)
            delete_tmpfiles_in_oss(jobIDNeededRM)
            if  delete_job_from_local(s3transferPath+'/jobs/'+jobIDNeededRM):
                print 'rm the job of '+str(jobIDNeededRM)+' successfully'
                return True
            else:
                print 'rm the job of '+str(jobID)+' failed, you can look log/log-commands.txt to see why failed'
                return False
    print 'rm '+jobIDNeededRM + ' failed, it doesn\'t exist'

def delete_tmpfiles_in_oss(jobID, job = None):
    print 'start to delete tmp files in oss, please wait...'
    if job is None:
        job = configoperations.read_job(s3transferPath+'/jobs/'+jobID+'/job.cfg', True)
    if job:
        s3Client = clientfactory.create_client(type.FileType.S3File, job['des-accesskey'], job['des-secretkey'], job['des-endpoint'])
        clean.clean_with_prefix(s3Client, job['des-bucketName'], jobID)
        return True
    return False

def delete_job_from_local(jobpath, isDeleteJobcfg = True):
    try:
        for filename in os.listdir(jobpath):
            if isDeleteJobcfg == False and filename == 'job.cfg':
                continue
            os.remove(jobpath+'/'+filename)
        if isDeleteJobcfg == True:
            os.rmdir(jobpath)
        return True
    except Exception,e:
        log.warning(e)
        return False

def delete_job_from_jobmanager(jobID):
    try:
        s = xmlrpclib.ServerProxy('http://127.0.0.1:'+str(port.ManagerPort.JobManager))
        isSuccess = s.deletejob(jobID)
        if isSuccess==True:
            log.info('delete the job of '+jobID+' from jobmanager successfully')
        else:
            log.warning('delete the job of '+jobID+' from jobmanager failed')      
        return True
    except Exception,e:
        log.warning(e)
        log.warning('delete the job of '+jobID+' from jobmanager failed')
        return True

def pause_job(jobIDNeededPaused):
    for jobID in os.listdir(s3transferPath+'/jobs'):
        if jobID == jobIDNeededPaused:
            print 'start to pause ' + jobID + ', please wait...'
            delete_job_from_jobmanager(jobIDNeededPaused)
            if save_job_status_to_local(jobID, 'paused\n') == True:
                print 'pause '+str(jobID)+' succefully, status:paused'
                return
            print 'pause '+str(jobIDNeededPaused)+' failed'
            break
    print 'pause '+jobIDNeededPaused + ' failed, it doesn\'t exist'        

def continue_job(jobNeededContinue):
    for jobID in os.listdir(s3transferPath+'/jobs'):
        if jobID == jobNeededContinue:
            jobstatus = get_job_status(jobNeededContinue)
            if jobstatus and jobstatus == 'paused':
                job = configoperations.read_job(s3transferPath+'/jobs/'+jobID+'/job.cfg', True)
                if job and save_job_status_to_local(jobNeededContinue, 'ready\n') and add_job_to_jobmaster(job):
                    print 'continue the job of '+str(jobNeededContinue)+' successfully, status:ready'
                else:
                    print 'continue the job of '+str(jobNeededContinue)+' failed, you can look log/log-commands.txt to see why failed'
            else:
                print 'the job\'s status is not paused, you can not use continue command'
            return
    print   'continue '+jobNeededContinue + ' failed, it doesn\'t exist'  

def redo_job(jobIDNeededRedo):
    for jobID in os.listdir(s3transferPath+'/jobs'):
        if jobID == jobIDNeededRedo:
            #set ready first to avoid mycrobtab to start it
            save_job_status_to_local(jobIDNeededRedo, 'failed\n')
            delete_job_from_jobmanager(jobIDNeededRedo)
            if delete_tmpfiles_in_oss(jobIDNeededRedo) and  delete_job_from_local(s3transferPath+'/jobs/'+jobIDNeededRedo, False):
                job = configoperations.read_job(s3transferPath+'/jobs/'+jobIDNeededRedo+'/job.cfg', True)
                if job:
                    save_job_status_to_local(jobIDNeededRedo, 'ready\n')
                    if job['sync-enable-increment'] == 'True':
                        save_job_syncinfo_to_local(jobID, '0\t' + str(job['sync-increment-interval']) + '\t0' + '\n')
                    add_job_to_jobmaster(job)
                    print 'redo job:' + str(jobIDNeededRedo) + ' successfully, set it stutus ready'
                    return True
            else:
                print 'redo job:'+str(jobIDNeededRedo)+' failed, you can look log/log-commands.txt to see why failed'
                return False
    print 'redo '+jobIDNeededRedo + ' failed, it doesn\'t exist'

def get_job_status(jobID):
    try:
        statusFile = open(s3transferPath+'/jobs/'+str(jobID)+'/status', 'r')
        return statusFile.readline().strip('\n').strip()
    except Exception, e:
        log.info(e)
        return None
    
def get_job_syncinfo(jobID):
    try:
        syncFile = open(s3transferPath+'/jobs/'+str(jobID)+'/sync', 'r')
        line = syncFile.readline().strip('\n').strip()
        pairs = line.split('\t')
        if not pairs or len(pairs) != 3:
            return None
        return pairs
    except Exception,e:
        log.warning(e)
        return None

def print_job_simple_info(jobID):
    try:
        jobstatus = get_job_status(jobID)
        sync = 'disable-increment-sync'
        if os.path.exists(s3transferPath+'/jobs/'+str(jobID)+'/sync'):
            syncInfo = get_job_syncinfo(jobID)
            if syncInfo:
                times, interval, lastdonetime = syncInfo
                sync = 'enable-increment-sync\t'+str(times)+'th'
                if jobstatus=='done' and long(times) != 0 and float(interval)+float(lastdonetime)-time.time() > 0:
                    sync += '\tstart in '+timeoperation.s_to_hms_string(float(interval)+float(lastdonetime)-time.time())
        print '\t'+jobID+'\t'+jobstatus+'\t'+sync
    except IOError,e:
        log.info(e)
        print '\t'+jobID+'    we didn\'t know it\'s status'

def get_local_s3transfer_status(ip='127.0.0.1'):
    TryTimes = 10
    while TryTimes:
        try:
            s = xmlrpclib.ServerProxy('http://'+ip+':'+str(port.ManagerPort.JobManager))
            s.get_status()
            s = xmlrpclib.ServerProxy('http://'+ip+':'+str(port.ManagerPort.WorkerManager))
            s.get_status()
            s = xmlrpclib.ServerProxy('http://'+ip+':'+str(port.ManagerPort.MasterManager))
            s.get_status()
            return status.S3TRANSFER.ok
        except Exception, e:
            pass
        TryTimes = TryTimes - 1
        sleep(1)
    return status.S3TRANSFER.closed

def set_things(option):
    if option.startswith('--max-master-num='):
        if set_max_master_num(long(option[len('--max-master-num='):])):
            save_ts_cfg_to_local(option[2:])
    elif option.startswith('--max-worker-num='):
        if set_max_worker_num(long(option[len('--max-worker-num='):])):
            save_ts_cfg_to_local(option[2:])
    elif option.startswith('--msg_from='):
        save_ts_cfg_to_local(option[2:])
    elif option.startswith('--msg_to='):
        save_ts_cfg_to_local(option[2:])
    else:
        print_command_error(sys.argv)

def save_ts_cfg_to_local(option):
    try:
        pairs = option.split('=')
        key = pairs[0].strip()
        value = pairs[1].strip()
        tsCfg = configoperations.read_ts_cfg()
        if not tsCfg:
            tsCfg = dict()
        tsCfg[key] = value
        tsCfgFile = open(s3transferPath+'/conf/ts.cfg', 'w')
        for key, value in tsCfg.items():
            tsCfgFile.write(key + '=' + value + '\n')
        tsCfgFile.close()
        log.info('save ' + option + ' success')
    except Exception, e:
        log.warning(e)
        log.warning('save ' + option + ' failed')

def set_max_worker_num(maxNum, isUserCommand = True):
    try:
        if maxNum > 1000:
            print 'the max num of wroker can not be bigger than 1000'
            return
        s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port.ManagerPort.WorkerManager))
        s.set_max_worker_num(maxNum)
        if isUserCommand:
            print ('set the max num of worker successfully, max-worker-num:'+str(maxNum))
        return True
    except Exception, e:
        log.info(e)
        if isUserCommand:
            print ('set the max num of worker failed')
        return False

def set_max_master_num(maxNum, isUserCommand = True):
    try:
        if maxNum > 1000:
            print 'the max num of master can not be bigger than 1000'
            return
        s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port.ManagerPort.MasterManager))
        s.set_max_master_num(maxNum)
        if isUserCommand:
            print ('set the max num of master successfully, max-master-num:'+str(maxNum))
        return True
    except Exception, e:
        log.info(e)
        if isUserCommand:
            print ('set the max num of master failed')
        return False

def look_jobcfg(jobIDNeededLook):
    for jobID in os.listdir(s3transferPath + '/jobs'):
        if jobID == jobIDNeededLook:
            os.system('cat ' + s3transferPath + '/jobs/' + jobID + '/job.cfg | less')
            return
    print 'look ' + jobIDNeededLook + ' failed, it doesn\'t exist'

def edit_jobcfg(jobIDNeededEdit):
    for jobID in os.listdir(s3transferPath + '/jobs'):
        if jobID == jobIDNeededEdit:
            jobstatus = get_job_status(jobIDNeededEdit)
            if jobstatus == 'running':
                print  ('job:'+jobIDNeededEdit + ' is running, you cann\'t edit it.'
                                                 'Just the job stutas is paused or done or failed, then you can execute this command')
            else:
                os.system('vi ' + s3transferPath + '/jobs/' + jobID + '/job.cfg')
            return
    print 'edit ' + jobIDNeededEdit + ' failed, it doesn\'t exist'

def show_status(option = None):
    if not option or option.startswith('--job'):
        show_all_job_status()
    elif option.startswith('--jobID='):
        show_job_status(option[option.find('--jobID=')+len('--jobID='):])
    elif option.startswith('--machine'):
        show_all_machine_status()
    elif option.startswith('--ip='):
        show_machine_status(option[option.find('--ip=')+len('--ip='):])
    elif option.startswith('--mail'):
        show_mail_status()
    else:
        print_command_error(sys.argv)

def show_s3transfer_status(ip='127.0.0.1'):
    if get_local_s3transfer_status(ip) == status.S3TRANSFER.ok:
        if ip == configoperations.get_master_ip():
            print '*' + ip + ', s3transfer is ok'
        else:
            print ip + ', s3transfer is ok'
        show_master_worker_num(ip)
    else:
        print 's3transfer is closed'

def show_machine_status(ip):
    ipList = configoperations.read_machines_ip()
    if not ip in ipList:
        print ip+' is not in the machines_ip.cfg, please check it'
    else:
        show_s3transfer_status(ip)

def show_all_machine_status():
    ipList = configoperations.read_machines_ip()
    for ip in ipList:
        show_machine_status(ip)

def show_master_worker_num(ip = '127.0.0.1'):
    try:
        s = xmlrpclib.ServerProxy('http://' + ip + ':' + str(port.ManagerPort.MasterManager))
        [max, active, busy] = s.get_master_num_info()
        print '\tmax-master-num:' + str(max) + '\tactive:' + str(active) + '\tbusy:' + str(busy) + '\tready:' + str(active - busy)

        s = xmlrpclib.ServerProxy('http://' + ip + ':' + str(port.ManagerPort.WorkerManager))
        [max, active, busy] = s.get_worker_num_info()
        print '\tmax-worker-num:'+str(max)+'\tactive:'+str(active)+'\tbusy:'+str(busy)+'\tready:'+str(active-busy)

    except Exception, e:
        log.warning(e)

def show_job_status(jobID):
    try:
        jobstatus = get_job_status(jobID)
        if jobstatus != 'running':
            print_job_simple_info(jobID)
            return
        s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port.ManagerPort.JobManager))
        isSuccess, response = s.get_job_status(jobID)
        if not isSuccess:
            print '\t' + jobID + '\t' + response
            return
        jobStatusInfo = '\t' + jobID + '\t' + jobstatus
        if os.path.exists(s3transferPath+'/jobs/'+str(jobID)+'/sync'):
            syncInfo = get_job_syncinfo(jobID)
            if syncInfo:
                times, interval, lastdonetime = syncInfo
                jobStatusInfo += '\tenable-increment-sync\t'+str(times)+'th'
        [jobType, finishedFiles, finishedSize, allfiles, allsize, roundStartTime, startTime, masterPort] = response
        jobStatusInfo += ('\t' + str(masterPort) + '\t' + jobType +
                     '\t' + str(finishedFiles) + '/' + str(allfiles) +
                     '\t' + str(finishedSize) + '/' + str(allsize) +
                     '\t' + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(startTime))))
        if long(allsize) != 0:
            jobStatusInfo += ('\t' + str(round((long(finishedSize) * 1.0/long(allsize))*100, 2)) + '%'+
                              '\t' + str(round(long(finishedSize) * 1.0 / (time.time() - roundStartTime) / 1000, 2))+' KB/s')
            if long(finishedSize)!=0:
                jobStatusInfo +=('\t' + timeoperation.s_to_hms_string(long(allsize)*(time.time()-roundStartTime)/long(finishedSize)-(time.time() - roundStartTime))+' left')
        print jobStatusInfo
    except Exception, e:
        log.info(e)

def show_all_job_status():
    if os.path.exists(s3transferPath+'/jobs'):
        for jobID in os.listdir(s3transferPath + '/jobs'):
            if os.path.isdir(s3transferPath + '/jobs/' + jobID):
                show_job_status(jobID)
    else:
        print 'there are no jobs'

def show_mail_status():
    tsCfg = configoperations.read_ts_cfg()
    if tsCfg.has_key('msg_from'):
        print('\tmsg_from\t'+tsCfg['msg_from'])
    else:
        print('\tmsg_from\t')
    if tsCfg.has_key('msg_to'):
        print('\tmsg_to\t\t'+tsCfg['msg_to'])
    else:
        print('\tmsg_to\t')


if __name__ == '__main__':
    pass
    #s['ss'] = 'ww'
    
    

    
    
    
    
    



