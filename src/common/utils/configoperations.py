#encoding=utf-8
import sys,os
path = os.path.abspath(__file__).replace('\\', '/')
utilsPath = path[0:path.rfind('/')]
sys.path.append(utilsPath[0:utilsPath.rfind('/',0,len(utilsPath))])

commonPath = utilsPath[0:utilsPath.rfind('/',0,len(utilsPath))]
srcPath = commonPath[0:commonPath.rfind('/',0,len(commonPath))]
s3transferPath = srcPath[0:srcPath.rfind('/',0,len(srcPath))]

from const import type
import time
import random
import string
import logging
log = logging.getLogger('root')

def get_master_ip():
    try:
        ipsFile = open(s3transferPath+'/machines_ip.cfg', 'r')
        for line in ipsFile:
            if not line.startswith('#'):
                return line.strip('\n').strip()
        return '127.0.0.1'
    except IOError, e:
        log.info(e)
        return '127.0.0.1'

def read_machines_ip():
    ipList = list()
    try:
        ipsFile = open(s3transferPath+'/machines_ip.cfg', 'r')
        for line in ipsFile:
            line = line.strip('\n').strip()
            if not line.startswith('#') and not len(line)==0:
                ipList.append(line.strip('\n').strip())
        return ipList
    except IOError, e:
        log.info(log)
        return ipList

def read_ts_cfg():
    try:
        options = dict()
        tsCfgFile = open(s3transferPath+'/conf/ts.cfg', 'r')
        for line in tsCfgFile:
            mystr = line.strip('\n').strip()
            if len(mystr) == 0 or mystr.startswith("#", 0, 1) or mystr.count("=") != 1:
                continue
            pair = mystr.split("=")
            for i in range(0, len(pair)):
                pair[i] = pair[i].strip()
            options[pair[0]] = pair[1]
        return options
    except Exception, e:
        log.info(e)
        log.info('ts.cfg is not exists, we will use default configuration')
        return None



def read_job(jobpath, isCheck):
    try:
        jobfile = open(jobpath, 'r')
        job = dict()
        for line in jobfile:
            mystr = line.strip('\n').strip()
            if len(mystr) == 0 or mystr.startswith("#",0,1) or mystr.count("=") != 1:
                continue
            pair = mystr.split("=")
            for i in range(0,len(pair)):
                pair[i] = pair[i].strip()
            job[pair[0]] = pair[1]
        jobfile.close()
        if isCheck == False:
            return job
        if check_job_and_fillin_default(job):
            return job
        else:
            return None
    except Exception, e:
        print e
        return None
#check 
def check_job_and_fillin_default(job):    
    #如果用户设置了job-ID,就使用用户设置的；否则，使用time+随机数作为job-ID
    if not check_optional_item(job, 'job-ID',None,generate_jobID()):
        return False
    if not check_item_must_have(job, 'job-type',['transfer','check']):
        return False
    if not check_src(job):
        return False
    if not check_des(job):
        return False
    if not check_sync(job):
        return False
    if not check_transfer_check_common(job):
        return False
    if not check_transfer(job):
        return False
    if not check_check(job):
        return False
    return True

def generate_jobID():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+'_'+''.join(random.sample(string.ascii_letters + string.digits, 8))
    
def check_src(job):
    if not check_optional_item(job,'src-filetype',['s3file','diskfile','qiniufile','aliyunfile','tencentfile', 'baidufile', 'urlfile'],'s3file'):
        return False
    if not check_optional_item(job,'src-file-list', None, ''):
        return False
    if not check_optional_item(job,'src-prefix', None, ''):
        return False

    if job['src-filetype'] == type.FileType.DiskFile:
        if not check_item_must_have(job, 'src-absolutepath'):
            return False
        elif not job['src-absolutepath'].endswith('/'):
            job['src-absolutepath'] += '/'
    elif job['src-filetype'] == type.FileType.UrlFile:
        if not check_item_must_have(job, 'src-file-list'):
            return False
    else:
        if not check_item_must_have(job, 'src-accesskey'):
            return False
        if not check_item_must_have(job, 'src-secretkey'):
            return False
        if not check_item_must_have(job, 'src-endpoint'):
            return False
        if not check_item_must_have(job, 'src-bucketName'):
            return False
        if not check_optional_item(job,'src-prefix', None, ''):
            return False
        if job['src-prefix'] and job['src-prefix'].endswith('/')==False:
            job['src-prefix'] += '/'

    if job['src-file-list'] and (not job['src-file-list'].startswith('/')):
        job['src-file-list'] = s3transferPath + '/' + job['src-file-list']
    return True

def check_des(job):
    if not check_item_must_have(job, 'des-accesskey'):
        return False
    if not check_item_must_have(job, 'des-secretkey'):
        return False
    if not check_item_must_have(job, 'des-endpoint'):
        return False
    if not check_item_must_have(job, 'des-bucketName'):
        return False
    if not check_optional_item(job,'des-prefix', None, ''):
        return False
    if job.has_key('des-prefix') and job['des-prefix'] and job['des-prefix'].endswith('/')==False:
        job['des-prefix'] += '/'
    return True

def check_sync(job):
    if not check_optional_item(job, 'sync-enable-increment',['True','False'] , 'True'):
        return False
    if not check_optional_item(job, 'sync-increment-interval', [] , '86400'):
        return False
    if not check_optional_item(job, 'sync-since', [] , '1970-01-01 00:00:00'):
        return False
    try:
        job['sync-since'] = str(time.mktime(time.strptime(job['sync-since'], '%Y-%m-%d %H:%M:%S')))
    except Exception,e:
        print e
        return False
    return True

def check_transfer_check_common(job):
    if job.has_key('task-size'):
        if not check_optional_item(job, 'task-size',None,5120):
            return False
    if job.has_key('task-filenumbers'):
        if not check_optional_item(job, 'task-filenumbers',None,50000):
            return False
    if job.has_key('round'):
        if not check_optional_item(job, 'round',None,3):
            return False
    return True

def check_transfer(job):
    if not check_optional_item(job, 'transfer-error-output',None,job['job-ID']+'-transfer-error-list.txt'):
        return False
    return True

def check_check(job):
    if not check_optional_item(job, 'check-time',['never','future','now'],'now'):
        return False
    if not check_optional_item(job, 'check-mode',['head','md5'],'head'):
        return False
    if not check_optional_item(job, 'check-error-output',None,job['job-ID']+'-check-error-list.txt'):
        return False
    if not check_optional_item(job, 'check-md5-output',None,job['job-ID']+'-check-md5-list.txt'):
        return False
    return True

#check_job using
def check_item_must_have(job,item,options=[]):
    if (not job.has_key(item)) or (not job[item]):
        print 'Please check the configuration of '+item
        return  False
    if options and (not job[item] in options):
        print ('Please check the configuration of '+item)
        return False
    return True
#check_job using 
def check_optional_item(job,item,options=[],default=''):
    if job.has_key(item) and job[item]:
        if options and (not (job[item] in options)):
            print ('Please check the configuration of '+item)
            return False
        else:
            return True
    job[item] = default
    return True
        

if __name__ == '__main__':
    pair = 'worker='.split('=')
    workerList = list()
    workerList.append(pair[1])
    
    
    
    
    
    
    
    