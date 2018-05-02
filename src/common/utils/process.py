#encoding=utf-8
import os
import re
import logging
from time import sleep
log = logging.getLogger('root')

def get_processID_loop_when_failed(port):
    TryTimes = 20
    while TryTimes:
        try:
            os.system('netstat -lpn  1>.tmp  2>.tmp')
            tmpfile = open('.tmp', 'r')
            for line in tmpfile:
                if str(port) in line:
                    process = re.split('[ ]+', line.strip('\n'))[6]
                    break
            return process[0:process.find('/')]
        except Exception,e:
            pass
        TryTimes = TryTimes - 1
        sleep(1)
    return -1

def get_processID_return_when_failed(port):
    TryTimes = 20
    while TryTimes:
        try:
            os.system('netstat -lpn  1>.tmp  2>.tmp')
            tmpfile = open('.tmp', 'r')
            for line in tmpfile:
                if str(port) in line:
                    process = re.split('[ ]+', line.strip('\n'))[6]
                    break
            processID = process[0:process.find('/')]
        except Exception,e:
            return -1
        TryTimes = TryTimes - 1
        sleep(1)
    return processID

def stop_process(port):
    processID = get_processID_loop_when_failed(port)
    log.info(processID)
    if processID != -1:
        os.system('kill '+str(processID))
    if get_processID_return_when_failed(port) == -1:
        log.info('kill '+str(port)+' successfully, process-id:'+str(processID))
    else:
        log.info('kill '+str(port)+' failed')
        
        