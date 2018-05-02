#encoding=utf-8
'''
Created on Dec 3, 2017
@author: eric
'''
import sys,os
import logging
import time
log = logging.getLogger('root')

def isBetween(value0, value1, value2):
    if value0 >= value1 and value0 <= value2:
        return True
    else:
        return False

def get_localfile_lastmodify(localFilePath):
    try:
        return os.path.getmtime(localFilePath)
    except Exception,e:
        log.info(e)
        return -1

def get_filesystem_time(abspath, syncEnableIncrement):
    try:
        serverTimeFile = open(abspath+'sourceservertime','w')
        serverTimeFile.close()
        t = os.path.getctime(abspath+'sourceservertime')
        os.system('rm '+abspath+'sourceservertime')
        return t
    except Exception,e:
        log.error(e)
        log.error('write file failed when get filesystem time')
        if syncEnableIncrement == 'True':
            return -1
        else:
            #如果没有开启增量同步，使用当前系统的时间再加上一年
            return time.time()+3600*24*365

def get_server_time(client, bucketName, synctask):
    count = 0
    serverTime = 0
    while count < 3:
        if not client.upload_localfile(bucketName, synctask+'/sourceservertime', os.path.abspath(__file__)):
            log.error('upload file failed when getting server time')
            return -1
        lastModify = client.get_object_lastmodify(bucketName, synctask+'/sourceservertime')
        if lastModify == -1:
            log.error('get file lastmodify failed when getting server time')
            return -1
        serverTime += lastModify
        count+=1
        if not client.delete_one_object(bucketName, synctask+'/sourceservertime'):
            log.error('delete '+synctask+'/sourceservertime'+' failed when getting server time')
            return -1
    return (serverTime*1.0/count-300)

def fmt_time(strTime):
    return (time.mktime(time.strptime(strTime, '%Y-%m-%d %H:%M:%S')))
        
def s_to_hms_string(seconds):
    '''
    把秒数转换成小时分钟字符串
    '''
    seconds = int(seconds)
    hours = 0
    mins = 0
    returnString = ''
    if seconds >= 3600:
        hours = seconds/3600
        returnString += str(hours)+'h '
        seconds = seconds%3600
    if seconds >= 60:
        mins = seconds/60
        returnString += str(mins)+'m '
        seconds = seconds%60
    returnString += str(seconds) + 's'
    return returnString

if __name__ == '__main__':
    '''
    print s_to_hms_string(0)
    print s_to_hms_string(10)
    print s_to_hms_string(100)
    print s_to_hms_string(1000)
    print s_to_hms_string(10000)
    print s_to_hms_string(200.34)
    '''
    #print get_filesystem_time('./')
    print fmt_time('1970-01-01 08:00:00')
    
    