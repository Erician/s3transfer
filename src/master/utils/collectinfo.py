#encoding=utf-8
import sys,os
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

from common.const import type
from common.utils import readdata
from common.myexceptions import ossexception

import logging
log = logging.getLogger('root')
def generate_errorlist(client, bucketName,jobType,synctask,currentRound,errorFileName):
    log.info('start to generate errorlist for '+jobType)
    try:
        errorFile = open(errorFileName,'a')
        errorFile.write('#################### '+synctask+' failed list ####################\n')
        taskDirs = client.get_alldir(bucketName,synctask+'/'+jobType+'/round'+str(currentRound)+'/')
        existFailedFile = False
        for mydir in taskDirs:
            lines = readdata.readlines(client, bucketName, mydir+'errorlist')
            line = lines.readline()
            line = lines.readline()
            while line:
                errorFile.write(line)
                line = lines.readline()
                existFailedFile = True
            errorFile.write('\n')
                
        errorFile.close()
        if not existFailedFile:
            log.info('there are no file failed,congratulation!')
        else:
            log.info('some file failed,and we save them in '+errorFileName+'. you can find why failed with it.')
        log.info('generate errorlist success')
    except (ossexception.GetObjectFailed,IOError, Exception), e:
        log.info(e)
        log.info('generate errorlist failed')
        
def generate_md5list(client, bucketName,synctask,md5FileName):
    log.info('start to generate md5list')
    try:
        success = 0
        md5File = open(md5FileName,'a')
        md5File.write('#################### '+synctask+' md5 list ####################\nls')
        roundDirs = client.get_alldir(bucketName,synctask+'/'+type.JobType.Check+'/')
        for roundDir in roundDirs:
            if 'round' in roundDir:
                taskDirs = client.get_alldir(bucketName,roundDir)
                for mydir in taskDirs:
                    lines = readdata.readlines(client, bucketName, mydir+'md5list')
                    line = lines.readline()
                    if line.strip('\n').strip() == '':
                        continue
                    while line:
                        success += 1
                        md5File.write(line)
                        line = lines.readline()
                    md5File.write('\n')
        md5File.close()
        log.info('generate md5list success')
        return success
    except (ossexception.GetObjectFailed,IOError, Exception), e:
        log.info(e)
        log.info('generate md5list failed')
        return 0

    
    
        
        
        

        
                
                
                
                
                
                
                
                
