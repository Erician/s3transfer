#coding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.utils import getpathinfo, setlogging, timeoperation

workerport = sys.argv[1] if len(sys.argv) == 2 else '5253'
logsdir = getpathinfo.get_logs_dir(os.path.abspath(__file__).replace('\\', '/'))
workerslogdir = logsdir+'log-workers/'
if os.path.exists(workerslogdir) == False:
    os.mkdir(workerslogdir)
logFileName = 'log-worker-' + str(workerport) + '.txt'
setlogging.set_logging(workerslogdir+logFileName)

reload(sys)
sys.setdefaultencoding('utf8')
from SimpleXMLRPCServer import SimpleXMLRPCServer
from common.sdk import s3client
from common.myexceptions import ossexception,taskexception
from common.utils import clean
from common.enum import status
from common.const import port, type
from master.task import GenerateDiskFileListTask, TransferOrCheckTask
from common.utils import readdata
from time import sleep
from collections import deque
import workeragent
import time
import xmlrpclib
import threading
from workermessage import WorkerMessage
import logging
from SocketServer import ThreadingMixIn
log = logging.getLogger('root')

"""异步的server类"""
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class Worker:
    generateDiskFileListLock = threading.Lock()
    transferOrCheckLock = threading.Lock()
    DiskFileListFileNumbers = 2
    def __init__(self, port):
        self.probeResponseList = list()
        self.port = port
        self.workerstatus = status.WORKER.ready
        self.current_taskID = ''

    def get_status(self):
        return self.workerstatus

    def get_current_taskID(self):
        self.current_taskID
    
    def do_task(self,taskDict):
        log.info('receive task:')
        log.info(taskDict)
        self.current_taskID = taskDict['ID']
        if taskDict['type'] == type.Task.TransferOrCheck:
            task = TransferOrCheckTask().with_attribute(taskDict)
            th = threading.Thread(target=self.do_transferOrCheckTask, args=(task,))
        elif taskDict['type'] == type.Task.GenerateFileList:
            task = GenerateDiskFileListTask().with_attribute(taskDict)
            th = threading.Thread(target=self.do_generateDiskFileListTask, args=(task,))
        th.setDaemon(True)
        th.start()
        return True
    
    def do_transferOrCheckTask(self, task):
        try:
            self.workerstatus = status.WORKER.busy
            self.transferOrCheckLock.acquire()
            workermessage = WorkerMessage('127.0.0.1', task.get_ID(), self.port, True, None)
            workermessage.set_type(type.Task.TransferOrCheck)
            log.info('start to execute a task:'+task.to_string())
            agent = workeragent.WorkerAgent(task)
            destination = task.get_destination()
            desClient = s3client.S3Client(destination['accessKey'],destination['secretKey'],destination['endpoint'])
            desClient.put_object(destination['bucketName'],task.get_keyForTaskPath()+'taskstatus', 
                                'running' + '\n' + task.get_ID())
            result = agent.execute()
            desClient.put_object(destination['bucketName'],task.get_keyForTaskPath()+'taskstatus', 
                                'done' + '\n' + task.get_ID())
            workermessage.set_result(result)
            send_message_to_workermanager(workermessage)
            log.info('execute the task successfully:'+task.to_string())
        except (ossexception.PutObjectFailed, ossexception.GetObjectFailed,
                taskexception.TaskIDIsNotTheSame,Exception), e:
            log.warning(e)
            log.info('execute the task failed:'+task.to_string())
            workermessage.set_isSuccess(False)
            send_message_to_workermanager(workermessage)
        finally:
            self.current_taskID = ''
            self.workerstatus = status.TASK.ready
            self.transferOrCheckLock.release()

    def __recover_generateDiskFileListTask_info(self, desClient, bucketName, prefix):
        try:
            filelistNumber = 0
            queue = deque()
            if desClient.does_object_exists(bucketName, prefix+'dirqueue'):
                lines = readdata.readlines(desClient, bucketName, prefix + 'dirqueue')
                line = lines.readline().strip('\n').strip()
                filelistNumber = long(line)
                line = lines.readline()
                while line:
                    line = line.strip('\n').strip()
                    if line == '':
                        continue
                    queue.append(line)
                    line = lines.readline()
            return [filelistNumber, queue]
        except Exception, e:
            return [filelistNumber, queue]
        finally:
            log.info('recover generateDiskFileListTask info, filelistNumber:'+str(filelistNumber)+' dirqueue:')
            log.info(queue)

    def do_generateDiskFileListTask(self, task):
        try:
            self.workerstatus = status.WORKER.busy
            self.generateDiskFileListLock.acquire()
            workermessage = WorkerMessage('127.0.0.1', task.get_ID(), self.port, True, None)
            workermessage.set_type(type.Task.GenerateFileList)
            desClient = s3client.S3Client(task.get_accessKey(), task.get_secretKey(), task.get_endpoint())
            log.info('start to generate file list')
            if desClient.does_object_exists(task.get_bucketName(), task.get_fileNamePrefix()+'status'):
                log.info('status exists, this means generating disk filelist is already finished')
                send_message_to_workermanager(workermessage)
                return True
            filelistNumber, queue = self.__recover_generateDiskFileListTask_info(desClient, task.get_bucketName(), task.get_fileNamePrefix())
            jobFileNumbers = 0
            jobSize = 0
            #文件列表的分割方法：以文件个数分割，每满达到DiskFileListFileNumbers个，存到下一个文件中
            fileNumbersCount = 0
            filelist = list()
            sync = task.get_sync()
            if len(queue) == 0:
                queue.append(task.get_absolutepath())
            while len(queue) != 0:
                mydir = queue.popleft()
                for dirname in os.listdir(mydir):
                    if os.path.isdir(mydir+dirname):
                        queue.append(mydir+dirname+'/')
                desClient.put_object(task.get_bucketName(), task.get_fileNamePrefix() + "dirqueue", str(filelistNumber)+'\n'+'\n'.join(queue))
                log.info('dirqueue is saved successfully, filelistNumber:'+str(filelistNumber))
                for filename in os.listdir(mydir):
                    if os.path.isfile(mydir+filename):
                        if fileNumbersCount >= Worker.DiskFileListFileNumbers:
                            desClient.put_object(task.get_bucketName(), task.get_fileNamePrefix() + "filelist" + str(filelistNumber),'\n'.join(filelist))
                            log.info('filelist' + str(filelistNumber) + ' is created,saved in ' + task.get_fileNamePrefix() + "filelist" + str(filelistNumber))
                            fileNumbersCount = 0
                            filelistNumber += 1
                            filelist[:] = []
                        abspath = mydir+filename
                        lastmodify = os.path.getmtime(abspath)
                        if not timeoperation.isBetween(float(lastmodify), float(sync['since']),float(sync['starttime'])):
                            log.info('the modfiy time:' + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(lastmodify)))) + ' of ' + abspath +
                                     ' is not between ' + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(sync['since'])))) +
                                     ' and ' + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(sync['starttime'])))))
                            continue
                        sizeOfBytes = os.path.getsize(abspath)
                        fileNumbersCount += 1
                        filelist.append(abspath + '\t' + str(sizeOfBytes))
                        jobFileNumbers += 1
                        jobSize += long(sizeOfBytes)

                if fileNumbersCount != 0:
                    desClient.put_object(task.get_bucketName(),task.get_fileNamePrefix()+"filelist"+str(filelistNumber), '\n'.join(filelist))
                    log.info('filelist'+str(filelistNumber)+' is created,saved in '+task.get_fileNamePrefix()+"filelist"+str(filelistNumber) )
                    fileNumbersCount = 0
                    filelistNumber += 1
                    filelist[:] = []

            desClient.put_object(task.get_bucketName(),task.get_fileNamePrefix()+'status', '')
            log.info('the flag of finish generating disk-filelist is put successfully')
            send_message_to_workermanager(workermessage)
            log.info('generate file list successfully,fileNumbers:' + str(jobFileNumbers) + ' size:' + str(jobSize))
            return True
        except IOError:
            log.warning('can not find the file or the file not exists!')
            log.info('you should check you config about absolutepath')
            workermessage.set_isSuccess(False)
            send_message_to_workermanager(workermessage)
            log.info('generate file list failed')
            return False
        except Exception, e:
            log.info(e)
            workermessage.set_isSuccess(False)
            send_message_to_workermanager(workermessage)
            log.info('generate file list failed')
        finally:
            self.current_taskID = ''
            self.workerstatus = status.TASK.ready
            self.generateDiskFileListLock.release()

def send_message_to_workermanager(workermessage):
    while True:
        try:
            s = xmlrpclib.ServerProxy('http://127.0.0.1:' + str(port.ManagerPort.WorkerManager), allow_none=True)
            s.send_message(workermessage)
            log.info('send workermessage to WorkerManager successfully')
            log.info(workermessage.to_string())
            return True
        except Exception, e:
            log.warning(e)
            log.warning('send workermessage to WorkerManager failed, will retry')
            sleep(10)

        
def main(port):
    log.info('worker is starting, and listening on port:'+str(port))
    server = ThreadXMLRPCServer(("0.0.0.0", int(port)),allow_none=True,logRequests=False)  
    server.register_instance(Worker(port))
    server.serve_forever()
    
def catch_main_except():
    try:
        main(workerport)
    except Exception,e:
        log.error(e)
    finally:
        log.info('the worker:' + str(workerport) + ' is closed!')

if __name__ == '__main__':
    catch_main_except()
    '''
    queue = deque()
    queue.append('a')
    queue.append('s')
    queue.append('x')
    log.info(queue)
    print 'ssss\n'+'\n'.join(queue)
    '''
    
    
