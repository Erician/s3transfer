#encoding=utf-8
"""
it is not thread-safe:http://boto3.readthedocs.io/en/latest/guide/resources.html
we need to change our code later
"""
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
absdir = absdir[0:absdir.rfind('/',0,len(absdir))]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
reload(sys)
sys.setdefaultencoding('utf8')
from common.myexceptions import ossexception
from common.utils import urltools
import hashlib
import boto3,botocore
import basesdk
import io
import urllib
import logging
import time
log = logging.getLogger('root')

class StreamingBodyIO(io.RawIOBase):
    """Wrap a boto StreamingBody in the IOBase API."""
    def __init__(self, body):
        self.body = body
    def readable(self):
        return True
    def read(self, n=-1):
        n = None if n < 0 else n
        return self.body.read(n)

class S3Client(basesdk.BaseSDK):

    #分片大小为8M, 和boto3的默认分片大小保持一致:http://boto3.readthedocs.io/en/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
    Multipart_Chunksize = 8*1024*1024
    def __init__(self,accessKey,secretKey,endPoint):
        # Both timeouts default to 60, you can customize them, seperately
        #这里又是会出现错误，Error:ValueError: Unknown component: endpoint_resolver
        #该问题是由于多线程使用同一个session,网址：https://geekpete.com/blog/multithreading-boto3/
        #boto3的文档建议每个thread应该独占一个s3client;
        #https://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
        #问题是我独占还是有时会有这个bug,临时的解决方法
        #现在是共用一个client,反而是安全的
        client = boto3.client(
            's3',
            aws_access_key_id=accessKey,
            aws_secret_access_key=secretKey,
            endpoint_url=endPoint,
        )
        self.client = client

    def list_objects(self,bucketName,prefix='',marker=''):
        '''
        说明：京东云存储返回的key是经过url编码的key,如%2B.txt,返回的结果是%252B.txt，因此要进行解码
        :return ：返回值是一个list,依次是objects列表、目录、是否list完整,nextmarker
        '''
        #修改逻辑，可能会遇到slow down的错误，因此要sleep 1秒后重试
        while True:
            try:
                response = self.client.list_objects(
                    Bucket=bucketName,
                    Delimiter='/',
                    Prefix=prefix,
                    Marker=marker
                )
                if response.has_key('Contents'):
                    objects = [urllib.unquote(myobject['Key']) for myobject in response['Contents']]
                else:
                    objects = []
                #for myobject in objects:
                #print myobject
                if response.has_key('CommonPrefixes'):
                    dirs = [urllib.unquote(myobject['Prefix']) for myobject in response['CommonPrefixes']]
                else:
                    dirs = []
                isTruncated = response['IsTruncated']
                nextmarker = self.get_nextmarker(objects, dirs)
                return [objects,dirs,isTruncated,nextmarker]
            except Exception,e:
                log.info(e)
                if 'SlowDown' in str(e):
                    time.sleep(1)
                    log.info('continue to list objects')
                else:
                    return None

    def get_nextmarker(self, objects, dirs):
        marker = ''
        if objects and dirs:
            #这里有问题，等贵军上线
            marker = dirs[len(dirs)-1]
        else:
            if objects:
                marker = objects[len(objects)-1]
            elif dirs:
                marker = dirs[len(dirs)-1]
        return marker
    
    def list_objects_without_delimiter(self,bucketName,prefix='',marker='',limit=1000):
        '''
        说明：
        不再使用delimiter，它会降低list的速度
        '''
        #修改逻辑，可能会遇到slow down的错误，因此要sleep 1秒后重试
        while True:
            try:
                #not use EncodingType='url' anymore,because boto3 won't decode the url-encoded key automatically and I don't know why
                response = self.client.list_objects(
                    Bucket=bucketName,
                    MaxKeys=limit,
                    Prefix=prefix,
                    Marker=marker)
                if response.has_key('Contents'):
                    objects = [myobject['Key'] for myobject in response['Contents']]
                else:
                    objects = []
                isTruncated = response['IsTruncated']
                if isTruncated == True:
                    nextmarker = objects[len(objects)-1]
                else:
                    nextmarker = ''
                return [objects,isTruncated,nextmarker]
            except Exception,e:
                log.info(e)
                if 'SlowDown' in str(e):
                    time.sleep(1)
                    log.info('continue to list objects')
                else:
                    raise ossexception.ListObjectFailed('')
    
    def get_alldir(self,bucketName, prefix = '', marker = ''):
        '''
        attention:not recurive
        :return if has no dir,return [];elif error,retrun None;else return a list of dirs,:
        '''
        alldir = list()
        while True:
            returnVal = self.list_objects(bucketName, prefix, marker)
            if not returnVal:
                return None
            [objects,dirs,isTruncated,marker] = returnVal
            if dirs:
                alldir += dirs
            if not isTruncated:
                break
        return alldir

    def get_allobject(self,bucketName, prefix = '', marker = ''):
        '''
        attention:not recurive
        :return if has no object,return [];elif error,retrun None;else return a list of objects,:
        '''
        allobject = list()
        while True:
            returnVal = self.list_objects(bucketName,prefix,marker)
            if not returnVal:
                exit(1)
            [objects,dirs,isTruncated,marker] = returnVal
            if objects:
                allobject += objects
            if not isTruncated:
                break
        return allobject
        
    """
    aws对文件默认使用流式上传
    #diskfile upload supprt stream:https://github.com/boto/boto3/issues/256
    #upload_fileobj:https://github.com/boto/boto3/issues/518
    """
    def put_object(self,bucketName,keyName,body):
        try:
            self.client.put_object(
                Body=body,
                Bucket=bucketName,
                Key=keyName
            )
            return True
        except Exception,e:
            log.warning(e)
            raise ossexception.PutObjectFailed('put '+keyName+' failed!')
    
    def upload_localfile(self,bucketName,keyName,localFilePath):
        try:
            localFile = open(localFilePath,'r')
            self.client.put_object(
                Body=localFile,
                Bucket=bucketName,
                Key=keyName
            )
            return True
        except Exception,e:
            log.warning(e)
            log.warning('put '+keyName+' failed!')
            return None
            
    #readline:https://github.com/boto/boto3/issues/890
    def get_object_with_stream(self,bucketName,keyName):
        try:
            response = self.client.get_object(
                Bucket=bucketName,
                Key=keyName
            )
            lines = StreamingBodyIO(response['Body'])
            return lines
        except Exception,e:
            log.info(e)
            raise ossexception.GetObjectFailed('get '+keyName+' failed!')
            
    def get_object_size(self,bucketName,keyName):
        """return the size if it exist, else -1"""
        try:
            response = self.client.head_object(Bucket=bucketName, Key=keyName)
        except botocore.exceptions.ClientError as e:
            log.info(e)
            if e.response['Error']['Code'] == "404":
                log.info(keyName+'doesn\'t exist')
                return -1
            else:
                log.info('get '+keyName+' size failed')
                return -1
        else:
            return response['ResponseMetadata']['HTTPHeaders']['content-length']

    def get_object_size_and_lastmodify(self,bucketName,keyName):
        """return the [size, lastmodify] if it exist, else [-1,-1]"""
        try:
            response = self.client.head_object(Bucket=bucketName, Key=keyName)
            size = (response['ResponseMetadata']['HTTPHeaders']['content-length'])
            lastModify = response['ResponseMetadata']['HTTPHeaders']['last-modified']
            return [size, time.mktime(time.strptime(lastModify, "%a, %d %b %Y %H:%M:%S GMT"))]
        except botocore.exceptions.ClientError as e:
            log.info(e)
            if e.response['Error']['Code'] == "404":
                log.info(keyName+'doesn\'t exist')
                return [-1,-1]
            else:
                log.info('get '+keyName+' size failed')
                return [-1,-1]
            
    def get_object_lastmodify(self,bucketName,keyName):
        """return the lastmodify if it exist, else -1"""
        try:
            response = self.client.head_object(Bucket=bucketName, Key=keyName)
            print response
            lastModify = response['ResponseMetadata']['HTTPHeaders']['last-modified']
            return time.mktime(time.strptime(lastModify, "%a, %d %b %Y %H:%M:%S GMT"))
        except botocore.exceptions.ClientError as e:
            log.error(e)
            if e.response['Error']['Code'] == "404":
                log.info(keyName+' doesn\'t exist')
                return -1
            else:
                log.error('get '+keyName+' size failed')
                return -1
            
    def does_object_exists(self,bucketName,keyName):
        """return the True if it exist, else None"""
        try:
            self.client.head_object(Bucket=bucketName, Key=keyName)
        except botocore.exceptions.ClientError as e:
            log.info(keyName + ' doesn\'t exist')
            return False
        else:
            return True
            
    def delete_one_object(self,bucketName,keyName):
        try:
            self.client.delete_object(
                Bucket=bucketName,
                Key=keyName
            )
            return True
        except Exception,e:
            log.warning(e)
            return False

    def delete_objectlist(self,bucketName,objectlist):
        for i in range(len(objectlist)):
            keyName = objectlist[i]
            try:
                self.client.delete_object(
                    Bucket=bucketName,
                    Key=keyName
                )
            except Exception,e:
                log.warning(e)
                return False
        return True
    
    #readline:https://github.com/boto/boto3/issues/890
    def abort_multipart_upload(self,bucketName,keyName,uploadID):
        try:
            response = self.client.abort_multipart_upload(
                Bucket=bucketName,
                Key=keyName,
                UploadId=uploadID,
            )
            return response
        except Exception,e:
            log.warning(e)
            return None
    
    def list_multipart_uploads(self,bucketName,prefix = ''):
        try:
            response = self.client.list_multipart_uploads(
                Bucket=bucketName,
                Prefix=prefix,
            )
            return response
        except Exception,e:
            log.info(e)
            return None

    def upload_fileobj(self, bucketName, keyName, streamBody):
        try:
            self.client.upload_fileobj(streamBody, bucketName, keyName)
            return [True,'']
        except Exception,e:
            log.info(e)
            log.info('put object failed!')
            return [None,e]

    def upload_fileobj_by_multipart_upload(self, bucketName, keyName, streamBody):
        try:
            chunk = streamBody.read(S3Client.Multipart_Chunksize)
            if len(chunk) < S3Client.Multipart_Chunksize:
                return self.put_object_with_small_content(bucketName, keyName, chunk)
            uploadID = None
            multiPartUpload = self.client.create_multipart_upload(Bucket=bucketName, Key=keyName)
            uploadID = multiPartUpload['UploadId']
            partInfoDict = {'Parts': []}
            partNumber = 1
            myhash = hashlib.md5()

            while chunk:
                myhash.update(chunk)
                part = self.client.upload_part(
                    Bucket=bucketName,
                    Key=keyName,
                    PartNumber=partNumber,
                    UploadId=multiPartUpload['UploadId'],
                    Body=chunk if chunk else ""
                )
                # PartNumber and ETag are needed
                partInfoDict['Parts'].append({
                    'PartNumber': partNumber,
                    # You can get this from the return of the uploaded part that we stored earlier
                    'ETag': part['ETag']
                })
                chunk = streamBody.read(S3Client.Multipart_Chunksize)

            # This what AWS needs to finish the multipart upload process
            completed_ctx = {
                'Bucket': bucketName,
                'Key': keyName,
                'UploadId': multiPartUpload['UploadId'],
                'MultipartUpload': partInfoDict
            }
            self.client.complete_multipart_upload(**completed_ctx)
            return [True, myhash.hexdigest().lower()]
        except Exception, e:
            log.info(e)
            # 如果分片上传失败，清理残片，因为对一个bucket来说，最多有10000个分片上传任务
            if uploadID:
                self.abort_multipart_upload(bucketName, keyName, uploadID)
            return [None, 'put object failed']

    def put_object_with_url(self, bucketName, keyName, url):
        try:
            size = urltools.get_info(url)
            if size == -1:
                raise Exception
            elif size < S3Client.Multipart_Chunksize:
                return self.put_object_with_small_content(bucketName, keyName, urltools.download(url))
            uploadID = None
            multiPartUpload = self.client.create_multipart_upload(Bucket=bucketName, Key=keyName)
            uploadID = multiPartUpload['UploadId']
            partInfoDict = {'Parts': []}
            myhash = hashlib.md5()

            for partNumber in range(1, size//S3Client.Multipart_Chunksize+2):
                content = urltools.download_by_rang(url, (partNumber-1)*S3Client.Multipart_Chunksize, partNumber*S3Client.Multipart_Chunksize-1)
                myhash.update(content)
                part = self.client.upload_part(
                    Bucket=bucketName,
                    Key=keyName,
                    PartNumber=partNumber,
                    UploadId=multiPartUpload['UploadId'],
                    Body=content if content else ""
                )
                # PartNumber and ETag are needed
                partInfoDict['Parts'].append({
                    'PartNumber': partNumber,
                    # You can get this from the return of the uploaded part that we stored earlier
                    'ETag': part['ETag']
                })

            # This what AWS needs to finish the multipart upload process
            completed_ctx = {
                'Bucket': bucketName,
                'Key': keyName,
                'UploadId': multiPartUpload['UploadId'],
                'MultipartUpload': partInfoDict
            }
            self.client.complete_multipart_upload(**completed_ctx)
            return [True, myhash.hexdigest().lower()]
        except Exception, e:
            log.info(e)
            # 如果分片上传失败，清理残片，因为对一个bucket来说，最多有10000个分片上传任务
            if uploadID:
                self.abort_multipart_upload(bucketName, keyName, uploadID)
            return [None, 'put object failed']

    '''
    :returns: if success, return true and md5
    '''
    def put_object_with_small_content(self, bucketName, keyName, content):
        try:
            myhash = hashlib.md5()
            myhash.update(content)
            self.put_object(bucketName, keyName, content)
            return [True, myhash.hexdigest().lower()]
        except Exception, e:
            log.info(e)
            return [None, 'put object failed']

    def get_object_with_StreamingBody(self,bucketName,keyName):
        try:
            response = self.client.get_object(
                Bucket=bucketName,
                Key=keyName
            )
            return [True,response['Body']]
        except Exception,e:
            log.info(e)
            raise ossexception.GetObjectFailed('get '+keyName+' failed!')

def clean_with_prefix(client, bucketName, prefix):
    try:
        marker = ''
        while True:
            returnVal = client.list_objects_without_delimiter(bucketName, prefix, marker)
            if not returnVal:
                return False
            [objects, isTruncated, marker] = returnVal
            if not client.delete_objectlist(bucketName, objects):
                return False
            if not isTruncated:
                break
        return True
    except Exception, e:
        log.info(e)
        return False
        
if __name__ == '__main__':
    client = S3Client('0DFA5B10A1B8EE0107CAAACD465EA7D4', '2DDFAA55EFEE553114929380EBD6CF88', 'http://s3.cn-north-1.jcloudcs.com')
    clean_with_prefix(client, 'src', '')






    
    
    
