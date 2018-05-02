#encoding=utf-8
'''
Created on Jan 15, 2018

@author: eric
'''
import os,sys
reload(sys)
sys.setdefaultencoding('utf8')
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

from common.myexceptions import ossexception
import basesdk
import logging
import time
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
log = logging.getLogger('root')

class TencentClient(basesdk.BaseSDK):
    """tencent cos 在requests　上做了重试，可在创建client时配置"""
    def __init__(self, access_key, secret_key, region):
        config = CosConfig(
            Region=region, 
            Secret_id=access_key, 
            Secret_key=secret_key, 
            Token='')
        self.client = CosS3Client(config, 5)
        
    def list_objects(self, bucketName, prefix='', marker='', limit=1000):
        try:
            response = self.client.list_objects(
                bucketName, 
                prefix, '/', 
                marker, 
                limit)
            if response.has_key('Contents'):
                objects = [(myobject['Key']).encode('utf-8') for myobject in response['Contents']]
            else:
                objects = []
            if response.has_key('CommonPrefixes'):
                if len(response['CommonPrefixes']) == 1:
                    dirs = []
                    dirs.append(response['CommonPrefixes']['Prefix'])
                else:
                    dirs = [(myobject['Prefix']).encode('utf-8') for myobject in response['CommonPrefixes']]
            else:
                dirs = []
            if response['IsTruncated'] == 'false':
                isTruncated = False
            else:
                isTruncated = True
            if response.has_key('NextMarker'):
                nextmarker = response['NextMarker']
            else:
                nextmarker = ''
            return [objects, dirs, isTruncated, nextmarker]
        except Exception, e:
            log.error(e)
            return None
    
    def list_objects_without_delimiter(self,bucketName,prefix='',marker='',limit=1000):
        try:
            response = self.client.list_objects(
                Bucket = bucketName, 
                Prefix = prefix,
                Marker = marker, 
                MaxKeys = limit)
            if response.has_key('Contents'):
                objects = [(myobject['Key']).encode('utf-8') for myobject in response['Contents']]
            else:
                objects = []
            if response['IsTruncated'] == 'false':
                isTruncated = False
            else:
                isTruncated = True
            if response.has_key('NextMarker'):
                nextmarker = response['NextMarker']
            else:
                nextmarker = ''
            return [objects, isTruncated, nextmarker]
        except Exception, e:
            log.error(e)
            raise ossexception.ListObjectFailed('')
    
    def get_object_with_StreamingBody(self, bucketName, keyName):
        """
        tencent cos will return a stream, and the s3clint can use it to upload directly with upload_fileobj
        """
        try:
            streamBody = self.client.get_object(bucketName, keyName)['Body']
            lines = streamBody.get_raw_stream()
            return [True, lines]
        except Exception, e:
            log.error(e)
            return [None,e] 
    
    def get_object_with_stream(self, bucketName, keyName):
        """
        like get_object_with_StreamingBody
        """
        try:
            streamBody = self.client.get_object(bucketName, keyName)['Body']
            return streamBody.get_raw_stream()
        except Exception, e:
            log.error(e)
            return None 
        
    def get_object_size(self, bucketName, keyName):
        """return the size if it exist, else -1"""
        try:
            response = self.client.head_object(bucketName, keyName)
            return response['Content-Length']
        except Exception,e:
            log.error(e)
            return -1
        
    def get_object_size_and_lastmodify(self,bucketName,keyName):
        """return the [size,lastmodify] if it exist, else [-1,-1]"""
        try:
            response = self.client.head_object(bucketName, keyName)
            return [response['Content-Length'],
                    time.mktime(time.strptime(response['Last-Modified'], "%a, %d %b %Y %H:%M:%S GMT"))]
        except Exception,e:
            log.error(e)
            return [-1,-1]
        
    def get_object_lastmodify(self,bucketName,keyName):
        """return the lastmodify if it exist, else -1"""
        try:
            response = self.client.head_object(bucketName, keyName)
            return time.mktime(time.strptime(response['Last-Modified'], "%a, %d %b %Y %H:%M:%S GMT"))
        except Exception,e:
            log.error(e)
            return -1
    
    def does_object_exists(self, bucketName, keyName):
        try:
            self.client.head_object(bucketName, keyName)
            return True
        except Exception,e:
            log.error(e)
            return None
    
    def upload_localfile(self, bucketName, keyName, localfilePath):
        try:
            # 文件流 简单上传
            with open(localfilePath, 'rb') as fp:
                self.client.put_object(
                    Bucket=bucketName,  # Bucket由bucketname-appid组成
                    Body=fp,
                    Key=keyName,
                    StorageClass='STANDARD',
                    CacheControl='no-cache'
                )       
            return True
        except Exception,e:
            log.error(e)
            return None
    def delete_one_object(self, bucketName, keyName):
        try:
            self.client.delete_object(bucketName, keyName)
            return True
        except Exception,e:
            log.error(e)
            return None
        
if __name__ == '__main__':
    pass


        
