# encoding=utf-8
'''
Created on Jan 9, 2018

@author: eric
'''
import os,sys
reload(sys)
sys.setdefaultencoding('utf8')
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
absdir = absdir[0:absdir.rfind('/',0,len(absdir))]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])

from common.myexceptions import ossexception
import oss2
import requests
import basesdk
import s3client
import logging

log = logging.getLogger('root')

class AliyunClient(basesdk.BaseSDK):
    """这个SDk没有自动重试，就需要自己重试了"""
    def __init__(self, access_key, secret_key, endpoint):
        self.auth = oss2.Auth(access_key, secret_key)
        self.endpoint = endpoint
        self.retryTimes = 5
        
    def list_objects(self, bucketName, prefix='', marker='', limit=1000):
        times = 0
        while times <= self.retryTimes:
            times += 1
            delimiter = '/'
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                result = bucket.list_objects(prefix, delimiter, marker, limit)
                objects = [objectInfo.key for objectInfo in result.object_list] 
                return [objects, result.prefix_list, result.is_truncated, result.next_marker]
            except Exception, e:
                log.error(e)
        return None
    
    def list_objects_without_delimiter(self,bucketName,prefix='',marker='',limit=1000):
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                result = bucket.list_objects(prefix, '', marker, limit)
                objects = [objectInfo.key for objectInfo in result.object_list]
                return [objects, result.is_truncated, result.next_marker]
            except Exception, e:
                print e
                log.error(e)
        raise ossexception.ListObjectFailed('')
        
    def get_object_with_StreamingBody(self, bucketName, keyName):
        """
        aliyun will return a stream, and the s3clint can use it to upload directly with upload_fileobj
        """
        times = 0
        recordErrorReason = ''
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                return [True, bucket.get_object(keyName)]
            except Exception, e:
                log.error(e)
                recordErrorReason = e
                pass
        return [None, recordErrorReason]
    
    def get_object_with_stream(self, bucketName, keyName):
        """
        like get_object_with_StreamingBody
        """
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                return bucket.get_object(keyName)
            except Exception, e:
                log.error(e)
        return None
        
    def get_object_size(self, bucketName, keyName):
        """return the size if it exist, else -1"""
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                result = bucket.head_object(keyName)
                return result.content_length
            except Exception, e:
                log.error(e)
        return -1
    
    def get_object_size_and_lastmodify(self,bucketName,keyName):
        """return the [lastmodify,size] if it exist, else [-1,-1]"""
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                result = bucket.head_object(keyName)
                return [result.content_length, result.last_modified]
            except Exception, e:
                log.error(e)
        return -1
    
    def get_object_lastmodify(self,bucketName,keyName):
        """return the lastmodify if it exist, else -1"""
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                result = bucket.head_object(keyName)
                return result.last_modified
            except Exception, e:
                log.error(e)
        return -1
    
    def does_object_exists(self, bucketName, keyName):
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                return bucket.object_exists(keyName)
            except Exception, e:
                log.error(e)
        return None
    
    def upload_localfile(self, bucketName, keyName, localfilePath):
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                bucket.put_object_from_file(keyName, localfilePath)
                return True
            except Exception, e:
                print e
                log.error(e)
        return None
    
    def delete_one_object(self,bucketName,keyName):
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                bucket = oss2.Bucket(self.auth, self.endpoint, bucketName)
                bucket.delete_object(keyName)
                return True
            except Exception, e:
                log.error(e)
        return None

if __name__ == '__main__':
    pass

        
        
        
    
        
