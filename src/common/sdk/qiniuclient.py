#coding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
absdir = absdir[0:absdir.rfind('/',0,len(absdir))]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
reload(sys)

from common.myexceptions import ossexception
from qiniu import Auth, put_file, etag, urlsafe_base64_encode
from qiniu import BucketManager
import qiniu.config
import requests
import basesdk
import s3client
import logging
import urllib
log = logging.getLogger('root')

class QiniuClient(basesdk.BaseSDK):
    """这个SDk没有自动重试，就需要自己重试了"""
    def __init__(self,access_key, secret_key,bucket_domain):
        self.q = Auth(access_key, secret_key)
        self.bucket_domain = bucket_domain
        self.retryTimes = 5
        
    def list_objects(self,bucketName,prefix='',marker='',limit=1000):
        times = 0
        while times<=self.retryTimes:
            times+=1
            bucket = BucketManager(self.q)
            try:
                ret, eof, info = bucket.list(bucketName, prefix, marker, limit, '/')
                objects = [(myobject['key']) for myobject in ret['items']]
                if ret.has_key('commonPrefixes'):
                    dirs = ret['commonPrefixes']
                else:
                    dirs = []
                if eof==True:
                    isTruncated = False
                else:
                    isTruncated = True
                if ret.has_key('marker') and ret['marker']:
                    nextmarker = ret['marker']
                else:
                    nextmarker = ''
                return [objects,dirs,isTruncated,nextmarker]
            except Exception,e:
                log.info(info)
                log.error(e)
        return None
    
    def list_objects_without_delimiter(self,bucketName,prefix='',marker='',limit=1000):
        times = 0
        while times<=self.retryTimes:
            times+=1
            bucket = BucketManager(self.q)
            try:
                ret, eof, info = bucket.list(bucketName, prefix, marker, limit)
                objects = [(myobject['key']) for myobject in ret['items']]
                if eof==True:
                    isTruncated = False
                else:
                    isTruncated = True
                if ret.has_key('marker') and ret['marker']:
                    nextmarker = ret['marker']
                else:
                    nextmarker = ''
                return [objects,isTruncated,nextmarker]
            except Exception,e:
                log.info(info)
                log.error(e)
        raise ossexception.ListObjectFailed('')
    
    def get_object_with_StreamingBody(self,bucketName,keyName):
        """
        Seriously,the qiniu doesn't return a stream,
        it is just the response
        """
        times = 0
        recordErrorReason = ''
        while times <= self.retryTimes:
            times += 1
            base_url = 'http://%s/%s' % (self.bucket_domain, self.urlEncode(keyName))
            try:
                private_url = self.q.private_download_url(base_url, expires=3600*12)
                return [True, private_url]
            except Exception,e:
                log.error(e)
                recordErrorReason = e
                pass
        return [None,recordErrorReason]
        
    def get_object_size(self,bucketName,keyName):
        """return the size if it exist, else -1"""
        times = 0
        while times <= self.retryTimes:
            times += 1
            #初始化BucketManager
            bucket = BucketManager(self.q)
            try:
                ret, info = bucket.stat(bucketName, keyName)
                if info.status_code == 200:
                    return ret['fsize']
                else:
                    return -1
            except Exception,e:
                log.error(e)
        return -1
    
    def get_object_size_and_lastmodify(self,bucketName,keyName):
        """return the [size,lastmodify] if it exist, else [-1,-1]"""
        times = 0
        while times <= self.retryTimes:
            times += 1
            #初始化BucketManager
            bucket = BucketManager(self.q)
            try:
                ret, info = bucket.stat(bucketName, keyName)
                if info.status_code == 200:
                    return [ret['fsize'], ret['putTime']]
                else:
                    return [-1,-1]
            except Exception,e:
                log.error(e)
        return [-1,-1]
    
    def get_object_lastmodify(self,bucketName,keyName):
        """return the lastmodify if it exist, else -1"""
        times = 0
        while times <= self.retryTimes:
            times += 1
            #初始化BucketManager
            bucket = BucketManager(self.q)
            try:
                ret, info = bucket.stat(bucketName, keyName)
                if info.status_code == 200:
                    return ret['putTime']
                else:
                    return -1
            except Exception,e:
                log.error(e)
        return -1
    
    def does_object_exists(self,bucketName,keyName):
        times = 0
        while times <= self.retryTimes:
            times += 1
            #初始化BucketManager
            bucket = BucketManager(self.q)
            try:
                ret, info = bucket.stat(bucketName, keyName)
                if info.status_code == 200:
                    return True
                else:
                    return None
            except Exception,e:
                log.error(e)
        return None
    

    def upload_localfile(self,bucketName,keyName,localfilePath):
        times = 0
        while times <= self.retryTimes:
            times += 1
            try:
                token = self.q.upload_token(bucketName, keyName, 3600)
                ret, info = put_file(token, keyName, localfilePath)
                if info.status_code == 200:
                    return True
                else:
                    return None
            except Exception,e:
                log.error(e)
        return None

    def delete_one_object(self,bucketName,keyName):
        times = 0
        while times <= self.retryTimes:
            times += 1
            #初始化BucketManager
            bucket = BucketManager(self.q)
            try:
                ret, info = bucket.delete(bucketName, keyName)
                if info.status_code == 200:
                    return True
                else:
                    return None
            except Exception,e:
                log.error(e)
        return None

    def urlEncode(self, string):
        encodedString = ''
        for ch in string:
            if ch == '@':
                encodedString += ch
            else:
                encodedString += urllib.quote(ch)
        return encodedString

if __name__ == '__main__':
    pass
    
        
        
        
    
        