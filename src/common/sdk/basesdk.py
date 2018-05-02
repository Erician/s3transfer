import boto3
import botocore
class BaseSDK():
    def __init__(self,accessKey,secretKey,endPoint):
        pass
    def create_client(self,accessKey,secretKey,endPoint):
        pass
    def list_objects(self,client,bucketName,prefix='',marker=''):
        pass
    def list_objects_without_delimiter(self,bucketName,prefix='',marker='',limit=1000):
        pass
    def upload_fileobj(self,bucketName,keyName,streamBody):
        pass
    def put_object(self,bucketName,keyName,body):
        pass
    def upload_localfile(self, bucketName, keyName, localFilePath):
        pass
    def put_object_with_url(self,bucketName,keyName,qiniu_res):
        pass
    def get_object_with_stream(self,client,bucketName,keyName):
        pass
    def get_object_with_StreamingBody(self,bucketName,keyName):
        pass
    def get_object_size(self,bucketName,keyName):
        pass
    def get_object_size_and_lastmodify(self,bucketName,keyName):
        pass
    def get_object_lastmodify(self,bucketName,keyName):
        pass
    def does_object_exists(self,bucketName,keyName):
        pass
    def delete_one_object(self,bucketName,keyName):
        pass
    def delete_objectlist(self,bucketName,objectlist):
        pass
    
        