#encoding=utf-8
'''
Created on Feb 1, 2018

@author: eric
'''
import os,sys
path = os.path.abspath(__file__)
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
import aliyunclient,qiniuclient,s3client,tencentclient
from common.const import type 

def create_client(fileType, accessKey, secretKey, endpoint):
    if fileType == type.FileType.DiskFile:
        return None
    elif fileType == type.FileType.BaiduFile or fileType == type.FileType.S3File:
        return s3client.S3Client(accessKey, secretKey, endpoint)
    elif fileType == type.FileType.TencentFile:
        return tencentclient.TencentClient(accessKey, secretKey, endpoint)
    elif fileType == type.FileType.AliyunFile:
        return aliyunclient.AliyunClient(accessKey, secretKey, endpoint)
    elif fileType == type.FileType.QiniuFile:
        return qiniuclient.QiniuClient(accessKey, secretKey, endpoint)

if __name__ == '__main__':
    pass
    
    