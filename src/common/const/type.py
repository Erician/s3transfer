#encoding=utf-8
class JobType:
    Transfer = 'transfer'
    Check = 'check'
class FileType:
    DiskFile = 'diskfile'
    QiniuFile = 'qiniufile'
    S3File = 's3file'
    AliyunFile = 'aliyunfile'
    TencentFile = 'tencentfile'
    BaiduFile = 'baidufile'
    UrlFile = 'urlfile'

class Task:
    GenerateFileList = 'generateFilelist'
    TransferOrCheck = 'transferORcheck'