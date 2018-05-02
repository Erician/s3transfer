#encoding=utf-8
import logging

def set_logging(logFileName):
    """
    setting log config
    :type：string
    :param logpath: path of log file
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [Thread-ID:%(thread)d] %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=logFileName,
                        filemode='a')
    #不需要打印到屏幕上
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('nose').setLevel(logging.WARNING)
    logging.getLogger('qcloud_cos.cos_client').setLevel(logging.WARNING)
    
    
    