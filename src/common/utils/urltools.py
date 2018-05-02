#encoding=utf-8
import requests
requests.exceptions
import logging
from time import sleep
log = logging.getLogger('root')
RetryTimes = 3
def get_info(url):
    #retunr resource size from url
    retrytime = 0
    while retrytime < RetryTimes:
        try:
            response = requests.head(url, allow_redirects=True)
            if response.status_code == 200:
                return long(response.headers['Content-Length'])
        except Exception, e:
            log.info(e)
        retrytime += 1
        sleep(1)
    return -1

def download_by_rang(url, start, end):
    retrytime = 0
    while retrytime < RetryTimes:
        try:
            headers = {'Range': 'bytes=%d-%d' % (start, end)}
            response = requests.get(url, stream=True, headers=headers, timeout=60)
            if response.status_code == 206:
                return response.text
        except Exception, e:
            log.info(e)
        retrytime += 1
        sleep(1)
    return None

def download(url):
    retrytime = 0
    while retrytime < RetryTimes:
        try:
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 200:
                return response.text
        except Exception, e:
            log.info(e)
        retrytime += 1
        sleep(1)
    return None


if __name__ == '__main__':
    url = 'http://1251412368.vod2.myqcloud.com/vod1251412368/9031868222807494016/f0.flv'
    print download_by_rang(url, 0, 88888)
    print get_info(url)
    print download(url)

