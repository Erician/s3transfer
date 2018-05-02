#encoding=utf-8

class PutObjectFailed(Exception):
    pass

class DeleteObjectFailed(Exception):
    pass

class GetObjectFailed(Exception):
    pass

class ListObjectFailed(Exception):
    pass

if __name__ == '__main__':
    try:
        raise Exception('err')
    except (PutObjectFailed, Exception), e:
        print e
    