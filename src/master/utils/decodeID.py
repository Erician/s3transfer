#encoding=utf-8
import base64
import time

def taskID(mytaskID):
    info = base64.b64decode(mytaskID)
    pair = info.split(' ')
    pos = int(pair[2])
    return pos
    
def taskID_just_decode(mytaskID):
    info = base64.b64decode(mytaskID)
    return info

if __name__ == '__main__':
    print taskID_just_decode('MTUyMzM1OTAxMC45MSBqc3MtdGVzdCA0Nw==')
    pos,isInTaskList = taskID('MjAxOS4yMyB5b3VoZSA5IFRydWU=')[2:]
    print pos
    print isInTaskList

    
    