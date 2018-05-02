#encoding=utf-8
'''
Created on Dec 21, 2017

@author: eric
'''
import hashlib

CheckPartSize = 8*1024*1024

def compute_diskfile_md5(fileName):
    try: 
        myhash = hashlib.md5()
        myfile = open(fileName,'r')
        mybytes = myfile.read(CheckPartSize)
        while mybytes:
            myhash.update(mybytes)
            mybytes = myfile.read(CheckPartSize)
        myfile.close()
        return myhash.hexdigest().lower()
    except Exception,e:
       
        return False
    
if __name__ == '__main__':
    myhash = hashlib.md5()
    myhash.update('')
    print myhash.hexdigest().lower()
    print compute_diskfile_md5('filelist')
