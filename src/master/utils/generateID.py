#encoding=utf-8
'''
Created on Nov 30, 2017

@author: eric
'''
import base64
def taskID(mystr):
    
    return base64.b64encode(mystr)


if __name__ == '__main__':
    
    print taskID('2019.23 youhe 9 True')
    
