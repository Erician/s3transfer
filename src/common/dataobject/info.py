#encoding=utf-8
import os,sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
from common.enum import status
from common.const import port
import SimpleXMLRPCServer
from collections import deque
import threading
import logging
log = logging.getLogger('root')

class Info(object):
    
    def __init__(self, port, status):
        self.port = port
        self.status = status
        
    def set_port(self, port):
        self.port = port
        
    def get_port(self):
        return self.port
    
    def set_status(self, status): 
        self.status = status
        
    def get_status(self):
        return self.status
    