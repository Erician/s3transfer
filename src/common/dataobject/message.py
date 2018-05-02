#encoding=utf-8
'''
Message 记录一个任务(task or job)的完成情况，失败还是成功, 以及是否需要重新执行
还有完成的结果（result)，如消耗的时间等
'''
class Message(object):

    def __init__(self, ip='', ID='', port='', isSuccess=True,  result=None):
        self.ip = ip
        self.ID = ID
        self.port = port
        self.isSuccess=isSuccess
        self.result = result
    
    def with_attribute(self, attributeDict):
        if attributeDict.has_key('ip'):
            self.ip = attributeDict['ip']
        if attributeDict.has_key('ID'):
            self.ID = attributeDict['ID']
        if attributeDict.has_key('port'):
            self.port = attributeDict['port']
        if attributeDict.has_key('isSuccess'):
            self.isSuccess = attributeDict['isSuccess']
        if attributeDict.has_key('result'):
            self.result = attributeDict['result']
        return self
    
    def set_ip(self, ip):
        self.ip = ip
    
    def get_ip(self):
        return self.ip
    
    def set_ID(self, ID):
        self.ID = ID
    
    def get_ID(self):
        return self.ID
        
    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port
        
    def set_isSuccess(self, isSuccess):
        self.isSuccess = isSuccess

    def get_isSuccess(self):
        return self.isSuccess

    def set_result(self, result):
        self.result = result
    
    def get_result(self):
        return self.result

'''
记录一个任务(task or job)的完成的结果，如耗时，平均速度等
'''
class Result:
    pass



