#encoding=utf-8
class Assignment(object):

    def __init__(self, ID = '', status = '', startTime = 0.0):
        self.ID = ID
        self.status = status
        #we will set startTime, when dispatched
        self.startTime = startTime
        self.isDeleted = False
    
    def set_ID(self, ID):
        self.ID = ID
    
    def get_ID(self):
        return self.ID

    def set_status(self, status):
        self.status = status
    
    def get_status(self):
        return self.status

    def set_startTime(self, startTime):
        self.startTime = startTime

    def get_startTime(self):
        return self.startTime

    def set_isDeleted(self, value):
        self.isDeleted = value

    def get_isDeleted(self):
        return self.isDeleted