#encoding=utf-8
import os

def get_logs_dir(abspath):
    abspath = abspath[0:abspath.rfind('/')]
    abspath = abspath[0:abspath.rfind('/')]
    logsdir = abspath[0:abspath.rfind('/')+1] + 'logs/'
    if not os.path.exists(logsdir):
        os.mkdir(logsdir)
    return logsdir
    
if __name__ == '__main__':
    #print get_absdir(os.path.abspath(__file__).replace('\\', '/'))
    print get_logs_dir(os.path.abspath(__file__).replace('\\', '/'))