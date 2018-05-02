#encoding=utf-8
import os,sys
from src.common.utils import setlogging
path = os.path.abspath(__file__).replace('\\', '/')
logsdir = path[0:path.rfind('/')]+'/logs/'
if os.path.exists(logsdir) == False:
    os.mkdir(logsdir)
logFileName = 'log-commands.txt'
setlogging.set_logging(logsdir+logFileName)

reload(sys)
sys.setdefaultencoding('utf8')
from src.manager import commands
from src.common.enum import status
import logging
log = logging.getLogger('root')

 
def execute_commands(argv):
    if len(argv) == 1:
        commands.show_help()
    elif len(argv) == 2:
        if argv[1] == '--help':
            commands.show_help()
        elif argv[1] == 'start':
            commands.start_s3transfer()
        elif argv[1] == 'stop':
            commands.stop_s3transfer()
        elif argv[1] == 'restart':
            commands.restart_s3transfer()
        elif argv[1] == 'status':
            commands.show_status()
        else:
            commands.print_command_error(argv)

    elif len(argv) == 3:
        if argv[1] == 'add':
            commands.add_job(argv[2])
        elif argv[1] == 'rm':
            commands.rm_job(argv[2])
        elif argv[1] == 'pause':
            commands.pause_job(argv[2])
        elif argv[1] == 'continue':
            commands.continue_job(argv[2])
        elif argv[1] == 'redo':
            commands.redo_job(argv[2])
        elif argv[1] == 'look':
            commands.look_jobcfg(argv[2])
        elif argv[1] == 'edit':
            commands.edit_jobcfg(argv[2])
        elif argv[1] == 'set':
            commands.set_things(argv[2])
        elif argv[1] == 'status':
            commands.show_status(argv[2])
        else:
            commands.print_command_error(argv)
    else:
        commands.print_command_error(argv)

def record_commands(argv):
    command = ''
    for a in argv:
        command += str(a)+' '
    log.info('---------------'+command+'---------------')
    
if __name__ == '__main__':
    
    if (len(sys.argv) == 2 and (sys.argv[1] == 'start' or sys.argv[1] == '--help')) or  commands.get_local_s3transfer_status() == status.S3TRANSFER.ok:
        record_commands(sys.argv)
        execute_commands(sys.argv)
    else:
        print 's3transfer is closed, please start it with command: s3transfer start'
    
    
    
    
    