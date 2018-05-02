#encoding=utf-8
'''
Created on Nov 30, 2017

@author: eric
'''
class TASK:
    (ready, dispatched, done, failed) = range(0, 4)
class WORKER:
    (ready, busy, shutdown) = range(0, 3)
class MASTER:
    (ready, busy, shutdown) = range(0, 3)
class CREATE_RPCSERVER:
    (initial, failed, success) = range(0,3)
class JOB:
    (ready, running, done, failed, paused) = range(0, 5)
class S3TRANSFER:
    (ok, closed) = range(0, 2)

