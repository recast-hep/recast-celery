from datetime import datetime
import emitter
import logging
import json
import redis
import logging
import os

log = logging.getLogger(__name__)

def get_redis():
    log.info('getting celery from %s',os.environ['RECAST_CELERY_REDIS_HOST'])
    return redis.StrictRedis(host = os.environ['RECAST_CELERY_REDIS_HOST'],
                               db = os.environ['RECAST_CELERY_REDIS_DB'],
                             port = os.environ['RECAST_CELERY_REDIS_PORT'])

def jobguid_message_key(jobguid):
    return 'recast:{}:msgs'.format(jobguid)

def socketlog(jobguid,msg):
    red = get_redis()
    io  = emitter.Emitter({'client': red})

    msg_data = {'date':datetime.now().strftime('%Y-%m-%d %X'),'msg':msg}

    #store msg in redis
    msglist = jobguid_message_key(jobguid)
    red.rpush(msglist,json.dumps(msg_data))

    io.Of('/monitor').In(str(jobguid)).Emit('room_msg',msg_data)

class RecastLogger(logging.StreamHandler):
    def __init__(self,jobid):
        self.jobid = jobid
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        socketlog(self.jobid,msg)

def get_stored_messages(jobguid):
    msglist = jobguid_message_key(jobguid)
    red = get_redis()
    return red.lrange(msglist,0,-1)

def setupLogging(jobguid):
    #setup logging
    log = logging.getLogger('RECAST')
    recastlogger = RecastLogger(jobguid)
    recastlogger.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    log.setLevel(logging.INFO)
    log.addHandler(recastlogger)
    return log,recastlogger
