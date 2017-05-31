from datetime import datetime
import emitter
import logging
import json
import redis
import os

log = logging.getLogger(__name__)

def get_redis():
    log.info('getting celery from %s',os.environ['RECAST_CELERY_REDIS_HOST'])
    return redis.StrictRedis(host = os.environ['RECAST_CELERY_REDIS_HOST'],
                               db = os.environ['RECAST_CELERY_REDIS_DB'],
                             port = os.environ['RECAST_CELERY_REDIS_PORT'])

def jobguid_message_key(jobguid):
    return 'recast:{}:msgs'.format(jobguid)


def store_and_emit(jobguid,msg_data):
    red = get_redis()
    io  = emitter.Emitter({'client': red})
    #store
    msglist = jobguid_message_key(jobguid)
    red.rpush(msglist,json.dumps(msg_data))
    #emit
    io.Of('/monitor').In(str(jobguid)).Emit('room_msg',msg_data)


def generic_message(jobguid,msgtype,jsonable):
    msg_data = {'type':msgtype,'date':datetime.now().strftime('%Y-%m-%d %X'),'data':jsonable}
    store_and_emit(jobguid,msg_data)

def socketlog(jobguid,msg):
    msg_data = {'type':'log_message','date':datetime.now().strftime('%Y-%m-%d %X'),'msg':msg}
    store_and_emit(jobguid,msg_data)

class RecastLogger(logging.StreamHandler):
    def __init__(self,jobid):
        self.jobid = jobid
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        socketlog(self.jobid,msg)

class RedisHandler(logging.StreamHandler):
    def __init__(self,jobguid):
        self.red = redis.StrictRedis(
            host = os.environ.get('RECAST_CELERY_REDIS_HOST','localhost'),
            port = os.environ.get('RECAST_CELERY_REDIS_DB',6379),
            db = os.environ.get('RECAST_CELERY_REDIS_PORT',0)
        )
        self.jobguid = jobguid
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        data = {
            'wflowguid': self.jobguid,
            'msg_type':'wflow_log', #__ for logstash
            'msg':msg,
            'date':datetime.now().strftime('%Y-%m-%d %X'),
            'type':'log_message'
        }
        self.red.publish(os.environ.get('PACKTIVITY_LOGGER_CHANNEL','logstash:in'),json.dumps(data))

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

    redishandler = RedisHandler(jobguid)
    log.addHandler(redishandler)


    return log,recastlogger
