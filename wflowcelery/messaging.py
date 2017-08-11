from datetime import datetime
import logging
import json
import redis
import os

log = logging.getLogger(__name__)

def get_redis():
    log.info('getting celery from %s',os.environ.get('WFLOW_CELERY_REDIS_HOST','localhost'))
    return redis.StrictRedis(host = os.environ.get('WFLOW_CELERY_REDIS_HOST','localhost'),
                               db = os.environ.get('WFLOW_CELERY_REDIS_DB',0),
                             port = os.environ.get('WFLOW_CELERY_REDIS_PORT',6379))

def emit(jobguid,msg_type,msg_data,redis_client = None):
    redis_client = redis_client or get_redis()
    data = {
        'wflowguid': jobguid,
        'msg_type': msg_type, #__ for logstash
        'date':datetime.now().strftime('%Y-%m-%d %X'),
    }
    data.update(**msg_data)
    redis_client.publish(os.environ.get('WFLOW_LOGGER_CHANNEL','logstash:in'),json.dumps(data))

class RedisHandler(logging.StreamHandler):
    def __init__(self,jobguid):
        self.red = get_redis()
        self.jobguid = jobguid
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        emit(self.jobguid, 'wflow_log', {'msg': self.format(record)}, redis_client = self.red)

def setupLogging(jobguid):
    log = logging.getLogger('WFLOWSERVICELOG')
    log.setLevel(logging.INFO)

    redishandler = RedisHandler(jobguid)
    log.addHandler(redishandler)

    return log,redishandler