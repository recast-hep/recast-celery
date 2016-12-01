import os
result_backend = 'redis'
redis_host = os.environ['RECAST_CELERY_REDIS_HOST']
redis_port = os.environ['RECAST_CELERY_REDIS_PORT']
redis_db = os.environ['RECAST_CELERY_REDIS_DB']
imports = ('recastcelery.backendtasks',)


task_track_started = True
task_acks_late = True
worker_prefetch_multiplier = 1


task_serializer = 'pickle'

broker_url = 'redis://{}'.format(redis_host)

# We don't want results (including Task States) to expire
result_expires = None

# this sets the time window in which a task must start on a worker that fetched the task
# before the task is redelivered to another worker. Since the prefetch multiplier means
# that one task can be fetched while another is still running on the worker this window
# must be larger then longest task duration. kind of annoying but that's what you get
# to have some sensible behavior in case of a failing worker. We'll set this to 24h
# more info: http://docs.celeryproject.org/en/latest/getting-started/brokers/redis.html
# this was a problem initially when we thought long EWK tasks being re-executed multiple times...
broker_transport_options = {'visibility_timeout':os.environ.get('RECAST_CELERY_VISIBILITY_TIMEOUT',86400)}
