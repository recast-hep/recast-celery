from celery import Celery
app = Celery('fromenvapp')
app.config_from_object('recastcelery.fromenvconfig')
