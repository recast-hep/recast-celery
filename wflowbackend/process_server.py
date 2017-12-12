import json
import argparse
import logging
import os

import wflowbackend.backendtasks as backendtasks
import wflowbackend.messaging as messaging
from flask import Flask, request, jsonify
log = logging.getLogger('process_server')

app = Flask('process_server')
app.debug = True

def get_context():
    return json.load(open(app.config['context_file']))

@app.route('/context')
def context():
    return jsonify(get_context())

@app.route('/finalize')
def finalize():
    log.info('finalizing')
    ctx = get_context()
    teardownfunc = getattr(backendtasks,app.config['successfunc'])
    teardownfunc(ctx)
    log.info('successfunc done')
    return jsonify({'status': 'ok'})

@app.route('/teardown')
def teardown():
    log.info('tearing down')
    ctx = get_context()
    teardownfunc = getattr(backendtasks,app.config['teardownfunc'])
    teardownfunc(ctx)
    log.info('teardown done')
    return jsonify({'status': 'ok'})

@app.route('/status', methods = ['GET','POST'])
def status():
    statusfile = app.config['status_file']
    if request.method == 'POST':
        status_data = request.json
        status_data = {'success': status_data['success'], 'ready': status_data['ready']}
        log.info('status is being set to %s (writting to file %s)', status_data, statusfile)
        with open(statusfile,'w') as f:
            json.dump(status_data, f)
            return jsonify({'set_status': True})
    else:
        try:
            with open(statusfile) as f:
                log.info('reading status')
                return jsonify(json.load(f))
        except IOError:
            with open(statusfile,'w') as f:
                log.info('initial setup of statusfile %s', statusfile)
                data = {'success': False, 'ready': False}
                json.dump(data, f)
                return jsonify(data)

def init():
    log.info('setting up')

    ctx = backendtasks.acquire_context(app.config['wflowid'])
    setupfunc = getattr(backendtasks,app.config['setupfunc'])
    setupfunc(ctx)

    CONTEXTFILE = '.wflow_context'
    STATUSFILE = '.wflow_status'

    app.config['context_file'] = os.path.join(ctx['workdir'],CONTEXTFILE)
    app.config['status_file'] = os.path.join(ctx['workdir'],STATUSFILE)
    log.info('declaring context and status files at %s %s',
             app.config['context_file'], app.config['status_file'])

    with open(app.config['context_file'],'w') as f:
        json.dump(ctx,f)

    log.info('setup done')

def main():
    logging.basicConfig(level = logging.INFO)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('setupfunc', metavar='setupfunc', help='setup function')
    parser.add_argument('successfunc', metavar='successfunc', help='sucess exit function')
    parser.add_argument('teardownfunc', metavar='teardownfunc', help='exit/cleanup function (always called)')
    parser.add_argument('wflowid', metavar='wflowid', help='workflow id')
    parser.add_argument('--stream-logs', dest='stream_logs',  action="store_true", help='stream logging')

    args = parser.parse_args()
    app.config['wflowid'] = args.wflowid
    app.config['setupfunc'] = args.setupfunc
    app.config['successfunc'] = args.successfunc
    app.config['teardownfunc'] = args.teardownfunc
    log.info('starting server')

    messaging.setupLogging(args.wflowid, add_redis = args.stream_logs)

    init()
    app.run(host='0.0.0.0', port=5000)
