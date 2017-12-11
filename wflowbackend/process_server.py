import json
import argparse
import logging
from flask import Flask, request, jsonify
log = logging.getLogger('process_server')


app = Flask('process_server')
app.debug = True


@app.route('/setup')
def setup():
    log.info('setting up')
    return '{} {}'.format(app.config['wflowid'], app.config['setupfunc'])

@app.route('/teardown')
def teardown():
    log.info('tearing down')
    return '{} {}'.format(app.config['wflowid'], app.config['teardownfunc'])

@app.route('/status', methods = ['GET','POST'])
def status():
    statusfile = app.config['statusfile']
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

def main():
    logging.basicConfig(level = logging.INFO)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('setupfunc', metavar='setupfunc', help='setup function')
    parser.add_argument('successfunc', metavar='successfunc', help='sucess exit function')
    parser.add_argument('teardownfunc', metavar='teardownfunc', help='exit/cleanup function (always called)')
    parser.add_argument('wflowid', metavar='wflowid', help='workflow id')
    args = parser.parse_args()
    app.config['statusfile'] = 'wflow_status.json'
    app.config['wflowid'] = args.wflowid
    app.config['setupfunc'] = args.setupfunc
    app.config['successfunc'] = args.successfunc
    app.config['teardownfunc'] = args.teardownfunc
    log.info('starting server')
    app.run(host='0.0.0.0', port=5000)
