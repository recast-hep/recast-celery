import sys
import logging
import argparse
from wflowbackend.backendtasks import wflow_context
from wflowbackend.backendtasks import *

def run_analysis_standalone(setupfunc,onsuccess,teardownfunc,jobguid,redislogging = True):
    with wflow_context(setupfunc, onsuccess, teardownfunc, jobguid, redislogging) as ctx:
        log = logging.getLogger('WFLOWSERVICELOG')
        try:
            pluginmodule,entrypoint = ctx['entry_point'].split(':')
            log.info('setting up entry point %s',ctx['entry_point'])
            m = importlib.import_module(pluginmodule)
            entry = getattr(m,entrypoint)
        except AttributeError:
            log.error('could not get entrypoint: %s',ctx['entry_point'])
            raise

        entry(ctx)

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('setupfunc', metavar='setupfunc', help='setup function')
    parser.add_argument('successfunc', metavar='successfunc', help='sucess exit function')
    parser.add_argument('teardownfunc', metavar='teardownfunc', help='exit/cleanup function (always called)')
    parser.add_argument('wflowid', metavar='wflowid', help='workflow id')
    args = parser.parse_args()




    log = logging.getLogger('wflow_process')
    logging.basicConfig(level = logging.INFO)
    log.info('processing %s', sys.argv[1:])
    run_analysis_standalone(globals()[args.setupfunc],
                            globals()[args.successfunc],
                            globals()[args.teardownfunc],
                            args.wflowid)
    log.info('done processing')
