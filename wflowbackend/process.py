import sys
import logging
import argparse
import json
from wflowbackend.backendtasks import wflow_context, acquire_context
from wflowbackend.messaging import setupLogging

def run_analysis_standalone(setupfunc,onsuccess,teardownfunc,ctx):
    with wflow_context(setupfunc, onsuccess, teardownfunc, ctx):
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
    parser.add_argument('--config-from-server', metavar='wflowid', dest='wflowid', help='acquire context from server via wflowid')
    parser.add_argument('--config-from-file', metavar='ctxfile', dest='ctxfile', help='read context from file')
    parser.add_argument('--stream-logs', dest='stream_logs',  action="store_true", help='stream logging')
    args = parser.parse_args()

    if args.wflowid:
        log = setupLogging(args.wflowid, add_redis = args.stream_logs)
        log.info('acquiring context from wflowserver for wflowid %s', args.wflowid)
        ctx = acquire_context(args.wflowid)
    elif args.ctxfile:
        log = setupLogging(args.wflowid, add_redis = False)
        log.info('acquiring context from file')
        ctx = json.load(open(args.ctxfile))
    else:
        raise RuntimeError('not sure how to aquire context')

    logging.basicConfig(level = logging.INFO)
    log.info('processing %s', sys.argv[1:])
    run_analysis_standalone(args.setupfunc,
                            args.successfunc,
                            args.teardownfunc,
                            ctx
                            )
    log.info('done processing')
