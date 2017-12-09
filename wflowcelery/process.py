import sys
import logging

from wflowcelery.backendtasks import run_analysis_standalone

if __name__ == '__main__':
    log = logging.getLogger('wflow_process')
    logging.basicConfig(level = logging.INFO)
    log.info('processing %s', sys.argv[1:])
    run_analysis_standalone(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
