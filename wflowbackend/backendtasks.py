import zipfile
import os
import shutil
import importlib
import yaml
import json
import logging
import requests
import glob2
import socket
import paramiko
from scp import SCPClient
from .messaging import setupLogging

log = logging.getLogger('WFLOWSERVICELOG')

def generic_upload_results(resultdir, upload_spec):
    #make sure the directory for this point is present

    user = upload_spec['user']
    host = upload_spec['host']
    port = upload_spec['port']
    remotelocation = upload_spec['location']

    client = paramiko.SSHClient()
    policy = paramiko.AutoAddPolicy()
    client.set_missing_host_key_policy(policy)
    client.load_system_host_keys()
    client.connect(host, int(port), user)
    client.exec_command('(test -d {remotelocation} && rm -rf {remotelocation}) || echo "not present yet" '.format(remotelocation = remotelocation))
    client.exec_command('mkdir -p {remotelocation}'.format(remotelocation = remotelocation))
    scp = SCPClient(client.get_transport())
    scp.put(resultdir, recursive=True, remote_path=remotelocation)
    scp.close()

def download_file(url,auth, download_dir):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter

    headers = {}
    if auth:
        headers['Authorization'] = 'Bearer {}'.format(os.environ['WFLOW_DOWNLOAD_TOKEN'])

    log.info('start file download from  %s', url)

    verify = yaml.load(os.environ['WFLOW_DOWNLOAD_VERIFY_SSL'])

    r = requests.get(url, stream=True, headers = headers, verify = verify)
    download_path = '{}/{}'.format(download_dir,local_filename)
    with open(download_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

    log.info('file download finished.')
    return download_path

def prepare_job_fromURL(ctx):
    workdir = ctx['workdir']
    log.info('preparing workdir %s', workdir)

    if not ctx['inputURL']:
        log.warning('No input archive specified, skipping download')
        return

    filepath = download_file(ctx['inputURL'], ctx.get('inputAuth',None), workdir)
    log.info('downloaded done (at: %s)',filepath)

    with zipfile.ZipFile(filepath)as f:
        f.extractall('{}/inputs'.format(workdir))

def setupFromURL(ctx):
    log.info('setting up for context %s',ctx)
    prepare_workdir(ctx['workdir'])
    prepare_job_fromURL(ctx)


def prepare_workdir(workdir):
    os.makedirs(workdir)
    log.info('prepared workdir %s',workdir)

def isolate_results(workdir,resultlist):
    resultdir = '{}/results'.format(workdir)

    if(os.path.exists(resultdir)):
        log.warning('resutl directory %s exists?!?',resultdir)
        shutil.rmtree(resultdir)

    os.makedirs(resultdir)

    for result,resultpath in ((r,os.path.abspath('{}/{}'.format(workdir,r))) for r in resultlist):
        globresult = glob2.glob(resultpath)
        if not globresult:
            log.warning('no matches for glob %s',resultpath)
        for thing in globresult:
            relpath = thing.replace(os.path.abspath(workdir),'')
            inresultpath = '{}/{}'.format(resultdir,relpath)
            dirname = os.path.dirname(inresultpath)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            log.debug('got globmatch %s',relpath)
            log.debug('move to file %s',inresultpath)
            shutil.move(thing,inresultpath)
    return resultdir


def getresultlist(ctx):
    """
    result list can either be provided as module:attribute nullary function
    under the key 'results' or as an actual list of strings under key 'resultlist'
    """
    if 'results' in ctx:
        resultlistname = ctx['results']
        modulename,attr = resultlistname.split(':')
        module = importlib.import_module(modulename)
        resultlister = getattr(module,attr)
        return resultlister()
    if 'resultlist' in ctx:
        return ctx['resultlist']


def generic_onsuccess(ctx):
    jobguid = ctx['jobguid']

    log.info('success for job %s, gathering results... ',jobguid)
    resultdir = isolate_results(ctx['workdir'],getresultlist(ctx))

    upload_spec = ctx['shipout_spec']
    log.info('uploading results to {}:{}'.format(upload_spec['host'],upload_spec['location']))

    generic_upload_results(resultdir,upload_spec)

    log.info('done with uploading results')

def dummy_onsuccess(ctx):
    log.info('success!')
    resultdir = isolate_results(ctx['workdir'],getresultlist(ctx))

    log.info('would be uploading results here..')
    for parent,dirs,files in os.walk(resultdir):
        for f in files:
            log.info('would be uploading this file %s','/'.join([parent,f]))
    log.info('done with uploading results')

def delete_all_but_log(directory, cutoff_size_MB = 50):
    """
    deletes all files in directory except *.log and *.txt which are
    assumed to be logfiles, except when they are too large,
    in which case they are shredded, too
    """
    bytes_per_megabyte = 1048576.0 #(2**20)

    for parent,directories,files in os.walk(directory):
        for fl in files:
            fullpath = '/'.join([parent,fl])
            islog = (fl.endswith('.log') or fl.endswith('.txt'))
            if not (os.path.exists(fullpath) and os.path.isfile(fullpath) and not os.path.islink(fullpath)):
                continue
            if islog:
                size_MB = os.stat(fullpath).st_size/bytes_per_megabyte
                if size_MB < cutoff_size_MB:
                    continue
                log.warning('size of log-like file %s is too large (%s MB), will be deleted',fullpath,size_MB)
            os.remove(fullpath)

def cleanup(ctx):
    workdir = ctx['workdir']
    log.info('cleaning up workdir: %s',workdir)

    quarantine_base = os.environ.get('WFLOW_QUARANTINE_DIR','/tmp/wflow_quarantine')
    rescuedir = '{}/{}'.format(quarantine_base,ctx['jobguid'])
    log.info('log files will be in %s',rescuedir)
    try:
        if os.path.isdir(workdir):
            delete_all_but_log(workdir)
            shutil.move(workdir,rescuedir)
    except:
        #this is again pretty harsh, but we really want to make sure the workdir is gone
        if os.path.isdir(workdir):
            shutil.rmtree(workdir)
        log.exception('Error in cleanup function for jobid %s, the directory is gone.', ctx['jobguid'])
        raise RuntimeError('Error in cleanup, ')
    assert not os.path.isdir(workdir)

from contextlib import contextmanager

def acquire_context(jobguid):
    log.info('running analysis on worker: %s %s',socket.gethostname(),os.environ.get('WFLOW_DOCKERHOST',''))

    wflow_server = os.environ.get('WFLOW_SERVER')
    log.info('acquiring wflow context from %s', wflow_server)

    ctx = requests.get('{}/workflow_config'.format(wflow_server),
        data = json.dumps({'workflow_ids': [jobguid]}),
        headers = {'Content-Type': 'application/json'}
    ).json()['configs'][0]

    jobguid = ctx['jobguid']
    ctx['workdir'] = 'workdirs/{}'.format('/'.join([jobguid[i:i+2] for i in range(0,8,2)]) + jobguid[8:])
    return ctx

@contextmanager
def wflow_context(setupfunc,onsuccess,teardownfunc,jobguid,redislogging = True):
    try:
        setupLogging(jobguid, add_redis = redislogging)
        log.info('running analysis on worker: %s %s',socket.gethostname(),os.environ.get('WFLOW_DOCKERHOST',''))

        ctx = acquire_context(jobguid)
        setupfunc(ctx)
        yield ctx
        log.info('back from entry point run onsuccess')
        onsuccess(ctx)
    except:
        log.exception('something went wrong :(!')
        #re-raise exception
        raise
    finally:
        log.info('''it's a wrap for job %s! cleaning up.''',jobguid)
        teardownfunc(ctx)
