from flask import request, jsonify
from datetime import timedelta, datetime as dt
import pdb
from db_conn import db_conn
import json
import yaml
import os 
import sys
import smtplib
from email.mime.text import MIMEText

import logging
log = logging.getLogger('wmko_rti_api')

from ingest_api.ingest_api_common import *
from ingest_api.ingest_api_lev0 import update_lev0_parameters
from ingest_api.ingest_api_lev1 import update_lev1_parameters

from utils.koa_pi_notify import KoaPiNotify


#module globals
LAST_EMAIL_TIMES = None

#Load config in global space (NOTE: Need chdir b/c cwd is not correct unless running via run.csh)
os.chdir(sys.path[0])
with open('config.live.ini') as f: CONFIG = yaml.safe_load(f)
CONFIG = CONFIG['ingest_api']

remove_whitespace_and_make_uppercase = lambda s: ''.join(s.split()).upper()
remove_whitespace_and_make_lowercase = lambda s: ''.join(s.split()).lower()
is_blank_msg = lambda s: f'{s} is blank'
not_in_set_msg = lambda s, st: f'{s} not found in set {st}'

def get_inst_long_name(instAbbr):
    '''Returns instrument name from abbreviation (e.g. HI -> HIRES).'''

    return [key for (key, vals) in CONFIG['INST_MAPPING'].items() if instAbbr in vals][0]

def assert_is_blank(param):
    assert len(param) > 0, is_blank_msg(param)
	
def assert_in_set(param, paramSet):
    assert param in paramSet, not_in_set_msg(param, paramSet)

def parse_status(status):
    '''
       Removes all whitespace and set to uppercase.
       Resulting string must be contained in STATUS_SET.
    '''
    status = remove_whitespace_and_make_uppercase(status)
    assert_is_blank(status)
    assert_in_set(status, CONFIG['STATUS_SET'])
    if status == 'DONE': status = 'COMPLETE'
    return status

def parse_inst(inst):
    '''
       Removes all whitespace and set to upper. Resulting string must be
       found in INST_SET
    '''

    inst = remove_whitespace_and_make_uppercase(inst)
    assert_is_blank(inst)
    assert_in_set(inst, CONFIG['INST_SET'])
    return inst  

def parse_reingest(reingest):
    '''remove whitespace and check if valid'''

    reingest = remove_whitespace_and_make_uppercase(reingest)
    assert_is_blank(reingest)
    assert_in_set(reingest, CONFIG['VALID_BOOL'])
    return reingest 

def parse_testonly(testonly):
    '''remove whitespace and check if valid'''

    testonly = remove_whitespace_and_make_uppercase(testonly)
    assert_is_blank(testonly)
    assert_in_set(testonly, CONFIG['VALID_BOOL'])
    return testonly

def parse_ingesttype(ingesttype):
    '''remove whitespace and set to lowercase. Result should be in INGEST_TYPES set'''

    ingesttype = remove_whitespace_and_make_lowercase(ingesttype)
    assert_is_blank(ingesttype)
    assert_in_set(ingesttype, CONFIG['INGEST_TYPES'])
    return ingesttype

def parse_metrics(metrics):
    '''verifies JSON type'''

    assert_is_blank(metrics)
    try:
        metrics = json.loads(metrics)
    except:
        assert metrics == None, 'Cannot parse metrics value'
    # Verify contents of metrics
    for key in CONFIG['METRICS_PARAMS']:
        if 'copy' in key and metrics[key] == '': continue
        assert key in metrics.keys(), f'Missing metrics data - {key}'
        try:
            t = dt.strptime(metrics[key], '%Y-%m-%d %H:%M:%S')
        except:
            raise ParameterException(f'Incorrect format for metrics key {key}')
    return metrics

def parse_utdate(utdate, format='%Y-%m-%d'):
    '''Verify UT date has correct format.'''

    try:
        t = dt.strptime(utdate, format)
    except:
        raise DateParseException(f'date {utdate} not valid. Is the format {format}?')
    return utdate

def parse_koaid(koaid):
    '''koaid is run through assertions to check that it fits koaid format II.YYYYMMDD.SSSSS.SS.fits.'''

    try:
        inst, utdate, seconds, dec, ftype = koaid.split('.')
    except:
        err = f'KOAID: {koaid} must be five parts (.) separated, ' \
              f'ending with file type (ie .fits)'
        raise ParameterException(err)

    assert_in_set(inst, CONFIG['INST_SET_ABBR'])
    t = parse_utdate(utdate, format='%Y%m%d')
    assert len(seconds) == 5, 'check seconds length'
    assert seconds.isdigit(), 'check if seconds is positive integer'
    assert len(dec) == 2, 'check decimal length'
    assert dec.isdigit(), 'check if decimal is positive integer'
    uttime = ''.join([seconds, '.', dec])
    assert float(uttime) < 86400, 'seconds exceed day'
    assert ftype == 'fits', 'check file type'
    koaid = koaid.replace(".fits", "")

    return koaid

def parse_message(msg):
    return msg

@try_assert
def parse_query_param(key, value):
    '''Call function associated with input parameter key.'''
    
    SWITCHER = {
        "instrument": parse_inst,
        "utdate": parse_utdate,
        "koaid": parse_koaid,
        "status": parse_status,
        "message": parse_message,
        "reingest": parse_reingest,
        "testonly": parse_testonly,
        "ingesttype": parse_ingesttype,
        "ingest_error": parse_message,
        "metrics": parse_metrics,
        "start": parse_message,
        "dev": parse_message,
        "datadir": parse_message,
        }
    key = ''.join(key.split()).lower()
    # Get the function from switcher dictionary
    func = SWITCHER.get(key, lambda x: f"Invalid param {key} has value {x}")
    assert not 'Invalid param' in func(value), f'{func(value)}'
    return func(value)

def validate_ingest(parsedParams, reqParams):
    '''Verify input parameters.'''

    if parsedParams.get('status') == 'ERROR' and not parsedParams.get('ingest_error', False):
        parsedParams['ingestErrors'].append('status==ERROR should include a message')
    if not len(parsedParams) > 0:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('params is empty')
    includesReqParams = all((req in parsedParams.keys() for req in reqParams))
    if not includesReqParams:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('required params not included')
    #  check if koaid inst portion matches inst
    koaid = parsedParams.get('koaid', False)
    if koaid:
        inst = get_inst_long_name(koaid.split('.')[0]) 
        if not inst == parsedParams.get('instrument'):
            parsedParams['ingestErrors'].append(f'check that koaid {koaid} matches inst {inst}')

    if len(parsedParams['ingestErrors']) == 0:
        parsedParams['apiStatus'] = 'COMPLETE'
    else:
        parsedParams['apiStatus'] = 'ERROR'
    return parsedParams

def parse_params(reqDict):
    '''Parse and verify input paramaters.'''

    # Need to grab the appropriate required parameters for lev1/2 if not from NExScI
    reqParams = CONFIG['LEV0_REQUIRED_PARAMS'].copy()
    if 'ingesttype' in reqDict.keys():
        if reqDict['ingesttype'] in ['lev1', 'lev2'] and not 'status' in reqDict.keys():
            param = f"{reqDict['ingesttype'].upper()}_REQUIRED_PARAMS"
            reqParams = CONFIG[param].copy()

    parsedParams = dict()
    parsedParams['timestamp'] = dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    parsedParams['ingestErrors'] = []
    for key, value in reqDict.items():
        if len(key) == '':
            parsedParams['ingestErrors'].append('key should not be blank')
            continue
        if value:
            parsedParams[key], err = parse_query_param(key, value)
            if err: parsedParams['ingestErrors'].append(str(err))
        elif key in reqParams:
            parsedParams['ingestErrors'].append(f'{key} is blank')
    parsedParams = validate_ingest(parsedParams, reqParams)
    return parsedParams


def ingest_api_get():
    '''API entry point from koa_rti_main.ingest_api route.'''

    funcs = {
        "lev0":update_lev0_parameters, 
        "lev1":update_lev1_parameters,
        "lev2":update_lev1_parameters
    }
    reqDict = request.args.to_dict()
    parsedParams = parse_params(reqDict)
    reingest = parsedParams.get('reingest', 'False')
    testonly = parsedParams.get('testonly', 'False')
    log.info(f'ingest_api_get: input parameters - {reqDict}')
    log.info(f'ingest_api_get: parsed parameters - {parsedParams}')

    #API usage error
    if parsedParams['apiStatus'] == 'ERROR':
        notify_error("API_STATUS_ERROR", json.dumps(parsedParams, indent=4), parsedParams.get('instrument'))

    #Good API call then update DB
    elif parsedParams['apiStatus'] != 'ERROR' and testonly.lower != 'true':

        #IPAC reports an error
        if 'status' in parsedParams.keys() and parsedParams['status'] == 'ERROR':
            notify_error("IPAC_STATUS_ERROR", json.dumps(parsedParams, indent=4), parsedParams.get('instrument'))

        dbname = 'koa'
        log.info(f'ingest_api_get: using database {dbname}')

        # send request
        conn = db_conn('./config.live.ini')
        parsedParams = funcs[parsedParams['ingesttype']](parsedParams, reingest, CONFIG, conn, dbUser=dbname)
        log.info(f'ingest_api_get: returned parameters - {parsedParams}')

        #One more error check in case anything went wrong with db update
        if 'status' in parsedParams.keys() and parsedParams['apiStatus'] == 'ERROR':
            notify_error("DATABASE_ERROR", json.dumps(parsedParams, indent=4), parsedParams.get('instrument'))

        #ok successful ingest, so lets email the PI:
        else:
            notify_pi(parsedParams)

    return jsonify(parsedParams)


def notify_pi(parsedParams):

    koaid = parsedParams['koaid']
    instr = parsedParams['instrument']
    level = parsedParams['ingesttype']
    #todo: turning on dev until we are ready to make this feature live
    #dev = 1 if parsedParams.get('dev') == 'true' else 0
    dev = 1

    kpn = KoaPiNotify(koaid, instr, level, dev)
    res, msg = kpn.on_ingest()
    if res is False:
        log.error(msg)


def notify_error(errcode, text='', instr='', service='', check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''

    #always log/print
    log.error(f'{errcode}: {text}')

    #Only send if we haven't sent one of same errcode recently
    interval = CONFIG['EMAIL_INTERVAL_MINUTES']
    if check_time:
        global LAST_EMAIL_TIMES
        if not LAST_EMAIL_TIMES: LAST_EMAIL_TIMES = {}
        last_time = LAST_EMAIL_TIMES.get(errcode)
        now = dt.now()
        if last_time and last_time + timedelta(minutes=interval) > now:
            log.info(f"No error notification: last_time={last_time}, now={now}")
            return
        LAST_EMAIL_TIMES[errcode] = now

    #get admin email.  Return if none.
    # adminEmail = CONFIG['ADMIN_EMAIL']
    adminEmail = ''
    if not adminEmail: return
    
    # Construct email message
    body = f'{errcode}\n{text}'
    subj = f'KOA INGEST API ERROR: [{instr}] {errcode}'
    msg = MIMEText(body)
    msg['Subject'] = subj
    msg['To']      = adminEmail
    msg['From']    = adminEmail
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()
