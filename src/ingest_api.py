from flask import request, jsonify
from datetime import datetime as dt
import pdb
from db_conn import db_conn
from functools import wraps

class DateParseException(Exception):
    pass

def try_assert(method):
    @wraps(method)
    def tryer(*args, **kw):
        err = None
        try:
            result = (method(*args, **kw), None)
        except (AssertionError, DateParseException) as err:
            result = (None, err)
        except Exception as err:
            result = (None, err)
        return result
    return tryer
INST_SET_ABBR = {'DE', 'DF', 'EI', 'HI', 'KB', 'KF', 'LB', 'LR', 'MF', 'N2', 'NI', 'NR', 'NC', 'NS', 'OI', 'OS'}
INST_SET = set('DEIMOS, ESI, HIRES, KCWI, LRIS, MOSFIRE, OSIRIS, NIRC2, NIRES, NIRSPEC'.split(', '))
INST_MAPPING = { 
                 'DEIMOS': {'DE', 'DF'},
                 'ESI': {'EI'},
                 'HIRES': {'HI'},
                 'KCWI': {'KB', 'KF'}, 
                 'LRIS': {'LB', 'LR'},
                 'MOSFIRE': {'MF'},
                 'OSIRIS': {'OI', 'OS'},
                 'NIRES': {'NR', 'NI', 'NS'},
                 'NIRC2': {'N2', 'NC'},
                }
VALID_DB_STATUS_VALUES = {'TRANSFERRED', 'ERROR', 'COMPLETE'} 
STATUS_SET = {'COMPLETE', 'DONE', 'ERROR'}
VALID_BOOL = {'TRUE', '1', 'YES', 'FALSE', '0', 'NO'}
# For now only allowing lev0
#INGEST_TYPES = {'lev0', 'lev1', 'lev2', 'try', 'psfr'}
INGEST_TYPES = {'lev0'}
REQUIRED_PARAMS = {'inst', 'ingesttype', 'koaid', 'status'}


remove_whitespace_and_make_uppercase = lambda s: ''.join(s.split()).upper()
remove_whitespace_and_make_lowercase = lambda s: ''.join(s.split()).lower()
is_blank_msg = lambda s: f'{s} is blank'
not_in_set_msg = lambda s, st: f'{s} not found in set {st}'

def get_inst_long_name(instAbbr):
    return [key for (key, vals) in INST_MAPPING.items() if instAbbr in vals][0]  # get long name of instrument

def assert_is_blank(param):
    assert len(param) > 0, is_blank_msg(param)
	
def assert_in_set(param, paramSet):
    assert param in paramSet, not_in_set_msg(param, paramSet)

def parse_status(status):
    '''
    removes all whitespace and set to upper. Resulting string must be contained in STATUS_SET
    checks if status is acceptible as stated in  
    https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    '''
    status = remove_whitespace_and_make_uppercase(status)
    assert_is_blank(status)
    assert_in_set(status, STATUS_SET)
    if status == 'DONE': status = 'COMPLETE'
    return status

def parse_inst(inst):
    '''removes all whitespace and set to upper. Resulting string must be found in INST_SET'''
    inst = remove_whitespace_and_make_uppercase(inst)
    assert_is_blank(inst)
    assert_in_set(inst, INST_SET)
    return inst  

def parse_reingest(reingest):
    '''remove whitespace and check if valid'''
    reingest = remove_whitespace_and_make_uppercase(reingest)
    assert_is_blank(reingest)
    assert_in_set(reingest, VALID_BOOL)
    return reingest 

def parse_testonly(testonly):
    '''remove whitespace and check if valid'''
    testonly = remove_whitespace_and_make_uppercase(testonly)
    assert_is_blank(testonly)
    assert_in_set(testonly, VALID_BOOL)
    return testonly

def parse_ingesttype(ingesttype):
    '''remove whitespace and set to lowercase. Result should be in INGEST_TYPES set'''
    ingesttype = remove_whitespace_and_make_lowercase(ingesttype)
    assert_is_blank(ingesttype)
    assert_in_set(ingesttype, INGEST_TYPES)
    return ingesttype

def parse_utdate(utdate, format='%Y-%m-%d'):
    try:
        t = dt.strptime(utdate, format)
    except:
        raise DateParseException(f'date {utdate} not valid. Is the format {format}?')
    return utdate

def parse_koaid(koaid):
    '''
    koaid is run through assertions to check that it fits koaid format II.YYYYMMDD.SSSSS.SS.fits as described in 
    https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    '''
    inst, utdate, seconds, dec, ftype = koaid.split('.')
    assert_in_set(inst, INST_SET_ABBR)
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


def query_unique_row(parsedParams, conn, dbUser):
    koaid = parsedParams['koaid']
    instrument = parsedParams['inst']

    #  check if unique
    query = f"select * from dep_status where instrument='{instrument}' and koaid='{koaid}'"
    print('query'.center(50,'='))
    print(query)
    result = conn.query(dbUser, query)
    print('result'.center(50, '='))
    print(result)
    #  This assert returns a null result
    if len(result) != 1:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('koaid is missing or should be unique')
    else:
        result = result[0]
    return result, parsedParams

def update_ipac_response_time(parsedParams, conn, dbUser, defaultMsg=''):
    koaid = parsedParams['koaid']
    now = dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(parsedParams['status'])
    updateQuery = f"update dep_status set ipac_response_time='{now}'"
    updateQuery = f"{updateQuery}, status='{parsedParams['status']}'"
    msg = defaultMsg if  parsedParams['status'] == 'COMPLETE' else parsedParams['message']
    updateQuery = f"{updateQuery}, status_code='{msg}' where koaid='{koaid}'"
    print('query'.center(50, '='))
    print(updateQuery)
    result = conn.query(dbUser, updateQuery)
    print('result'.center(50, '='))
    print(result)
    if result != 1:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('error updating ipac_response_time')
#    parsedParams['dbStatus'] = result.get('status', 'no db status key in result')
#    parsedParams['dbStatusCode'] = result.get('status_code', 'no db status code in result')
    return result, parsedParams

def update_lev_parameters(parsedParams, reingest, conn, dbUser='koa_test'):

    #  check if unique
    result, parsedParams = query_unique_row(parsedParams, conn, dbUser)
    if len(result) != 1:
        return parsedParams

    #  verify that status is TRANSFERRED, ERROR or COMPLETE
    if result['status'] not in VALID_DB_STATUS_VALUES:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append(f"current status ({result['status']}) does not allow request")
        return parsedParams

    #  check if reingest (type string)
    if str(reingest).upper() == 'FALSE' and result['ipac_response_time']:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('ipac_response_time already exists')
        return parsedParams

    _, parsedParams = update_ipac_response_time(parsedParams, conn, dbUser)

    return parsedParams

@try_assert
def parse_query_param(key, value):
    SWITCHER = {
        "inst": parse_inst,
        "utdate": parse_utdate,
        "koaid": parse_koaid,
        "status": parse_status,
        "message": parse_message,
        "reingest": parse_reingest,
        "testonly": parse_testonly,
        "ingesttype": parse_ingesttype,
        }
    key = ''.join(key.split()).lower()
    # Get the function from switcher dictionary
    func = SWITCHER.get(key, lambda x: f"Invalid param {key} has value {x}")
    assert not 'Invalid param' in func(value), f'{func(value)}'
    return func(value)

def validate_ingest(parsedParams):

    if parsedParams.get('status') == 'ERROR' and not parsedParams.get('message', False):
        parsedParams['ingestErrors'].append('status==ERROR should include a message')
    if not len(parsedParams) > 0:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('params is empty')
    includesReqParams = all((req in parsedParams.keys() for req in REQUIRED_PARAMS))
    if not includesReqParams:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('required params not included')
    #  check if koaid inst portion matches inst

    koaid = parsedParams.get('koaid', False)
    if koaid:
        inst = get_inst_long_name(koaid.split('.')[0]) 
        if not inst == parsedParams.get('inst'):
            parsedParams['ingestErrors'].append(f'check that koaid {koaid} matches inst {inst}')

    if len(parsedParams['ingestErrors']) == 0:
        parsedParams['apiStatus'] = 'COMPLETE'
    else:
        parsedParams['apiStatus'] = 'ERROR'
    return parsedParams

def parse_params(reqDict):
    parsedParams = dict()
    parsedParams['timestamp'] = dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    parsedParams['ingestErrors'] = []
    for key, value in reqDict.items():
        if len(key) == '':
            iparsedParams['ingestErrors'].append('key should not be blank')
            continue
        if value:
            parsedParams[key], err = parse_query_param(key, value)
            if err: parsedParams['ingestErrors'].append(str(err))
        else:
            parsedParams['ingestErrors'].append(f'{key} is blank')
    parsedParams = validate_ingest(parsedParams)
    return parsedParams

def ingest_api_get():
    print(f'type: {type(request.args)} keys {request.args.keys()}')
    reqDict = request.args.to_dict()
    parsedParams = parse_params(reqDict)
    reingest = parsedParams.get('reingest', False)
    testonly = parsedParams.get('testonly', False)
    if parsedParams['apiStatus'] != 'ERROR' and not testonly:
        #  create database object
        conn = db_conn('./config.live.ini')
        parsedParams = update_lev_parameters(parsedParams, reingest, conn)
    return jsonify(parsedParams)
