from flask import request, jsonify
from datetime import datetime
import pdb

class DateParseException(Exception):
    pass

INST_SET = {'DE', 'DF', 'EI', 'HI', 'KB', 'KF', 'LB', 'LR', 'MF', 'N2', 'NI', 'NR', 'NC', 'NS', 'OI', 'OS'}
# STATUS_SET = {'QUEUED', 'PROCESSING', 'COMPLETE', 'INVALID', 'EMPTY_FILE', 'DUPLICATE_FILE', 'ERROR'}
STATUS_SET = {'DONE', 'ERROR'}
VALID_BOOL = {'TRUE', '1', 'YES', 'FALSE', '0', 'NO'}
INGEST_TYPES = {'lev0', 'lev1', 'lev2', 'try', 'psfr'}


remove_whitespace_and_make_uppercase = lambda s: ''.join(s.split()).upper()
remove_whitespace_and_make_lowercase = lambda s: ''.join(s.split()).lower()
is_blank_msg = lambda s: f'{s} is blank'
not_in_set_msg = lambda s, st: f'{s} not found in set {st}'

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

def parse_utdate(utdate):
    try:
        t = datetime.strptime(utdate, '%Y-%m-%d')
    except:
        raise DateParseException('date not valid. Is the format YYYY-MM-DD?')
    return utdate

def parse_koaid(koaid):
    '''
    koaid is run through assertions to check that it fits koaid format II.YYYYMMDD.SSSSS.SS.fits as described in 
    https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    '''
    inst, date, seconds, dec, ftype = koaid.split('.')
    assert inst in INST_SET, 'instrument not valid'
    try:
        t = datetime.strptime(date, '%Y%m%d')
    except:
        raise DateParseException('date not valid. Is the format YYYYMMDD?')
    assert len(seconds) == 5, 'check seconds length'
    assert seconds.isdigit(), 'check if seconds is positive integer'
    assert len(dec) == 2, 'check decimal length'
    assert dec.isdigit(), 'check if decimal is positive integer'
    uttime = ''.join([seconds, dec])
    assert float(uttime) < 86400, 'seconds exceed day'
    assert fit == '.fits', 'check file type'
    return koaid

def parse_message(msg):
    return msg

def parse_query_param(key, value):
    SWITCHER = {
        "inst": parse_status,
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
    func = SWITCHER.get(key, lambda x: f"Invalid key {x}")
    return func(value)

def ingest_api_fun():
    print(f'type: {type(request.args)} keys {request.args.keys()}')
    reqDict = request.args.to_dict()
    parsedParams = dict()
    for key, value in reqDict.items():
        parsedParams[key] = parse_query_param(key, value)
    return jsonify(parsedParams)