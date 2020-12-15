from flask import request, jsonify
from datetime import datetime
import pdb

INST_SET = {'DE', 'DF', 'EI', 'HI', 'KB', 'KF', 'LB', 'LR', 'MF', 'N2', 'NI', 'NR', 'NC', 'NS', 'OI', 'OS'}
# STATUS_SET = {'QUEUED', 'PROCESSING', 'COMPLETE', 'INVALID', 'EMPTY_FILE', 'DUPLICATE_FILE', 'ERROR'}
STATUS_SET = {'DONE', 'ERROR'}
VALID_BOOL = {'true', '1', 'yes', 'false', '0', 'no'}
INGEST_TYPES = {'lev0', 'lev1', 'lev2', 'try', 'psfr'}

def parse_status(status):
    '''
    removes all whitespace and set to upper. Resulting string must be contained in STATUS_SET
    checks if status is acceptible as stated in  
    https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    '''
    status = ''.join(status.split()).upper()
    assert len(status) > 0, f'status is blank'
    assert status in STATUS_SET, f'{status} not in status set'
    return status

def parse_inst(inst):
    '''removes all whitespace and set to upper. Resulting string must be found in INST_SET'''
    inst = ''.join(inst.split()).upper()
    assert inst in INST_SET, 'instrument not found in set'
    return inst 

def parse_utdate(utdate):
    try:
        datetime = datetime.datetime.strptime(utdate, '%Y-%m-%d')
    except:
        raise Exception('date not valid. Is the format YYYY-MM-DD?')
    return utdate 

def parse_koaid(koaid):
    '''
    koaid is run through assertions to check that it fits koaid format II.YYYYMMDD.SSSSS.SS.fits as described in 
    https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    '''
    inst, date, seconds, dec, ftype = koaid.split('.')
    assert inst in INST_SET, 'instrument not valid'
    try:
        datetime = datetime.datetime.strptime(date, '%Y%m%d')
    except:
        raise Exception('date not valid. Is the format YYYYMMDD?')
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

def parse_reingest(reingest):
    '''remove whitespace and check if valid'''
    reinjest = ''.join(reinjest.split())
    assert reingest.lower() in VALID_BOOL, 'reingest not valid boolean'
    return reinjest 

def parse_dev(dev):
    '''remove whitespace and check if valid'''
    dev = ''.join(dev.split())
    assert dev.lower() in VALID_BOOL, 'dev not valid boolean'
    return dev

def parse_ingesttype(ingesttype):
    '''remove whitespace and set to lowercase. Result should be in INGEST_TYPES set'''
    ingesttype = ''.join(ingesttype.split())
    assert ingesttype in INGEST_TYPES, 'ingesttype not valid'
    return ingesttype

def parse_query_param(key, value):
    SWITCHER = {
        "inst": parse_status,
        "utdate": parse_utdate,
        "koaid": parse_koaid,
        "status": parse_status,
        "message": parse_message,
        "reingest": parse_reingest,
        "dev": parse_dev,
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