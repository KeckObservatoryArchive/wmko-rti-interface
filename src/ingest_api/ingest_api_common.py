from functools import wraps
from datetime import datetime as dt

class DateParseException(Exception):
    pass

class ParameterException(Exception):
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

def query_unique_row(parsedParams, conn, dbUser, level=0):
    '''Return result and status of database query for instrument/koaid.'''

    koaid = parsedParams['koaid']
    instrument = parsedParams['instrument']
    #  check if unique
    query = f"select * from koa_status where instrument='{instrument}' and koaid='{koaid}' and level={level}"
    result = conn.query(dbUser, query)
#    print("LEN", len(result), result, "Q", query)
    #  This assert returns a null result
    if result is False:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append(f'Database query error')
    elif len(result) != 1:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append(f'lev{level} koaid is missing or should be unique')
    return result, parsedParams

def update_db_data(parsedParams, config, conn, dbUser, defaultMsg=''):
    '''Update the database for ingesttype=lev0'''

    koaid = parsedParams['koaid']
    level = parsedParams['ingesttype'].replace('lev','')
    now = dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    updateQuery = f"update koa_status set"
    for key in config['METRICS_PARAMS']:
        if parsedParams['metrics'][key] == '':
            continue
        updateQuery = f"{updateQuery} {key}='{parsedParams['metrics'][key]}',"
    updateQuery = f"{updateQuery} ipac_response_time='{now}',"
    updateQuery = f"{updateQuery} status='{parsedParams['status']}'"
    msg = defaultMsg if parsedParams['status'] == 'COMPLETE' else parsedParams['ingest_error']
    if msg != None: updateQuery = f"{updateQuery}, status_code_ipac='{msg}'"
    updateQuery = f"{updateQuery} where koaid='{koaid}' and level={level}"
    result = conn.query(dbUser, updateQuery)
    if result is False:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append(f'Database query error')
    elif result != 1:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('error updating ipac_response_time')

    return result, parsedParams

def query_all_koaid(conn, dbUser, instrument, level, utdate):
    '''Return all koaids for level and utdate.'''

    #  check if unique
    query = ' '.join((
        f"select koaid from koa_status where instrument='{instrument}'",
        f"and level={level} and koaid like '%{utdate.replace('-', '')}%'"
    ))
    result = conn.query(dbUser, query)

    return result
