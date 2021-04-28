from datetime import datetime as dt
from ingest_api_common import query_unique_row

def update_lev0_parameters(parsedParams, reingest, config, conn, dbUser='koa_test'):
    '''For ingesttype=lev0, verify can continue, then update the database.'''


    #  check if unique
    result, parsedParams = query_unique_row(parsedParams, conn, dbUser, 0)
    if len(result) != 1:
        return parsedParams
    result = result[0]
    #  verify that status is TRANSFERRED, ERROR or COMPLETE
    if result['status'] not in config['VALID_DB_STATUS_VALUES']:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append(f"current status ({result['status']}) does not allow request")
        return parsedParams

    #  check if reingest (type string)
    if str(reingest).upper() == 'FALSE' and result['ipac_response_time']:
        parsedParams['apiStatus'] = 'ERROR'
        parsedParams['ingestErrors'].append('ipac_response_time already exists')
        return parsedParams

    _, parsedParams = update_db_data(parsedParams, conn, dbUser)

    return parsedParams
