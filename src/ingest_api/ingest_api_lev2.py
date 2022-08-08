from os import listdir, walk
from os.path import isdir
from datetime import datetime as dt
from ingest_api.ingest_api_common import *
from ingest_api.ingest_api_lev1 import update_lev1_parameters

def update_lev2_parameters(parsedParams, reingest, config, conn, dbUser='koa_test'):
    '''
    For ingesttype=lev2, verify can continue, then
        1. update the database with new level 2 KOAID entry (triggered from DRP) or
        2. update all WAITING entries to koa_status to QUEUED (triggered from DRP) or
        3. update the database with completion status(triggered from IPAC)

    DRP calls
        1. instrument, ingesttype, koaid, datadir
        2. instrument, ingesttype, utdate

    IPAC calls
        3. instrument, ingesttype, utdate, status, message, metrics...
    '''

    instrument = parsedParams['instrument']
    level = parsedParams['ingesttype'].replace('lev','')
    reingest = str(reingest).upper()

    # koaid and utdate will only exist for some of the queries, default to None
    try:
        koaid = parsedParams['koaid']
    except:
        koaid = None
    try:
        utdate = parsedParams['utdate']
    except:
        utdate = None

    # If KOAID exists, then this will do the same as level 1 calls, use that function
    if koaid != None:
        # This data is processed as a whole, add a WAITING database entry and
        # wait for the final call
        if instrument == 'OSIRIS':
            return update_lev1_parameters(parsedParams, reingest, config, conn, dbUser, 'QUEUED')
        else:
            return update_lev1_parameters(parsedParams, reingest, config, conn, dbUser, 'WAITING')

    # metrics will exist for calls from IPAC
    if 'metrics' in parsedParams.keys():
        # Loop through KOAIDs and mark all with the status provided
        koaidList = query_all_koaid(conn, dbUser, instrument, level, utdate)
        for entry in koaidList:
            koaid = entry['koaid']
            parsedParams['koaid'] = koaid
            # Check if unique
            result, parsedParams = query_unique_row(parsedParams, conn, dbUser, level)
            if len(result) == 1:
                result = result[0]
                # check status for lev1/2
                if result['status'] not in config['VALID_DB_STATUS_VALUES']:
                    parsedParams['apiStatus'] = 'ERROR'
                    parsedParams['ingestErrors'].append(f"{koaid} current status ({result['status']}) does not allow request")
                # check reingest
                elif reingest == 'FALSE' and result['ipac_response_time']:
                    parsedParams['apiStatus'] = 'ERROR'
                    parsedParams['ingestErrors'].append(f'{koaid} ipac_response_time already exists')

                else:
                    # do database update
                    _, parsedParams = update_db_data(parsedParams, config, conn, dbUser)
    # DRP completion
    else:
        # For DEIMOS we will get a datadir with multiple KOAIDs
        # Need to add a database entry for each
        # This may work for other instruments with slitmasks as well
        if instrument == 'DEIMOS':
            # Check datadir
            processDir = 'error'
            if 'datadir' in parsedParams.keys():
                processDir = parsedParams['datadir']
            if not isdir(processDir):
                parsedParams['apiStatus'] = 'ERROR'
                parsedParams['ingestErrors'].append(f'datadir ({processDir}) does not exist')
            else:
                # Get list of KOAIDs and add to queue
                koaidList = []
                for rootdir, dirs, files in walk(processDir):
                    for file in files:
                        if file.endswith('.fits'):
                            koaidList.append(file)
                found = []
                for koaid in koaidList:
                    for k in koaid.split('_'):
                        if k.startswith('DE.'):
                            parsedParams['koaid'] = k[:20]
                            if k[:20] not in found:
                                lev1Params = update_lev1_parameters(parsedParams, reingest, config, conn, dbUser, 'QUEUED')
                                if lev1Params['apiStatus'] == 'COMPLETE':
                                    found.append(k[:20])
                # Construct message with each koaid
                if len(found) > 0:
                    parsedParams['koaid'] = ''
                    msg = ''
                    for k in found:
                        msg = f'{msg} {k}'
                    parsedParams['statusMessage'] = f'{msg.strip()} added to DRP archiving queue'

            return parsedParams

        # Change all WAITING koa_status entries to QUEUED
        query = "update koa_status set status='QUEUED'"
        query = f"{query} where instrument='{instrument}' and level=2"
        query = f"{query} and status='WAITING' and koaid like '%{utdate.replace('-', '')}%'"

        result = conn.query(dbUser, query)
        if result is False:
            parsedParams['apiStatus'] = 'ERROR'
            parsedParams['ingestErrors'].append(f'Database query error')
        elif result < 1:
            parsedParams['apiStatus'] = 'ERROR'
            parsedParams['ingestErrors'].append('error updating status entries')

    return parsedParams
