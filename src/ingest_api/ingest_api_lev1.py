from os.path import isdir
from datetime import datetime as dt
from ingest_api.ingest_api_common import *

def update_lev1_parameters(parsedParams, reingest, config, conn, dbUser='koa_test', status='QUEUED'):
    '''
    For ingesttype=lev1, verify can continue, then
        - start process to archive data products (triggered from DRP) or
        - update the database (triggered from IPAC)
    '''

    instrument = parsedParams['instrument']
    koaid = parsedParams['koaid']
    level = parsedParams['ingesttype'].replace('lev','')
    reingest = str(reingest).upper()

    # DRP triggering archiving
    if 'datadir' in parsedParams.keys():
        # Verify that datadir exists
        processDir = parsedParams['datadir']
#        assert isdir(processDir), f"{processDir} does not exist"

        # Verify lev0 entry exists
        result, parsedParams = query_unique_row(parsedParams, conn, dbUser, 0)
        if len(result) == 0:
            parsedParams['apiStatus'] = 'ERROR'
            return parsedParams

        # Verify entry not in DB already or reingest=true
        result, parsedParams = query_unique_row(parsedParams, conn, dbUser, level)
        if len(result) == 0 and reingest == 'TRUE':
            parsedParams['apiStatus'] = 'ERROR'
            parsedParams['ingestErrors'].append('no entry in database and reingest=true')
            return parsedParams
        if len(result) == 1 and reingest == 'FALSE':
            parsedParams['apiStatus'] = 'ERROR'
            parsedParams['ingestErrors'].append(f"{koaid} already archived, use reingest=true to replace")
            return parsedParams

        # New entry
        if len(result) == 0:
            # Add QUEUED or WAITING entry to koa_status
            now = dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            query = ("insert into koa_status set "
                    f"level={level},"
                    f"instrument='{instrument}',"
                    f"service='DRP',"
                    f"koaid='{koaid}',"
                    f"status='{status}',"
                    f"stage_file='{processDir}',"
                    f"creation_time='{now}'")
            result = conn.query(dbUser, query)
            if result != 1:
                parsedParams['apiStatus'] = 'ERROR'
                parsedParams['ingestErrors'].append('error adding to koa_status')
            parsedParams['apiStatus'] = 'COMPLETE'
            parsedParams['ingestErrors'] = []
        else:
            result = result[0]
            if result['status'] == 'COMPLETE' and reingest != 'TRUE':
                parsedParams['apiStatus'] = 'ERROR'
                parsedParams['ingestErrors'].append(f"{koaid} already archived, use reingest=true to replace")
                return parsedParams

            # Copy from koa_status to koa_status_history
            query = ("insert into koa_status_history "
                    f"select * from koa_status where "
                    f"level={level} and instrument='{instrument}' and "
                    f"service='DRP' and koaid='{koaid}'")
            result = conn.query(dbUser, query)

            # Add QUEUED or WAITING entry to koa_status
            now = dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            query = (f"update koa_status set status='{status}',")
            for key in config['BLANK']:
                query += f"{key}=null,"
            query += (f"process_dir='{processDir}',"
                      f"creation_time='{now}' where "
                      f"level={level} and instrument='{instrument}' and "
                      f"service='DRP' and koaid='{koaid}'")

            result = conn.query(dbUser, query)
            if result != 1:
                parsedParams['apiStatus'] = 'ERROR'
                parsedParams['ingestErrors'].append('error adding to koa_status')
                return parsedParams

        # Note whether or not things are good
        parsedParams['statusMessage'] = f"{koaid} added to DRP archiving queue"

    # Ingestion complete from IPAC
    else:
        #  check if unique
        result, parsedParams = query_unique_row(parsedParams, conn, dbUser, level)
        if len(result) != 1:
            return parsedParams
        result = result[0]

        # check status for lev1/2
        if result['status'] not in config['VALID_DB_STATUS_VALUES']:
            parsedParams['apiStatus'] = 'ERROR'
            parsedParams['ingestErrors'].append(f"current status ({result['status']}) does not allow request")
            return parsedParams

        # check reingest
        if reingest == 'FALSE' and result['ipac_response_time']:
            parsedParams['apiStatus'] = 'ERROR'
            parsedParams['ingestErrors'].append(f'ipac_response_time already exists')
            return parsedParams

        # do database update
        _, parsedParams = update_db_data(parsedParams, config, conn, dbUser)

    return parsedParams
