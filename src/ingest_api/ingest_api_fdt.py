from datetime import datetime as dt
from db_conn import db_conn
from flask import request, jsonify
import logging
import os
import sys
import yaml
import ingest_api.ingest_api as ingest_api
from ingest_api.ingest_api_common import *
from ingest_api.ingest_api_lev0 import update_lev0_parameters
from ingest_api.ingest_api_lev1 import update_lev1_parameters
from ingest_api.ingest_api_lev2 import update_lev2_parameters

# Load config in global space (NOTE: Need chdir b/c not running in dir)
os.chdir(sys.path[0])
with open("config_ingest_api.ini") as f: CONFIG = yaml.safe_load(f)
CONFIG = CONFIG["ingest_api"]

# Get the logger
log = logging.getLogger("wmko_rti_api")

def log_and_close_db(message, conn):
    log.info(message)
    if conn:
        conn.close()

def ingest_api_get_fdt():
    """API entry point from koa_rti_main.ingest_api route."""

    # Let's only do lev0 for now
    funcs = {
        "lev0":update_lev0_parameters, 
#        "lev1":update_lev1_parameters,
#        "lev2":update_lev2_parameters
    }

    parse_funcs = {
        "reingest": ingest_api.parse_reingest,
        "testonly": ingest_api.parse_testonly,
        "instrument": ingest_api.parse_inst,
        "ingesttype": ingest_api.parse_ingesttype,
    }

    dbname = 'koa'
    log.info(f'ingest_api_get_fdt: using database {dbname}')
    conn = None #db_conn("./config.live.ini")

    reqDict = request.get_json()

    log.info(f"ingest_api_get: input parameters - {reqDict}")

    # Verify parameters
    for key in parse_funcs.keys():
        try:
            value = reqDict.get(key, "false")
            reqDict[key] = parse_funcs[key](value)
        except ValueError as e:
            msg = f"ingest_api_get_fdt: invalid {key} ({reqDict.get(key, '')}) - {e}"
            log_and_close_db(msg, conn)
            return {"apiStatus":"ERROR", "message":msg}
        
    # Verify koaid is a dictionary
    koaid_status = {}
    koaid = reqDict.get("koaid", {})

    if not isinstance(koaid, dict):
        msg = f"ingest_api_get_fdt: koaid is not a dictionary"
        log_and_close_db(msg, conn)
        return {"apiStatus":"ERROR", "message":msg}
    
    # Redefine parse_funcs
    parse_funcs = {
        "koaid": ingest_api.parse_koaid,
        "status": ingest_api.parse_status,
        "metrics": ingest_api.parse_metrics,
    }

    # Loop through the dictionary and do the updates
    for kid, data in koaid.items():
        koaid_status[kid] = {"apiStatus":"COMPLETE", "message":""}
        data["koaid"] = kid

        # Verify parameters
        for key in parse_funcs.keys():
            try:
                value = data.get(key, "false")
                print(key, value)
                data[key] = parse_funcs[key](value)
            except ParameterException as e:
                msg = f"ingest_api_get_fdt: invalid/{key} {kid} ({data}) - {e}"
                koaid_status[kid] = {"apiStatus":"ERROR", "message":msg}
                log.info(msg)
                continue

        # Verify that tarfile exists in the fdt_packages table
        tarfile = reqDict.get("tarfile", "")
        if verify_tarfile_exists(tarfile, conn) == False:
            log.info(f"ingest_api_get_fdt: tarfile {tarfile} does not exist in fdt_packages")
            continue

        # Verify that KOAID exists in the koa_status and fdt_observations tables

        # Send the request to update koa_status for this koaid
        parsedParams = {
            "instrument": reqDict.get("instrument"),
            "ingesttype": reqDict.get("ingesttype"),
            "koaid": data.get("kid"),
            "status": data.get("status"),
            "metrics": data.get("metrics")
        }
        log.info(f"ingest_api_get_fdt: updating koa_status for {kid}")
        reingest = reqDict.get("reingest")
        
        # Send the request to update koa_status for this koaid
        #parsedParams = funcs[parsedParams["ingesttype"]](parsedParams, reingest, CONFIG, conn, dbUser=dbname)
        #koaid_status[kid] = "ERROR" if parsedParams["apiStatus"] == "ERROR" \
        #    else "SUCCESS"

        # Send the request to update fdt_observations for this koaid
        log.info(f"ingest_api_get_fdt: updating fdt_observations for {kid}")
        update_fdt_observations(parsedParams, reingest, CONFIG, conn)

        # Send the request to update fdt_packages for this tarfile
        log.info(f"ingest_api_get_fdt: updating fdt_packages for {tarfile}")
        update_fdt_packages(parsedParams, reingest, CONFIG, conn)

    # Update the FDT database tables
    # Update the status of the tar archive based on status of all KOAID entries
    status = [i for i in koaid_status.values() if i == "ERROR"]
    hasError = False if len(status) == 0 else True

    log_and_close_db("ingest_api_get_fdt: complete", conn)

    return jsonify(koaid_status)

def verify_tarfile_exists(tarfile, conn):
    return True
    query = "SELECT * FROM fdt_packages WHERE filename = %s"
    result = conn.cursor.execute(query, (tarfile,))
    msg = ""
    if len(result) != 1:
        msg = f"ingest_api_get_fdt: tarfile {tarfile} does not exist in fdt_packages"
    elif result[0]["status"] not in ["COMPLETE", "TRANSFERRED"]:
        msg = f"ingest_api_get_fdt: tarfile {tarfile} is not COMPLETE/TRANSFERRED"
    if msg:
        log_and_close_db(msg, conn)
        return {"apiStatus":"ERROR", "message":msg}

def update_fdt_observations(parsedParams, reingest, config, conn):
    """For ingesttype=lev0, verify can continue, then update the database."""

    return True
    #  check if unique
    query = "SELECT * FROM fdt_observations WHERE koaid = %s"
    result = conn.cursor.execute(query, (parsedParams["koaid"],))
    print(result)

    if result == False or len(result) != 1:
        return False
#    result = result[0]
#    #  verify that status is TRANSFERRED, ERROR or COMPLETE
#    if result['status'] not in config['VALID_DB_STATUS_VALUES']:
#        parsedParams['apiStatus'] = 'ERROR'
#        parsedParams['ingestErrors'].append(f"current status ({result['status']}) does not allow request")
#        return parsedParams

    #  check if reingest (type string)
#    if str(reingest).upper() == 'FALSE' and result['ipac_response_time']:
#        parsedParams['apiStatus'] = 'ERROR'
#        parsedParams['ingestErrors'].append('ipac_response_time already exists')
#        return parsedParams

#    _, parsedParams = update_db_data(parsedParams, config, conn, dbUser)

    return True

def update_fdt_packages(parsedParams, reingest, config, conn):
    return True




# fdt_observations
# +----------------------+-----------------------------------------------------------------------------------+------+-----+-------------------+-----------------------------------------------+
# | Field                | Type                                                                              | Null | Key | Default           | Extra                                         |
# +----------------------+-----------------------------------------------------------------------------------+------+-----+-------------------+-----------------------------------------------+
# | obsid                | bigint                                                                            | NO   | PRI | NULL              | auto_increment                                |
# | koaid                | varchar(48)                                                                       | NO   | MUL | NULL              |                                               |
# | instrument           | varchar(255)                                                                      | NO   |     | NULL              |                                               |
# | level                | int                                                                               | NO   |     | NULL              |                                               |
# | filepath             | varchar(255)                                                                      | NO   |     | NULL              |                                               |
# | filepath_replacement | varchar(255)                                                                      | YES  |     | NULL              |                                               |
# | inserted_time        | datetime                                                                          | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED                             |
# | pkg_id               | bigint                                                                            | YES  |     | NULL              |                                               |
# | pkg_start_time       | datetime                                                                          | YES  |     | NULL              |                                               |
# | pkg_end_time         | datetime                                                                          | YES  |     | NULL              |                                               |
# | status               | enum('PENDING','PACKAGING','PACKAGED','IGNORE','TRANSFERRING','COMPLETE','ERROR') | NO   |     | NULL              |                                               |
# | last_mod             | datetime                                                                          | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED on update CURRENT_TIMESTAMP |
# +----------------------+-----------------------------------------------------------------------------------+------+-----+-------------------+-----------------------------------------------+

# +-------+----------------------------+------------+-------+---------------------------------------------------------------+----------------------+---------------------+--------+---------------------+---------------------+----------+---------------------+
# | obsid | koaid                      | instrument | level | filepath                                                      | filepath_replacement | inserted_time       | pkg_id | pkg_start_time      | pkg_end_time        | status   | last_mod            |
# +-------+----------------------------+------------+-------+---------------------------------------------------------------+----------------------+---------------------+--------+---------------------+---------------------+----------+---------------------+
# |   219 | SI.20260603.86362.13       | SCALES     |     0 | /koadata/SCALES/20260603/lev0/SI.20260603.86362.13.fits       | NULL                 | 2026-07-13 11:32:09 |     11 | 2026-07-14 10:55:15 | 2026-07-14 10:55:16 | PACKAGED | 2026-07-14 10:55:16 |
# |   220 | SI.20260603.86362.13_qramp | SCALES     |     0 | /koadata/SCALES/20260603/lev0/SI.20260603.86362.13_qramp.fits | NULL                 | 2026-07-13 11:32:09 |     11 | 2026-07-14 10:55:16 | 2026-07-14 10:55:16 | PACKAGED | 2026-07-14 10:55:16 |
# |   218 | SI.20260603.86298.13_qramp | SCALES     |     0 | /koadata/SCALES/20260603/lev0/SI.20260603.86298.13_qramp.fits | NULL                 | 2026-07-13 11:32:09 |     11 | 2026-07-14 10:55:14 | 2026-07-14 10:55:15 | PACKAGED | 2026-07-14 10:55:15 |
# +-------+----------------------------+------------+-------+---------------------------------------------------------------+----------------------+---------------------+--------+---------------------+---------------------+----------+---------------------+


# fdt_packages
# +----------------+------------------------------------------------------------------------------------+------+-----+-------------------+-----------------------------------------------+
# | Field          | Type                                                                               | Null | Key | Default           | Extra                                         |
# +----------------+------------------------------------------------------------------------------------+------+-----+-------------------+-----------------------------------------------+
# | pkg_id         | bigint                                                                             | NO   | PRI | NULL              | auto_increment                                |
# | run_number     | bigint                                                                             | NO   |     | 1                 |                                               |
# | filename       | varchar(255)                                                                       | NO   |     | NULL              |                                               |
# | filepath       | varchar(255)                                                                       | NO   |     | NULL              |                                               |
# | instrument     | varchar(255)                                                                       | NO   |     | NULL              |                                               |
# | level          | int                                                                                | NO   |     | NULL              |                                               |
# | status         | enum('OPEN','CLOSED','TRANSFERRING','COMPLETE','ERROR','CLOSE_REQUESTED','IGNORE') | NO   |     | NULL              |                                               |
# | xfr_pid        | bigint                                                                             | YES  |     | NULL              |                                               |
# | creation_time  | datetime                                                                           | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED                             |
# | closed_time    | datetime                                                                           | YES  |     | NULL              |                                               |
# | xfr_start_time | datetime                                                                           | YES  |     | NULL              |                                               |
# | xfr_end_time   | datetime                                                                           | YES  |     | NULL              |                                               |
# | filesize_mb    | double                                                                             | NO   |     | 0                 |                                               |
# | koaid_count    | int                                                                                | NO   |     | 0                 |                                               |
# | source_deleted | tinyint(1)                                                                         | NO   |     | 0                 |                                               |
# | last_mod       | datetime                                                                           | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED on update CURRENT_TIMESTAMP |
# +----------------+------------------------------------------------------------------------------------+------+-----+-------------------+-----------------------------------------------+

# +--------+------------+-------------------------------------+-------------------------------+------------+-------+--------+---------+---------------------+---------------------+----------------+--------------+-------------+-------------+----------------+---------------------+
# | pkg_id | run_number | filename                            | filepath                      | instrument | level | status | xfr_pid | creation_time       | closed_time         | xfr_start_time | xfr_end_time | filesize_mb | koaid_count | source_deleted | last_mod            |
# +--------+------------+-------------------------------------+-------------------------------+------------+-------+--------+---------+---------------------+---------------------+----------------+--------------+-------------+-------------+----------------+---------------------+
# |     11 |          3 | SI.20260603.20333.76_lev0.tar       | /koadata/SCALES/tarfiles/lev0 | SCALES     |     0 | CLOSED |    NULL | 2026-07-14 10:52:15 | 2026-07-14 10:57:20 | NULL           | NULL         |    20876.66 |         220 |              0 | 2026-07-14 10:57:20 |
# |      9 |          2 | SI.20260603.20333.76_lev0.tar       | /koadata/SCALES/tarfiles/lev0 | SCALES     |     0 | CLOSED |    NULL | 2026-07-14 10:40:00 | 2026-07-14 10:45:00 | NULL           | NULL         |    29013.53 |         391 |              1 | 2026-07-14 10:57:20 |
# |     10 |          1 | SI.20260604.71296.39_qramp_lev0.tar | /koadata/SCALES/tarfiles/lev0 | SCALES     |     0 | CLOSED |    NULL | 2026-07-14 10:45:00 | 2026-07-14 10:50:04 | NULL           | NULL         |     5861.31 |         171 |              0 | 2026-07-14 10:50:04 |
# +--------+------------+-------------------------------------+-------------------------------+------------+-------+--------+---------+---------------------+---------------------+----------------+--------------+-------------+-------------+----------------+---------------------+
