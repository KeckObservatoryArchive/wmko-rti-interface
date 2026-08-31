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
    conn = db_conn("./config.live.ini")

    reqDict = request.get_json()

    log.info(f"ingest_api_get: input parameters - {reqDict}")

    # Verify that tarfile exists in the fdt_packages table
    tarfile = reqDict.get("tarfile", "")
    status, msg = verify_tarfile_exists(tarfile, conn, dbname)
    if status == False:
        log_and_close_db(msg, conn)
        return {"apiStatus":"ERROR", "message":msg}

    # This is just to let us know that the tarfile was received by NExScI
    if tarfile and not reqDict.get("koaid"):
        status = reqDict.get("status", "").upper()
        parsedParams = {"tarfile":tarfile, "status":status}
        update_fdt_packages(parsedParams, conn, dbname)
        msg = f"{tarfile} status at NExScI = {status}"
        log_and_close_db(f"ingest_api_get_fdt: {msg}", conn)
        return {"apiStatus":"COMPLETE", "message":f"{msg}"}

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
        kid = kid.replace(".fits", "")
        koaid_status[kid] = {"apiStatus":"COMPLETE", "message":""}
        data["koaid"] = kid

        # Verify parameters
        for key in parse_funcs.keys():
            try:
                value = data.get(key, "false")
                data[key] = parse_funcs[key](value)
            except ParameterException as e:
                msg = f"ingest_api_get_fdt: invalid/{key} {kid} ({data}) - {e}"
                koaid_status[kid] = {"apiStatus":"ERROR", "message":msg}
                log.info(msg)
                continue

        # Verify that KOAID exists in the koa_status and fdt_observations tables
        status, msg = verify_koaid_exists(kid, conn, dbname)
        if status == False:
            log_and_close_db(msg, conn)
            return {"apiStatus":"ERROR", "message":msg}
        tableIds = msg

        # Send the request to update koa_status for this koaid
        reingest = reqDict.get("reingest")
        parsedParams = {
            "instrument": reqDict.get("instrument"),
            "ingesttype": reqDict.get("ingesttype"),
            "reingest": reingest,
            "koaid": data.get("koaid"),
            "status": data.get("status"),
            "metrics": data.get("metrics"),
            "ingestErrors": [],
            "tarfile": reqDict.get("tarfile"),
        }
        log.info(f"ingest_api_get_fdt: updating koa_status for {kid}")
        
        # Send the request to update koa_status for this koaid
        # Use the previously implemented function for koa_status updates
        parsedParams = funcs[parsedParams["ingesttype"]](
                parsedParams, 
                reingest, 
                CONFIG, 
                conn, 
                dbUser=dbname
            )
        koaid_status[kid] = "ERROR" \
            if parsedParams.get("apiStatus", "") == "ERROR" \
            else "SUCCESS"

        # Send the request to update fdt_observations for this koaid
        log.info(f"ingest_api_get_fdt: updating fdt_observations for {kid}")
        status = update_fdt_observations(parsedParams, conn, dbname)
        koaid_status[kid] = "ERROR" if status == False else "SUCCESS"

    # Send the request to update fdt_packages for this tarfile
    status = "COMPLETE"
    log.info(f"ingest_api_get_fdt: updating fdt_packages for {tarfile} -- {status}")
    parsedParams["status"] = status
    update_fdt_packages(parsedParams, conn, dbname)
    koaid_status[tarfile] = status
    
    log.info(f"ingest_api_get_fdt: {koaid_status}")
    log_and_close_db("ingest_api_get_fdt: complete", conn)

    return jsonify(koaid_status)

def verify_tarfile_exists(tarfile, conn, dbname="koa_test"):
    """ Make sure that the tarfile name exists in fdt_packages """

    if tarfile == "":
        return False, f"ingest_api_get_fdt: no tarfile provided in request"

    status = ["RECEIVED", "TRANSFERRED"]

    # If rootname provided, add .tar extension
    if not tarfile.endswith(".tar"):
        tarfile += ".tar"

    query = "SELECT * FROM fdt_packages WHERE filename = %s"
    result = conn.query(dbname, query, values=(tarfile,))

    if len(result) == 0:
        return False, f"ingest_api_get_fdt: {tarfile} not in fdt_packages"
    if len(result) > 1:
        return False, f"ingest_api_get_fdt: multiple {tarfile} in fdt_packages"
    elif result[0]["status"] not in status:
        return False, f"ingest_api_get_fdt: {tarfile} is not {status}"

    return True, ""

def verify_koaid_exists(koaid, conn, dbname="koa_test"):
    """
    Make sure that the koaid exists in both koa_status and fdt_observations
    """

    status = ["TRANSFERRED", "COMPLETE", "ERROR", "FDT_READY", "PACKAGED"]

    ids = {"fdt_observations":"obsid", "koa_status":"id"}
    tableIds = {}

    koaid = koaid.replace(".fits", "")

    for table in ["fdt_observations", "koa_status"]:
        query = f"SELECT * FROM {table} WHERE koaid = %s"
        result = conn.query(dbname, query, values=(koaid,))

        if len(result) == 0:
            return False, f"ingest_api_get_fdt: {koaid} not in {table}"
        if len(result) > 1:
            return False, f"ingest_api_get_fdt: multiple {koaid} in {table}"
        elif result[0]["status"] not in status:
            return False, f"ingest_api_get_fdt: {koaid} in {table} is not {status}"
        tableIds[table] = result[0][ids[table]]

    return True, tableIds

def update_fdt_observations(parsedParams, conn, dbname="koa_test"):
    """ Change the status for this observation """

    query = "UPDATE fdt_observations SET status = %s WHERE koaid = %s"
    values = (parsedParams["status"], parsedParams["koaid"],)
    result = conn.query(dbname, query, values=values)

    if result == False:
        return False

    return True

def update_fdt_packages(parsedParams, conn, dbname="koa_test"):
    """ Change the status for this package """

    # If rootname provided, add .tar extension
    tarfile = parsedParams["tarfile"]
    if not tarfile.endswith(".tar"):
        tarfile += ".tar"
    
    query = "UPDATE fdt_packages SET status = %s WHERE filename = %s"
    values = (parsedParams["status"], tarfile,)
    result = conn.query(dbname, query, values=values)

    if result == False:
        return False

    return True

