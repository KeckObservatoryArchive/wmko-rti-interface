import calendar
import sys
import time
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta, date
from flask import Flask, render_template, request, send_from_directory, jsonify
from os import path, stat
from ingest_api import *
from koa_rti_api import KoaRtiApi
from koa_rti_db import DatabaseInteraction
from koa_rti_helpers import get_api_help_string, InstrumentReport
from koa_rti_helpers import year_range, replace_datetime
from koa_rti_plots import TimeBarPlot, OverlayTimePlot
from koa_tpx_gui import tpx_gui

import json
import argparse
import logging


APP_PATH = path.abspath(path.dirname(__file__))
TEMPLATE_PATH = path.join(APP_PATH, "templates/")
API_INSTANCE = None


def get_resource_as_string(name, charset='utf-8'):
    """ Initiliaze the flask APP"""
    with app.open_resource(name) as f:
        return f.read().decode(charset)


app = Flask(__name__, template_folder=TEMPLATE_PATH)
app.jinja_env.globals['get_resource_as_string'] = get_resource_as_string


@app.route("/ingest_api", methods=["GET"])
def ingest_api():
    log.info('ingest_api: starting api call')
    return ingest_api_get(log)


@app.route("/koarti_api", methods=['GET'])
def tpx_rti_api():
    global API_INSTANCE
    var_get = parse_request(default_utd=False)
    rti_api = KoaRtiApi(var_get)
    API_INSTANCE = rti_api

    results = api_results()
    if not results['data']:
        help_str = f"No Results for query parameters:<BR><BR> {var_get}<BR>"
        help_str += get_api_help_string()
        return help_str

    return json.dumps(results)


@app.route("/koarti", methods=['GET'])
def tpx_rti_page():
    """
    Main page used to display an updated table.

    :return: html rendered page
    """
    global API_INSTANCE
    var_get = parse_request()
    rti_api = KoaRtiApi(var_get)
    API_INSTANCE = rti_api

    db_columns = rti_api.getDbColumns()
    years = year_range()
    opt_lists = rti_api.getOptionLists()

    if var_get.page == 'health':
        results = InstrumentReport(rti_api.getInst()).results()
        page_name = "rti_health.html"
    elif var_get.page == 'stats':
        page_name = "rti_metrics.html"
        results = rti_api.getPlots()
    elif var_get.page in ['koatpx', 'koadrp']:
        page_name = "tpx_gui.html"
        results, db_columns = tpx_gui(var_get.page, API_INSTANCE)
    else:
        # results are loaded by the long-polling routine
        page_name = "rti_table.html"
        results = None

    return render_template(page_name, results=results, params=var_get,
                           inst_lists=rti_api.getInstruments(), yrs=years,
                           months=calendar.month_name, db_columns=db_columns,
                           opt_lists=opt_lists)


@app.route("/koarti/header/<header_val>", methods=['GET'])
def return_header(header_val):
    """
    Page to return the header keywords and values.

    :param header_val: (str) KOAID.

    :return: html rendered page
    """
    var_get = parse_request()
    rti_api = KoaRtiApi(var_get)

    header = rti_api.read_header(header_val)

    result_dict = {}
    for key, val in header.items():
        try:
            result_dict[key] = val['value']
        except:
            pass

    if not result_dict:
        result_dict[header_val] = "No Data"

    return render_template("rti_header.html", data=result_dict)


@app.route("/koarti/data-update")
def data_update():
    """
    Long-polling used to keep the webpage table up-to-date.  Page is connected
    via routines in poll.js.

    Returns 'data.txt' content when the resource has  changed after the last
    request time.
    """
    while not API_INSTANCE:
        time.sleep(0.1)

    end_time = datetime.now() + timedelta(seconds=280)
    request_time = time.time()
    while not API_INSTANCE.has_changed(request_time):
        time.sleep(10.0)
        if datetime.now() > end_time:
            return {'results': 'null',
                    'columns': 'null',
                    'date': datetime.now().strftime('%Y/%m/%d %H:%M:%S')}

    results, columns = get_results()
    results = json.dumps(results)

    return {'results': results,
            'columns': columns,
            'date': datetime.now().strftime('%Y/%m/%d %H:%M:%S')}


@app.route("/koarti/data")
def load_data():
    """
    Returns the current data content.  This is used to display the table the
    on initial load.
    """
    results, columns = get_results()
    results = json.dumps(results)

    return {'results': results,
            'columns': columns,
            'date': datetime.now().strftime('%Y/%m/%d %H:%M:%S')}


def api_results():
    """
    The results from querying the database.

    :return: (list/dict, list) the database query results,  the keys/columns of
                               the query.
    """
    if not API_INSTANCE:
        return None

    rti_api = API_INSTANCE
    var_get = rti_api.get_params()
    results = None
    cmd = None

    if var_get.search:
        cmd = 'search' + var_get.search.upper().replace('_', '')
    elif var_get.update:
        cmd = 'update' + var_get.update.upper()

    if cmd:
        try:
            results = getattr(rti_api, cmd)()
        except AttributeError as err:
            return return_results(success=0, msg=err)
        except ValueError as err:
            return return_results(success=0, msg=err)

        results = replace_datetime(results)

    return return_results(results=results)


def return_results(success=1, results=None, msg=None):
    """
    Return the results.  If json,  the results are a dictionary of
    success,  data (the database query results), and any error messages.

    :param results: <str> the results,  either a json formatted string or
                          a string of the database query results.
    :param success: <int> 1 for success, 0 for error
    :param msg: <str> any message to return
    :return: <dict> the results as a dictionary response
    """
    return {'success': success, 'data': results, 'msg': msg}


def get_results():
    """
    The results from querying the database.

    :return: (list/dict, list) the database query results,  the keys/columns of
                               the query.
    """
    if not API_INSTANCE:
        return None, None

    rti_api = API_INSTANCE
    var_get = rti_api.get_params()

    if not var_get.page or var_get.page == 'daily':
        if var_get.search:
            cmd = 'search' + var_get.search.upper().replace('_', '')
            try:
                results = getattr(rti_api, cmd)()
            except ValueError as err:
                return return_error(err, True, True)
            except AttributeError as err:
                results = rti_api.searchDATE()
        else:
            results = rti_api.searchDATE()

        results = rti_api.parse_results(results)
    else:
        results = rti_api.monthly_results()

    db_columns = rti_api.getDbColumns()
    results = replace_datetime(results)

    return results, db_columns


def return_error(err, return_json, web_out):
    if return_json:
        result = {'success': 0, 'data': {}, 'msg': str(err)}
        if web_out:
            return jsonify(result)
        return str(result)
    else:
        return proposalAPI_usage(err=err)


def parse_request(default_utd=True):
    """
    Parse the url for the variable values,  set defaults

    :return: (named tuple) day parameters
    """
    args = ['utd', 'utd2', 'search', 'update', 'val', 'view', 'tel', 'inst',
            'page', 'yr', 'month', 'limit', 'chk', 'chk1', 'dev', 'plot',
            'columns', 'key', 'add', 'update_val']

    vars = dict((name, request.args.get(name)) for name in args)
    for key, val in vars.items():
        if val and val == "None":
            vars[key] = None
        elif val and val.isdigit():
            vars[key] = int(val)

        if not vars[key] and key in ['tel', 'dev', 'view']:
            vars[key] = 0

    if not vars['utd'] and default_utd:
        vars['utd'] = datetime.utcnow().strftime("%Y-%m-%d")

    if not vars['month']:
        vars['month'] = datetime.utcnow().strftime("%m")

    if not vars['page']:
        vars['page'] = 'daily'

    # set defaults
    if not request.args.get('posted'):
        vars['chk'] = 1

    return namedtuple('params', vars.keys())(*vars.values())


def parse_args():
    """
    Parse the command line arguments.

    :return: <obj> commandline arguments
    """
    parser = argparse.ArgumentParser(description="Start KOA RTI DB API.")
    parser.add_argument("--logdir", type=str, default='/koadata',
                        help="Define the directory for the log.")
    parser.add_argument("--port", type=int, default=0, help="Server Port.")
    parser.add_argument("--mode", type=str, choices=['dev', 'release'],
                        default='release',
                        help="Determines database access and debugging mode.")

    return parser.parse_args()


def create_logger(name, logdir):
    logfile = f'{logdir}/{name}.log'
    try:
        #Create logger object
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        #file handler (full debug logging)
        handler = logging.FileHandler(logfile)
        handler.setLevel(logging.DEBUG)
        handler.suffix = "%Y%m%d"
        logger.addHandler(handler)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        #stream/console handler (info+ only)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(' %(levelname)8s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    except Exception as error:
        print(f"ERROR: Unable to create logger '{name}' in dir "
              f"{logfile}.\nReason: {error}")


if __name__ == '__main__':

    args = parse_args()

    port = args.port
    mode = args.mode
    debug = False if mode == 'release' else True
    host = '0.0.0.0'
    assert port != 0, "ERROR: Must provide port"

    # if debug and args.logdir == 'log':
    #     logdir = '/tmp'
    # else:
    #     logdir = args.logdir

#    logdir = APP_PATH + '/log/'

    create_logger('wmko_rti_api', f'{args.logdir}/')
    log = logging.getLogger('wmko_rti_api')

    # run flask server
    log.info(f"Starting RTI API:\nPORT = {port}\nMODE = {mode}")
    app.run(host=host, port=port, debug=debug)
    log.info("Stopping KOA RTI API.\n")




