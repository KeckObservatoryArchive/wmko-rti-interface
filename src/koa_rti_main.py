import os
import sys
import calendar
import time
import json
import logging

from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, render_template, request, send_from_directory, jsonify
from flask_cors import CORS

from ingest_api.ingest_api import ingest_api_get
from utils.koa_rti_api import KoaRtiApi
from utils.koa_rti_helpers import get_api_help_string, InstrumentReport
from utils.koa_rti_helpers import parse_request, parse_results, parse_args
from utils.koa_rti_helpers import api_results, get_results, year_range
from utils.koa_tpx_gui import tpx_gui


APP_PATH = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_PATH = os.path.join(APP_PATH, "templates/")
API_INSTANCE = None


def get_resource_as_string(name, charset='utf-8'):
    """ Initiliaze the flask APP"""
    with app.open_resource(name) as f:
        return f.read().decode(charset)


app = Flask(__name__, template_folder=TEMPLATE_PATH)
app.jinja_env.globals['get_resource_as_string'] = get_resource_as_string
CORS(app)
cors = CORS(app, resources = {
    r"/*": {
        "origins": "*"
    }
})
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route("/ingest_api", methods=["GET"])
def ingest_api():
    log.info('ingest_api: starting api call')
    return ingest_api_get()


@app.route("/koarti_api", methods=['GET'])
def tpx_rti_api():
    global API_INSTANCE
    var_get = parse_request(default_utd=False)
    rti_api = KoaRtiApi(var_get)
    API_INSTANCE = rti_api

    if not var_get.search and not var_get.metrics and not var_get.update:
        help_str = f"No Results for query parameters:<BR><BR> {var_get}<BR>"
        help_str += get_api_help_string(API_INSTANCE)
        return help_str

    results = api_results(API_INSTANCE)
    if var_get.data == 0:
        del results['data']

    return jsonify(results)


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
    elif var_get.page in {'koatpx', 'koadrp'}:
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

    results, columns = get_results(API_INSTANCE)
    try:
        results = json.dumps(results)
    except:
        log.error("ERROR! cannot json.dump data in data_update")

    return {'results': results,
            'columns': columns,
            'date': datetime.now().strftime('%Y/%m/%d %H:%M:%S')}


@app.route("/koarti/data")
def load_data():
    """
    Returns the current data content.  This is used to display the table the
    on initial load.
    """
    results, columns = get_results(API_INSTANCE)
    try:
        results = json.dumps(results)
    except:
        log.error("ERROR! cannot json.dump data in load_data")

    return {'results': results,
            'columns': columns,
            'date': datetime.now().strftime('%Y/%m/%d %H:%M:%S')}


@app.route("/koarti/koa_status/reviewed", methods=['PUT'])
def update_koa_status_reviewed():
    """
    Update koa_status.reviewed for given list of ids.

    json inputs:
        val (int): Value to set
        ids (array): Array of record IDs to update

    :return: (int) num rows affected
    """
    #get passed json vars
    val = request.json.get('val')
    ids = request.json.get('ids')

    #send update query for each id
    api = KoaRtiApi(parse_request(method='PUT'))
    num = 0
    for id in ids:
        res = api.update_status_reviewed(id, val)
        num += res
    return str(num)


@app.route("/koarti/log/<id>", methods=['GET'])
def get_log(id):
    """
    Returns the log file contents of a processed record
    """
    var_get = parse_request()
    api = KoaRtiApi(var_get)
    format = request.args.get('format')
    res = api.get_log(id, format)
    return str(res)


def create_logger(name, logdir):

    try:
        #Create logger object
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        #create directory if it does not exist
        logfile = f'{logdir}/{name}.log'
        Path(os.path.dirname(logfile)).mkdir(parents=True, exist_ok=True)

        #file handler (full debug logging)
        handler = logging.FileHandler(logfile)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
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

    #cd to script dir so relative paths work
    os.chdir(sys.path[0])

    #parse args
    args = parse_args()
    port = args.port
    mode = args.mode
    debug = False if mode == 'release' else True
    logdir = args.logdir if mode == 'release' else '/tmp'
    host = '0.0.0.0'
    assert port != 0, "ERROR: Must provide port"

    #create logger
    create_logger('wmko_rti_api', logdir)
    log = logging.getLogger('wmko_rti_api')

    # run flask server
    log.info(f"Starting RTI API:\nPORT = {port}\nMODE = {mode}")
    app.run(host=host, port=port, debug=False)
    log.info("Stopping KOA RTI API.\n")




