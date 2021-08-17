from datetime import datetime
from calendar import monthrange
import sys
import json
import calendar
import argparse
from os import path
from flask import jsonify, request
from collections import namedtuple

sys.path.append('/kroot/rel/default/bin/')
import testall

APP_PATH = path.abspath(path.dirname(__file__))
TESTALL_PATH = '/kroot/rel/default/data'


def year_range():
    """
    Create a list of years to be used with the drop down selection menu on the
    web pages.

    :return: (list) a list of years,  earliest is commissioning and
                    final is the current year.
    """
    current_yr = int(datetime.utcnow().strftime("%Y"))
    first_yr = 1992 # 1993 -1
    years = []
    for i in range(current_yr, first_yr, -1):
        years.append(str(i))

    return years


def set_utd(day, month, yr):
    """
    Determine the utd (YYYYMMDD),  current ut of from input day, mo, yr

    :param day: (str) optional day for utd
    :param month: (str) optional month for utd
    :param yr: (str) optional year for utd

    :return: (str, int, int, int) utd YYYYMMDD, day, month, yr
    """

    if day and month and yr:
        day, month, yr = _check_input_dates(int(day), int(month), int(yr))
        if yr > 90:
            yr = f'19{int(yr):02}'
        else:
            yr = f'20{int(yr):02}'

        utd = f'{yr}-{month:02}-{int(day):02}'
    else:
        ut_now = datetime.utcnow()
        utd = ut_now.strftime("%Y-%m-%d")
        yr = ut_now.strftime("%Y")
        month = ut_now.strftime("%m")
        day = ut_now.strftime("%d")

    return utd, day, month, yr


def _check_input_dates(day, month, yr):
    """
    Validate the dates,  if out of range round to closest valid date
    or current utd.

    :param day: (int) the day
    :param month: (int) the integer month
    :param yr: (int) YY 2 digit format.

    :return: (int, int, int) day, month, yr
    """
    if month < 1:
        month = 1
    elif month > 12:
        month = 12

    current_yr = int(datetime.utcnow().strftime("%y"))
    first_yr = 93
    if yr > 100 or current_yr < yr < first_yr or yr < 0:
        yr = current_yr

    last_day = monthrange(yr, month)[-1]
    if day > last_day:
        day = last_day
    elif day < 1:
        day = 1

    return day, month, yr


def set_month_name(month):
    """
    determine the month name

    :param month: (datetime) the month,  can be None

    :return: (str) month name
    """
    if month:
        datetime_object = datetime.strptime(month, "%m")
        return datetime_object.strftime("%B")
    else:
        return datetime.utcnow().strftime("%B")


def replace_datetime(results):
    """
    A function to replace the datetime objects in the result lists.

    :param results: <list> the query results.
    :return: <list> the list with date instead of a datetime object.
    """
    if not results or type(results) != list:
        return results

    for result in results:
        for key_name in result:
            if isinstance(result[key_name], datetime):
                date_obj = result[key_name]
                result[key_name] = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    return results


def get_api_help_string(api_instance):
    search_list = []
    update_list = []
    metrics_list = []

    for attribute in dir(api_instance):

        if '_' in attribute:
            continue

        if 'search' in attribute:
            query_name = attribute.split('search')[1]
            search_list.append(query_name)
        elif 'update' in attribute:
            query_name = attribute.split('update')[1]
            update_list.append(query_name)
        elif 'metrics' in attribute:
            query_name = attribute.split('metrics')[1]
            metrics_list.append(query_name)

    help_str = "<BR><BR>Options: <BR><UL>"
    help_str += f"<li>search={search_list}<BR>"
    help_str += f"<li>metrics={metrics_list}<BR>"
    help_str += f"<li>update={update_list}<BR>"

    help_str += f"<BR>"
    help_str += "<li>key=key,  search key"
    help_str += "<li>val=value to match search,  LastEntry does use value.<BR>"
    help_str += "<li>month=MM (integer month)"
    help_str += "<li>year=YYYY (integer month)"
    help_str += "<li>utd=YYYY-MM-DD"
    help_str += "<li>utd2=YYYY-MM-DD,  search for a date range"
    help_str += "<li>inst=inst-name,  the instrument to limit the search"
    help_str += "<li>tel=#,  the number (1,2) of the telescope to limit the search"
    help_str += "<li>obsid=####,  Observer ID (PYKOA only)"
    help_str += "<li>progid=KNYYYY[A/B]_###,  Program ID (PYKOA only)"
    help_str += "<li>level=#,  the data processing level (0,1,2)"
    help_str += "<li>limit=###,  the number of results to limit the search"
    help_str += "<li>data=[0 or 1], toggle the return of data array.  default=1"
    help_str += "<li>add=string to add to end of query"
    help_str += "<li>plot=#,  the bokeh plot to return [1-5]"
    help_str += "<li>columns=column1,column2,...,  columns to return"
    help_str += "<BR><BR>Example: <BR><UL>"
    help_str += "<li>/koarti_api?search=GENERAL&val=TRANSFERRED&"
    help_str += "columns=koaid,status,ofname,stage_file,archive_dir,ofname_deleted"
    help_str += "&key=status&add=AND OFNAME_DELETED=0&utd=2020-12-20"
    help_str += "&utd2=2020-12-21<BR>"
    help_str += "<li>/koarti_api?search=STATUS&val=Transferred&utd=2020-11-21"
    help_str += "<li>/koarti_api?update=GENERAL&columns=ofname_deleted"
    help_str += "&update_val=True&key=koaid&val=HI.20201104.1120.04"

    return help_str


def parse_results(results):
    """
    Transform any columns that need to be parsed.  Currently it is
    only the filename being split from OFNAME.

    :param results: (list/dict) the query results.
    :return: (list/dict) the parsed query results.
    """
    if not results:
        return None

    for i, res in enumerate(results):
        res['stage_dir'], res['filename'] = parse_filename_dir(res)
        results[i] = res

    return results


def parse_filename_dir(result):
    """
    Get the path and filename from the OFNAME DB column

    :param result: <dict> one line query result.

    :return: <str, str> path, filename
    """
    fullpath = result.get('ofname', None)
    if not fullpath:
        return '', ''

    split_path = fullpath.rsplit('/', 1)

    if len(split_path) > 1:
        return split_path[0], split_path[1]
    else:
        return split_path[0], ''


def query_prefix(columns=None, key=None, val=None, table='koa_status'):
    if columns and key and val:
        query = f"SELECT {columns} FROM {table} WHERE {key} LIKE %s"
        params = ("%" + val + "%",)
        add_str = " AND "
    elif columns:
        query = f"SELECT {columns} FROM {table}"
        params = ()
        add_str = " WHERE "
    elif key and val:
        query = f"SELECT * FROM {table} WHERE {key} LIKE %s"
        params = ("%" + val + "%",)
        add_str = " AND "
    elif key:
        query = f"SELECT {key} FROM {table}"
        params = ()
        add_str = " WHERE "
    else:
        query = f"SELECT * FROM {table}"
        params = ()
        add_str = " WHERE "

    return query, params, add_str


def date_iter(year, month):
    """
    iterate over the days in a month.

    :param year: (str) YYYY format
    :param month: (str) MM format
    :return: (str) YYYY-MM-DD format,  one date at a time
    """
    ndays = calendar.monthrange(int(year), int(month.lstrip('0')))[1]
    for i in range(1, ndays + 1):
        yield f'{year}-{month:0>2}-{i:0>2}'


# --- Main Helpers ---
def api_results(API_INSTANCE):
    """
    The results from querying the database.

    :return: (list/dict, list) the database query results,  the keys/columns of
                               the query.
    """
    if not API_INSTANCE:
        return None

    params = API_INSTANCE.get_params()
    results = None
    cmd = None

    # find if one of the cmds was defined.
    cmd_types = ['metrics', 'search', 'update', 'pykoa']
    for cmd_type in cmd_types:
        cmd_attr = getattr(params, cmd_type)
        if cmd_attr:
            cmd = cmd_attr.upper().replace('_', '')
            break

        if cmd_type == 'pykoa' and not params.progid:
            return return_results(success=0, msg="use: progid=####")

    sums = None
    if cmd:
        results, sums = get_cmd_results(API_INSTANCE, cmd, cmd_type, sums)

    response = return_results(results=results, cmd=cmd, cmd_type=cmd_type,
                              api=API_INSTANCE)

    if cmd_type == 'metrics':
        response['sums'] = sums

    return response


def get_cmd_results(API_INSTANCE, cmd, cmd_type, sums):
    """
    return the results given the cmd and the cmd type.

    :param API_INSTANCE: The instance of the API.
    :param cmd: <str> the string associated to the API command.
    :param cmd_type: <str> the kind of command for the API.
    :param sums: <object> the sum object that is calculated.

    :return: The database results,  the sum dictionary
    """
    try:
        if cmd_type == 'metrics':
            results, sums = getattr(API_INSTANCE, cmd_type + cmd)()
        else:
            results = getattr(API_INSTANCE, cmd_type + cmd)()
    except (AttributeError, ValueError) as err:
        return return_results(success=0, msg=str(err))

    results = replace_datetime(results)

    return results, sums


def get_results(API_INSTANCE):
    """
    The results from querying the database -- used by the update function for
    the GUI.  Not needed when not using the GUI.

    :param API_INSTANCE: The instance of the API.

    :return: (list/dict, list) the database query results,  the keys/columns of
                               the query.
    """
    if not API_INSTANCE:
        return None, None

    params = API_INSTANCE.get_params()

    if not params.page or params.page == 'daily':
        results = update_search_page(API_INSTANCE, params)
    elif params.page == 'monthly':
        results = API_INSTANCE.monthly_results()
    else:
        results = []

    db_columns = API_INSTANCE.getDbColumns()
    results = replace_datetime(results)

    return results, db_columns


def update_search_page(API_INSTANCE, params):
    """
    Used to update the daily page or search results.

    :param API_INSTANCE: The instance of the API.
    :param params: <named tuple> the request parameters
    :return:
    """
    if params.search:
        cmd = 'search' + params.search.upper().replace('_', '')
        try:
            results = getattr(API_INSTANCE, cmd)()
        except ValueError as err:
            return return_results(success=0, msg=str(err))
        except AttributeError:
            results = API_INSTANCE.searchDATE()
    else:
        results = API_INSTANCE.searchDATE()

    results = parse_results(results)

    return results

def return_results(success=1, results=None, cmd=None, cmd_type='command',
                   msg=None, api=None):
    """
    Return the results.  If json,  the results are a dictionary of
    success,  data (the database query results), and any error messages.

    :param results: the results,  either a json formatted string or
                          a string of the database query results.
    :param success: <int> 1 for success, 0 for error
    :param cmd: <str> The command called by the observer
    :param msg: <str> any message to return
    :param api: <api instance> the instantiated API.
    :return: <dict> the results as a dictionary response
    """
    if success == 1:
        api_status = 'COMPLETE'
    else:
        api_status = 'ERROR'

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if api:
        dates = api.get_date_range()
        ut_start = dates[0]
        ut_end = dates[1]
    else:
        ut_start = None
        ut_end = None

    nfiles = get_number_files(results)

    return {'success': success, 'msg': msg, 'apiStatus': api_status,
            'timestamp': now, 'ut_start': ut_start, 'ut_end': ut_end,
            'num_files': nfiles, cmd_type: cmd, 'data': results}


def get_number_files(results):
    """
    Determine the number of files in the results.  If both lev0 and lev1 are in
    the results,  return the results as a dictionary.

    :param results: The database query results.

    :return: The number of files in the results
    """
    if results:
        if type(results) == dict:
            nfiles = {}
            for key, lev_results in results.items():
                nfiles[key] = len(lev_results)
        else:
            nfiles = len(results)
    else:
        nfiles = 0

    return nfiles


def parse_request(default_utd=True, method='GET'):
    """
    Parse the url for the variable values,  set defaults

    :return: (named tuple) day parameters
    """
    args = ['utd', 'utd2', 'search', 'update', 'metrics', 'pykoa', 'val',
            'view', 'tel', 'inst', 'page', 'yr', 'month', 'limit', 'chk',
            'chk1', 'obsid', 'progid', 'plot', 'columns', 'key', 'add',
            'level', 'data', 'update_val']

    if method == 'GET':
        vars = dict((name, request.args.get(name)) for name in args)
    elif method in ['PUT', 'POST']:
        vars = dict((name, request.json.get(name)) for name in args)
    else:
        return

    for key, val in vars.items():
        if val and val == "None":
            vars[key] = None
        elif val:
            try:
                vars[key] = int(val)
            except ValueError:
                pass

        if not vars[key] and key in {'tel', 'view'}:
            vars[key] = 0

    if not vars['utd'] and default_utd or not vars['utd'] and vars['metrics']:
        if vars['month']:
            if not vars['yr']:
                vars['yr'] = int(datetime.utcnow().strftime("%Y"))
            first_last = calendar.monthrange(vars['yr'], vars['month'])
            vars['utd'] = f"{vars['yr']}-{vars['month']:0>2}-01"
            vars['utd2'] = f"{vars['yr']}-{vars['month']:0>2}-{first_last[1]}"
        else:
            vars['utd'] = datetime.utcnow().strftime("%Y-%m-%d")

    if not vars['month']:
        vars['month'] = datetime.utcnow().strftime("%m")

    if not vars['page']:
        vars['page'] = 'daily'

    if vars['level'] and type(vars['level']) != int and vars['level'].lower() == 'all':
        vars['level'] = None

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
    parser.add_argument("--logdir", type=str, default='/koadata/log',
                        help="Define the directory for the log.")
    parser.add_argument("--port", type=int, default=0, help="Server Port.")
    parser.add_argument("--mode", type=str, choices=['dev', 'release'],
                        default='release',
                        help="Determines database access and debugging mode.")

    return parser.parse_args()


class InstrumentReport:

    def __init__(self, inst):
        self.inst = inst
        self.test_output = self.run_tests()

    def run_tests(self):
        """
        Run the testAll for the instrument

        :return: (str) json output of results from testAll
        """
        try:
            results = testall.test_all('koa', datadir=TESTALL_PATH, level=1)
        except:
            return None

        return results

    def results(self):
        """
        Load json output into a python dictionary

        :return: (dict) results from testAll as dictionary
        """
        if self.test_output:
            return json.loads(self.test_output)
        else:
            return {"stats": {"num_pass": 'No Result', "num_warn": "No Result",
                              "num_error": "No Result", "num_skip": "No Result"}}
