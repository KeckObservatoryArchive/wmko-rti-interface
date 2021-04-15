from datetime import datetime
from calendar import monthrange
import sys
import json
from os import path
from koa_rti_api import KoaRtiApi


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


# def clear_file(file_location):
#     """
#     Check the existence of the file, ie stage_file:
#         /s/sdata125/hires6/2020nov23/hires0003.fits
#
#     :param file_location: <str> path or path+filename for the file
#     :param filename: <str> filename to add to path
#
#     :return: <bool> does the file exist?
#     """
#     if not file_location:
#         return False
#
#     if path.exists(file_location):
#         remove(file_location)

def get_api_help_string():
    search_list = []
    update_list = []
    for attribute in KoaRtiApi.__dict__:
        if 'search' in attribute:
            query_name = attribute.split('search')[1]
            search_list.append(query_name)
        elif 'update' in attribute:
            query_name = attribute.split('update')[1]
            update_list.append(query_name)

    help_str = "<BR><BR>Options: <BR><UL>"
    help_str += f"<li>search={search_list}<BR>"
    help_str += f"<li>update={update_list}<BR>"
    help_str += "<li>columns=column1,column2,...,  columns to return"
    help_str += "<li>key=key,  search key"
    help_str += "<li>val=value to match search,  LastEntry does use value.<BR>"
    help_str += "<li>add=string to add to end of query"
    help_str += "<li>utd=YYYY-MM-DD"
    help_str += "<li>utd2=YYYY-MM-DD,  search for a date range"
    help_str += "<li>limit=###,  the number of results to limit the search"
    help_str += "<li>inst=inst-name,  the instrument to limit the search"
    help_str += "<li>tel=#,  the number (1,2) of the telescope to limit the search"
    help_str += "<BR><BR>Example: <BR><UL>"
    help_str += "<li>/koarti_api?search=GENERAL&val=TRANSFERRED&"
    help_str += "columns=koaid,status,ofname,stage_file,archive_dir,ofname_deleted"
    help_str += "&key=status&add=AND OFNAME_DELETED=0&utd=2020-12-20"
    help_str += "&utd2=2020-12-21<BR>"
    help_str += "<li>/koarti_api?search=STATUS&val=Transferred&utd=2020-11-21"
    help_str += "<li>/koarti_api?update=GENERAL&columns=ofname_deleted"
    help_str += "&update_val=True&key=koaid&val=HI.20201104.1120.04"

    return help_str


class InstrumentReport:

    def __init__(self, inst):

        self.inst = inst
        self.test_output = self.run_tests()

    def json_sting(self):
        return self.test_output

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
