from datetime import datetime
from calendar import monthrange
import sys
import json
from os import path


sys.path.append('/kroot/rel/default/bin/')
import testall

APP_PATH = path.abspath(path.dirname(__file__))
#TESTALL_PATH = path.join(APP_PATH, "config/")
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
        print(f'{day} {month} {yr}')
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
    if not results:
        return results

    datetime_keys = ["utdatetime","creation_time","dep_start_time",
                     "dep_end_time", "xfr_start_time", "xfr_end_time",
                     "ipac_notify_time", "ipac_response_time", "stage_time",
                     "last_mod"]

    for key_name in datetime_keys:
        for result in results:
            if key_name not in result:
                continue
            date_obj = result[key_name]
            if date_obj:
                result[key_name] = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    return results


def get_api_help_string():
    help_str = "<BR><BR>Options: <BR><UL>"
    help_str += "<li>search=STATUS, LastEntry, KOAID, SEMID, ImageType, HEADER<BR>"
    help_str += "<li>val=value to match search,  LastEntry does use value.<BR>"
    help_str += "<li>utd=YYYY-MM-DD"
    help_str += "<li>utd2=YYYY-MM-DD,  search for a date range"
    help_str += "<li>limit=###,  the number of results to limit the search"
    help_str += "<li>inst=inst-name,  the instrument to limit the search"
    help_str += "<li>tel=#,  the number (1,2) of the telescope to limit the search"
    help_str += "<BR><BR>Example: <BR><UL>"
    help_str += "<li>/api?search=STATUS&val=Transferred&utd=2020-11-21"

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
