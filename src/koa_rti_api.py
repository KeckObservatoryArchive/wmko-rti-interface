import argparse
import calendar
import json
import logging
import sys
from datetime import datetime, timedelta
from os import stat

from koa_rti_db import DatabaseInteraction
from koa_rti_plots import TimeBarPlot, OverlayTimePlot


class KoaRtiApi:

    def __init__(self, var_get):
        self.db_functions = DatabaseInteraction(var_get.dev)

        self.query_opts = ['None', 'HEADER', 'Image_Type', 'KOAID',
                           'Last_Entry', 'STATUS', 'SEMID']
        self.status_opts = ['TRANSFERRED', 'TRANSFERRING',  'PROCESSING',
                            'ERROR', 'INVALID', 'WARN']
        self.img_types = ['OBJECT', 'BIAS', 'ARCLAMP', 'FLATLAMP', 'FOCUS']

        self.keck1_inst = ['None', 'HIRES', 'LRIS', 'MOSFIRE', 'OSIRIS']
        self.keck2_inst = ['None', 'DEIMOS', 'ESI', 'KCWI', 'NIRES', 'NIRC2',
                           'NIRSPEC']

        # change these to change the order of the results
        self.query_keys = ['STATUS', 'KOAID', 'UTDATETIME', 'Filename',
                           'INSTRUMENT', 'KOAIMTYP', 'SEMID', 'Stage_Dir',
                           'Archive_Dir', 'Status_Code', 'Creation_Time',
                           'Dep_Start_Time', 'Dep_End_Time', 'IPAC_Notify_Time',
                           'IPAC_Response_Time', 'Stage_time', 'Filesize_MB',
                           'Archsize_MB', 'Last_Mod', 'OFNAME', 'ID']

        self.monthly_header = ['Date', 'Total_Files']
        self.monthly_header += self.status_opts + ['Instruments']

        # pre-pend num_ for the key to 'stats'
        self.health_head = ['Pass', 'Warn', 'Error', 'Skip']

        self.default_num_columns = 7
        self.search_val = var_get.val
        self.table_view = None
        self.params = var_get
        self.limit = var_get.limit
        self.utd = var_get.utd

        self.update_table_name(var_get.view)

        if var_get.yr and var_get.month:
            self.monthly_date = f'{var_get.yr}-{var_get.month:0>2}'
        else:
            self.monthly_date = datetime.utcnow().strftime("%Y-%m")

    def getOptionLists(self):
        """
        Get a dictionary of the option lists.

        :return: (dict/list) option lists
        """
        opt_lists = {'query': self.query_opts, 'status': self.status_opts,
                     'img_type': self.img_types}

        return opt_lists

    def get_params(self):
        """
        Access the url request parameters.

        :return: (named tuple) the parameters
        """
        return self.params

    def is_daily(self):
        """
        Determine if the query / page is the daily page.

        :return: (bool) True for the daily page.
        """
        if self.params.page == 'daily':
            return True
        return False

    def searchDATE(self):
        """
        Find all results for a date or date range.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query()
        results = list(self.db_functions.make_query(query, params))

        return results

    def searchLASTENTRY(self):
        """
        Return the last entry into the database.

        :return: (list/dict) the 1 element list of db query results
        """
        self.limit = 1
        query, params = self._generic_query()

        return self.db_functions.make_query(query, params)

    def searchSTATUS(self):
        """
        Find all results with a certain status.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query("STATUS", self.search_val)

        return self.db_functions.make_query(query, params)

    def searchKOAID(self):
        """
        Find all results with the defined KOAID.  Results should be one row.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query("KOAID", self.search_val)

        return self.db_functions.make_query(query, params)

    def searchSEMID(self):
        """
        Find all results with the defined SEMID.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query("SEMID", self.search_val)

        return self.db_functions.make_query(query, params)

    def searchIMAGETYPE(self):
        """
        Find all results with the defined Image Type.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query("KOAIMTYP", self.search_val)

        return self.db_functions.make_query(query, params)

    def searchHEADER(self):
        self.query_keys = ['KOAID', 'STATUS', 'HEADER_KEYWORD', 'HEADER_VALUE',
                           'HEADER_COMMENT', 'INSTRUMENT', 'KOAIMTYP', 'SEMID',
                           'LAST_MOD', 'STAGE_FILE']
        params = ()
        query = "SELECT headers.*, status, stage_file, instrument, koaimtyp, "
        query += "semid FROM headers JOIN dep_status ON headers.koaid"
        query += " = dep_status.koaid"

        query, params = self._add_general_query(query, params, "WHERE")
        results = self.db_functions.make_query(query, params)

        for i, result in enumerate(results):
            result['last_mod'] = self._grab_value(result, 'last_mod')
            result['header_keyword'] = self.search_val

            header = self._grab_value(result, 'header')
            if header:
                head_dict = json.loads(header)
                head_vals = self._grab_value(head_dict, self.search_val)
                result['header_value'] = self._grab_value(head_vals, 'value')
                result['header_comment'] = self._grab_value(head_vals, 'comment')
            else:
                result['header_value'] = None
                result['header_comment'] = None

            result['header'] = None
            results[i] = result

        return results

    def searchLOCATION(self):
        """
        provide access to file locations by date.  The results include the
        original filename (OFNAME), the orginal file (stage file) and the
        archive files location (archive dir)

        :return: (list / dict) the file locations by date
        """
        column_keys = "koaid, ofname, stage_file, archive_dir"
        query, params = self._generic_query(key=column_keys)
        results = self.db_functions.make_query(query, params)

        return results

    def searchARCHIVED(self):
        """
        Get the koaid and status for a date,   or the search parameters

        :return: (dict) db results
        """
        keys = "koaid, status, ofname, stage_file, archive_dir"
        key = "status"
        #TODO change to archived once the status has been updated
        # val = "ARCHIVED"
        val = "Transferred"
        query, params = self._generic_query(keys=keys, key=key, val=val)
        results = self.db_functions.make_query(query, params)

        return results

    # -- monthly view section ---

    def monthlyINST(self, day):
        """
        Determine the instruments used over the month.

        :param day: (str) date of results to get,  can be full or partial date.

        :return: (str) comma separated instruments
        """
        query = f'SELECT DISTINCT instrument FROM dep_status '
        query += f'WHERE utdatetime LIKE %s'
        params = ("%" + day + "%", )

        if self.params.inst:
            query += f' AND instrument LIKE %s'
            params += ("%" + self.params.inst + "%",)
        else:
            query += self._add_tel_query("AND")

        results = self.db_functions.make_query(query, params)
        inst_list = []
        if results:
            for result in results:
                if result not in inst_list and 'instrument' in result:
                    inst_list.append(result['instrument'])

        inst_str = ', '.join(inst_list)

        return inst_str

    def monthlyTOTAL(self, day):
        """
        Tally of results by day or month -- dependent on value of
        self.monthly_date.

        :param day: (str) date of results to get,  can be full or partial date.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._monthly_query(day)

        results = self.db_functions.make_query(query, params)
        if results:
            results = results[0]['COUNT(*)']

        return results

    def monthlySTATUS(self, status, day):
        """
        Tally of files with the state = status  by day or month -- dependent
        on value of self.monthly_date.

        :param day: (str) date of results to get,  can be full or partial date.
        :param status: the state of the file,  PROCESSING, INVALID, ERROR

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._monthly_query(day)
        query += f' AND status LIKE %s'
        params += ("%" + status + "%",)

        results = self.db_functions.make_query(query, params)

        if results:
            results = results[0]['COUNT(*)']

        return results

    def _date_iter(self, year, month):
        """
        iterate over the days in a month.

        :param year: (str) YYYY format
        :param month: (str) MM format
        :return: (str) YYYY-MM-DD format,  one date at a time
        """
        for i in range(1, calendar.monthlen(year, month) + 1):
            yield f'{year}-{month}-{i:0>2}'

    def monthly_results(self):
        """
        Find the sums over a month by day.

        :return: (list) a list of rows/columns for the monthly table
        """
        results = []
        yr, mo = self.monthly_date.split('-')
        for day in self._date_iter(int(yr), int(mo)):
            if day > self.utd:
                break

            tally = self.monthlyTOTAL(day)
            insts = self.monthlyINST(day)

            day_result = {'date': day, 'total_files': tally,
                          'instruments': insts}
            for key in self.status_opts:
                keyword = key.lower()
                day_result[keyword] = self.monthlySTATUS(keyword, day)

            results += [day_result]

        return results

    def parse_filename_dir(self, result):
        fullpath = self._grab_value(result, 'ofname')
        if not fullpath:
            return '', ''

        split_path = fullpath.rsplit('/',1)

        if len(split_path) > 1:
            return split_path[0], split_path[1]
        else:
            return split_path[0], ''

    def parse_results(self, results):
        """
        Transform any columns that need to be parsed.  Currently it is
        only the filename being split from OFNAME.

        :param results: (list/dict) the query results.
        :return: (list/dict) the parsed query results.
        """
        if not results:
            return None
        for i in range(0, len(results)):
            res = results[i]
            res['stage_dir'], res['filename'] = self.parse_filename_dir(res)
            results[i] = res

        return results

    @staticmethod
    def _grab_value(result_dict, key_name):
        """
        Use to avoid an error while accessing a key that does not exist.

        :param result_dict: (dict) dictionary to check
        :param key_name: (str) key name

        :return: dictionary value
        """
        if key_name in result_dict:
            return result_dict[key_name]

        return None

    def update_table_name(self, table_view):
        """
        Update the variable for the table view.

        :param table_view: (str) 'default' or 'full'
        """
        self.table_view = table_view

    def getDbColumns(self):
        """
        Return the columns for the table view,  either the full table or subset.

        :return: (list) the table header/keys
        """
        if self.params.page:
            if self.params.page == 'monthly':
                return self.monthly_header
            elif self.params.page == 'health':
                return self.health_head

        if (len(self.query_keys) >= self.default_num_columns
                and not self.table_view or self.table_view == 0):
            return self.query_keys[:self.default_num_columns]
        elif self.table_view == 1:
            return self.query_keys

    def getInst(self):
        return self.params.inst

    def getInstruments(self):
        """
        Return a list of the instrument lists (separated by telescope)

        :return: (list/list) 2 element list of instrument lists
        """
        return [self.keck1_inst, self.keck2_inst]

    """  ------------  metrics section    ------------  """

    def statDepTime(self):
        stats = self._calc_time_length('dep_start_time', 'dep_end_time')

        plot_obj = OverlayTimePlot(stats, 'DEP Time')

        return plot_obj.get_plot()

    def statTransferTime(self):
        stats = self._calc_time_length('dep_end_time', 'ipac_notify_time')

        plot_obj = OverlayTimePlot(stats, 'Transfer Time - DEP End to IPAC Notify')

        return plot_obj.get_plot()

    def statTotalTime(self):
        stats = self._calc_time_length('creation_time', 'ipac_notify_time')

        plot_obj = OverlayTimePlot(stats, 'Total Time - File Write to IPAC Notify')

        return plot_obj.get_plot()

    def getPlots(self):
        """ Determine the plots to return by the input plot type """
        if not self.params.plot or self.params.plot == 0:
            results = {'plots': [self.statTotalTime()]}
        elif self.params.plot == 1:
            results = {'plots': [self.statTransferTime()]}
        elif self.params.plot == 2:
            results = {'plots': [self.statDepTime()]}
        else:
            results = {'plots': [self.statTotalTime(), self.statTransferTime(),
                                 self.statDepTime()]}

        return results

    def _calc_time_length(self, start_key, end_key):
        """
        Tally the number for each time bin.  ie, {DEIMOS : {'0:00:02: 42, ...}}

        :return: (dict/dict/sum)
        """
        query, params = self._generic_query()
        results = self.db_functions.make_query(query, params)

        if self.params.utd2:
            results = self._loop_date_range(results)

        stats = {}

        for result in results:
            inst = result['instrument']
            end = result[end_key]
            start = result[start_key]

            if end and start:
                tm = int((end - start).total_seconds())

                if inst in stats:
                    if tm in stats[inst]:
                        stats[inst][tm] += 1
                    else:
                        stats[inst][tm] = 1
                else:
                    stats[inst] = {}
                    stats[inst][tm] = 1

        return stats

    def _generic_query(self, keys=None, key=None, val=None, table='dep_status'):
        """
        Only uses UTD if added as an additional parameter.

        :param key: (str) table column to use.
        :param val: (str) value for column match

        :return: (str, tuple) query string and escaped parameters for query
        """
        if keys and key and val:
            query = f"SELECT {keys} FROM {table} WHERE {key} LIKE %s"
            params = ("%" + val + "%",)
            add_str = " AND "
        elif key and val:
            if full:
                query = f"SELECT * FROM {table} WHERE {key} LIKE %s"
                params = ("%" + val + "%", )
                add_str = " AND "
        elif key:
            query = f"SELECT {key} FROM {table}"
            params = ()
            add_str = " WHERE "
        else:
            query = f"SELECT * FROM {table}"
            params = ()
            add_str = " WHERE "

        if self.params.chk and self.params.chk == 1:
            if self.utd and self.params.utd2:
                query += f" {add_str} utdatetime between %s and %s"
                params += (self.utd, self.params.utd2 + ' 23:59:59')
                add_str = " AND "
            elif self.utd:
                query += f" {add_str} utdatetime LIKE %s"
                params += ("%" + self.utd + "%", )
                add_str = " AND "

        query, params = self._add_general_query(query, params, add_str)

        print(query)
        print(params)

        return query, params

    def _monthly_query(self, day):
        """
        Query a day for the Month Specific table columns

        :param day: (str) day or date to query,  ie 2020-10-31 or 2020-10

        :return: (str, tuple) query string and escaped parameters for query
        """
        query = f'SELECT COUNT(*) FROM dep_status WHERE utdatetime LIKE %s'
        params = ("%" + day + "%",)
        if self.params.inst:
            query += f' AND instrument LIKE %s'
            params += ("%" + self.params.inst + "%",)

        query += self._add_tel_query("AND")

        return query, params

    def _add_general_query(self, query, params, add_str):
        if self.params.inst:
            query += f" {add_str} instrument LIKE %s"
            params += ("%" + self.params.inst + "%", )
            add_str = " AND "

        query += self._add_tel_query(add_str)
        query += " ORDER BY utdatetime DESC"

        if self.limit:
            query += " LIMIT %s"
            params += (self.limit, )

        return query, params

    def _add_tel_query(self, add_str):
        """
        Include the telescope number as part of the DB query.

        :param add_str: (str) the query conjunction.
        :return: (str) the new query
        """
        if self.params.tel == 1 and not self.params.inst:
            add_query = f" {add_str} instrument IN {str(tuple(self.keck1_inst))}"
        elif self.params.tel == 2 and not self.params.inst:
            add_query = f" {add_str} instrument IN {str(tuple(self.keck2_inst))}"
        else:
            add_query = ''

        return add_query

    def read_header(self, koaid):
        """
        Get the header by KOAID.

        :param koaid: (str) the koaid to quewry the header.
        :return:
        """
        query = f"SELECT HEADER FROM headers WHERE KOAID='{koaid}';"

        header = self.db_functions.make_query(query, None)
        if header and type(header) == list:
            header = (dict(header[0])['HEADER'])
            header = json.loads(header)
        else:
            header = {}

        return header

    def is_updated(self, request_time):
        """
        Returns True if resource is updated or it's the first time it has
        been requested.

        :param request_time: (time.time) current time.

        :return: (bool) True if modified after request_time.
        """
        if self.params.dev == 1:
            filepath = "/var/lib/mysql/koa_test/dep_status.ibd"
        else:
            filepath = "/var/lib/mysql/koa/dep_status.ibd"

        try:
            if self.is_daily() and stat(filepath).st_mtime > request_time:
                return True
        except:
            pass

        return False

