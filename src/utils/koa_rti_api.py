import json

from datetime import datetime, timedelta
import os
from os import stat

from utils.koa_rti_helpers import query_prefix, date_iter
from utils.koa_rti_pykoa import PyKoaApi
from utils.koa_rti_db import DatabaseInteraction
from utils.koa_rti_plots import TimeBarPlot, OverlayTimePlot


class KoaRtiApi:

    def __init__(self, var_get):
        self.db_functions = DatabaseInteraction()

        self.query_opts = ['None', 'HEADER', 'Image_Type', 'KOAID',
                           'Last_Entry', 'STATUS', 'STATUS_CODE', 'SEMID']
        self.status_opts = ['COMPLETE', 'TRANSFERRED', 'TRANSFERRING',
                            'PROCESSING', 'ERROR', 'INVALID', 'WARN']
        self.status_code_opts = ['INVALID_PROGID_WARN', 'SET_DATEOBS_WARN',
                                 'MD_DISCRETE_VAL_ERROR', 'MD_TRUNCATE']
        self.img_types = ['OBJECT', 'BIAS', 'ARCLAMP', 'FLATLAMP', 'FOCUS']

        self.keck1_inst = ['None', 'HIRES', 'LRIS', 'MOSFIRE', 'OSIRIS']
        self.keck2_inst = ['None', 'DEIMOS', 'ESI', 'KCWI', 'NIRES', 'NIRC2',
                           'NIRSPEC']

        # change these to change the order of the results
        self.query_keys = ['STATUS', 'KOAID', 'UTDATETIME', 'Filename',
                           'INSTRUMENT', 'KOAIMTYP', 'SEMID', 'Stage_Dir',
                           'Process_Dir', 'Archive_Dir', 'Status_Code',
                           'Creation_Time', 'Dep_Start_Time', 'Dep_End_Time',
                           'IPAC_Notify_Time', 'IPAC_Response_Time',
                           'Stage_time', 'Filesize_MB', 'Archsize_MB',
                           'Last_Mod', 'OFNAME', 'ID']

        self.monthly_header = ['Date', 'Total_Files']
        self.monthly_header += self.status_opts + ['Instruments']

        # pre-pend num_ for the key to 'stats'
        self.health_head = ['Pass', 'Warn', 'Error', 'Skip']

        self.default_num_columns = 7
        self.search_val = var_get.val
        self.update_val = var_get.update_val
        self.params = var_get
        self.limit = var_get.limit
        self.utd = var_get.utd

        self.table_view = None
        self.change_table_name(var_get.view)

        if var_get.yr and var_get.month:
            self.monthly_date = f'{var_get.yr}-{var_get.month:0>2}'
        else:
            self.monthly_date = datetime.utcnow().strftime("%Y-%m")

    def get_date_range(self):
        dates = (self.utd, self.params.utd2)

        return dates

    def getOptionLists(self):
        """
        Get a dictionary of the option lists.

        :return: (dict/list) option lists
        """
        opt_lists = {'query': self.query_opts, 'status': self.status_opts,
                     'status_code': self.status_code_opts,
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

    # --- pyKoa section ---
    def pykoaALL(self):
        # koaid, filehand, progid, semid, imagetyp
        pykoa_api = PyKoaApi()
        results = pykoa_api.progid_results(self.params.progid)

        return results

    # --- Begin search section ---
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
        query, params = self._generic_query(key="STATUS", val=self.search_val)

        return self.db_functions.make_query(query, params)

    def searchSTATUSCODE(self):
        """
        Find all results with a certain status.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query(key="STATUS_CODE", val=self.search_val)

        return self.db_functions.make_query(query, params)

    def searchKOAID(self):
        """
        Find all results with the defined KOAID.  Results should be one row.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query(key="KOAID", val=self.search_val)

        return self.db_functions.make_query(query, params)

    def searchSEMID(self):
        """
        Find all results with the defined SEMID.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query(key="SEMID", val=self.search_val)

        return self.db_functions.make_query(query, params)

    def searchIMAGETYPE(self):
        """
        Find all results with the defined Image Type.

        :return: (list) row/columns to be used for the table.
        """
        query, params = self._generic_query(key="KOAIMTYP", val=self.search_val)

        return self.db_functions.make_query(query, params)

    def searchHEADER(self):
        self.query_keys = ['KOAID', 'STATUS', 'HEADER_KEYWORD', 'HEADER_VALUE',
                           'HEADER_COMMENT', 'INSTRUMENT', 'KOAIMTYP', 'SEMID',
                           'LAST_MOD', 'STAGE_FILE']
        params = ()
        query = "SELECT headers.*, status, stage_file, instrument, koaimtyp, "
        query += "semid FROM headers JOIN koa_status ON headers.koaid"
        query += " = koa_status.koaid"

        query, params = self._add_general_query(query, params, "WHERE")
        results = self.db_functions.make_query(query, params)

        for i, result in enumerate(results):
            result['last_mod'] = result.get('last_mod', None)
            result['header_keyword'] = self.search_val

            header = result.get('header', None)
            result['header_value'] = None
            result['header_comment'] = None
            if header:
                head_dict = json.loads(header)
                head_vals = head_dict.get(self.search_val, None)
                if head_vals:
                    result['header_value'] = head_vals.get('value', None)
                    result['header_comment'] = head_vals.get('comment', None)

            result['header'] = None
            results[i] = result

        return results

    def searchKOATPX(self):
        """
        Find all results for the TPX GUI.

        :return: (list) row/columns to be used for the table.
        """
        query = "select koatpx.*,koadrp.endTime as lev1_done from koatpx left "
        query += "join koadrp on koatpx.instr=koadrp.instr and koatpx.utdate="
        query += "koadrp.utdate where "
        if self.params.inst:
            query += "koatpx.instr like %s and "
            params = (self.params.inst,)
        else:
            params = ()
        if self.params.utd2:
            query += "koatpx.utdate >= %s and koatpx.utdate <= %s"
            params += (self.utd, self.params.utd2)
        else:
            query += "koatpx.utdate like %s"
            params += (self.utd,)
        query += " order by utdate desc, instr asc"

        results = self.db_functions.make_query(query, params, "koaserver")

        return results

    def searchKOADRP(self):
        """
        Find all results for the TPX DRP GUI.

        :return: (list) row/columns to be used for the table.
        """
        query = "select * from koadrp where "
        if self.params.inst:
            query += "instr like %s and "
            params = (self.params.inst, )
        else:
            params = ()

        if self.params.utd2:
            query += "utdate >= %s and utdate <= %s"
            params += (self.utd, self.params.utd2, )
        else:
            query += "utdate like %s order by utdate desc, instr asc"
            params += (self.utd, )

        results = self.db_functions.make_query(query, params, "koaserver")

        return results

    def searchLEV1(self, columns=None):
        """
        Search for lev1 (DRP) files.

        :return: [{results}] array of DB search results
        """
        if not columns:
            columns = self.params.columns
        query, params, add_str = query_prefix(
            columns, self.params.key, self.params.val)

        query += f" {add_str} LEVEL=1 "
        add_str = "AND"

        if self.params.add:
            query += f" {add_str} {self.params.add} "
            add_str = "AND"

        query = self._add_koaid_daterange(query, 'koaid', add_str)
        results = self._lev1_query(query, params)

        return results

    def _add_koaid_daterange(self, query, val, add_str):
        utd2 = self.params.utd2
        if self.utd and not utd2:
            utd2 = self.utd

        current_date = datetime.strptime(self.utd, '%Y-%m-%d')
        end_date = datetime.strptime(utd2, '%Y-%m-%d')

        date_str = current_date.strftime('%Y%m%d')
        query += f" {add_str} ({val} LIKE {date_str}"


        while current_date < end_date:
            date_str = current_date.strftime('%Y%m%d')
            current_date += timedelta(days=1)
            query += f" OR {val} LIKE '%{date_str}%'"

        query += ")"

        return query

    def _lev1_query(self, query, params):
        try:
            results = self.db_functions.make_query(query, params)
        except Exception as err:
            raise ValueError(err)

        return results

    def searchGENERAL(self):
        query, params = self._generic_query(
            columns=self.params.columns, key=self.params.key,
            val=self.search_val, add=self.params.add)

        try:
            results = self.db_functions.make_query(query, params)
        except Exception as err:
            raise ValueError(err)

        return results

    # -- update section for api --
    def updateGENERAL(self):
        """
        General update function to update a single column in the koa_satus table.

        :return:
        """
        query = f"UPDATE koa_status SET {self.params.columns}=%s "
        query += f"WHERE {self.params.key}=%s"
        params = (self.params.update_val, self.search_val)

        if self.params.add:
            query += f'AND {self.params.add}'

        try:
            self.db_functions.make_query(query, params)
        except Exception as err:
            return str(err)

        self.utd = None
        results = self.searchGENERAL()

        return results

    def updateMARKDELETED(self):
        query = f"UPDATE koa_status SET source_deleted = True WHERE koaid=%s"
        params = (self.params.val, )

        return query + str(params)

    def update_status_reviewed(self, dbid, val):
        """
        Update koa_status.reviewed

        :return:
        """
        query = ("UPDATE koa_status SET reviewed=%s "
                 "WHERE id=%s;")
        params = (val, dbid)

        try:
            res = self.db_functions.make_query(query, params)
        except Exception as err:
            return str(err)
        return res

    def get_log(self, dbid, format):
        """
        Returns the log file contents of a processed record
        :return: (str) log text
        """
        query = "select * from koa_status where id=%s"
        params = (dbid)
        rows = self.db_functions.make_query(query, params)
        if not rows:
            return "LOG NOT FOUND"

        row = rows[0]
        filepath = f"{row['process_dir']}/{row['koaid']}.log"
        if not os.path.exists(filepath):
            return f"Log path not found: {filepath}"

        txt = ''
        with open(filepath) as f:
            txt += f.read()

        if format == 'html':
            lines = txt.split("\n")
            txt = ''
            for line in lines:
                if   "ERROR" in line.upper(): line = f"<span style='background-color:#ee8888'>{line}</span>"
                elif "WARN"  in line.upper(): line = f"<span style='background-color:#eecc88'>{line}</span>"
                txt += line + "<br>"

        return txt


    """  ------------  monthly view section  ------------  """
    def monthlyINST(self, day):
        """
        Determine the instruments used over the month.

        :param day: (str) date of results to get,  can be full or partial date.

        :return: (str) comma separated instruments
        """
        query = f'SELECT DISTINCT instrument FROM koa_status '
        query += f'WHERE utdatetime LIKE %s AND level=0'
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
    

    """  ------------  metrics API section  ------------  """
    def metricsPROCESS(self):
        """
        Find the the time difference between creation and process end.

        /koarti_api?time=PROCESS&month=6

        :return: [{koaid, instrument, seconds}]
        """
        stats, sums = self._get_time_diff('creation_time', 'process_end_time')

        return stats, sums

    def metricsTRANSFER(self):
        """
        Find the the time difference between transfer start and end.

        :return: [{koaid, instrument, seconds}]
        """
        stats, sums = self._get_time_diff('xfr_start_time', 'xfr_end_time')

        return stats, sums

    def metricsINGEST(self):
        """
        Find the the time difference between ingestion start and end.

        :return: [{koaid, instrument, seconds}]
        """
        stats, sums = self._get_time_diff('ingest_start_time', 'ingest_end_time')

        return stats, sums

    def metricsCOPYINGEST(self):
        """
        Find the the time difference between copyingestion start and end.

        :return: [{koaid, instrument, seconds}]
        """
        stats, sums = self._get_time_diff('ingest_copy_start_time', 'ingest_copy_end_time')

        return stats, sums

    def metricsDRP(self):
        """
        Find the time difference between the end of the KOA processing and the
        end of the DRP processing.

        :return:
        """
        stats, sums = self._get_time_diff('lev0.process_end_time', 'lev1.creation_time')

        return stats, sums

    def metricsTOTAL(self):
        """
        Find the the time difference between start and end.

        :return: [{koaid, instrument, seconds}]
        """
        stats, sums = self._get_time_diff('creation_time', 'ingest_end_time')

        return stats, sums

    """  ------------  metrics Plots section  ------------  """
    def statDrpTime(self):
        stats = self._bin_time_length('lev0.process_end_time', 'lev1.creation_time')

        plot_obj = OverlayTimePlot(stats, 'Processing Time', xrange=72000)

        return plot_obj.get_plot()

    def statProcessTime(self):
        stats = self._bin_time_length('creation_time', 'process_end_time')

        plot_obj = OverlayTimePlot(stats, 'Processing Time')

        return plot_obj.get_plot()

    def statTransferTime(self):
        stats = self._bin_time_length('xfr_start_time', 'xfr_end_time')

        plot_obj = OverlayTimePlot(stats, 'Transfer Time - Transfer Start to End')

        return plot_obj.get_plot()

    def statIngestTime(self):
        stats = self._bin_time_length('ingest_start_time', 'ingest_end_time')

        plot_obj = OverlayTimePlot(stats, 'Ingest Time')

        return plot_obj.get_plot()

    def statTotalTime(self):
        stats = self._bin_time_length('creation_time', 'ingest_end_time')
        plot_obj = OverlayTimePlot(stats, 'Total Time - File Write to IPAC Response')
        return plot_obj.get_plot()

    def getPlots(self):
        """ Determine the plots to return by the input plot type """
        if not self.params.plot or self.params.plot == 0:
            results = {'plots': [self.statTotalTime()]}
        elif self.params.plot == 1:
            results = {'plots': [self.statProcessTime()]}
        elif self.params.plot == 2:
            results = {'plots': [self.statTransferTime()]}
        elif self.params.plot == 3:
            results = {'plots': [self.statIngestTime()]}
        elif self.params.plot == 4:
            results = {'plots': [self.statDrpTime()]}
        else:
            results = {'plots': [self.statTotalTime(), self.statProcessTime(),
                                 self.statTransferTime(), self.statIngestTime()]}

        return results

    """  ------------  Helpers section  ------------  """

    def monthly_results(self):
        """
        Find the sums over a month by day.

        :return: (list) a list of rows/columns for the monthly table
        """
        results = []
        yr, mo = self.monthly_date.split('-')
        for day in date_iter(yr, mo):
            tally = self.monthlyTOTAL(day)
            insts = self.monthlyINST(day)

            day_result = {'date': day, 'total_files': tally,
                          'instruments': insts}
            for key in self.status_opts:
                keyword = key.lower()
                day_result[keyword] = self.monthlySTATUS(keyword, day)

            results += [day_result]

        return results

    def change_table_name(self, table_view):
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

    def _get_time_diff(self, start_key, end_key, table='koa_status'):
        """
        Find the time difference

        :param start_key: <str> The database field for the starting time.
        :param end_key: <str> The database field for the ending time.
        :param table: <str> The database table name.
        :return: [{koaid, instrument, TIMEDIFF}]
        """
        sums = {}
        cln_results = {}

        if not start_key or not end_key:
            return [], sums

        unit = "SECOND"
        tdiff_str = f"TIMEDIFF({end_key}, {start_key})"
        if 'lev0' in start_key or 'lev0' in end_key:
            results_dict = self._drp_results(tdiff_str, table)
        else:
            fields = f'koaid, instrument, level, filesize_mb, archsize_mb, {tdiff_str}'
            results_dict = self._metrics_results(fields, table)

        for level in results_dict.keys():
            cln_results[level] = []
            sums[level] = {'total_time_seconds': 0,
                           'total_filesize': 0.0,
                           'total_arch_size': 0.0}
            results = results_dict[level]
            for result in results:
                try:
                    seconds = result[tdiff_str].total_seconds()
                    if seconds < 0:
                        continue
                except Exception:
                    continue

                sums[level] = self._metrics_sums(sums[level], result, seconds)
                result['seconds'] = seconds
                del result[tdiff_str]
                cln_results[level].append(result)

        return cln_results, sums

    def _metrics_results(self, fields, table):
        results = {}

        # level can be 0
        if not self.params.level:
            query, params = self._generic_query(key=fields, table=table)
            results['lev0'] = self.db_functions.make_query(query, params)

        if self.params.level in [None, 1]:
            results['lev1'] = self.searchLEV1(columns=fields)

        return results

    def _drp_results(self, tdiff, table):

        results = {'DRP': []}

        query = f"SELECT lev0.koaid, lev0.instrument, lev1.level, " \
                f"lev0.filesize_mb, lev1.archsize_mb, {tdiff} " \
                f"FROM {table} lev1, {table} lev0 " \
                f"WHERE lev0.koaid=lev1.koaid AND lev0.level=0 AND lev1.level=1"

        query = self._add_koaid_daterange(query, 'lev1.koaid', " AND ")

        results['DRP'] = self.db_functions.make_query(query, ())

        return results

    def _metrics_sums(self, sums, result, seconds):
        sums['total_time_seconds'] += seconds
        filesize = result.get('filesize_mb', 0.0)
        if filesize:
            sums['total_filesize'] += filesize
        archive_size = result.get('archsize_mb', 0.0)
        if archive_size:
            sums['total_arch_size'] += archive_size

        return sums

    def _bin_time_length(self, start_key, end_key):
        """
        Tally the number for each time bin.  ie, {DEIMOS : {'0:00:02: 42, ...}}

        :return: (dict/dict/sum)
        """
        stats = {}
        results_dict, sums = self._get_time_diff(start_key, end_key)

        for level in results_dict.keys():
            results = results_dict[level]

            for result in results:
                inst = f"{result['instrument']}_{level}"
                tm = int(result['seconds'])

                if inst in stats:
                    if tm in stats[inst]:
                        stats[inst][tm] += 1
                    else:
                        stats[inst][tm] = 1
                else:
                    stats[inst] = {}
                    stats[inst][tm] = 1

        return stats

    def _generic_query(self, columns=None, key=None, val=None,
                       add=None, table='koa_status'):
        """
        Only uses UTD if added as an additional parameter.

        :param key: (str) table column to use.
        :param val: (str) value for column match

        :return: (str, tuple) query string and escaped parameters for query
        """
        query, params, add_str = query_prefix(columns, key, val, table)

        if self.params.chk and self.params.chk == 1:
            if self.utd and self.params.utd2:
                query += f" {add_str} utdatetime between %s and %s"
                params += (self.utd, self.params.utd2 + ' 23:59:59')
                add_str = " AND "
            elif self.utd:
                query += f" {add_str} utdatetime LIKE %s"
                params += ("%" + self.utd + "%", )
                add_str = " AND "

        if add:
            query += f" {add_str} {add} "

        query, params = self._add_general_query(query, params, add_str)

        return query, params

    def _monthly_query(self, day):
        """
        Query a day for the Month Specific table columns

        :param day: (str) day or date to query,  ie 2020-10-31 or 2020-10

        :return: (str, tuple) query string and escaped parameters for query
        """
        query = f'SELECT COUNT(*) FROM koa_status WHERE utdatetime LIKE %s'
        query += f' AND level=0'
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

    def has_changed(self, request_time):
        """
        Returns True if resource is updated or it's the first time it has
        been requested.

        :param request_time: (time.time) current time.

        :return: (bool) True if modified after request_time.
        """
        filepath = "/var/lib/mysql/koa/koa_status.ibd"

        try:
            if self.is_daily() and stat(filepath).st_mtime > request_time:
                return True
        except:
            pass

        return False

