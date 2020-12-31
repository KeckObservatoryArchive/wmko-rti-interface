import unittest
import pdb
import sys
from test_ingest_api import ingestTestBed
sys.path.append('..')
from ingest_api import *
from db_conn import db_conn
from unittest import mock
import datetime

class dbIngestTestBed(ingestTestBed):
    '''
    Checks that ingestion API correctly adds values according to database. 
    this test bed inherits from ingestTestBed.
    '''

    def setUp(self):
        super().setUp()
        self.dbName = 'koa_test'
        self.tblName = 'dep_status'
        self.TEST_ROW = [{'id': 412, 'koaid': 'HI.20201108.58267.64',\
                         'instrument': 'HIRES',\
                         'utdatetime': datetime.datetime(2020, 11, 8, 16, 11, 7),\
                         'status': 'TRANSFERRED', 'status_code': 'HEADER_TABLE_INSERT_FAIL',\
                         'ofname': '/s/sdata125/hires1/2020nov08_B/j3930126.fits', \
                         'stage_file': '/koadata/test/HIRES/stage/20201108/s/sdata125/hires1/2020nov08_B/j3930126.fits',\
                         'archive_dir': '/koadata/test/HIRES/20201108/lev0', \
                         'creation_time': datetime.datetime(2020, 11, 17, 1, 4, 57), \
                         'dep_start_time': datetime.datetime(2020, 11, 17, 1, 4, 57), \
                         'dep_end_time': datetime.datetime(2020, 11, 17, 1, 4, 59),\
                         'xfr_start_time': datetime.datetime(2020, 11, 17, 1, 4, 59), \
                         'xfr_end_time': datetime.datetime(2020, 11, 17, 1, 5),\
                         'ipac_notify_time': datetime.datetime(2020, 11, 17, 1, 5), \
                         'ipac_response_time': datetime.datetime(2020, 12, 17, 9, 3, 13),\
                         'stage_time': None, 'filesize_mb': 17.67744, 'archsize_mb': 17.937428,\
                         'koaimtyp': 'flatlamp', 'semid': '2020B_NONE',\
                         'last_mod': datetime.datetime(2020, 12, 17, 9, 3, 13), 'ofname_deleted': 0}]

        #reqDict = self.generate_random_query_param_dict()
	# mimick of         # curl http://vm-koarti:55557/ingest_api?instrument=hires&koaid=HI.20201108.58267.64.fits&utdate=2020-11-08&ingesttype=lev0&status=DONE&message-ok        
        reqDict = {}
        reqDict['koaid'] = 'HI.20201108.58267.64.fits'
        reqDict['inst'] = 'HIRES'
        reqDict['status'] = 'COMPLETE'
        reqDict['ingesttype'] = 'lev0'
        reqDict['reingest'] = 'True'
        reqDict['testonly'] = 'True'
        self.parsedParams = parse_params(reqDict)
        self.parsedParams['instrument'] = self.TEST_ROW[0].get('instrument')
        self.parsedParams['koaid'] = self.TEST_ROW[0].get('koaid')
        self.conn = db_conn('./../config.live.ini')

    def tearDown(self):
    	pass

    def test_query_unique_row(self):
        ingestErrors = self.parsedParams['ingestErrors']
        print(ingestErrors)
        self.assertTrue(len(ingestErrors) == 0, f'ingest error should be empty {ingestErrors}')
        result, parsedParams = query_unique_row(self.parsedParams, self.conn,self.dbName)
        ingestErrors = parsedParams['ingestErrors']
        self.assertTrue(len(ingestErrors) == 0, f'ingest errors found: {ingestErrors}')
    
    def test_update_ipac_response_time(self):
        result, parsedParams = update_ipac_response_time(self.parsedParams,self.conn, self.dbName)
        print('update test result'.center(50))
        print(result)

    def transform_db_rows_into_req(self):
        requests = []
        keys = ['id', 'instrument', 'status', 'status_code', 'koaid', ]
        query = f'SELECT {",".join(keys)} FROM {self.tblName} LIMIT 900;'.lower()
        result = self.conn.query(self.dbName, query)
        for row in result:
            t1 = datetime.datetime.now()
            reqDict = { key: value for key, value in row.items()}
            reqDict['testonly'] = True
            reqDict['reingest'] = True
            reqDict['ingesttype'] = 'lev0'
            reqDict['koaid'] = '.'.join([reqDict['koaid'], 'fits'])
            reqDict['inst'] = reqDict.pop('instrument')
            requests.append(reqDict)
        return requests

    def add_requests_to_db(self, requests):
        for reqDict in requests:
            val = {}
            val['instrument'] = reqDict['inst']
            val['status'] = 'COMPLETE'
            val['status_code'] = 'ttucker test row'
            val['ipac_response_time'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            val['koaid'] = reqDict['koaid'].replace('.fits', '')
            dvals = str([x for x in val.values()]).replace('[', '').replace(']', '')
            insertQuery = f"INSERT INTO {self.tblName}(instrument, status, status_code, ipac_response_time, koaid) VALUES({dvals})"
            self.conn.query(self.dbName, insertQuery)

    def remove_requests_from_db(self, requests):
        deleteQuery = f"DELETE FROM {self.tblName} WHERE status_code='ttucker test row';"
        delResult = self.conn.query(self.dbName, deleteQuery)
        selQuery = f"SELECT COUNT(*) FROM {self.tblName} WHERE status_code='ttucker test row';"
        selResult = self.conn.query(self.dbName, selQuery)
        self.assertEqual(selResult[0]['COUNT(*)'], 0, 'check that test entries were removed');

    def generate_unique_random_request_list(self, nSamp=1000):
        '''list of reqDict samples of unique koaid'''
        koaids = set()
        requests = []
        while len(koaids) < nSamp:
            reqDict = self.generate_random_query_param_dict()
            if not reqDict['koaid'] in koaids:
                koaids.add(reqDict['koaid'])
                requests.append(reqDict)
        return requests

    def test_update_lev_parameters(self):
        #  test ingestion of current items in database
        # get lots of database entries
        requests = self.generate_unique_random_request_list(1000)
        self.add_requests_to_db(requests)
        for reqDict in requests:
            t1 = datetime.datetime.now()
            parsedParams = parse_params(reqDict)
            #  check if unique
            queryResult, parsedParams = query_unique_row(parsedParams, self.conn, self.dbName)
            self.assertNotEqual(len(queryResult), 1, 'check query_unique_row')

            #  verify that status is TRANSFERRED, ERROR or COMPLETE
            self.assertTrue(queryResult['status'] in VALID_DB_STATUS_VALUES, 'check status')

            updateRes, parsedParams = update_ipac_response_time(parsedParams, self.conn, self.dbName, reqDict.get('status_code'))
            t2 = datetime.datetime.now()
            dt = (t2-t1).total_seconds()
            self.assertEqual(updateRes,1, 'check that update res is working')
            print(f'update took {dt} seconds'.center(50, '='))

        self.remove_requests_from_db(requests)

            


if __name__ == '__main__':
    unittest.main()
