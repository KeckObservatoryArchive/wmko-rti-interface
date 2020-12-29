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
        self.tblName = 'koa_test'

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
        result, parsedParams = query_unique_row(self.parsedParams, self.conn,self.tblName)
        ingestErrors = parsedParams['ingestErrors']
        self.assertTrue(len(ingestErrors) == 0, f'ingest errors found: {ingestErrors}')
    
    def test_update_ipac_response_time(self):
        result, parsedParams = update_ipac_response_time(self.parsedParams,self.conn , self.tblName)
        print('update test result'.center(50))
        print(result) 


    def test_update_lev_parameters(self):
        pass

if __name__ == '__main__':
    unittest.main()
