import unittest
import pdb
import sys
from test_ingest_api_fun import ingestTestBed
sys.path.append('..')
from ingest_api_fun import *
from db_conn import db_conn

class dbIngestTestBed(ingestTestBed):
    '''
    Checks that ingestion API correctly adds values according to database. 
    this test bed inherits from ingestTestBed.
    '''
    


'''
    def setUp(self):
	self.conn = db_conn('./../config.ini')
    def tearDown(self):
    	pass
'''
    def test_update_lev_parameters(self):
	reqDict = self.generate_random_query_param_dict()
        reqDict['reingest'] = True
        reqDict['testonly'] = True

if __name__ == '__main__':
    unittest.main()
