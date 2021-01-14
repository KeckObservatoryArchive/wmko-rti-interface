import pytest
import unittest
import pdb
import sys
from random import randrange, randint, choice
sys.path.append('..')
from ingest_api import * as iaf
import itertools
import string
from datetime import timedelta, datetime
from unittest import mock
import pdb

class MockDB(unittest.TestCase):
    def setUp(self):
        self.CFG = './../config.ini'

    def tearDown(self):
        pass
    
    @mock.patch('iaf.db_conn')
    def test_conn(self,mock_db_conn):
        
        conn = db_conn(self.CFG)
        mock_db_conn.assert_called_with(self.CFG)    

    def query(self, tblName, string):
        pass

    def check_if_unique(self):
        pass

    def check_if_ipac_response_time_exists(self):
        pass

    def update_query(self):
        pass


if __name__ == '__main__':
    unittest.main()
