# from .. import ingest_api_fun
import pytest
import unittest
import pdb
import sys
sys.path.append('..')
from ingest_api_fun import *
import itertools

class ingestTestBed(unittest.TestCase):

    @staticmethod
    def generate_bad_strings(badString, repeat=2):
        '''
        creates generator of a cartesian product (all combinations) of badString
        Inputs:
        badString is an iterator (i.e. list, tuple, set)
        repeat lists how many elements to combine into one string
        Output:
        cartesian product of badString
        ex: list(generate_bad_strings(['a', 'b', 'c'], repeat=2))
        returns: ['ab', 'bc', 'ca', ']
        '''
        for elem in itertools.combinations(badString, r=repeat):
            yield "".join(elem)

    def setUp(self):
        self.BAD_STRINGS = {'', ' ', '', '\n', '\r', '\t'}

    def tearDown(self):
        pass


    def test_parse_status(self):
        '''removes all whitespace and set to upper. Resulting string must be contained in STATUS_SET'''

        # all whitespace
        for status in self.generate_bad_strings(self.BAD_STRINGS, 2):
            try:
                pStatus = parse_status(status)
                self.assertTrue(pStatus in STATUS_SET)
            except AssertionError as err:
                self.assertTrue('status is blank' in str(err))
        STATUS_SET_WS = self.BAD_STRINGS.union(STATUS_SET)
        STATUS_SET_WS.add('Error')
        STATUS_SET_WS.add('DoNe')
        # whitespace with valid strings
        for status in self.generate_bad_strings(STATUS_SET_WS, 2):
            try:
                pStatus = parse_status(status)
                if len(pStatus) == 0:
                    continue # ignore whitespace for this test
                self.assertTrue(pStatus in STATUS_SET)
            except AssertionError as err:
                continue

        INVALID_STATUS_SET = {'Err', 'Eror', 'CompleTED', '0'}.union()
        INVALID_STATUS_SET_WS = set([INVALID_STATUS_SET_WS.add(ST) for ST in INVALID_STATUS_SET])
        # invalid strings with no whitespace
        for status in self.generate_bad_strings(self.INVALID_STATUS_SET_WS, 2):
            with self.assertRaises(AssertionError) as context:
                parse_status(status)
                status = ''.join(status.split()).upper()
            self.assertTrue(f'{status} not in STATUS_SET' in str(context.exception))
        
        # invalid strings with whitespace
        for status in self.generate_bad_strings(INVALID_STATUS_SET, 1):
            with self.assertRaises(AssertionError) as context:
                parse_status(status)
                status = ''.join(status.split()).upper()
            self.assertTrue(f'{status} not in STATUS_SET' in str(context.exception))




    # def test_parse_inst(inst):
    #     # '''removes all whitespace and set to upper. Resulting string must be found in INST_SET'''
    #     # inst = ''.join(inst.split()).upper()
    #     # assert inst in INST_SET, 'instrument not found in set'
    #     # return inst 

    # def test_parse_utdate(utdate):
    #     # try:
    #     #     datetime = datetime.datetime.strptime(utdate, '%Y-%m-%d')
    #     # except:
    #     #     raise Exception('date not valid. Is the format YYYY-MM-DD?')
    #     # return utdate 

    # def test_parse_koaid(koaid):
    #     '''
    #     koaid is run through assertions to check that it fits koaid format II.YYYYMMDD.SSSSS.SS.fits as described in 
    #     https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    #     '''
    #     # inst, date, seconds, dec, ftype = koaid.split('.')
    #     # assert inst in INST_SET, 'instrument not valid'
    #     # try:
    #     #     datetime = datetime.datetime.strptime(date, '%Y%m%d')
    #     # except:
    #     #     raise Exception('date not valid. Is the format YYYYMMDD?')
    #     # assert len(seconds) == 5, 'check seconds length'
    #     # assert seconds.isdigit(), 'check if seconds is positive integer'
    #     # assert len(dec) == 2, 'check decimal length'
    #     # assert dec.isdigit(), 'check if decimal is positive integer'
    #     # uttime = ''.join([seconds, dec])
    #     # assert float(uttime) < 86400, 'seconds exceed day'
    #     # assert fit == '.fits', 'check file type'
    #     # return koaid

    # def test_parse_status(status):
    #     '''
    #     checks if status is acceptible as stated in  
    #     https://keckobservatory.atlassian.net/wiki/spaces/DSI/pages/402882573/Ingestion+API+Interface+Control+Document+for+RTI
    #     '''
    #     # assert status in STATUS_SET, 'status not found in STATUS_SET'
    #     # return status

    # def test_parse_message(msg):
    #     return msg

    # def test_parse_reingest(reingest):
    #     '''remove whitespace and check if valid'''
    #     # reinjest = ''.join(reinjest.split())
    #     # assert reingest.lower() in VALID_BOOL, 'reingest not valid boolean'
    #     # return reinjest 

    # def test_parse_dev(dev):
    #     '''remove whitespace and check if valid'''
    #     # dev = ''.join(dev.split())
    #     # assert dev.lower() in VALID_BOOL, 'dev not valid boolean'
    #     # return dev

    # def test_parse_ingesttype(self):
    #     '''remove whitespace and set to lowercase. Result should be in INGEST_TYPES set'''
    #     self.assertRaises(AssertionError, parse_ingesttype(' '))
    #     # ingesttype = ''.join(ingesttype.split())
    #     # assert ingesttype in INGEST_TYPES, 'ingesttype not valid'
    #     # return ingesttype

if __name__ == '__main__':
    print('__file__={0:<35} | __name__={1:<20} | __package__={2:<20}'.format(__file__,__name__,str(__package__)))



    unittest.main()
