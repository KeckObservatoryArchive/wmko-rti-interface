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

    def err_message_match(self, param, key, approvedSet, err):
        return str(err).upper() in is_blank_msg(param).upper() or str(err).upper() in not_in_set_msg(param, approvedSet).upper()

    def valid_set_checker(self, approvedSet, parse_fun, key):

        #  smoke test
        for testParam in approvedSet:
            try:
                parsedString = parse_fun(testParam)
                self.assertTrue(True)
            except Exception as err:
                self.assertTrue(False, str(err))

        #  only whitespace
        for testParam in self.generate_bad_strings(self.BAD_STRINGS, 2):
            try:
                parsedString = parse_fun(testParam)
                self.assertFalse(parsedString in approvedSet)
            except AssertionError as err:
                errMsgMatch = self.err_message_match(testParam, key, approvedSet, err)
                if not errMsgMatch:
                    pdb.set_trace()
                self.assertTrue(errMsgMatch)

        #  valid strings with whitespace
        approvedSetWithWhitespace = self.BAD_STRINGS.union(STATUS_SET)
        for testParam in self.generate_bad_strings(approvedSetWithWhitespace, 2):
            try:
                parsedString = parse_fun(testParam)
                self.assertTrue(parsedString in approvedSet)
            except AssertionError as err:
                continue

    def invalid_set_checker(self, approvedSet, invalidSet, parse_fun, key):
        invalidSetWithWhitespace = self.BAD_STRINGS.union(invalidSet)
        #  invalid strings with whitespace
        for testParam in self.generate_bad_strings(invalidSetWithWhitespace, 2):
            try:
                parsedString = parse_fun(testParam)
                self.assertFalse(parsedString in approvedSet)
            except AssertionError as err:
                strP = ''.join(testParam.split()).upper()
                blankErrMsg = is_blank_msg(strP).upper()
                notInSetMsg = not_in_set_msg(strP, approvedSet).upper()
                errMsgMatch = self.err_message_match(strP, key, approvedSet, err)
                self.assertTrue(errMsgMatch)
        
        #  invalid strings with no whitespace
        for testParam in self.generate_bad_strings(invalidSet, 1):
            try:
                parsedString = parse_fun(testParam)
                self.assertFalse(parsedString in approvedSet)
            except AssertionError as err:
                strP = ''.join(testParam.split()).upper()
                errMsgMatch = self.err_message_match(strP, key, approvedSet, err)
                self.assertTrue(errMsgMatch)

    def test_parse_status(self):
        invalidSet = {'Err', 'Eror', 'CompleTED', '0'}
        self.valid_set_checker(STATUS_SET, parse_status, key='status')
        self.invalid_set_checker(STATUS_SET, invalidSet, parse_status, key='status')

    def test_parse_inst(self):
        invalidSet = {'dee', 'ri', 'new_inst', 'eyeball'}
        self.valid_set_checker(INST_SET, parse_inst, key='inst')
        self.invalid_set_checker(INST_SET, invalidSet, parse_inst, key='inst')

    def test_parse_ingesttype(self):
        invalidSet = {'LEV0oops', 'defcon5', 'pft', 'ttry'}
        self.valid_set_checker(INGEST_TYPES, parse_ingesttype, key='ingesttype')
        self.invalid_set_checker(INGEST_TYPES, invalidSet, parse_ingesttype, key='ingesttype')

    def test_parse_reingest(self):
        invalidSet = {'yep', 'nu-uh', 'negatory'}
        self.valid_set_checker(VALID_BOOL, parse_reingest, key='reingest')
        self.invalid_set_checker(VALID_BOOL, invalidSet, parse_reingest, key='reinget')

    def test_parse_testonly(self):
        invalidSet = {'yep', 'nu-uh', 'negatory', 'ok...'} 
        self.valid_set_checker(VALID_BOOL, parse_testonly, key='testonly')
        self.invalid_set_checker(VALID_BOOL, invalidSet, parse_testonly, key='testonly')  

    def test_parse_utdate(self):
        goodDates = {'2020-03-01', '2020-02-29', '2020-10-31'}
        badDates = {'2020-03-99', '20-12-31', '31-12-2020', '2029-02-29', '1977-1-1'}
        #  smoke test
        for testDate in goodDates:
            parsedDate = parse_utdate(testDate)
            self.assertTrue(True)

        for testDate in badDates:
            try:
                parsedDate = parse_utdate(testDate)
            except DateParseException as err:
                self.assertEqual(str(err), 'date not valid. Is the format YYYY-MM-DD?')

    def test_parse_koaid(self):
        validKoaids = {}
        invalidKoaids = {}
        for testId in validKoaids:
            pkoaid = parse_koaid(testId)
            self.assertTrue(True)

        for testId in invalidKoaids:
            try:
                pkoaid = parse_koaid(testId)
            except Exception as err:
                self.assertEqual(str(err))

    def test_parse_message(self):
        msg = 'a message sHould NoT Be \n CHANGED.'
        pMsg = parse_message(msg)
        self.assertEqual(msg, pMsg)

if __name__ == '__main__':
    unittest.main()
