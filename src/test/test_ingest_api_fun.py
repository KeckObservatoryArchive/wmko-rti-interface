
# from .. import ingest_api_fun
import pytest
import unittest
import pdb
import sys
from random import randrange, randint, choice
sys.path.append('..')
from ingest_api_fun import *
import itertools
import string
from datetime import timedelta, datetime

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

    @staticmethod
    def generate_date_sample(baseDateStr='1990-01-01', nSamp=10, dformat='%Y-%m-%d'):
        '''
        samples dates falling within baseDate and today nSamp times.
        '''

        dateSet = set()
        today = datetime.today()
        baseDate = datetime.strptime(baseDateStr, dformat)
        dayRange = (today-baseDate).days
        while len(dateSet) < nSamp:
            randDays = randrange(dayRange)
            sampleDate = baseDate + timedelta(days=randDays)
            dateSet.add(sampleDate.strftime(dformat))
        return dateSet

    def generate_koaid(self, baseDateStr='19900101', nDateSamp=10, dformat='%Y%m%d'):
        dates = self.generate_date_sample(baseDateStr, nDateSamp, dformat)
        seconds = [str(randrange(0, 86400, 1000)).zfill(5) for _ in range(0, nDateSamp)]
        dec = [str(randrange(0, 100)).zfill(2) for _ in range(0, nDateSamp)]
        ftype = ['fits']
        koaids = []
        for inst, date, sec, dec, ftype in  itertools.product(*[INST_SET_ABBR, dates, seconds, dec, ftype]):
            koaid = '.'.join([inst, date, sec, dec, ftype])
            koaids.append(koaid)
        return koaids

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
        '''
        instrument should have full name, case insensitive
        '''
        invalidSet = {'dee', 'ri', 'new_inst', 'eyeball'}
        approvedSet = INST_SET.union([x.lower() for x in INST_SET])
        self.valid_set_checker(approvedSet, parse_inst, key='inst')
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
        goodDates = self.generate_date_sample(baseDateStr='19000101', nSamp=25, dformat='%Y%m%d') #{'2020-03-01', '2020-02-29', '2020-10-31'}
        badDates = {'20200399', '201231', '31122020', '20290229', '197711'}
        #  smoke test
        dformat = '%Y%m%d'
        for testDate in goodDates:
            parsedDate = parse_utdate(testDate, dformat)
            self.assertTrue(True)
        for testDate in badDates:
            try:
                parsedDate = parse_utdate(testDate, dformat)
            except DateParseException as err:
                self.assertEqual(str(err), f'date {testDate} not valid. Is the format YYYY-MM-DD?')

    def test_parse_koaid(self):
        validKoaids = self.generate_koaid()
        for testId in validKoaids:
            pkoaid = parse_koaid(testId)
            self.assertTrue(True)

    def test_parse_message(self):
        msg = 'a message sHould NoT Be \n CHANGED.'
        pMsg = parse_message(msg)
        self.assertEqual(msg, pMsg)

    @staticmethod
    def sample_from_set(mySet):
        samp = tuple(mySet)[randint(0, len(mySet)-1)]
        return samp

    def generate_random_query_param_dict(self):
        reqDict = dict()
        koaid = self.generate_koaid(nDateSamp=1)[0]
        instAbbr, utdate, seconds, dec, ftype = koaid.split('.')
        # VALID_INST = INST_MAPPING[instAbbr] # gets set of instrument tags
        validInstKey = [key for (key, vals) in INST_MAPPING.items() if instAbbr in vals][0]  # get long name of instrument
        reqDict['inst'] = validInstKey 
        reqDict['ingesttype'] = self.sample_from_set(INGEST_TYPES)
        reqDict['koaid'] = koaid
        reqDict['status'] = self.sample_from_set(STATUS_SET)
        if reqDict['status'] == 'ERROR':
            reqDict['message'] = 'status is ERROR'
        return reqDict

    def test_parse_query_param(self):
        for _ in range(0, 1000):
            reqDict = self.generate_random_query_param_dict()
            parsedParams = parse_params(reqDict)
            self.assertFalse(parsedParams.get('ingestErrors', False), 'there should be no ingest errors')

        # test if required fields missing error is working
        for _ in range(0, 1000):
            reqDict = self.generate_random_query_param_dict()
            for _ in range(randint(1,3)):
                reqDict.pop(choice(list(reqDict.keys())))

            parsedParams = parse_params(reqDict)
            ingestErrors = parsedParams.get('ingestErrors', False)
            self.assertTrue(ingestErrors, 'there should be ingest errors')
            self.assertNotEqual(len(ingestErrors), 0, 'there should be a missing key error')
    
    def test_for_missing_status_message(self):
        '''
        if status == ERROR there should be a message
        '''
        for _ in range(0, 100):
            reqDict = self.generate_random_query_param_dict()
            parsedParams = parse_params(reqDict)
            if reqDict.get('status') == 'ERROR':
                self.assertTrue(parsedParams.get('message', False), 'status should have message')
                reqDict.pop('message')
                pp = parse_params(reqDict)
                ingestErrors = pp['ingestErrors']
                self.assertEqual(len(ingestErrors), 1, 'should be one ingestError')
                self.assertTrue('should include a message' in ingestErrors[0], f'check for correct error message {ingestErrors[0]}')

    def test_parse_no_query_param(self):
        '''
        no params returns an error message
        '''
        parsedParams = parse_params(dict())
        self.assertEqual(parsedParams['apiStatus'], 'ERROR', 'error should be reported')
        self.assertEqual(len(parsedParams['ingestErrors']), 1, 'json should include one ingestError')
        self.assertEqual(parsedParams['ingestErrors'][0], 'required params not included', 'check error msg')

    def test_parse_invalid_param(self):
        '''
        invalid param returns an error
        '''
        letters = string.ascii_letters

        for _ in range(0, 500):
            reqDict = self.generate_random_query_param_dict()
            # inject invalid param(s)
            lValidKeys = len(reqDict)
            for _ in range(randint(1, 5)):
                lKey = randint(0,10)
                lValue = randint(0, 100)
                key = ''.join(choice(letters) for x in range(lKey)) # slight chance this will be valid...
                value = ''.join(choice(letters) for x in range(lValue))
                reqDict[key] = value

            nInvalidParams = len(reqDict) - lValidKeys
            
            parsedParams = parse_params(reqDict)
            self.assertEqual(parsedParams['apiStatus'], 'ERROR', 'error should be reported')
            self.assertEqual(len(parsedParams['ingestErrors']), nInvalidParams, f'number of errors should be {nInvalidParams}')


            


if __name__ == '__main__':
    unittest.main()
