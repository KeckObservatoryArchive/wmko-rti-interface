import json
import requests
from pykoa.koa import Koa
from astropy.table import Table, Column


class PyKoaApi:
    def __init__(self):
        self.id = None

    def login(self, obsid, user, pw):
        # obsid is sent from the PILogin
        return

    def query_prog_stats(self, progid):
        pykoa_results = json.loads(self.query_by_semid(progid, 'hires'))
        if not pykoa_results:
            return pykoa_results

        new_res = []
        lens = []
        for key in pykoa_results.keys():
            lens.append(len(list(pykoa_results[key].values())))

        min_len = min(lens)

        for i in range(0, min_len):

            next_row = {}
            for key in pykoa_results.keys():
                next_row[key] = pykoa_results[key][str(i)]
            new_res.append(next_row)

    def query_obsid(self, obsid):
        url = 'https://www.keck.hawaii.edu/software/db_api/telSchedule.php'
        url += f'?cmd=getScheduleByUser&obsid={obsid}&type=pi'

        response = requests.get(url)
        results = response.content
        try:
            sched_results = json.loads(results)
        except Exception as err:
            return

        return

    def progid_results(self, progid):
        pykoa_results = json.loads(self.query_by_semid(progid, 'hires'))
        if not pykoa_results:
            return pykoa_results

        return self._flatten_results(pykoa_results)

    def _flatten_results(self, pykoa_results):
        new_res = []
        lens = []
        for key in pykoa_results.keys():
            lens.append(len(list(pykoa_results[key].values())))

        min_len = min(lens)

        for i in range(0, min_len):

            next_row = {}
            for key in pykoa_results.keys():
                next_row[key] = pykoa_results[key][str(i)]
            new_res.append(next_row)

        return new_res

    def iter_results(self, sched_results):
        pykoa_results = []
        supported_insts = ['hires', 'osiris']
        for rslt in sched_results:
            required = ["Instrument", "ProjCode", "Semester"]
            for val in required:
                if val not in rslt:
                    return
            inst = (rslt['Instrument']).lower()
            if inst not in supported_insts:
                continue
            semid = f"{(rslt['Semester']).upper()}_{(rslt['ProjCode']).upper()}"

            pykoa_results.append(json.loads(self.query_by_semid(semid, inst)))

        return pykoa_results


    def query_by_semid(self, progid, inst):
        """
         OI.20190529.10484.fits | /koadata23/OSIRIS/20190529/lev0/OI.20190529.10484.fits | N021 | 2019A_N021
        :return:
        """
        serv = 'https://koa.ipac.caltech.edu/'
        test = 'http://vmkoatest.ipac.caltech.edu:8000/'
        out = '/usr/local/home/koarti/lfuhrman/PyKOA/outputOS/tmp.tbl'
        open(out, 'w').close()

        #TODO I believe this has to be old data because it is public?
        inst='hires'
        query = f"select date_ut, koaid, instrume,  ra, dec, equinox, airmass, obstype " \
                f"from koa_{inst} where (progid='N049')"

        Koa.query_adql(query, out, server=test, overwrite=True, format='ipac')

        # <class 'astropy.table.table.Table'>
        ap_table = Table.read(out, format='ascii.ipac')
        pd = ap_table.to_pandas()
        json_table = pd.to_json()

        return json_table
