'''
Desc:  A standalone module that will manage sending email notifications to PIs when the first data file for a 
semid/instr/date is archived.  This can be called from ingest_api on successful FITS record ingestion.
Basic flow:

- Check if email already sent (db record in koa_pi_notify with same instrument/semid/utdate)
- Check that that PROGID is scheduled for the night (telsched, ToO and twilight)
- Get PI and proprietary period information
- Insert record into koa_pi_notify (do this before email to be extra safe )
- Send email to PI (cc: koaadmin)

Tests:
  Sched:    koa_pi_notify.py DE.20210809.51930.60.fits DEIMOS lev0 

Database info:
    create table `koa_pi_notify` (
        id          int(11)         NOT NULL AUTO_INCREMENT COMMENT 'Unique ID for this table',
        instrument  varchar(15)     NOT NULL,
        semid       varchar(15)     NOT NULL,
        utdate      date            NOT NULL,
        level       int(11)         NOT NULL,
        pi_email    varchar(64)     NOT NULL,
        last_mod    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT 'Timestamp of entry',

      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;

    CREATE USER 'koaru'@'vm-koartibuild' IDENTIFIED BY 'xxx';
    grant select, insert, update, delete on `koa`.`koa_pi_notify` to 'koaru'@'vm-koartibuild';

'''
import datetime as dt
import db_conn
import sys
import os
import json
from urllib.request import urlopen

#hook into whatever logging wmko_rti_api is doing
import logging
log = logging.getLogger('wmko_rti_api')

#map any instrument names to base name for easy query searching (ie 'nirspec' should search on '%nirsp%')
INSTR_BASE = {
    'NIRSPEC': 'NIRSP'
}


class KoaPiNotify:

    def __init__(self, koaid, instr, level, config, dev=False):
        self.koaid = koaid
        self.instr = instr
        self.level = level
        self.dev = dev

        self.proposal_api = config['PROPOSALS_API']
        self.telsched_api = config['TELSCHED_API']
        self.max_old_days = config['MAX_OLD_DAYS']
        self.admin_email = config['ADMIN_EMAIL']
        self.dev_email = config['DEV_EMAIL']
        self.allowed_levels = config['ALLOWED_LEVELS']

        log.info(f"KoaPiNotify: {koaid}, {instr}, {level}, {dev}")
        print(f"KoaPiNotify: {koaid}, {instr}, {level}, {dev}")

    def on_ingest(self):
        """
        Looks for entry in koa.koa_pi_notify with same semid, instr, date, and level.
        If no entry found, insert record and email.
        Returns True/False if PI was emailed
        """

        # Skip if this is a GUIDER ingestion
        if self.instr == 'GUIDER':
            return True, ''

        # Skip if NIRC2 data cube
        if self.instr == 'NIRC2' and '_unp' in self.koaid:
            return True, ''

        # db init (assuming relative location to config)
        self.dbname = 'koa'
        self.db = db_conn.db_conn('config.live.ini')

        # We are only dealing with level 0 for now
        self.level = self.get_numerical_level(self.level)
        if self.level is False:
            return False, f"Could not parse ingest type to numerical level: {self.level}"
        if self.level not in self.allowed_levels:
            return False, f"ERROR: Level {self.level} not in allowed ingest types."

        # Find koa_status record and get semid
        # get utdate from koaid
        self.semid = self.get_semid_from_db(self.koaid, self.instr, self.level)
        if not self.semid:
            return False, f"Could not lookup SEMID in koa_status for {self.koaid}"

        # get utdate from koaid
        self.utdate = self.get_utdate_from_koaid(self.koaid)
        if self.utdate is False:
            return False, f"Could not parse KOAID {self.koaid}"

        # todo: validate inputs

        # Look for existing entry
        if self.is_existing_entry(self.semid, self.instr, self.utdate, self.level):
            return False, f"Existing entry found for {self.semid}, {self.instr}, {self.level}"

        # Make sure this utdate is not too old
        utdatets = dt.datetime.strptime(self.utdate, '%Y-%m-%d')
        diff = dt.datetime.now() - utdatets
        if diff.days > self.max_old_days:
            return False, f"ERROR: date {self.utdate} is more than " \
                          f"{self.max_old_days} days ago"

        # Make sure this instrument and program were scheduled this day
        if not self.is_scheduled(self.utdate, self.semid, self.instr):
            msg = f'ERROR: Program {self.semid}, instrument {self.instr} was ' \
                  f'not scheduled on UT date {self.utdate}'
            return False, msg

        # added to exclude files that did not complete ingest
        if self.level == 0:
            answer, msg = self._check_status_codes()
            if not answer:
                return False, msg

        # get PI info, return if fail
        pi_email = self.get_pi_email(self.semid)
        if not pi_email:
            return False, f"ERROR: Could not get PI info for {self.semid}"

        # insert record and ensure it was successful before proceeding to email
        res = self.insert_new_entry(self.semid, self.instr, self.utdate,
                                    self.level, pi_email)
        if not res:
            return False, "Insert failed.  Not sending PI email."

        # do email to PI
        if not self.send_pi_email(pi_email, self.semid, self.instr,
                                  self.utdate, self.level):
            return False, "Email to PI failed."

        return True, ''

    def _check_status_codes(self):
        query = (f"select utdatetime, status, status_code, status_code_ipac"
                 f" from koa_status where koaid='{self.koaid}' "
                 f"and instrument='{self.instr}' and level='{self.level}' ")

        status_rows = self.db.query(self.dbname, query)
        for row in status_rows:
            if (row['status'] != 'COMPLETE' or row['status_code']
                    or row['status_code_ipac']):
                msg = f"New record has errors and not eligible for a PI Email." \
                      f" The status: {row['status']}, status_code: " \
                      f"{row['status_code']}, status_ipac: {row['status_code_ipac']}"

                return False, msg

        return True, None

    def get_numerical_level(self, level):
        level= level.replace('lev', '')
        try:
            level = int(level)
            return level
        except:
            return False

    def get_semid_from_db(self, koaid, instr, level):
        koaid = koaid.replace('.fits', '')
        query = (f"select * from koa_status where "
                f" koaid='{koaid}' and instrument='{instr}' "
                f" and level={level} ") 
        row = self.db.query(self.dbname, query, getOne=True)
        if row and len(row) > 0:
            return row['semid']
        else:
            return False

    def is_existing_entry(self, semid, instr, utdate, level):
        query = (f"select * from koa_pi_notify where "
                f" semid='{semid}' and instrument='{instr}' "
                f" and utdate='{utdate}' and level={level} ") 
        row = self.db.query(self.dbname, query, getOne=True)
        if row and len(row) > 0:
            return True
        else:
            return False


    def insert_new_entry(self, semid, instr, utdate, level, pi_email):
        log.info(f"New koa_pi_notify entry for {utdate}, {semid}, {instr}, {level}")
        query = ("insert into koa_pi_notify set "
                f"  semid='{semid}' "
                f", utdate='{utdate}' "
                f", instrument='{instr}' "
                f", level='{level}' "
                f", pi_email='{pi_email}' ")
        result = self.db.query(self.dbname, query)
        return result


    def get_utdate_from_koaid(self, koaid):
        '''
        Get utdate from KOAID.  Ex: KB.20210609.54491.60.fits is 2021-06-09'''
        try:
            parts = koaid.split('.')
            d = parts[1]
            return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
        except Exception as e:
            return False


    def get_pi_email(self, semid):
        url = f'{self.proposal_api}ktn={semid}&cmd=getPIEmail'
        try:
            result = urlopen(url).read().decode('utf-8')
            result = json.loads(result)
            email = result['data']['Email']
            return email
        except Exception as e:
            log.error(f'ERROR: Could not get data from API call {url}\nException: {str(e)}')
            return None


    def get_propint_data(self, semid):

        url = f'{self.proposal_api}ktn={semid}&cmd=getApprovedPP'
        try:
            result = urlopen(url).read().decode('utf-8')
            result = json.loads(result)
            pp = result['data']['ProprietaryPeriod']
            return pp, pp, pp, pp
        except Exception as e:
            if '_E' in semid:
                return 18, 18, 18, 18
            else:
                log.error(f'ERROR: Could not get data from API call {url}\nException: {str(e)}')
        return '', '', '', ''


    def is_scheduled(self, utdate, semid, instr):
        '''Ensure that it was scheduled for this day (must check for ToO and Twilight)'''

        yester = self.get_delta_date(utdate, -1)
        shortinstr = INSTR_BASE.get(instr, instr)
        sem, projcode = semid.split('_')

        #check telschedule
        try:
            url = f'{self.telsched_api}cmd=getSchedule&date={yester}&instr={shortinstr}&projcode={projcode}'
            result = urlopen(url).read().decode('utf-8')
            result = json.loads(result)
            if len(result) > 0: 
                return True
        except Exception as e:
            log.error(f'ERROR: Could not get data from API call {url}\nException: {str(e)}')

        #check ToO
        try:
            url = f'{self.telsched_api}cmd=getToORequest&date={yester}&instr={shortinstr}&projcode={projcode}'
            result = urlopen(url).read().decode('utf-8')
            result = json.loads(result)
            if len(result) > 0: 
                return True
        except Exception as e:
            log.error(f'ERROR: Could not get data from API call {url}\nException: {str(e)}')

        #check Twilight 
        #NOTE: We can't check keckOperations.twilightObserving b/c the entries are inserted at 7am by cron

        #Twilight Method 1: check proposalsAPI.php?cmd=getType == 'cadence'
        # try:
        #     url = f'{self.proposal_api}cmd=getType&ktn={semid}'
        #     result = urlopen(url).read().decode('utf-8')
        #     result = json.loads(result)
        #     if result and result['success'] == 1 and result['data']['ProgramType'] == 'Cadence': 
        #         return True
        # except Exception as e:
        #     log.error(f'ERROR: Could not get data from API call {url}\nException: {str(e)}')

        #Twilight Method 2: check proposalsAPI.php?cmd=getTwilightPrograms
        try:
            url = f'{self.proposal_api}cmd=getTwilightPrograms&semester={sem}'
            result = urlopen(url).read().decode('utf-8')
            result = json.loads(result)
            if result and result['success'] == 1:
                if semid in result['data']:
                    instrs = result['data'][semid]
                    if instr in ' '.join(instrs):
                        return True
        except Exception as e:
            log.error(f'ERROR: Could not get data from API call {url}\nException: {str(e)}')

        #can't find it scheduled anywhere
        return False


    def get_delta_date(self, datestr, delta):
        date = dt.datetime.strptime(datestr, "%Y-%m-%d")
        newdate = date + dt.timedelta(days=delta)
        return dt.datetime.strftime(newdate, "%Y-%m-%d")        


    def send_pi_email(self, pi_email, semid, instr, utdate, level):

        #get propint data
        #NOTE: Not exiting if propint data not found
        pp, pp1, pp2, pp3 = self.get_propint_data(semid)

        #massage some data
        sem, progid = semid.split('_')
        instr = instr.upper()

    #todo
        # to = pi_email
        to = self.admin_email
        frm = self.admin_email
        bcc = self.admin_email
        subject = f"The archiving and future release of your {instr} data";
        if self.dev: 
            to = self.dev_email
            bcc = ''
            subject = '[TEST] ' + subject
        msg = self.get_pi_send_msg(instr, sem, progid, pp, pp1, pp2, pp3)
        log.info(f"SENDING EMAIL TO: {to}")
        try:
            send_email(to, frm, subject, msg, bcc=bcc)
            return True
        except Exception as e:
            log.error(f'ERROR: could not send email: {str(e)}')
            return False


    def get_pi_send_msg(self, instr, semester, progid, pp, pp1, pp2, pp3):

        instr = instr.upper()

        msg = f"Dear {instr} program PI,\n\n";
        msg += f"Your {instr} data for\n\n";
        msg += f"Semester: {semester}\n";
        msg += f"Program: {progid}\n\n";
        msg += f"are now being archived in real-time.\n\n";
        msg += f"You can stream your data directly to your local storage while ";
        msg += f"observing by using the Observers' Data Access Portal at\n";
        msg += f"https://koa.ipac.caltech.edu/rti-gui/login\n\n";
        msg += f"You can grant data access to others at\n";
        msg += f"https://koa.ipac.caltech.edu/applications/KOA/grant.html\n\n";
        msg += f"The proprietary period for your program, as approved by your Selecting Official, is\n\n";
        if pp or instr != "HIRES":
            msg += f"{pp} months\n\n";
        else:
            msg += f"CCD1 = {pp1} months\n";
            msg += f"CCD2 = {pp2} months\n";
            msg += f"CCD3 = {pp3} months\n\n";
        msg += f"from the date of observation, after which the data will be ";
        msg += f"made public via KOA.  Policy details can be found at\n";
        msg += f"http://www2.keck.hawaii.edu/koa/public/KOA_data_policy.pdf.";
        msg += f"\n\n";
        msg += f"If the proprietary period shown above is not what you expect, ";
        msg += f"please contact your current Selecting Official.  The most ";
        msg += f"up-to-date list of Selecting Officials can be found at\n";
        msg += f"http://www2.keck.hawaii.edu/koa/public/soList.html\n\n";

        msg += f"To access your proprietary data, visit the password-protected ";
        msg += f"KOA User Interface (UI) at\n";
        msg += f"http://koa.ipac.caltech.edu\n\n";

        msg += f"If you have forgotten your username or password, please ";
        msg += f"submit a help desk ticket at\n";
        msg += f"https://koa.ipac.caltech.edu/applications/Helpdesk\n\n";

        msg += f"We encourage you to use the KOA to access your data, and we ";
        msg += f"welcome any comments and suggestions for improving the archive\n\n";

        msg += f"About KOA:\n";
        msg += f"Funded by NASA, KOA is a collaborative effort between the ";
        msg += f"W. M. Keck Observatory and the NASA Exoplanet Science ";
        msg += f"Institute (NExScI) to build, operate and maintain a data ";
        msg += f"archive for Keck Observatory.\n\n";

        msg += f"Sincerely,\n\n";
        msg += f"The Keck Observatory Archive\n";
        msg += f"koaadmin@keck.hawaii.edu";

        return msg


def send_email(to_email, from_email, subject, message, cc=None, bcc=None):
    '''
    Sends email using the input parameters
    '''
    import smtplib
    from email.mime.text import MIMEText

    # Construct email message
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['To'] = to_email
    msg['From'] = from_email
    if cc: msg['Cc'] = cc
    if bcc: msg['Bcc'] = bcc

    # Send the email
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


if __name__ == "__main__":

    #test
    kpn = KoaPiNotify(sys.argv[1], sys.argv[2], sys.argv[3], True)
    res, msg = kpn.on_ingest()
    print(res, msg)
