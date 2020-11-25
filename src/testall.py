#!/kroot/rel/default/bin/kpython3
'''
Description: Check basic functionality of Keck instrument systems
(NOTE: This is a Python port of Keck testall instrument test scripts into one shared code base.)

'''

#modules
import os
import sys
import argparse
import json
import yaml
import subprocess
import psutil
import datetime as dt
from copy import deepcopy

#create optional route if KTL module is not available (ie kpython3 is not used)
use_ktl = True
try:
    import ktl
except ImportError:
    print("WARNING: Could not load ktl module! Will use 'show' command line.")
    use_ktl = False



def test_all(instr=None, datadir=None, systems=[], level=0):
    '''Run all tests defined in instrument test config file.

    Parameters:
        instr (str): Instrument to test. Default is $INSTRUMENT env var.
        datadir (str): Dir to look for testall config file. Default is $RELDIR/data/.
        systems (arr): Array of strings for specific systems to test (as defined in testall config).

    Returns:
        str: JSON string of test results object.
    '''

    #get needed input data
    instr = get_instr(instr)
    datadir = get_datadir(datadir)
    config = get_config(datadir, instr)

    #create results object from copy of config
    results = {}
    results['instr']      = instr
    results['timestamp']  = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results['stats']      = {'num_pass':0, 'num_warn':0, 'num_error':0, 'num_skip':0}
    results['crit_error'] = 0
    results['tests']      = deepcopy(config)

    #run each test by type of check and update results array
    for system, tests in results['tests'].items():
        if results['crit_error'] == 1: break
        if systems and system not in systems: continue   

        print(f"Checking {instr} {system}:")
        for test in tests:

            check_test_params(test)

            # check test level
            test['skip'] = False
            if test['lev'] > level: 
                print(f"Skipping test for: {test['name']}")
                results['stats']['num_skip'] += 1
                test['skip'] = True
                continue

            #run test by type
            if   test['check'] == 'ping':   ok, err = check_ping(test)
            elif test['check'] == 'show':   ok, err = check_show(test)
            elif test['check'] == 'ps':     ok, err = check_ps(test)
            elif test['check'] == 'script': ok, err = check_script(test)
            elif test['check'] == 'sshkey': ok, err = check_sshkey(test)

            #add results to test dict
            test['ok']  = ok
            test['err'] = err
            print_test_result(test)

            #overallstats
            if ok:                          results['stats']['num_pass']  += 1
            elif test['errtype'] == 'warn': results['stats']['num_warn']  += 1
            else:                           results['stats']['num_error'] += 1

            #system stats
            if system not in results['stats']:
                results['stats'][system] = {'num_pass':0, 'num_warn':0, 'num_error':0}
            if ok:                          results['stats'][system]['num_pass']  += 1
            elif test['errtype'] == 'warn': results['stats'][system]['num_warn']  += 1
            else:                           results['stats'][system]['num_error'] += 1

            #exit tests
            if ok == False and test['errtype'] == 'error' and 'exit' in test:
                if   test['exit'] == 'group':  
                    print(f"Critical test failed. Skipping {system} system tests.")
                    break
                elif test['exit'] == 'all':  
                    results['crit_error'] = 1
                    print("Critical test failed. Exiting testall.")
                    break

    #output
    show_stats(results)
    return json.dumps(results)


def handle_fatal_error(errmsg):
    print(f"\n*** FATAL ERROR ***\n{errmsg}")
    sys.exit({'critical_error': errmsg})


def get_instr(instr=None):
    '''Determine instrument if not given'''
    if not instr: 
        instr = os.getenv('INSTRUMENT')        
        if instr: print(f"Found INSTRUMENT env var")
    if not instr:
        handle_fatal_error(f"ERROR: Could not read 'INSTRUMENT' env variable.  Use --instr parameter.  Exiting.")
    print(f"Testing instrument: {instr}")
    return instr


def get_datadir(datadir=None):
    '''Determine data dir if not given'''
    if not datadir: 
        datadir = os.getenv("RELDIR")
        if datadir: 
            print(f"Found RELDIR env var")
            datadir += "/data"
    if not datadir:
        print("WARNING: No data dir provided and could not find RELDIR.")
        datadir = f"{sys.path[0]}/data"
    print(f"Looking for data file in {datadir}")
    return datadir


def get_config(datadir, instr):
    '''Get tests config data from data dir'''
    configfile = f"{datadir}/testall-{instr.lower()}.yaml"
    if not os.path.isfile(configfile):
        handle_fatal_error(f"ERROR: Config file '{configfile}' does not exist.  Exiting.")
    with open(configfile) as f: config = yaml.safe_load(f)
    print(f"Using config file: {configfile}")
    return config


def show_stats(results):
    '''Print formatted test results stats (num pass, warn, error, etc).'''
    print(f"-----------------------------------------------------------------------------")
    if results['stats']['num_warn'] == 0 and results['stats']['num_error'] == 0: 
        print('\tAll tests passed.')
    else: 
        print(f"\t{results['stats']['num_error']} errors and {results['stats']['num_warn']} warnings were issued.")
    print(f"-----------------------------------------------------------------------------")


def check_test_params(test):
    '''Make sure all tests have necessary params and provide defaults in some cases.'''

    #Default name assignment if not given
    if 'name' not in test:
        if   test['check'] == 'ping':   test['name'] = test['server']
        elif test['check'] == 'show':   test['name'] = f"{test['service']}.{test['keyword']}"
        elif test['check'] == 'ps':     test['name'] = test['pattern']
        elif test['check'] == 'script': test['name'] = test['cmd']
        elif test['check'] == 'sshkey': test['name'] = f"ssh to {test['user']}@{test['server']}"

    #some names are ktl lookups
    if type(test['name']) is dict:
        val, stat = get_show_val(test['name']['service'], test['name']['keyword'])
        if val and stat == 0: test['name'] = val
        else:                 test['name'] = '***ktl name ERROR***'

    #default values
    if 'errtype' not in test: test['errtype'] = 'error'
    if 'lev' not in test: test['lev'] = 1


def print_test_result(test):
    '''Print nicely formatted result of test.'''
    text = test['name'].ljust(40, '.')
    errtype = test['errtype'].upper() 

    errmsg = ''
    if not test['ok'] and 'errmsg' in test: errmsg = f" ({test['errmsg']})"

    fixmsg = ''
    if not test['ok'] and 'fixmsg' in test: fixmsg = f" ({test['fixmsg']})"

    err = "OK" if test['ok'] else f"{errtype}: {test['err']}"
    print (f"\t{text}{err}{errmsg}{fixmsg}")


def check_sshkey(test):
    '''Simple test to make sure ssh key is working.'''
    try:
        command = ['ssh', test['server'], '-l', test['user'], '-i', test['keypath'], 
                   '-n', '-o', 'PasswordAuthentication=no', 'whoami']
        proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        output = proc.stdout.readline().decode("utf-8").strip()
        if output != test['user']:  raise Exception('ERROR')
        if proc.poll() is not None: raise Exception('ERROR')
    except Exception as e:
        return False, f"SSH key not working for {test['user']}@{test['server']}"
    return True, ''


def check_ps(test):
    '''Check for running process.'''

    flags    = test['flags']

    patterns = test['pattern']
    if not (isinstance(patterns, list)): patterns = [patterns]

    excludes = test['excludes'] if 'excludes' in test else []
    if not (isinstance(excludes, list)): excludes = [excludes]
    excludes += ['grep']

    #run full ps and store output in lines array
    cmd = f'ps {flags}'
    if 'server' in test: cmd = f"ssh {test['server']} " + cmd
    output, stat = get_cmd_output(cmd)
    if stat != 0:
        return False, 'Call to ps failed'
    lines = output.split("\n")

    #count lines that have patterns and do not have any excludes
    count = 0
    for line in lines:
        if not all(word in line for word in patterns): continue
        if any(word in line for word in excludes): continue
        count += 1

    #Pass test based on num count desired or any =if num not defined
    if 'num' in test:
        if   count == test['num']: return True, ''
        elif count == 0:           return False, f"{test['name']} is not running."
        else:                      return False, f"{test['name']} has {count} instances instead of {test['num']}."
    else:
        if count > 0: return True, ''
        else:         return False, f"{test['name']} is not running."


def check_ping(test):
    '''Ping server to see if it is alive'''
#todo: What ping options will work on all instr servers?
    server = test['server']
    cmd = f'ping -c 1 -W 2 {server} > /dev/null'
    stat = os.system(cmd)
    if stat == 0: return True, ''
    else:         return False, f"Could not ping server {test['server']}"


def check_show(test):
    '''Check KTL keyword for proper function and/or value based on test definitions.'''

    #get and check params
    service = test.get('service')
    keyword = test.get('keyword')
    val     = test.get('val')
    lock    = test.get('lock')
    lockval = test.get('lockval')
    minval  = test.get('minval')
    maxval  = test.get('maxval')
    thresh  = test.get('thresh')

    if service == None or keyword == None:
        return False, f"Improperly formatted test configuration."

    return do_check_show(service, keyword, val, lock, lockval, minval, maxval, thresh)


def do_check_show(service, keyword, val=None, lock_kw=None, lockval=None, minval=None, maxval=None, thresh=None):

    #get show value and check return code for failure
    showval, stat = get_show_val(service, keyword)
    if stat != 0: 
        return False, "Call to show command did not work"

    #lockout check?
    if lock_kw != None:
        lock, stat = get_show_val(service, lock_kw)
        if stat != 0: return False, f"Could not determine lock state of {lock_kw}"
        if lockval != None:
            unlock_val = str(get_test_val(lockval))
        else:
            unlock_val = 'unlocked'
        if lock != unlock_val: return False, "Stage is locked out"

    #If 'val' defined (as value or array of values), look for matching value
    if val != None:        
        val = get_test_val(val)
        if  isnumber(val) or (isinstance(val, list) and isnumber(val[0])):
            showval = int(showval) if isinstance(val, int) else float(showval)
        if (isinstance(val, list) and showval in val) or (showval == val):
            return True, ''
        else:
            return False, f"Current: '{showval}', want: '{str(val)}'"

    #if min or max defined, validate in range
    elif minval != None or maxval != None:
        showval = float(showval)
        if thresh == None: thresh = 0
        if minval != None: minval = float(get_test_val(minval)) - thresh
        if maxval != None: maxval = float(get_test_val(maxval)) + thresh
        if minval != None and maxval == None:
            if showval >= minval: return True, ''
            else:                 return False, f"Current: {showval} < min val of {minval}" 
        elif maxval != None and minval == None:
            if showval <= maxval: return True, ''
            else:                 return False, f"Current: {showval} > max val of {maxval}" 
        else:
            if minval <= showval <= maxval: return True, ''
            else:                           return False, f"Current: {showval} outside of good range {minval} - {maxval}"

    #else we assume we only care that there is output
    else: return True, ''


def get_show_val(service, keyword):
    '''
    Get keyword value from KTl service.  Will use ktl module if it imported, 
    otherwise will use command link 'show'.    
    '''
    if use_ktl:
        try:
            val = ktl.cache(service, keyword).read()
            return val, 0
        except:
            try:
                return get_using_show(service, keyword)
            except Exception as e:
                return str(e), -1
    else:
        return get_using_show(service, keyword)


def get_using_show(service, keyword):
    '''Uses show to get keyword value'''
    cmd = f'show -terse -s {service} {keyword}'
    val, stat = get_cmd_output(cmd)
    return val, stat

def check_script(test):
    '''Run a script and verify output and/or return status.'''

    #get cmd value and status
    #NOTE: grep returns non-zero status of 1 if grep finds nothing. This can be used for tests looking for non-existance of words like 'ERROR.
    out, stat = get_cmd_output(test['cmd'])
    stdout = f"\n{out}" if 'stdout' in test else ''

    #if status defined, test that only
    if 'status' in test:
        if test['status'] == stat: return True, ''
        else:                      return False, f"Script status returned {stat}, expecting {test['status']}{stdout}"

    #Otherwise, status that is not zero is bad
    if stat != 0: 
        return False, "Call to script did not work"

    #if pattern defined, test that only
    if 'pattern' in test:
        pattern = test['pattern']
        if pattern in out: return True, ''
        else:              return False, f"Could not find text '{pattern}' in script output"

    #If 'val' defined, look for matching value
    if 'val' in test:        
        val = test['val']
        if  isnumber(val): 
            out = int(out) if isinstance(val, int) else float(out)
        if  out == val: return True, ''
        else:           return False, f"Current: '{out}', want: '{str(val)}'{stdout}"

    #else we assume we only care that there is output
    else: return True, ''


def get_cmd_output(cmd):
    '''Run command and get output and return code.'''
    try:
        ps = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = ps.communicate()[0]
        out = out.decode("utf-8").strip()
    except Exception as e:
        return None, -1
    return out, ps.returncode


def isnumber(val):
    '''Is a variable a number?'''
    try:
       val = float(val)
    except Exception as e:
        return False
    return True


def get_test_val(val):
    '''Some vals are defined as the value from a show command.'''
    if type(val) is dict:
        index = val.get('index')
        val, stat = get_show_val(val['service'], val['keyword'])
        if index is not None:
            val = get_index_val_from_json_string(val, index)
    return val


def get_index_val_from_json_string(val, index):
    try:
        val = json.loads(val)
        val = val[index]
        return val
    except Exception as e:
        return None


#--------------------------------------------------------------------------------
# main command line entry
#--------------------------------------------------------------------------------
if __name__ == '__main__':

    # define arg parser
    parser = argparse.ArgumentParser(description="Start testall script.")
    parser.add_argument("--instr", type=str, default=None, help="Instrument to test. Default is INSTRUMENT environment variable.")
    parser.add_argument('--systems', nargs='+', default=[], help='Which systems to test')
    parser.add_argument("--datadir", type=str, default=None, help='Dir path to config data for tests.  If not specified, will look in script dir.  File should be "testall-[instr].yaml"')
    parser.add_argument("--level", type=int, default=1, help='Tests with lev value <= to level are run. All tests are assumed level 1 unless defined in config.')
    args = parser.parse_args()

    #run
    test_all(instr=args.instr, datadir=args.datadir, systems=args.systems, level=args.level)
