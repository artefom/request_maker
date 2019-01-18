
import begin
import time

import logging

import sys, traceback

import json

import os

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import glob
import re

import importlib

LOG_FILENAME = 'scheduler.log'
SCHEDULE_SAVE = 'schedule.json'


# Session begin hour
SESSION_BEGIN_HOUR = 23

# Session interval in days
SESSION_DAY_INTERVAL = 1


# Initialize logger
logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.DEBUG)
# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=1024*1024*5, backupCount=5)
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt='%m-%d %H:%M')
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def prase_fname(pattern,fname):
        
    fname_path,fname = os.path.split(fname)
    
    pattern_path, pattern = os.path.split(pattern)
    pattern_fname, pattern_ext = os.path.splitext(pattern)

    if fname==pattern:
        ret = re.findall(r'^({}{})$'.format(pattern_fname,pattern_ext),fname)
        if len(ret) == 1:
            return (os.path.join(fname_path,ret[0]),0)
        
    ret = re.findall(r'^({}_(\d+){})$'.format(pattern_fname,pattern_ext),fname)
    if len(ret) != 1:
        return None
    
    return os.path.join( fname_path,ret[0][0] ) ,int(ret[0][1])

#prase_fname('some_file.json','some_file_3.json')

def glob_ext(pattern):
    pattern_path, pattern_fname = os.path.split( pattern )
    pattern_fname_new = os.path.join(pattern_path,'{}_*{}'.format(*os.path.splitext(pattern_fname)))
    files = glob.glob(pattern)
    files.extend( glob.glob(pattern_fname_new) )
    files = [ os.path.normpath(i) for i in files ]
    if len(files) == 0:
        raise FileNotFoundError()
    
    ret = sorted( [prase_fname(pattern_fname,i) for i in files if prase_fname(pattern_fname,i) is not None], key=lambda x: -int(x[1]) )
    if len(ret) == 0:
        raise FileNotFoundError()
        
    return ret[0][0]


def get_schedule_fname():
    return glob_ext(SCHEDULE_SAVE)

def get_schedule_fname_new():
    fname,ext = os.path.splitext(SCHEDULE_SAVE)
    
    last_avaliable = None
    count = 0

    while True:
        if count <= 0:
            new_fname = '{}{}'.format(fname,ext)
        else:
            new_fname = '{}_{}{}'.format(fname,count,ext)

        if not os.path.exists(new_fname):
            try:
                return open(new_fname,'w')
            except FileNotFoundError:
                pass
            except IOError:
                pass

        count += 1


def load_json():
    sched_data = dict()

    try:
        fname = get_schedule_fname()
        with open(fname,'r') as f:
            sched_data = json.load(f)
    except FileNotFoundError:
        with get_schedule_fname_new() as f:
            json.dump(sched_data,f)
        #File not found, create new file
    except IOError:
        #Could not read file, create new one
        with get_schedule_fname_new() as f:
            json.dump(sched_data,f)
    return sched_data

def write_json(js):
    fname = get_schedule_fname()
    try:
        with open(fname,'w') as f:
            json.dump(js,f)
            return f.name

    except FileNotFoundError:
        #File not found, create new file
        with get_schedule_fname_new() as f:
            json.dump(js,f)
            return f.name

    except IOError:
        #Could not read file, create new one
        with get_schedule_fname_new() as f:
            json.dump(js,f)
            return f.name

def parse_date(d):
    return datetime.strptime(d,'%d.%m.%Y %H:%M:%S')

def format_date(d):
    return datetime.strftime(d,'%d.%m.%Y %H:%M:%S')


def format_interval(d_from,d_to):
    i = relativedelta(d_to,d_from)
    ret = []
    attrs = ['years','months','days','hours','minutes','seconds']
    for attr in attrs:
        val = getattr(i,attr)
        name = attr
        if val != 0:
            if int(abs(val)) == 1:
                name=name[:-1]
            ret.append('{} {}'.format(val,name))

    return ', '.join(ret)

def run_operation():
    print("Executing operation")


def calculate_next_scrape(last_scrape):
    ret = datetime(last_scrape.year,last_scrape.month,last_scrape.day,SESSION_BEGIN_HOUR)
    print(ret)
    while ret < datetime.now() or ret < last_scrape:
        ret = ret+timedelta(SESSION_DAY_INTERVAL)
    return ret

def ask(question,default=0):

    while True:
        print()
        print()

        if default == 1:
            try:
                ans = input('{} (Y/n): '.format(question)).strip().lower()
            except EOFError:
                raise KeyboardInterrupt()

            if len(ans) == 0:
                ans = 'y'

        elif default == -1:
            try:
                ans = input('{} (y/N): '.format(question)).strip().lower()
            except EOFError:
                raise KeyboardInterrupt()

            if len(ans) == 0:
                ans = 'n'
        else:
            try:
                ans = input('{} (y/n): '.format(question)).strip().lower()
            except EOFError:
                raise KeyboardInterrupt()

        if ans == 'y':
            return True
        elif ans == 'n':
            return False
        else:
            print("Uknown answer: {}, please type in 'Y' or 'N'".format(ans))

@begin.start
def run( execution_script, 
         input_file,
         odir, 
         recalc_file=None, 
         request_per_second=5, 
         dump_every=1000, 
         n_threads=30, 
         duration_hours=8, 
         start_hour=SESSION_BEGIN_HOUR, 
         day_interval=SESSION_DAY_INTERVAL, 
         main_function='main' ):

    # Set global variables
    global SESSION_BEGIN_HOUR, SESSION_DAY_INTERVAL
    SESSION_BEGIN_HOUR = int( start_hour )
    SESSION_DAY_INTERVAL = int( day_interval )

    # Get last execution time
    sched_data = load_json()

    if 'scrape_last' not in sched_data:
        sched_data['scrape_last'] = format_date( datetime.now()-timedelta(SESSION_DAY_INTERVAL) )

    #Determine next scrape time
    # Get last scraping time
    last_scrape = parse_date( sched_data['scrape_last'] )
    next_scrape = calculate_next_scrape(last_scrape)

    print("Next operation in {}".format( format_interval(datetime.now(),next_scrape) ))
    if ask("Start now?",default=1):
        next_scrape = datetime.now()+timedelta(seconds=5)


    print("Beginning operation")
    while True:
        try:
            print("\rTime until next event: {}".format(format_interval(datetime.now(),next_scrape)),end='') 
            interrupted = False
            if datetime.now() >= next_scrape:
                print()
                print("Running event...")
                logger.info("Running event...")
                finished_with_exception = False
                #Create new scrape history record

                sched_data['scrape_last'] = format_date( datetime.now() )
                sched_data['exception'] = None

                try:
                    import importlib

                    child_module = importlib.import_module('run_scraping')
                    importlib.reload(child_module)
                    result = getattr(child_module, 'main')( execution_script, input_file,odir, 
                                                            recalc_file=recalc_file, 
                                                            request_per_second=request_per_second, 
                                                            dump_every=dump_every,
                                                            n_threads=n_threads, 
                                                            duration_hours=duration_hours )

                except KeyboardInterrupt as ex:
                    interrupted = True
                except:
                    finished_with_exception = True

                    exception_lines = traceback.format_exc().splitlines()
                    exception_line_max_length = max(*[len(i) for i in exception_lines])

                    sched_data['exception'] = '\n'.join(exception_lines)

                    logger.error( "Exception while request session:" )
                    print( "Exception while request session:" )
                    print('-'*exception_line_max_length)
                    logger.error( '-'*exception_line_max_length )
                    for l in exception_lines:
                        print(l)
                        logger.error(l)
                    print('-'*exception_line_max_length)
                    logger.error( '-'*exception_line_max_length )

                sched_data['scrape_last_end'] = format_date( datetime.now() )

                #DUmp data
                write_json(sched_data)

                last_scrape = parse_date( sched_data['scrape_last'] )
                next_scrape = calculate_next_scrape(last_scrape)

                print("Finished event")
                logger.info("Event finished...")

                if 'exception' in sched_data and sched_data['exception'] is not None and len(sched_data['exception']) > 0:
                    print("WARNING: {}".format('EVENT FINISHED WITH AND ERROR') )
                    logger.error('EVENT FINISHED WITH AND ERROR')

            if interrupted:
                raise KeyboardInterrupt()

            time.sleep(1)
        except KeyboardInterrupt:
            if ask("Are you sure you want to abort?",default=-1):
                write_json(sched_data)
                print("Aborting")
                exit()
