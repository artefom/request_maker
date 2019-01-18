import begin

import glob
import numpy as np
import pandas as pd
import os
import configparser
import run_in_background
import caching
from caching import DumpFailed
import no_exceptions
import time
import requests
import logging
import pickle as pkl
from pickle import UnpicklingError
from collections import deque
from concurrent.futures import TimeoutError
from datetime import datetime, timedelta
from requests.exceptions import ConnectionError, BaseHTTPError

#LOad config


cfg_file = 'run_scraping.ini'

cache_fname_pattern = 'api_*.pkl'
cache_fname_template = 'api_call_cache_{}_{}.pkl'

# RESULTS_FOLDER = './scraping_results'

# Load configuration
config = configparser.ConfigParser()
config['SCRIPTS'] = {}
config['SCRIPTS']['dir'] = 'scrape_scripts'
config['LOGGING'] = {}
config['LOGGING']['file name'] = 'run_scraping.log'
config['LOGGING']['max bytes'] = str( 1024*1024*5 )
config['LOGGING']['backup count'] = str( 5 )

if os.path.exists(cfg_file):
    config.read(cfg_file)
else:
    with open(cfg_file, 'w') as configfile:
        config.write(configfile)

#Initialize logger
# Initialize logger
logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              config['LOGGING']['file name'], maxBytes= int( config['LOGGING']['max bytes'] ), backupCount=int(config['LOGGING']['backup count']) )
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt='%m-%d %H:%M')
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def get_cached_results(cache_dir):
    """ Read cached results from directory
    """
    cache_files = glob.glob(os.path.join(cache_dir,cache_fname_pattern))
    cached_input = set()
    for fname in cache_files:

        with open(fname,'rb') as f:

            try:
                loaded_cache = pkl.load(f)
            except EOFError:
                continue
            except UnpicklingError:
                continue
            except TypeError:
                continue
            except UnicodeDecodeError:
                continue
            cached_input.update( list(loaded_cache) )
    return cached_input


# def get_recalc_vins():
#     ret = set()
#     if os.path.exists(config['SITES']['recalc']):
#         with open(config['SITES']['recalc'],'r',encoding='utf-8') as f:
#             print("Reading {}".format(config['SITES']['recalc']))
#             logging.info("Reading vins from {}".format(config['SITES']['recalc']))

#             ret = set([i.strip() for i in f.readlines() if len(i.strip()) > 0])
#             logging.debug('loaded vins from recalculation file {}, number: {}'.format(config['SITES']['recalc'],len(ret)) )

#         try:
#             os.remove(config['SITES']['recalc'])
#             logging.debug('droped vin recalculation file {}'.format(config['SITES']['recalc']))
#         except OSError:
#             pass

#     return ret

# def get_total_vins():
#     with open(config['SITES']['total'],'r',encoding='utf-8') as f:
#         return set( [i.strip() for i in f.readlines() if len(i.strip()) > 0] )

# def get_cache_fname():
#     i = 1
#     while True:
#         cache_fname = os.path.join(RESULTS_FOLDER,cache_fname_template.format(i))
#         if not os.path.exists(cache_fname):
#             return cache_fname
#         i+=1



#################################### DEFINE API REQUEST FUNCTIONS ####################################
######################################################################################################


# key = '12a794eb-69f3-471a-821e-71ee31d84cc1'

# @run_in_background.run_in_background(n_threads=10)
# @caching.cached
# @no_exceptions.no_exceptions
# def send_request_api(vin):
    
#     vin = vin.upper().strip().replace('O','0')
#     ret = requests.get( 'https://data.av100.ru/api.ashx?key={}&vin={}'.format(key,vin), timeout=60 )
#     if ret.status_code != 200:
#         raise ValueError("status code {}".format(ret.status_code))
#     ret = ret.json()
#     if ret.get('error') or len(ret.get('result',[])) == 0:
#         raise ValueError("Could not get request result")
#     return ret['result']

# import re
# from urllib.parse import urlparse, urlunparse
# def format_site(x,protocol='http'):f
#     url_parsed = list(urlparse(x))
#     url_parsed[0] = protocol

#     return urlunparse(url_parsed).replace('///','//')


# @run_in_background.run_in_background(n_threads=NUM_RUNNING_TASKS)
# @caching.cached
# @no_exceptions.no_exceptions
# def send_request_api(site_orig):
    
#     attempt_http = False
#     attempt_http_message = (-1,'')
#     attempt_https = False
#     attempt_https_message = (-1,'')
    
#     headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
#     'From': 'https://www.google.com/'  # This is another valid field
#     }
    
#     try:
#         site = format_site(site_orig,protocol='http')
#         ret = requests.get( site, headers=headers, timeout=10 )
#         if ret.status_code != 200:
#             attempt_http_message = (ret.status_code, ret.text)
#             if str(ret.status_code)[:1] == '3':
#                 return (ret.status_code, ret.text)

#         else:
#             attempt_http = True
#             return ret.text
#     except ConnectionError as ex:
#         attempt_http_message = ('HTTP ConnectionError', str(ex) )
#     except BaseHTTPError as ex:
#         attempt_http_message = ('BASE HTTP ERROR', str(ex) )

#     try:
#         site = format_site(site_orig,protocol='https')
#         ret = requests.get( site, headers=headers, timeout=5 )
#         if ret.status_code != 200:
#             attempt_https_message = (ret.status_code, ret.text)
#             if str(ret.status_code)[:1] == '3':
#                 return (ret.status_code, ret.text)


#         else:
#             attempt_https = True
#             return ret.text
#     except ConnectionError as ex:
#         attempt_https_message = ('HTTPS ConnectionError', str(ex) )
#     except BaseHTTPError as ex:
#         attempt_https_message = ('BASE HTTP ERROR', str(ex) )

#     raise ValueError("#SITE: {}\n#HTTP ERORR {}: {}\n#HTTPS ERROR {}: {}".format(site_orig,attempt_http_message[0],attempt_http_message[1],
#                                                                     attempt_https_message[0],attempt_https_message[1]))

def get_num_request_per_second(ex_times,monitor_seconds=10):
    time_threshold = datetime.now()-timedelta(0,monitor_seconds)
    last_events = []
    for i in ex_times[::-1]:
        if i < time_threshold:
            break
        last_events.append(i)

    if len(last_events) == 0:
        return 0
    
    true_seconds = max(1,(datetime.now()-min(last_events)).total_seconds())
    return len(last_events)/true_seconds

def get_time_since_last_event(ex_times):
    if len(ex_times) == 0:
        return float('inf')
    
    return (datetime.now()-max(ex_times)).total_seconds()


def get_input(file_path):
    ret = set()
    with open(file_path,'r',encoding='utf-8') as f:
        for l in f:
            l = l.strip()
            if len(l) > 0:
                ret.add(l)
    return ret

def get_input_recalc(file_path):
    ret = set()
    if file_path is None:
        return ret

    if os.path.exists(file_path):
        with open(file_path,'r',encoding='utf-8') as f:
            print("Reading {}".format(file_path))
            logging.info("Reading input from {}".format(file_path))

            ret = set([i.strip() for i in f.readlines() if len(i.strip()) > 0])
            logging.debug('loaded input from recalculation file {}, number: {}'.format(file_path,len(ret)) )

        try:
            os.remove(file_path)
            logging.debug('droped recalculation file {}'.format(file_path))
        except OSError:
            pass

    return ret


def get_cache_fname(cache_dir):
    dt_str = datetime.now().strftime('%Y%d%m')
    i = 1
    while True:
        cache_fname = os.path.join(cache_dir,cache_fname_template.format(dt_str,i))
        if not os.path.exists(cache_fname):
            return os.path.normpath( cache_fname )
        i+=1

def get_request_function(execution_script,n_threads):
    import importlib
    child_module = importlib.import_module( '{}.{}'.format( config['SCRIPTS']['dir'],execution_script ) )
    child_function = getattr(child_module, 'main')

    @run_in_background.run_in_background(n_threads=n_threads)
    @caching.cached
    @no_exceptions.no_exceptions
    def result(*args,**kwargs):
        return child_function(*args,**kwargs)

    return result

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
def main(execution_script,input_file,odir,recalc_file=None,request_per_second=5,dump_every=1000,n_threads=30,duration_hours=8):

    request_per_second = float(request_per_second)
    dump_every = int(dump_every)
    n_threads = int(n_threads)
    duration_hours = float(duration_hours)
    TIME_SINCE_LAST_EVENT = 1/float(request_per_second)/3
    REQUESTS_PER_SECOND = request_per_second
    NUM_RUNNING_TASKS = n_threads

    # Create results folder
    if not os.path.exists(odir):
        os.mkdir(odir)

    # Read file
    logger.info("\n\n\nReading input from file {}".format(input_file))
    input_data = get_input(input_file)
    recalc_data = list( get_input_recalc(recalc_file) )
    cached_data = [ i[0] for i in get_cached_results(odir) ]
    print("Script file: {}".format(execution_script))
    print("Input: {}".format(len( input_data ) ))
    print("Recalc: {}".format(len( recalc_data ) ))
    print("Cached: {}".format(len( cached_data ) ))

    # Request recalc first (randomized), then, request all other data(randomized,too)
    final_input_list = list( input_data.difference(cached_data).difference(recalc_data) ) # Don't request already cached data and that is asked for recalculation
    np.random.shuffle(final_input_list)
    np.random.shuffle(recalc_data)
    final_input_list = recalc_data+final_input_list

    # Used for dropping from it
    recalc_data_left = set(recalc_data)

    # Get request maker function from file
    send_request_api = get_request_function(execution_script,n_threads)

    print("Running request procedure")
    cache_fname = get_cache_fname(odir)
    print("Cache fname: {}".format(cache_fname))
    logger.info("Running request procedure")
    logger.info("Cache fname: {}".format(cache_fname))

    # Set new cache filename
    send_request_api._original.set_cache_fname(cache_fname,int(dump_every*2),dump_cache=False) # We're going to dump cache inside loop, but set dump cache here for safety
    print("Function cache size: {}".format(len(send_request_api._original._original.cache)))

    logger.debug( "Function cache size: {}".format(len(send_request_api._original._original.cache)) )
    if len(send_request_api._original._original.cache) > 0:
        logger.warning("Cache size is more than 0!")

    # Store async results here
    results = []

    # Create request queue
    queued_requests = deque()
    for i in final_input_list:
        queued_requests.append(i)

    # Infer hardstop time
    hardstop_time = datetime.now()+timedelta(0,duration_hours*60*60)

    logger.info("Queued requests: {}".format( len(queued_requests) ))
    logger.info("Harstop time: {}".format(hardstop_time.strftime('%H:%M\n%d.%m.%y')))
    
    print('Queued requests: {}'.format(len(queued_requests)))
    print("Harstop time:", hardstop_time.strftime('%H:%M\n%d.%m.%y'))
    print()

    dumped_and_reseted = 0 # Number of calculations that were dumped to disk and erased from memory
    num_running_tasks = 0
    results_before = len(results)
    cache_before = len(send_request_api._original._original.cache)
    processed_results_i = 0
    interrupted = False
    is_stuck = False

    #Report interval for logger
    logger_report_interval = 1*60*60 # 1 Hour report interval
    logger_report_next = datetime.now()+timedelta(seconds=logger_report_interval)

    # Function for reporting current progress
    def report():

        nonlocal logger_report_interval
        nonlocal logger_report_next

        num_running_tasks = len(send_request_api._original.get_running_tasks())
        request_per_second = get_num_request_per_second(send_request_api._original.execution_times)
        time_since_last_event = get_time_since_last_event(send_request_api._original.execution_times)

        possible_errors = (len(results)-results_before)-(len(send_request_api._original._original.cache)+dumped_and_reseted-cache_before)-num_running_tasks

        report_slug = '\rrunning tasks: {}, rps: {:.1f}, last: {:.1f}, processed: {},'\
              ' queued: {}, cache size: {}, errors?: {}'.format(
            num_running_tasks,
            request_per_second,
            time_since_last_event,
            len(results),
            len(queued_requests),
            len(send_request_api._original._original.cache),
            possible_errors)

        print(report_slug,end='')

        if datetime.now() > logger_report_next:
            # Write some output to logger every while
            logger_report_next = datetime.now()+timedelta(seconds=logger_report_interval)
            logger.info(report_slug)

    while (len(queued_requests) > 0 and datetime.now() < hardstop_time and not interrupted) or num_running_tasks > 0:    

        try:
            report()

            # Get statistics for deciding what to do
            num_running_tasks = len(send_request_api._original.get_running_tasks())
            request_per_second = get_num_request_per_second(send_request_api._original.execution_times)
            time_since_last_event = get_time_since_last_event(send_request_api._original.execution_times)

            # Protection against stucking on something after execution was aborted
            if (len(queued_requests) == 0 or interrupted or datetime.now() > hardstop_time) and time_since_last_event > 120:
                print('\r')
                print("STUCK ON TASKS {}".format( [ str(k) for k,v in send_request_api._original.get_running_tasks().items() ] ))
                print("Interrupting")
                is_stuck = True
                break

            if time_since_last_event > TIME_SINCE_LAST_EVENT and request_per_second <= REQUESTS_PER_SECOND and num_running_tasks < NUM_RUNNING_TASKS and datetime.now() < hardstop_time and not interrupted:
                try:
                    req = queued_requests.popleft()
                    recalc_data_left.discard(req)
                    results.append( send_request_api(req) )
                except IndexError:
                    pass

            process_time = datetime.now()+timedelta(seconds=TIME_SINCE_LAST_EVENT)
            while datetime.now() < process_time and processed_results_i < len(results):
                if len( send_request_api._original._original.cache ) > dump_every: # We want to dump cache in this thread, avoid dumping it in other threads
                    cache_len = len(send_request_api._original._original.cache)

                    try:
                        dump_len = len(send_request_api._original._original.cache)
                        dumped_fname = send_request_api._original.dump_cache()
                        dumped_and_reseted += dump_len
                        print('\r')
                        print("CACHE DUMPED TO {}. CACHE SIZE: {}".format(dumped_fname,cache_len))
                    except DumpFailed as ex:
                        print('\r')
                        print("Dump failed, aborting")
                        logger.error("Dump failed, aborting operation")
                        interrupted = True

                    # Set new cache name
                    cache_fname = get_cache_fname(odir)
                    print("Setting new cache name to {}".format(cache_fname))
                    logger.info("Setting new cache to {}".format(cache_fname))
                    send_request_api._original.set_cache_fname( cache_fname, int(dump_every*2), dump_cache=False)

                try:
                    res = results[processed_results_i].result(0)
                    if res is not None and res[-1] is not None and len( res[-1] ) > 0:
                        logger.error("During processing of the request exception occured:")
                        logger.error('-'*60)

                        if len(res[-1]) > 1100:
                            logger.error(res[-1][:500]+'\n'+'...'+'\n'+res[-1][500:])
                        else:
                            logger.error(res[-1])


                        logger.error('-'*60)

                        print('\r')
                        print('During processing of the request exception occured:')
                        print('-'*60)

                        if len(res[-1]) > 1100:
                            print(res[-1][:500]+'\n'+'...'+'\n'+res[-1][500:])
                        else:
                            print(res[-1])

                        print('-'*60)
                        print()
                        print()
                        report()


                    processed_results_i+=1
                except TimeoutError: # If we're timed out of getting result, break loop of printing result info
                    break

            #Sleep remaining time
            time.sleep( max( 0,(process_time-datetime.now()).total_seconds()) )
        except KeyboardInterrupt:
            if ask("Abort?",default=-1):
                print()
                print()
                print('-'*60)
                print('Aborting!'+' '*60)
                print()
                print()
                report()
                interrupted = True
    print()

    logger.info("Number of recalculation input left: {}".format(len(recalc_data_left)))
    print("Number of recalculation input left: {}".format(len(recalc_data_left)))

    if not is_stuck:
        if len(recalc_data_left) > 0:
            if recalc_file is not None:
                with open( recalc_file, 'w', encoding='utf-8' ) as f:
                    f.writelines( ['{}\n'.format(i) for i in sorted(list(recalc_data_left)) ] )
                logger.debug("Recalculation input dumped back to disk")
            else:
                logger.error("recalc_file file is None")

        logger.info("Dumping cache")
        #Sava all processed requests (including errors)
        dumped_fname = os.path.normpath( send_request_api._original.dump_cache() )

        exceptions = [ repr( i.exception() ) for i in results if i.exception() is not None ]

        #Print exceptions2
        for i in results:
            res = i.result(0)
            if res is not None and res[-1] is not None and len(res[-1]) > 0:
                exceptions.append(res[-1])

        common_exceptions = dict()
        for ex in exceptions:
            common_exceptions[ex] = common_exceptions.get(ex,0)+1


        print("Number of occured exceptions: {}".format(len(exceptions)))
        if len(exceptions) > 0:
            logger.warning("Number of exceptions: {}".format(len(exceptions)))

            exception_i = 0
            print("Exceptions")
            for k,v in common_exceptions.items():
                print('EXCEPTION OCCURED {} times'.format(v))
                if len(k) > 1100:
                    print(k[:500]+'\n'+'...'+'\n'+k[500:])
                else:
                    print(k)
                exception_i += 1
                if exception_i > 100:
                    break


        print("Cache file: {} Saved cache size: ".format(dumped_fname),end='')
        with open(dumped_fname,'rb') as f:
            len_f = len(pkl.load(f))
            print( len_f )
            logger.info("Saved cache size: {}".format(len_f))

    # Pass keyboardInterrupt further
    if interrupted:
        logger.info("Passing KeyboardInterrupt")
        raise KeyboardInterrupt()