from functools import wraps
import os
from slugify import slugify
import inspect
import pickle as pkl
from filelock import FileLock
from threading import Lock
import errno
from uuid import uuid4
import traceback
from pickle import PicklingError

_CACHING_DIR = 'func_cache'

# store all dumping functions
_all_dump = []

class DumpFailed(Exception):
    pass

def set_cache_dir(d):
    _CACHING_DIR = d
    
def dump_all():
    for d in _all_dump:
        d()

def _optional_arg_decorator(fn):
    def wrapped_decorator(*args, **kwargs):
        if len(args) == 1 and callable(args[0]):
            return fn(args[0])

        else:
            def real_decorator(decoratee):
                return fn(decoratee, *args, **kwargs)

            return real_decorator

    return wrapped_decorator

@_optional_arg_decorator
def cached(func,fname=None,dump_every=-1,cache_kwargs=False):
    
    if cache_kwargs:
        @wraps(func)
        def wrapper(*args,**kwargs):
            cache_key = tuple(list(args)+list(kwargs.items()))
            try:
                with func.cache_lock:
                    return func.cache[ cache_key ]
            except KeyError:
                pass

            result = func(*args,**kwargs)

            with func.cache_lock:
                func.cache[ cache_key ] = result

            if func.dump_every > 0 and (len(func.cache)-func.saved_cache_size >= func.dump_every):
                func.dump_cache()

            return result
    else:
        @wraps(func)
        def wrapper(*args,**kwargs):
            try:
                with func.cache_lock:
                    return func.cache[args]
            except KeyError:
                pass

            result = func(*args,**kwargs)

            with func.cache_lock:
                func.cache[args] = result

            if func.dump_every > 0 and (len(func.cache)-func.saved_cache_size >= func.dump_every):
                func.dump_cache()

            return result

    def dump_cache(fname=None,update_from_disk=True, reset_after_dump=False, max_retries = 1000):
        # Don't dump if we have no filename specified
        if fname is None:
            fname = func.cache_fname

        if fname is None:
            raise FileNotFoundError("Could not dump cache, pleasef specify filename")
    
        # Cache may contain data that costs money, dump it at any cost
        new_fname = fname
        def get_new_fname(fname): # function for generating new filenames if others dont work
            return '{0}_{2}{1}'.format(*os.path.splitext(fname),str(uuid4()))

        #Lock on file
        cache_saved = False # Keep track if cache was saved in the end
        retry_n = 0 # Keep track of number of retries, we don't want infinite loop
        if retry_n >= max_retries: # Well, maybe sometimes we do
            max_retries = -1
                               
        exception_text = None # Store last exception text for debug purposes
        keyboard_interrupt = None # If user invoked KeyboardInterrupt during cache saving, save it and then raise it
        
        while not cache_saved and (retry_n < max_retries or max_retries == -1):
            retry_n += 1
            try:
                with FileLock(new_fname+"_lock",10): # Lock on that file. Timeout 10 seconds. If it fails, file name will be changed
                    # Append cache to file
                    # Load existing cache from file
                    if update_from_disk:
                        try:
                            with open(new_fname,'rb') as f:
                                existing_cache = pkl.load(f)
                                
                            with func.cache_lock:
                                # Update cache from file
                                for k,v in existing_cache.items():
                                    if k not in func.cache:
                                        func.cache[k] = v

                        except (OSError, IOError) as e:
                            if getattr(e, 'errno', 0) == errno.ENOENT:
                                # File not found
                                pass
                            else:
                                # Could not update cache from file for some reason, we can't dump it there, dump somewhere else
                                new_fname = get_new_fname(fname)
                                while os.path.exists(new_fname):
                                    new_fname = get_new_fname(fname)
                                exception_text = traceback.format_exc()
                                continue # Something went wrong, continue to next iteration

                    # Ok, we've loaded cache from disk or skipped this procedure
                    with func.cache_lock: # Lock on cache, we may be multi-threading
                        # Dump updated cache to file
                        with open(new_fname,'wb') as f:
                            pkl.dump(func.cache,f)
                            if reset_after_dump: # We may want to free memory after dumping
                                func.reset_cache()
                            cache_saved = True # Finally, yes, we did save cache
                        
                    func.saved_cache_size = len(func.cache)
            except PicklingError as ex:
                # We could not pickle that fucking file, raise error immedeatelly
                raise
            except Exception as ex:
                # We could not dump cache for some reason, generate new filename to dump to
                while os.path.exists(new_fname):
                    new_fname = get_new_fname(fname)
                exception_text = traceback.format_exc()
                continue # Something went wrong, continue to next iteration
            except KeyboardInterrupt as ex:
                keyboard_interrupt = ex
                continue # Another attempt, we still want to save that fucking cache

        if not cache_saved:
            raise DumpFailed("Dump failed after {} attempts. Last exception traceback:\n{}".format(retry_n,exception_text))

        if keyboard_interrupt:
            raise keyboard_interrupt
            
        return new_fname

    def reset_cache():
        """ Reset current cache in memory, but keep it on disk
        """
        try:
            del func.cache
        except AttributeError:
            pass
        func.cache = dict()
        func.saved_cache_size = 0

    def set_cache_fname(fname, dump_every, dump_cache=True, reset_cache=True):
        """ Use this function to initialize function caching. It will make sure that all necessary data is dumped
        """
        if dump_cache:
            if len(func.cache) > 0:
                func.dump_cache()

        if reset_cache:
            func.reset_cache()

        # Deduct cache fname
        if fname is None and dump_every > 0:
            frame = inspect.stack()[2]
            fname_prefix = slugify( os.path.splitext( os.path.split(frame[1])[1] )[0] )
            fname_postfix = '_'.join( list( inspect.signature(func).parameters.keys() ) )
            fname = os.path.join(_CACHING_DIR,'py_{}_{}_{}_cache.pkl'.format(fname_prefix,func.__name__,fname_postfix))
     
        # Create directory if necessary
        if fname is not None:
            file_dir = os.path.split(fname)[0]
            if len(file_dir)>0 and not os.path.exists(file_dir):
                os.makedirs(file_dir)
            
        # Create cache attribute if necessary
        try:
            func.cache
        except AttributeError:
            func.cache = dict()

        # Update cache from file
        if fname is not None and os.path.exists(fname):
            with FileLock(fname+"_lock",10):
                
                new_cache = None
                try:
                    with open(fname,'rb') as f:
                        new_cache = pkl.load(f)
                except EOFError: # pkl may fail with EOFError if file contains no data
                    pass

                # Update func cache from file if possible
                if new_cache is not None and len(new_cache) > 0:
                    for k,v in new_cache.items():
                        if k not in func.cache:
                            func.cache[k] = v

        func.cache_fname = fname
        func.dump_every = dump_every
        func.saved_cache_size = len(func.cache)


    func.cache_fname = None
    func.dump_every = -1

    func.reset_cache = reset_cache
    func.set_cache_fname = set_cache_fname
    func.dump_cache = dump_cache
    func.cache_lock = Lock()

    wrapper._original = func
    wrapper.dump_cache = dump_cache
    wrapper.reset_cache = reset_cache
    wrapper.set_cache_fname = set_cache_fname
    
    _all_dump.append(dump_cache)
    
    return wrapper