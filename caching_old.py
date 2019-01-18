from functools import wraps
import os
from slugify import slugify
import inspect
import pickle as pkl
from filelock import FileLock
from threading import Lock
from uuid import uuid4

_CACHING_DIR = 'func_cache'

# store all dumping functions
_all_dump = []

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
def cached(func,fname=None,dump_every=-1):
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
        
    def dump_cache(fname=None,update_from_disk=True):
        # Don't dump if we have no filename specified
        if fname is None:
            fname = func.cache_fname

        if fname is None:
            raise FileNotFoundError("Could not dump cache, please specify filename")


        #new_fname = fname
        new_fname = '{0}_{2}{1}'.format(*os.path.splitext(fname),str(uuid4()))
        while os.path.exists(new_fname):
            new_fname = '{0}_{2}{1}'.format(*os.path.splitext(fname),str(uuid4()))

        #Lock on file
        cache_saved = False
        while not cache_saved:
            try:
                with FileLock(new_fname+"_lock",10):
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

                        except FileNotFoundError:
                            pass
                        except IOError:
                            pass

                    with func.cache_lock:
                        # Dump updated cache to file
                        with open(new_fname,'wb') as f:
                            pkl.dump(func.cache,f)
                            del func.cache
                            func.cache = dict()
                            func.saved_cache_size = 0
                            cache_saved = True
                        
                    func.saved_cache_size = len(func.cache)
            except Exception as ex:
                while os.path.exists(new_fname):
                    new_fname = '{0}_{2}{1}'.format(*os.path.splitext(fname),str(uuid4()))


            return new_fname
            # File lock released

    def reset_cache():
        func.cache = {}
        func.saved_cache_size = 0
        func.cache_fname = None

    def set_cache_fname(fname, dump_every, dump_cache=True, reset_cache=True):
        
        if dump_cache:
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
            
        try:
            func.cache
        except AttributeError:
            func.cache = {}

        # Update cache from file
        if fname is not None and os.path.exists(fname):
            with FileLock(fname+"_lock",10):
                with open(fname,'rb') as f:
                    new_cache = pkl.load(f)

                # Update func cache from file
                for k,v in new_cache:
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