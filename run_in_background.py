from functools import wraps
import inspect
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime

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
def run_in_background(func,n_threads=None):
    @wraps(func)
    def wrapper(*args,**kwargs):
        def counter(*args,**kwargs):
            
            with func._running_tasks_lock:
                #Keep track of how many tasks per second
                func.execution_times.append(datetime.now())
                ex = None
            
            try:
                ret = func(*args,**kwargs)
            except BaseException as e:
                ex = e
                
            # Keep track of running tasks
            with func._running_tasks_lock:
                # Remove current task from running
                del func._running_tasks[(args, tuple(kwargs) )]
                
            # Raise exception after clearing from current tasks if any
            if ex is not None:
                raise ex
                
            return ret
        
        with func._running_tasks_lock:
            if (args,tuple(kwargs) ) in func._running_tasks:
                #That task is already running!
                return func._running_tasks[ (args,tuple(kwargs) ) ][1]
            else:
                #Run task
                future = func._POOL_EXECUTOR.submit(counter, *args, **kwargs)
                func._running_tasks[ (args,tuple(kwargs) ) ] = (datetime.now(), future)
                
        return future
    
        
    def get_running_tasks():
        with func._running_tasks_lock:
            return func._running_tasks.copy()
    
    func._POOL_EXECUTOR = ThreadPoolExecutor(max_workers=n_threads)
    func._running_tasks = dict()
    func.get_running_tasks = get_running_tasks
    func._running_tasks_lock = Lock()
    func.execution_times = []
    
    wrapper._original = func
    return wrapper