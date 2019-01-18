from functools import wraps
from datetime import datetime
import sys, traceback

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
def no_exceptions(func,n_threads=None):
    @wraps(func)
    def wrapper(*args,**kwargs):
        start_time = datetime.now()
        ex_text = None
        ret = None
        try:
            ret = func(*args,**kwargs)
        except Exception as ex:
            ex_text = '\n'.join( traceback.format_exc().splitlines() )
        end_time = datetime.now()

        return (ret, start_time, end_time, ex_text)
    
    wrapper._original = func
    return wrapper