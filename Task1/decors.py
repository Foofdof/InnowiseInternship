import time
from functools import wraps

def time_it(t=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            total_time = 0
            result = None
            for _ in range(t):
                start = time.time()
                result = func(*args, **kwargs)
                end = time.time()
                total_time += (end - start)
            avg_time = total_time / t
            print(f"Avg: {avg_time:.6f} sec")
            return result
        return wrapper
    return decorator