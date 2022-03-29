import csv
import os
import threading
import time
from functools import wraps

lock = threading.Lock() ## i need this for printing things in the correct order on the standard output


def _timeFunctionCalls(rounds, function, *args, **kwargs):
    results = dict()
    for i in range(rounds):
        run = 0
        time1 = time.perf_counter()
        function(*args, **kwargs)
        run = time.perf_counter() - time1
        results[i+1] = run
    return results


def benchmark(warmups=0, iter=1, verbose=False, csv_file=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args,**kwargs) ##store the function result for return
            warm = dict()
            if warmups != 0:
                warm = _timeFunctionCalls(warmups, func, *args, **kwargs)
            invoke = _timeFunctionCalls(iter, func, *args, **kwargs)
            # without the lock things were overlapping on the screen
            lock.acquire()
            if verbose:
                
                print("{:<8} {:<15} {:<10}".format(
                    'run num', 'is warmup', 'time'))
                
                for k, v in warm.items():
                    print("{:<8} {:<15} {:<10}".format(k, 'yes', v))

                for k, v in invoke.items():
                    print("{:<8} {:<15} {:<10}".format(k+warmups, 'no', v))

            avg = sum(invoke.values()) / iter
            variance = sum((v - avg) ** 2 for v in invoke.values()) / iter
            print("\n{:<8} {:<15}\
                    \n{:<8} {:<15}\n"\
                    .format('average time', avg,\
                            'variance',variance))
            
            lock.release()
            if csv_file:
                with open(csv_file, "a") as f:
                    writer = csv.writer(f)
                    

                    writer.writerow(['THREAD:',threading.current_thread().getName(),''])
                    writer.writerow(['run num', 'is warmup', 'timing'])
                    for k, v in warm.items():
                        writer.writerow([k, 'yes', v])
                    for k, v in invoke.items():
                        writer.writerow([k+warmups, 'no', v])
                        
                    
                    writer.writerow(['average', avg,''])
                    writer.writerow(['variance', variance,''])
                    writer.writerow(['','',''])
            return result
        return wrapper
    return decorator


def _rec_fibonacci(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return _rec_fibonacci(n-1) + _rec_fibonacci(n-2)

def fibonacci(n=0):
    #using an helper function so the decorator doesn't execute its extra code for each recursive call
    return _rec_fibonacci(n)


def test():
    files = ["f_1_16.csv","f_2_8.csv","f_4_4.csv","f_8_2.csv"]
    #clear the existing files
    [os.remove(f) for f in files if os.path.isfile(f)]
    threads = list()
    for i in range(4):
        threads.clear()
        it = 2**i
        print("\n\nNUMBER OF THREADS:{}\n\n".format(it))
        fib = benchmark(iter=int(16/it),verbose=True,warmups=3, csv_file=files[i])
        fib = fib(fibonacci)
        [threads.append(threading.Thread(target=fib,args=(10,),name='{}-{}'.format(it,j+1))) for j in range(it)]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]

"""
By executing the test function and comparing the results  we can see
that increasing the number of threads does not bring any
significant improvement in the run time. 
This is caused by the Global Interpreter Lock.
As we know only one thread at a time is allowed to run python bytecode and this siginificantly impact on the effects
that multithreading usually has.
The fibonacci function used as a test is CPU bound, so the GIL is not released until completion.
"""


if __name__ == "__main__":
    test()