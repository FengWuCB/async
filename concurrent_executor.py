# coding: utf-8
from concurrent.futures import ThreadPoolExecutor as Pool
from concurrent.futures import as_completed
import requests

URLS = ['http://qq.com', 'http://sina.com', 'http://www.baidu.com', 'http://c']


def task(url, timeout=10):
    return requests.get(url, timeout=timeout)


# pool可以被重复使用，而且只要不运行.result()不会抛出异常
pool = Pool(max_workers=3)
task1 = pool.submit(task, URLS[0])
task2 = pool.submit(task, URLS[0])
# task3的网址是错误的
task3 = pool.submit(task, URLS[3])
task4 = pool.submit(task, URLS[0])
print(task4.result())
# 在这之前，没有实际调用task3.result(), 没有抛错
try:
    task3.result()
except Exception:
    print("task3 fail")
# 就算pool中有task fail，也可以被shutdown
pool.shutdown()  # 用完之后要自己shutdown，用with的话会系统会自己处理
try:
    task5 = pool.submit(task, URLS[0])
except RuntimeError:
    print("can't use pool after it was shutdown")
# import pdb; pdb.set_trace()


with Pool(max_workers=3) as executor:
    future_tasks = [executor.submit(task, url) for url in URLS]
    print(type(future_tasks))

    for f in future_tasks:
        if f.running():
            print('%s is running' % str(f))

    # 很多地方都可以设置timeout参数，也可以捕捉异常
    # as_completed，先执行的线程先返回
    for f in as_completed(future_tasks, timeout=None):
        try:
            ret = f.done()
            if ret:
                try:
                    f_ret = f.result(timeout=None)
                except Exception as e:
                    print("func run error: %s" % e)
                else:
                    print('%s, done, result: %s, %s' % (str(f), f_ret.url, len(f_ret.content)))
        except Exception as e:
            f.cancel()
            print(str(e))
