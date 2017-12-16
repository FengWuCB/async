# coding: utf-8
from concurrent.futures import ThreadPoolExecutor as Pool
from concurrent.futures import as_completed
import requests

URLS = ['http://qq.com', 'http://sina.com', 'http://www.baidu.com', ]


def task(url, timeout=10):
    return requests.get(url, timeout=timeout)


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
