# coding: utf-8

from concurrent.futures import ThreadPoolExecutor as Pool
import requests

URLS = ['http://q45354q.com', 'http://www.baidu.com', 'http://sina.com']


def task(url, timeout=10):
    try:
        result = requests.get(url, timeout=timeout)
    except Exception as e:
        result = {}
        print("func error: %s" % e)
    return result


pool = Pool(max_workers=3)
results = pool.map(task, URLS)

# 这里会按照顺序输出, 如果用as_completed则不会按照参数顺序，而是按照完成顺序
# 但是用来map之后，就不能使用as_completed的方法
# 用map要自己在函数里面处理好异常,否则会全部抛出,影响其他线程
for ret in results:
    if ret:
        print('%s, %s' % (ret.url, len(ret.content)))
