# coding: utf-8

import asyncio
import aiohttp
import time


url = 'http://sina.com'
aio_num = 1100
headers = {"Authorization": 'Bearer eyJhbGciOiJIUzI1NiIsImlhdCI6MTUxNjMyNzQyNSwiZXhwIjoxNTE2MzMxMDI1fQ.eyJ0b2tlbl9pZCI6N30.zF1QZ2N_QkOoQfWfDVK_Vigh3DtWq_2W-b5hrtzHH3Q'}


# async def getIndex(url):
    # async with aiohttp.ClientSession() as session:
        # async with session.get(url) as response:
            # return await response.text()


loop = asyncio.get_event_loop()

#### first method to get result
# tasks = []
# for i in range(10):
    # tasks.append(getIndex(url))
# futures = asyncio.gather(*tasks)
# loop.run_until_complete(futures)
# results = futures.result()
# print results[0]


### second method to get result
# tasks = []
# for i in range(10):
    # tasks.append(asyncio.ensure_future(getIndex(url)))
# loop.run_until_complete(asyncio.wait(tasks))
# print(tasks[0].result())


### third method to get result
# tasks = []
# for i in range(10):
    # tasks.append(asyncio.ensure_future(getIndex(url)))
# results = loop.run_until_complete(asyncio.wait(tasks))
# print(results[1])

### fourth method to get result
# tasks = []
# for i in range(aio_num):
    # tasks.append(getIndex(url))
# start_time = time.time()
# results = loop.run_until_complete(asyncio.gather(*tasks))
# print(results[0])
# print("time cost: %s" % (time.time() - start_time))

### aiohttp的正确使用方式, 一个session用户多个get
# Don’t create a session per request. Most likely you need a session per application which performs all requests altogether.
# A session contains a connection pool inside, connection reusage and keep-alives (both are on by default) may speed up total performance.
async def getUrl(session, url, **kwargs):
    async with session.get(url, **kwargs) as res:
        return await res.text()

tasks = []
try:
    session = aiohttp.ClientSession()
    for _ in range(aio_num):
        tasks.append(getUrl(session, url, headers=headers))
    start_time = time.time()
    results = loop.run_until_complete(asyncio.gather(*tasks))
finally:
    session.close()
print(results[0])
print("time cost: %s" % (time.time() - start_time))


### can't do
# futures = asyncio.run_coroutine_threadsafe(asyncio.wait(tasks), loop)
# print(futures.result()[0])
