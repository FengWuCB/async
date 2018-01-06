# coding: utf-8

import asyncio
import aiohttp


url = 'http://www.sina.com'

async def getIndex(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


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
tasks = []
for i in range(10):
    tasks.append(getIndex(url))
results = loop.run_until_complete(asyncio.gather(*tasks))
print(results[0])



### can't do
# futures = asyncio.run_coroutine_threadsafe(asyncio.wait(tasks), loop)
# print(futures.result()[0])
