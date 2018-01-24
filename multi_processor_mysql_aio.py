# coding: utf-8

import time
import asyncio
from concurrent.futures import ProcessPoolExecutor as Pool

import aiomysql
from flashtext import KeywordProcessor


class AttrDict(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            return None

    def __setattr__(self, name, value):
        self[name] = value


class AttrDictCursor(aiomysql.DictCursor):
    dict_type = AttrDict


class MultiProcessMysql(object):
    """用多进程和协程处理MySQL数据"""

    def __init__(self, workers=2, start=0, end=2000):
        self.host = "192.168.12.35"
        self.port = 3306
        self.user = "root"
        self.password = "123456"
        self.db = "civil"

        self.pool = 20    # 协程数和MySQL连接数
        self.aionum = self.pool
        self.step = 2000  # 一次性从MySQL拉取的行数
        self.workers = workers  # 进程数
        self.start = start  # MySQL开始的行数
        self.end = end  # MySQL结束的行数

        self.keyword = ["庞氏骗局", "非法集资", "高额回报", "快速致富", "非法从事资金清算", "高息揽储",
                        "供应链金融", "网络传销", "虚拟货币传销", "网络借贷", "股权众筹", "第三方支付", "小额贷款",
                        "股权融资", "非法吸收公众存款", "高息诱饵", "虚构借贷协议", "高回报", "虚构投资项目", "资金池",
                        "集资诈骗", "期限错配", "投资欺诈", "平台洗钱", "网络保险", "网络信托", "资金错配", "虚构项目",
                        "虚假信息诈骗", "P2P", "p2p"]
        self.kp = KeywordProcessor()
        self.kp.add_keywords_from_list(self.keyword)

    async def createMysqlPool(self, loop):
        """每个进程要有独立的pool，所以不绑定self"""
        pool = await aiomysql.create_pool(
            loop=loop, host=self.host, port=self.port, user=self.user,
            password=self.password, db=self.db, maxsize=self.pool,
            charset='utf8', cursorclass=AttrDictCursor
        )
        return pool

    def cutRange(self, start, end, times):
        """将数据区间分段"""
        partition = (end - start) // times
        ranges = []
        tmp_end = start
        while tmp_end < end:
            tmp_end += partition
            # 剩下的不足以再分
            if (end - tmp_end) < partition:
                tmp_end = end
            ranges.append((start, tmp_end))
            start = tmp_end
        return ranges

    async def findKeyword(self, db, start, end):
        """从MySQL数据中匹配出关键字"""
        async with db.acquire() as conn:
            async with conn.cursor() as cur:
                while start < end:
                    tmp_end = start + self.step
                    if tmp_end > end:
                        tmp_end = end
                    print("aio start: %s, end: %s" % (start, tmp_end))
                    # <=id 和 id<
                    await cur.execute("select uuid, title, party_info, trial_process, plt_claim, dft_rep, crs_exm, court_find, court_idea, judge_result, reason from judgment_civil_56w where %s<=id and id<%s;", (start, tmp_end))
                    datas = await cur.fetchall()
                    uuids = []
                    for data in datas:
                        if data:
                            for key in list(data.keys()):
                                if not data[key]:
                                    data.pop(key)
                            keyword = self.kp.extract_keywords(
                                " ".join(data.values()))
                            if keyword:
                                keyword = ' '.join(keyword)
                                # print(keyword)
                                uuids.append(
                                    (data.uuid, data.title, data.reason, keyword))
                    await cur.executemany("insert into civil_finance (uuid, title, reason, keyword) values (%s, %s, %s, %s)", uuids)
                    await conn.commit()
                    start = tmp_end

    def singleProcess(self, start, end):
        """单个进程的任务"""
        loop = asyncio.get_event_loop()
        db = loop.run_until_complete(asyncio.ensure_future(
            self.createMysqlPool(loop)))

        tasks = []
        ranges = self.cutRange(start, end, self.aionum)
        print(ranges)
        for start, end in ranges:
            tasks.append(self.findKeyword(db, start, end))
        loop.run_until_complete(asyncio.gather(*tasks))

    def run(self):
        """多进程跑"""
        tasks = []
        ranges = self.cutRange(self.start, self.end, self.workers)
        start_time = time.time()
        with Pool(max_workers=self.workers) as executor:
            for start, end in ranges:
                print("processor start: %s, end: %s" % (start, end))
                tasks.append(executor.submit(self.singleProcess, start, end))
            for task in tasks:
                task.result()
        print("total time: %s" % (time.time() - start_time))


if __name__ == "__main__":
    mp = MultiProcessMysql(workers=2, end=560000)
    mp.run()
