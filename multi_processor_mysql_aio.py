# coding: utf-8

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


mysql_config = {
    "HOST": "192.168.12.35",
    "PORT": 3306,
    "USER": "root",
    "PASSWORD": "123456",
    "DB": "civil",
    "POOLSIZE": 20
}


KEYWORDS = ["庞氏骗局", "非法集资", "高额回报", "快速致富", "非法从事资金清算", "高息揽储", "P2P", "p2p",
            "供应链金融", "网络传销", "虚拟货币传销", "网络借贷", "股权众筹", "第三方支付", "小额贷款",
            "股权融资", "非法吸收公众存款", "高息诱饵", "虚构借贷协议", "高回报", "虚构投资项目", "资金池",
            "集资诈骗", "期限错配", "投资欺诈", "平台洗钱", "网络保险", "网络信托", "资金错配", "虚构项目",
            "虚假信息诈骗"]


async def createMysqlPool(loop, config):
    "传入字典格式的配置"
    pool = await aiomysql.create_pool(
        loop=loop,
        host=config.get('HOST'),
        port=config.get('PORT'),
        user=config.get("USER"),
        password=config.get("PASSWORD"),
        db=config.get("DB"),
        maxsize=config.get("POOLSIZE", 10),
        charset='utf8',
        cursorclass=AttrDictCursor
    )
    return pool


def cutRange(start, end, times):
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


async def findKeyword(db, kp, start, end):
    step = 2000  # 一次性从MySQL拉取的行数
    async with db.acquire() as conn:
        async with conn.cursor() as cur:
            while start < end:
                tmp_end = start + step
                if tmp_end > end:
                    tmp_end = end
                print("aio start: %s, end: %s" % (start, tmp_end))
                await cur.execute("select uuid, title, party_info, trial_process, plt_claim, dft_rep, crs_exm, court_find, court_idea, judge_result, reason from judgment_civil_56w where %s<=id and id<%s;", (start, tmp_end))
                datas = await cur.fetchall()
                uuids = []
                for data in datas:
                    if data:
                        for key in list(data.keys()):
                            if not data[key]:
                                data.pop(key)
                        keyword = kp.extract_keywords(" ".join(data.values()))
                        if keyword:
                            keyword = ' '.join(keyword)
                            # print(keyword)
                            uuids.append((data.uuid, data.title, data.reason, keyword))
                await cur.executemany("insert into civil_finance (uuid, title, reason, keyword) values (%s, %s, %s, %s)", uuids)
                await conn.commit()
                start = tmp_end


def singleProcess(start, end, aionum):

    kp = KeywordProcessor()
    kp.add_keywords_from_list(KEYWORDS)
    loop = asyncio.get_event_loop()
    db = loop.run_until_complete(asyncio.ensure_future(createMysqlPool(loop, mysql_config)))

    tasks = []
    ranges = cutRange(start, end, aionum)
    print(ranges)
    for start, end in ranges:
        tasks.append(findKeyword(db, kp, start, end))
    # for i in range(start, total, partition):
        # tasks.append(findKeyword(db, kp, i, partition, step))
    loop.run_until_complete(asyncio.gather(*tasks))


if __name__ == "__main__":
    total = 560000
    workers = 3  # 进程数
    aionum = 20  # 协程数，不要超过MySQL连接数
    tasks = []
    ranges = cutRange(0, total, workers)
    with Pool(max_workers=workers) as executor:
        for start, end in ranges:
            print("processor start: %s, end: %s" % (start, end))
            tasks.append(executor.submit(singleProcess, start, end, aionum))
        for task in tasks:
            task.result()
