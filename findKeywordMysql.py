# coding: utf-8

import asyncio
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
    "HOST": "192.168.42.35",
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


async def findKeyword(db, kp, start_id, partition, step):
    async with db.acquire() as conn:
        async with conn.cursor() as cur:
            for start in range(start_id, start_id+partition, step):
                print(start)
                await cur.execute("select uuid, title, party_info, trial_process, plt_claim, dft_rep, crs_exm, court_find, court_idea, judge_result, reason from judgment_civil_56w where %s<id and id<%s;", (start, start+step))
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
                            print(keyword)
                            uuids.append((data.uuid, data.title, data.reason, keyword))
                await cur.executemany("insert into civil_finance (uuid, title, reason, keyword) values (%s, %s, %s, %s)", uuids)
                await conn.commit()


if __name__ == "__main__":
    kp = KeywordProcessor()
    kp.add_keywords_from_list(KEYWORDS)
    loop = asyncio.get_event_loop()
    db = loop.run_until_complete(asyncio.ensure_future(createMysqlPool(loop, mysql_config)))

    step = 2000
    aionum = 10
    total = 200000
    partition = total // aionum
    tasks = []
    for i in range(0, total, partition):
        tasks.append(findKeyword(db, kp, i, partition, step))
    loop.run_until_complete(asyncio.gather(*tasks))

