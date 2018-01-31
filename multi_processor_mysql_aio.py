# coding: utf-8

import time
import asyncio
import random
from concurrent.futures import ProcessPoolExecutor as Pool

import aiomysql
from flashtext import KeywordProcessor
import click


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

    def __init__(self, workers=2, pool=10, start=0, end=2000):
        """第一段的参数需要跟随需求变动"""
        self.host = "192.168.12.34"
        self.port = 3306
        self.user = "root"
        self.password = "root"
        self.db = "laws_doc2"
        self.origin_table = 'judgment2'  # part_v2
        self.dest_table = 'laws_finance2'
        self.s_sql = f"select uuid, title, party_info, trial_process, court_find from {self.origin_table} where %s<=id and id<%s;"
        # self.origin_table = "judgment2_main_etl"  # main
        # self.dest_table = "laws_finance1"
        # self.s_sql = f"select uuid, court_idea, judge_result, reason, plt_claim, dft_rep, crs_exm from {self.origin_table} where %s<=id and id<%s;"

        self.i_sql = f"insert into {self.dest_table} (uuid, title, reason, keyword) values (%s, %s, %s, %s)"

        self.pool = pool    # 协程数和MySQL连接数
        self.aionum = self.pool
        self.step = 2000  # 一次性从MySQL拉取的行数
        self.workers = workers  # 进程数
        self.start = start  # MySQL开始的行数
        self.end = end  # MySQL结束的行数

        # self.keyword = ["庞氏骗局", "非法集资", "高额回报", "快速致富", "非法从事资金清算", "高息揽储",
                        # "供应链金融", "网络传销", "虚拟货币传销", "网络借贷", "股权众筹", "第三方支付", "小额贷款",
                        # "股权融资", "非法吸收公众存款", "高息诱饵", "虚构借贷协议", "高回报", "虚构投资项目", "资金池",
                        # "集资诈骗", "期限错配", "投资欺诈", "平台洗钱", "网络保险", "网络信托", "资金错配", "虚构项目",
                        # "虚假信息诈骗", "P2P", "p2p"]
        self.keyword = ['非法经营支付业务', '网络洗钱', '资金池', '支付牌照', '清洁算', '网络支付', '网上支付', '移动支付', '聚合支付', '保本保息', '担保交易', '供应链金融', '网贷', '网络借贷', '网络投资', '虚假标的', '自融', '资金池', '关联交易', '庞氏骗局', '网络金融理财', '线上投资理财', '互联网私募', '互联网股权', '非法集资', '合同欺诈', '众筹投资', '股权转让', '互联网债权转让', '资本自融', '投资骗局', '洗钱', '非法集资', '网络传销', '虚拟币泡沫', '网络互助金融', '金融欺诈', '网上银行', '信用卡盗刷', '网络钓鱼', '信用卡信息窃取', '网上洗钱', '洗钱诈骗', '数字签名更改', '支付命令窃取', '金融诈骗', '引诱投资', '隐瞒项目信息', '风险披露', '夸大收益', '诈骗保险金', '非法经营保险业务', '侵占客户资金', '征信报告窃取', '金融诈骗', '破坏金融管理']
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
        # 随机休息一定时间，防止数据同时到达，同时处理, 应该是一部分等待,一部分处理
        await asyncio.sleep(random.random() * self.workers * 2)
        print("coroutine start")
        async with db.acquire() as conn:
            async with conn.cursor() as cur:
                while start < end:
                    tmp_end = start + self.step
                    if tmp_end > end:
                        tmp_end = end
                    print("aio start: %s, end: %s" % (start, tmp_end))
                    # <=id 和 id<
                    await cur.execute(self.s_sql, (start, tmp_end))
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
                                keyword = ' '.join(set(keyword))   # 对关键字去重
                                # print(keyword)
                                uuids.append(
                                    (data.uuid, data.title, data.reason, keyword))
                    await cur.executemany(self.i_sql, uuids)
                    await conn.commit()
                    start = tmp_end

    def singleProcess(self, start, end):
        """单个进程的任务"""
        loop = asyncio.get_event_loop()
        # 为每个进程创建一个pool
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


@click.command(help="运行")
@click.option("-w", "--workers", default=2, help="进程数")
@click.option('-p', "--pool", default=10, help="协程数")
@click.option('-s', '--start', default=0, help='MySQL开始的id')
@click.option('-e', "--end", default=60000, help="MySQL结束的id")
def main(workers, pool, start, end):
    mp = MultiProcessMysql(workers=workers, pool=pool, start=start, end=end)
    if workers * pool > 100:
        if not click.confirm('MySQL连接数超过100(%s)，确认吗?' % (workers * pool)):
            return
    mp.run()


if __name__ == "__main__":
    main()
