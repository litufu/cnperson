import tushare as ts
import pandas as pd
import numpy as np
import time
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup
import io
from sqlalchemy import create_engine
import utils
import settings
import logging

# 设置日志文件
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log.txt",encoding="utf-8")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
# 设置数据库引擎
engine = create_engine('sqlite:///cnperson.db')
# 设置tushare token
ts.set_token('bf9ac3f395ddedda4e8be0cbc6243098ba839ca9a42c0170f44a1b20')
pro = ts.pro_api()
# 设置企查查爬虫headers
headers = {
    'user-agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36"
}
# 追溯股东层数
level = 1
# 下载股东列表
times = 1


def get_cookies():
    # 将cookies字符串转化为dict
    f = open(r'cookies.txt', 'r')  # 打开所保存的cookies内容文件
    cookies = {}  # 初始化cookies字典变量
    for line in f.read().split(';'):  # 按照字符：进行划分读取
        # 其设置为1就会把字符串拆分成2份
        name, value = line.strip().split('=', 1)
        cookies[name] = value  # 为字典cookies添加内容
    return cookies


def get_company_detail_url(company_name):
    # 获取公司企查查详情页url
    payload = {'key': company_name}
    cookies = get_cookies()
    origin_url = 'https://www.qichacha.com/search'
    r = requests.get(origin_url, params=payload, headers=headers, cookies=cookies)
    time.sleep(3)
    content = r.text
    soup = BeautifulSoup(content, 'html.parser')
    trs = soup.find_all("tr")
    for tr in trs:
        a = tr.find(href=re.compile("firm"))
        if company_name == a.get_text():
            href = a['href']
            return href
    return ""


def download_company_holders(company_name):
    # 判断是否已经存储了该公司，存储则返回
    try:
        df = pd.read_sql_table('holders', con=engine)
    except ValueError as e:
        df = pd.DataFrame()
    if not df.empty:
        df = df[df['name'].str.match(company_name)]
        if df.shape[0] > 0:
            return df

    #  没有存储则到企查查爬取
    #  获取详情页页面
    url = get_company_detail_url(company_name)
    if url.isspace():
        logger.info("{}没有获得详情页url".format(company_name))
        return df
    origin_url = 'https://www.qichacha.com'
    # 请求详情页面
    r = requests.get(origin_url + url, headers=headers)
    time.sleep(3)
    content = r.text
    soup = BeautifulSoup(content, 'html.parser')
    # 获取网页中股东部分
    partners = soup.find("section", id="partnerslist")
    if partners is None:
        logger.info("{}详情页没有股东部分".format(company_name))
        return df
    # 获取股东表格
    table = partners.find('table')
    if table is None:
        logger.info("{}股东部分没有表格".format(company_name))
        return df
    # 处理股东表格，使得其成为标准的html的table格式
    # 去除掉表头的a标签
    for th in table.find_all('th'):
        a = th.find('a')
        if a:
            a.replace_with('')
    # 去除掉单元格中的表格，用表格中的h3标签内容代替
    for tab in table.find_all('table'):
        name = tab.find('h3').string
        tab.replace_with(name)
    # 获取表格中所有的行
    rows = table.find_all('tr')
    # 利用StringIO制作df
    csv_io = io.StringIO()
    for row in rows:
        row_texts = []
        for cell in row.findAll(['th', 'td']):
            text = cell.get_text().strip()
            text = text.replace(',', '')
            res = text.split('\n')
            if len(res) > 1:
                text = res[0].strip()
            row_texts.append(text)
        row_string = ','.join(row_texts) + '\n'
        csv_io.write(row_string)
    csv_io.seek(0)
    df = pd.read_csv(csv_io)
    pattern = re.compile(r'(.*)[\(（].*?')
    df = df.rename(index=str, columns=lambda x: re.match(pattern, x)[1] if re.match(pattern, x) else x)
    df = df.rename(index=str, columns={
        "序号": "no",
        "股东": "holder_name",
        "持股比例": "ratio",
        "认缴出资额": "promise_to_pay_amount",
        "认缴出资日期": "promise_to_pay_date",
        "实缴出资额": "pay_amount",
        "实缴出资日期": "pay_date",
    })
    df['name'] = company_name
    df = df[['no', 'holder_name', 'ratio', 'promise_to_pay_amount', 'promise_to_pay_date', 'name']]
    # 保存到数据库
    df.to_sql('holders', con=engine, if_exists='append', index=False)
    return df


def download_stocks():
    # 下载股票列表
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,list_date')
    data.to_sql('stocks', con=engine,if_exists='replace', index=False)


def get_stocks():
    # 获取股票列表
    try:
        df = pd.read_sql_table('stocks', con=engine)
    except ValueError as e:
        download_stocks()
        df = pd.read_sql_table('stocks', con=engine)
    finally:
        return df


def get_top10_holders(dbname, start_date, end_date):
    # 下载所有股票的前10大股东
    # 示例 ：
    # get_top10_holders(start_date="20180901", end_date="20181231")
    stocks = get_stocks()
    for ts_code in stocks["ts_code"]:
        # 遍历所有的股票列表，获取所有的前10大股东列表
        if utils.has_record(dbname, ts_code):
            continue
        try:
            df = pro.top10_holders(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except requests.exceptions.ConnectTimeout as e:
            get_top10_holders(dbname=dbname, start_date=start_date, end_date=end_date)
        time.sleep(2)
        df_top10 = df[0:10]
        df_top10.to_sql(dbname, con=engine, if_exists="append", index=False)
        utils.save_record(dbname, ts_code)


def download_holders(company_name):
    global times
    if times > 10:
        logger.warning('没有能够下载{}股东'.format(company_name))
        raise Exception('没有能够下载{}股东'.format(company_name))
    try:
        df = download_company_holders(company_name)
        return df
    except Exception as e:
        times += 1
        download_holders(company_name)


def get_all_holders(name):
    global level
    if level > 10:
        return
    df = pd.read_sql_table(name, con=engine)
    try:
        not_found_holders_company = pd.read_sql_table('not_found_holders_company', con=engine)
        not_found_holders_company = not_found_holders_company[['ts_code', 'ann_date', 'end_date', 'holder_name', 'hold_amount', 'hold_ratio']]
    except ValueError as e:
        not_found_holders_company = pd.DataFrame()
    stocks = get_stocks()

    name = settings.NUMBER_CONSTANT[level]
    try:
        new_df = pd.read_sql_table(name, con=engine)
    except ValueError as e:
        new_df = pd.DataFrame()

    df = df[['ts_code', 'ann_date', 'end_date', 'holder_name', 'hold_amount', 'hold_ratio']]
    # 将股东分为两种类型，一种是自然人或者基金，无法或不需要追查股东的；另一类是公司需要进一步追查股东的;
    # 第一类,直接存入数据库
    now_df = df[(~df['holder_name'].str.endswith('公司')) & (~df['holder_name'].str.contains('自有资金'))]
    if new_df.empty:
        now_df.to_sql(name, con=engine, if_exists="append", index=False)
    # 第二类
    # 新建一个所有没有找到股东的公司列表
    # 用来存储向股东数据库增加的数据
    df_not_found_holders = pd.DataFrame(columns=('ts_code', 'ann_date', 'end_date',
                                                 'holder_name', 'hold_amount', 'hold_ratio'))
    # 用来存储向未找到股东数据库增加的数据
    df_not_found_holders_company = pd.DataFrame(columns=('ts_code', 'ann_date', 'end_date',
                                                'holder_name', 'hold_amount', 'hold_ratio'))
    # 2.1以公司结尾的
    company_df = df[df['holder_name'].str.endswith('公司')]
    # 2.2没有以公司结尾但包含“公司”和“自有资金”的
    own_funds_df = df[(df['holder_name'].str.contains('自有资金')) & (df['holder_name'].str.contains('公司'))]
    company_df = company_df.append(own_funds_df, ignore_index=True)
    # 如果要找的公司全部在未找到公司列表中，则停止查找
    not_found_holders_company_length = len(set(company_df['ts_code'].values.tolist()) -
                                           set(df_not_found_holders_company['ts_code'].values.tolist()))
    logger.info('{}次待查找的公司数量是{}'.format(level,not_found_holders_company_length))
    if not_found_holders_company_length == 0:
        return
    for row in company_df.itertuples(index=True, name='Pandas'):
        pattern1 = re.compile(r'(.*)公司.*?')
        company_name = re.match(pattern1, row.holder_name)[0]
        logger.info('正在处理{}'.format(company_name))
        company_code = row.ts_code
        company_hold_amount = row.hold_amount
        company_hold_ratio = row.hold_ratio
        company_ann_date = row.ann_date
        company_end_date = row.end_date
        # 判断公司是否为未找到股东的公司
        if not not_found_holders_company.empty:
            if company_code in not_found_holders_company['ts_code']:
                df_not_found_holders.append(
                    pd.DataFrame({'ts_code': [company_code], 'ann_date': [company_ann_date],
                                  'end_date': [company_end_date], 'holder_name': [company_name],
                                  'hold_amount': [company_hold_amount], 'hold_ratio': [company_hold_ratio]}),
                    ignore_index=True)
                continue

        # 如果上市公司股东还是上市公司，则直接从上市公司前10大股东中取出数据,计算后存入数据库
        if company_name in stocks['fullname']:
            company_code = stocks[stocks['fullname'] == company_name]['ts_code'].values[0]
            company_top10_holders = df[df['ts_code'] == company_code]
            # 修改原来的hold_ratio列名为ratio
            company_top10_holders = company_top10_holders.rename(index=str, columns={
                "hold_ratio": "ratio",
            })
            # 重新计算hold_ratio和hold_amount列
            company_top10_holders['hold_ratio'] = company_top10_holders.ratio * company_hold_ratio
            company_top10_holders['hold_amount'] = company_top10_holders.ratio * company_hold_amount
            company_top10_holders = company_top10_holders[['ts_code', 'ann_date', 'end_date',
                                                           'holder_name', 'hold_amount', 'hold_ratio']]
            company_top10_holders.to_sql(name, con=engine, if_exists="replace", index=False)
        else:
            new_holders_df = download_holders(company_name)
            # 如果没有找到股东列表，将该公司添加到未找到公司列表中
            if new_holders_df.empty:
                df_not_found_holders = df_not_found_holders.append(
                    pd.DataFrame({'ts_code': [company_code], 'ann_date': [company_ann_date],
                                  'end_date': [company_end_date],'holder_name': [company_name],
                                  'hold_amount': [company_hold_amount],'hold_ratio': [company_hold_ratio]}),
                    ignore_index=True)
                df_not_found_holders_company = df_not_found_holders_company.append(
                    pd.DataFrame({'ts_code': [company_code], 'ann_date': [company_ann_date],
                                  'end_date': [company_end_date], 'holder_name': [company_name],
                                  'hold_amount': [company_hold_amount], 'hold_ratio': [company_hold_ratio]}),
                    ignore_index=True)
            else:
                new_holders_df['ts_code'] = company_code
                new_holders_df['ann_date'] = company_ann_date
                new_holders_df['end_date'] = company_end_date
                # 判断是否存在股本占比，如果不存在的话使用认缴金额重新计算
                if new_holders_df['ratio'].str.contains('%').sum() == 0:
                    if new_holders_df['promise_to_pay_amount'].str.contains('\d').sum() > 1:
                        new_holders_df['ratio_float'] = new_holders_df['promise_to_pay_amount'] / \
                                                  new_holders_df['promise_to_pay_amount'].sum()
                    else:
                        df_not_found_holders = df_not_found_holders.append(
                            pd.DataFrame({'ts_code': [company_code], 'ann_date': [company_ann_date],
                                          'end_date': [company_end_date], 'holder_name': [company_name],
                                          'hold_amount': [company_hold_amount], 'hold_ratio': [company_hold_ratio]}),
                            ignore_index=True)
                        df_not_found_holders_company = df_not_found_holders_company.append(
                            pd.DataFrame({'ts_code': [company_code], 'ann_date': [company_ann_date],
                                          'end_date': [company_end_date], 'holder_name': [company_name],
                                          'hold_amount': [company_hold_amount], 'hold_ratio': [company_hold_ratio]}),
                            ignore_index=True)
                        continue
                else:
                    new_holders_df['ratio_float'] = new_holders_df['ratio'].str.strip("%").astype(float) / 100
                new_holders_df['hold_amount'] = new_holders_df['ratio_float'] * company_hold_amount
                new_holders_df['hold_ratio'] = new_holders_df['ratio_float'] * company_hold_ratio
                new_holders_df = new_holders_df[['ts_code', 'ann_date', 'end_date',
                                                 'holder_name', 'hold_amount', 'hold_ratio']]
                new_holders_df.to_sql(name, con=engine, if_exists="append", index=False)
    # 最后将未找到股东的公司，分别存入分类数据库和未找到公司数据库
    df_not_found_holders.to_sql(name, con=engine, if_exists="append", index=False)
    df_not_found_holders_company.to_sql('not_found_holders_company', con=engine, if_exists="append", index=False)
    level += 1
    get_all_holders(name)


def compute_cn_rich_persons():
    # 获取今天日期
    today_date = datetime.now().strftime('%Y%m%d')
    # 获取上一个交易日
    trade_date = pro.trade_cal(exchange='', start_date=today_date,
                               end_date=today_date, fields='cal_date,is_open,pretrade_date')['pretrade_date'].values[0]
    # 检查是否已经存储上一个交易日的交易信息
    # try:
    #     pre_trade_info_df = pd.read_sql_table('trade_info',con=engine)
    #     if len(pre_trade_info_df[pre_trade_info_df['trade_date'].str.match(trade_date)]) == 0:
    #         pre_trade_info_has_saved = False
    #     else:
    #         return
    # except ValueError as e:
    #     pre_trade_info_has_saved = False
    # if not pre_trade_info_has_saved:
    if not False:
        trade_info_df = pro.daily_basic(ts_code='', trade_date=trade_date,
                                        fields='ts_code,trade_date,close,total_share,total_mv')
        trade_info_df.to_sql('trade_info', con=engine, if_exists='replace', index=False)
        df = pd.read_sql_table('one', con=engine)
        df = df.merge(trade_info_df, how='left', on='ts_code')
        df['wealth'] = df['total_mv'] * df['hold_ratio']
        table = df.pivot_table(values='wealth', index=['holder_name'], columns=['trade_date'], aggfunc=np.sum)
        table = table.reset_index()
        table = table.sort_values(by=[trade_date], ascending=False)
        table.to_json('{}.json'.format(trade_date),orient='records')
        return table


if __name__ == '__main__':
    # 第一步获取所有上市公司的前10大股东,
    # get_top10_holders(dbname='top10', start_date="20180901", end_date="20181231")
    # 第二步：获取所有上市公司前10大股东的股东列表
    get_all_holders('top10')
    # 第三步下载当天的股票价格，计算财富排行榜
    # compute_cn_rich_persons()
