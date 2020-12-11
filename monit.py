import tushare as ts
import pymysql
import pandas as pd
import logging
import time

def init_log():
    file_name = './monit.log'
    log_format = '[%(levelname)s] %(asctime)s %(filename)s(L%(lineno)d): %(message)s'
    date_fmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(
        filename=file_name,
        format=log_format,
        datefmt=date_fmt,
        level=logging.INFO
    )
    # 定义一个Handler打印INFO及以上级别的日志到sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    # 设置日志打印格式
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(filename)s(L%(lineno)d): %(message)s')
    console.setFormatter(formatter)
    # 将定义好的console日志handler添加到root logger
    logging.getLogger('').addHandler(console)


def connect_db():
    return pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           password='chuangxin',
                           database='tushare',
                           charset='utf8',
                           connect_timeout=60)

def insert_big_a(ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs):
        con = connect_db()
        cur = con.cursor()
        ret = True
        try:
            sql_str = ("INSERT INTO big_a (ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs) \
                        VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')".format(ts_code,symbol,name,area,industry,fullname,pymysql.escape_string(enname),market,exchange,curr_type,list_status,list_date,delist_date,is_hs))
            cur.execute(sql_str)
            con.commit()
        except:
            con.rollback()
            logging.exception('---------')
            logging.error(sql_str)
            ret = False
        finally:
            cur.close()
            con.close()
            return ret

my_token = '23995691f99dc9e3e195881658b20faf724a25b7f517377c9ae69282'

def get_big_a_stock_list():
    ts.set_token(my_token)
    pro = ts.pro_api()
    # df = pro.trade_cal(exchange='', start_date='20180901', end_date='20181001', fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
    big_a_data = pro.stock_basic(
        exchange='', 
        list_status='', 
        fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')

    print('Begin to collect...')
    cnt = 0
    for row in big_a_data.iterrows():
        if insert_big_a(row[1][0],row[1][1],row[1][2],row[1][3],row[1][4],row[1][5],row[1][6],row[1][7],row[1][8],row[1][9],row[1][10],row[1][11],row[1][12],row[1][13]):
            cnt += 1
    print('{} has been inserted to MySQL!'.format(cnt))
        
def insert_one_big_a_stock(
    table_name,
    ts_code,
    trade_date,
    open,
    high,
    low,
    close,
    pre_close,
    change,
    pct_chg,
    vol,
    amount
):
    con = connect_db()
    cur = con.cursor()
    ret = True
    try:
        sql_str = ("INSERT INTO {0} (ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs) \
                    VALUES ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')".format(table_name,ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount))
        cur.execute(sql_str)
        con.commit()
    except Exception as e:
        con.rollback()
        logging.error('Insert big_a list error: {0}, {1}'.format(sql_str, e))
        ret = False
    finally:
        cur.close()
        con.close()
        return ret

def create_table_for_one_big_a_stock(table_name):
    con = connect_db()
    cur = con.cursor()
    ret = True
    try:
        sql_str = ("CREATE TABLE IF NOT EXISTS {0} (ts_code varchar(255) NOT NULL, trade_date varchar(255) PRIMARY KEY, open float, high float, low float, close float, pre_close float, price_change float, pct_chg float, vol float, amount float)".format(table_name.replace('.','')))
        cur.execute(sql_str)
        con.commit()
    except Exception as e:
        con.rollback()
        logging.error('creata table error for {0}: {1}, Error: {2}'.format(table_name, sql_str, e))
        ret = False
    finally:
        cur.close()
        con.close()
        return ret

def insert_one_big_a_stock_history_price(table_name, ts_code, trade_date, open, high, low, close, pre_close, price_change, pct_chg, vol, amount):
    con = connect_db()
    cur = con.cursor()
    ret = True
    try:
        sql_str = ("INSERT INTO {0} (ts_code, trade_date, open, high, low, close, pre_close, price_change, pct_chg, vol, amount) \
                    VALUES ('{1}','{2}',{3},{4},{5},{6},{7},{8},{9},{10},{11})".format(table_name.replace('.',''), ts_code, trade_date, open, high, low, close, pre_close, price_change, pct_chg, vol, amount))
        cur.execute(sql_str)
        con.commit()
    except Exception as e:
        con.rollback()
        logging.error('Insert into {0} error: {1}, Error: {2}'.format(table_name, sql_str, e))
        ret = False
    finally:
        cur.close()
        con.close()
        return ret

def query_big_a_list():
    con = connect_db()
    cur = con.cursor()
    try:
        sql_str = ("SELECT * FROM big_a LIMIT 888,5000")
        cur.execute(sql_str)
        big_a_list = cur.fetchall()
    except Exception as e:
        logging.error('creata table error for {0}: {1}, Error: {2}'.format(table_name, sql_str, e))
    finally:
        cur.close()
        con.close()
        return big_a_list

def get_one_big_a_stock_history_price(ts_code):
    pro = ts.pro_api()
    # 未复权
    # df = pro.daily(ts_code=ts_code, start_date='20000101', end_date='20201209')
    # 前复权
    df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date='20000101', end_date='20201209')
    if not df.empty:
        return df
    else:
        logging.info('Get history price of {} error!'.format(ts_code))
        return None

def get_all_big_a_stock_history_price():
    big_a_list = query_big_a_list()
    for stock in big_a_list:
        ts_code = stock[0]
        if ts_code:
            logging.info('Now create table for {}'.format(ts_code))
            create_ret = create_table_for_one_big_a_stock(ts_code)
            if create_ret:
                logging.info('Create table for {} succeed!'.format(ts_code))
                #TODO: get history price data
                history_price = get_one_big_a_stock_history_price(ts_code)
                #TODO: insert to mysql
                if not history_price.empty:
                    for row in history_price.iterrows():
                        ret = insert_one_big_a_stock_history_price(ts_code, ts_code, row[1][1], row[1][2], row[1][3], row[1][4], row[1][5], row[1][6], row[1][7], row[1][8], row[1][9], row[1][10])
                        if ret:
                            logging.info('SKR {}'.format(ts_code))
                        else:
                            logging.error('insert {} error'.format(ts_code))
                        # time.sleep(0.01)
                else:
                    logging.error('Get history price of {} error!!!'.format(ts_code))
            else:
                logging.error('Create table for {} FAILED!!!'.format(ts_code))
    

if __name__ == "__main__":
    init_log()
    logging.info('\n\n')
    logging.info('************  Begin to Run...  ************')
    # get_big_a_stock_list()
    # get_a_stock_daily_from_big_a('689009.SH')
    get_all_big_a_stock_history_price()