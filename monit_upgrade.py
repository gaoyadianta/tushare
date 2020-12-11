import tushare as ts
import pymysql
import pandas as pd
import logging
import time
import datetime

class BigA(object):
    def __init__(self):
        self.con = None
        self.init_log()
        self.connect_db()

    def init_log(self):
        file_name = './monit.log'
        log_format = '[%(levelname)s] %(asctime)s %(filename)s(L%(lineno)d): %(message)s'
        date_fmt = '%Y-%m-%d %H:%M:%S'
        logging.basicConfig(
            filename=file_name,
            format=log_format,
            datefmt=date_fmt,
            level=logging.INFO
        )
        # # 定义一个Handler打印INFO及以上级别的日志到sys.stderr
        # console = logging.StreamHandler()
        # console.setLevel(logging.ERROR)
        # # 设置日志打印格式
        # formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(filename)s(L%(lineno)d): %(message)s')
        # console.setFormatter(formatter)
        # # 将定义好的console日志handler添加到root logging
        # logging.getLogger('').addHandler(console)


    def connect_db(self):
        self.con = pymysql.connect(host='127.0.0.1',
                            port=3306,
                            user='root',
                            password='chuangxin',
                            database='tushare',
                            charset='utf8',
                            connect_timeout=60)
        logging.info('connect...')

    def insert_big_a(self, ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs):
            con = self.con
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
                return ret

    my_token = '23995691f99dc9e3e195881658b20faf724a25b7f517377c9ae69282'

    def get_big_a_stock_list(self):
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
        self,
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
        con = self.con
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
            return ret

    def create_table_for_one_big_a_stock(self, table_name):
        con = self.con
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
            return ret

    def insert_one_big_a_stock_history_price(self, table_name, ts_code, trade_date, open, high, low, close, pre_close, price_change, pct_chg, vol, amount):
        con = self.con
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
            return ret

    def query_big_a_list(self, begin, total):
        con = self.con
        cur = con.cursor()
        try:
            sql_str = ("SELECT * FROM big_a LIMIT {0}, {1}".format(begin, total))
            cur.execute(sql_str)
            big_a_list = cur.fetchall()
        except Exception as e:
            logging.error('creata table error for {0}: {1}, Error: {2}'.format(table_name, sql_str, e))
        finally:
            cur.close()
            return big_a_list

    def get_one_big_a_stock_history_price(self, ts_code):
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

    def get_all_big_a_stock_history_price(self):
        big_a_list = self.query_big_a_list(0, 5000)
        for stock in big_a_list:
            ts_code = stock[0]
            if ts_code:
                logging.info('Now create table for {}'.format(ts_code))
                create_ret = self.create_table_for_one_big_a_stock(ts_code)
                if create_ret:
                    logging.info('Create table for {} succeed!'.format(ts_code))
                    #TODO: get history price data
                    history_price = self.get_one_big_a_stock_history_price(ts_code)
                    #TODO: insert to mysql
                    if not history_price.empty:
                        for row in history_price.iterrows():
                            ret = self.insert_one_big_a_stock_history_price(ts_code, ts_code, row[1][1], row[1][2], row[1][3], row[1][4], row[1][5], row[1][6], row[1][7], row[1][8], row[1][9], row[1][10])
                            if ret:
                                logging.info('SKR {}'.format(ts_code))
                            else:
                                logging.error('insert {} error'.format(ts_code))
                            # time.sleep(0.01)
                    else:
                        logging.error('Get history price of {} error!!!'.format(ts_code))
                else:
                    logging.error('Create table for {} FAILED!!!'.format(ts_code))    

    def query_all_tables():
        con = self.con
        cur = con.cursor()
        try:
            sql_str = ("SHOW TABLES")
            cur.execute(sql_str)
            table_list = cur.fetchall()
        except Exception as e:
            logging.error('Query table({0}) error: {1}'.format(sql_str, e))
            table_list = None
        finally:
            cur.close()
            return table_list

    def table_exists(self, table_name):
        con = self.con
        cur = con.cursor()
        ret = False
        try:
            sql_str = ("SHOW TABLES LIKE '{}'".format(table_name))
            ret = cur.execute(sql_str)
        except Exception as e:
            logging.error('Query table exist({0}) error: {1}'.format(sql_str, e))
        finally:
            cur.close()
            return ret

    def match_history_price(self):
        big_a_list = self.query_big_a_list(0, 5000)
        for stock in big_a_list:
            ts_code = stock[0]
            if self.table_exists(ts_code.replace('.', '')):
                print('{} Exists!'.format(ts_code))
                pass
            else:
                logging.info('Now create table for {}'.format(ts_code))
                create_ret = self.create_table_for_one_big_a_stock(ts_code)
                if create_ret:
                    logging.info('Create table for {} succeed!'.format(ts_code))
                    #TODO: get history price data
                    history_price = self.get_one_big_a_stock_history_price(ts_code)
                    #TODO: insert to mysql
                    if not history_price.empty:
                        for row in history_price.iterrows():
                            ret = self.insert_one_big_a_stock_history_price(ts_code, ts_code, row[1][1], row[1][2], row[1][3], row[1][4], row[1][5], row[1][6], row[1][7], row[1][8], row[1][9], row[1][10])
                            if ret:
                                logging.info('SKR {}'.format(ts_code))
                            else:
                                logging.error('insert {} error'.format(ts_code))
                            # time.sleep(0.01)
                    else:
                        logging.error('Get history price of {} error!!!'.format(ts_code))
                else:
                    logging.error('Create table for {} FAILED!!!'.format(ts_code))    

    def get_one_history_price(self, ts_code, start_date):
        pro = ts.pro_api()
        # 未复权
        # df = pro.daily(ts_code=ts_code, start_date='20000101', end_date='20201209')
        # 前复权
        today = datetime.date.today().isoformat().replace('-', '')
        if int(start_date) > int(today):
            logging.info('{} do not need update'.format(ts_code))
            return None
        logging.info('Get {0}  price, date: {1} - {2}'.format(ts_code, start_date, today))
        df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=today)
        if df is not None:
            if not df.empty:
                return df
            else:
                logging.info('Daily get history price of {} error!'.format(ts_code))
                return None
        else:
            return None
    
    def query_update_start_date(self, ts_code):
        '''
        get the start date which need to get daily price
        '''
        con = self.con
        cur = con.cursor()
        try:
            sql_str = ("SELECT * FROM {} ORDER BY trade_date DESC LIMIT 1".format(ts_code.replace('.', '')))
            cur.execute(sql_str)
            result = cur.fetchone()
            if result:
                ret = result[1]
            else:
                ret = None
        except Exception as e:
            logging.error('Query table({0}) error: {1}'.format(sql_str, e))
            ret = None
        finally:
            cur.close()
            return ret

    def dateTransform(self, date):
        '''
        date: format 20201001
        '''
        if date is not None:
            return (datetime.date(int(date[:4]), int(date[4:6]), int(date[6:8])) + datetime.timedelta(days=1)).isoformat().replace('-', '')
        else:
            return ''

    def dialy_update(self):
        big_a_list = self.query_big_a_list(0, 5000)
        for stock in big_a_list:
            ts_code = stock[0]
            start_date = self.dateTransform(self.query_update_start_date(ts_code))
            if start_date:
                updated_price = self.get_one_history_price(ts_code, start_date)
                if updated_price is not None:
                    if not updated_price.empty:
                        for row in updated_price.iterrows():
                            ret = self.insert_one_big_a_stock_history_price(ts_code, ts_code, row[1][1], row[1][2], row[1][3], row[1][4], row[1][5], row[1][6], row[1][7], row[1][8], row[1][9], row[1][10])
                            if ret:
                                logging.info('Update {0} from {1} succeed!'.format(ts_code, start_date))
                            else:
                                logging.error('Update insert {} error'.format(ts_code))
                    

if __name__ == "__main__":
    bigA = BigA()
    logging.info('\n\n')
    logging.info('************  Begin to Run...  ************')
    # # get all the history data of all A stocks
    # bigA.get_all_big_a_stock_history_price()
    bigA.dialy_update()