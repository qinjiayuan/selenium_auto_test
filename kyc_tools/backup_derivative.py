# -*- coding: utf-8 -*-
# @Author  : qinjiayuan
# @File    : backup_derivative.py
import datetime
import cx_Oracle
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

#固收信创mysql
config_ficc = {"host": "10.128.12.222",
          "user": "otcmtst",
          "password": "Gf_otcmtst_test",
          "port": 15006,
          "db": "gf_ficc"}

#股衍
config_otc = {"host": "10.62.146.18",
              "user": "gf_otc",
              "password": "otc1qazXSW@",
              "port": 1521,
              "db": "jgjtest"}

#信创股衍（目前交易对手表还在oracle，故暂不启用）
# config_tdsql_otc = {"host": "10.128.12.222",
#           "user": "otcmtst",
#           "password": "Gf_otcmtst_test",
#           "port": 15006,
#           "db": "gf_otc"}

#备份固收数据库(后续股衍oracle迁移到tdsql的话,直接复制backup_derivative_mysql,并且修改函数方法名（最好与下面的backup_derivative_oracle 同名）、数据库配置)
def backup_derivative_mysql():
    _today  = datetime.datetime.today()
    _today = _today.strftime('%Y%m%d')
    backup_table = f"counterparty_{_today}"
    # backup_table = "otc_derivative_counterparty_202404010"
    #连接数据库
    try :
        db = pymysql.connect(**config_ficc)
        cursor = db.cursor()

        if cursor:
            print("固收数据库连接成功")
    except pymysql.Error as e:
        print("固收数据库连接失败")
        return

    #若备份表的数量多于五张,则删除最早备份的表
    drop_table = '''SELECT table_name, create_time
                    FROM information_schema.tables
                    WHERE table_schema = 'gf_ficc' AND table_name LIKE 'counterparty_20%'
                    ORDER BY create_time DESC;'''
    cursor.execute(drop_table)
    select_result = cursor.fetchall()
    cursor.execute(f"drop table {select_result[-1][0]}") if  len(select_result) >= 5 else print("备份表数量不多,无需删除")

    try:
        #备份
        back_sql = f'create table {backup_table} like otc_derivative_counterparty'
        insert_sql = f'insert into  {backup_table} select * from otc_derivative_counterparty'
        cursor.execute(back_sql )
        cursor.execute(insert_sql)
        db.commit()

        #判断备份是否成功
        cursor.execute(f"select * from {backup_table}" )
        backup_data = cursor.fetchall()
        cursor.execute("select * from otc_derivative_counterparty")
        original_data = cursor.fetchall()
        if backup_data == original_data :

            print(f'固收:{backup_table} has back up')
        else:
            raise Exception("数据备份失败,请检查数据")
    except Exception as e:
        db.rollback()
        print(f"Error is {e}")
    finally:
        cursor.close()
        db.close()



def getdate(table  :str = "COUNTERPARTY_20240402"):
    split_list = table.split('Y_')
    date = datetime.datetime.strptime(split_list[1], "%Y%m%d")
    return date

def backup_derivative_oracle():
    _today = datetime.datetime.today()
    _today = _today.strftime('%Y%m%d')
    backup_table = f"counterparty_{_today}"

    # 连接数据库
    try:

        db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
        cursor = db.cursor()

        if cursor:
            print("股衍数据库连接成功")
    except cx_Oracle.Error as e:
        print(f"股衍数据库连接失败:{e}")
        return None


    table_create_date_dict = {}
    # 若备份表的数量多于五张,则删除最早备份的表
    select_table = "SELECT table_name FROM all_tables WHERE owner = 'GF_OTC' AND table_name LIKE 'COUNTERPARTY_20%'"
    cursor.execute(select_table)

    table_num  = cursor.fetchall()
    if table_num :
        for table_name in table_num:
            table_date = getdate(table_name[0])
            table_create_date_dict[table_name] = table_date
        #找出最早的备份表
        sorted_time_dict = dict(sorted(table_create_date_dict.items(), key=lambda item: item[1]))
        need_drop_table = next(iter(sorted_time_dict))[0]
        #当备份表 的数量至少有5张的时候，删除最早的一份备份
        cursor.execute(f"drop table {need_drop_table}") if len(table_num) > 5 else print('备份表数量不多,无需删除')
        # 备份
    try:
        back_sql = f'create table {backup_table} as select * from otc_derivative_counterparty'
        cursor.execute(back_sql)
        db.commit()

        # 判断备份是否成功
        cursor.execute(f"select * from {backup_table}")
        backup_data = cursor.fetchall()
        cursor.execute("select * from otc_derivative_counterparty")
        original_data = cursor.fetchall()
        if backup_data == original_data:

            print(f'股衍:{backup_table} has back up')
        else:
            raise Exception("数据备份失败,请检查数据")
    except Exception as e:
        print(f"Error is {e}")
        db.rollback()
    finally :

        cursor.close()
        db.close()

#创建定时任务
def time_job():
    scheduler = BlockingScheduler()

    # 设定在每个月的15号触发
    trigger_ficc = CronTrigger(day=15,hour=0,minute=30)
    trigger_otc = CronTrigger(day=15,hour=0,minute=30)

    # 测试定时任务
    # trigger_ficc = CronTrigger(minute=1)
    # trigger_otc = CronTrigger(minute=1)

    scheduler.add_job(backup_derivative_mysql, trigger_ficc)
    scheduler.add_job(backup_derivative_oracle, trigger_otc)

    scheduler.start()
if __name__ == '__main__':
    time_job()





#备份股衍oracle数据库