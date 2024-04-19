import asyncio
import time
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import List, Optional
from uuid import uuid4
from enum import Enum
import aiomysql
import cx_Oracle
import jsonpath
import pymysql
import requests
import uvicorn as uvicorn
from fastapi import FastAPI, APIRouter, Query, Depends
from unittest.mock import  MagicMock
from pydantic import BaseModel, Field
import aiohttp
import json
from aiohttp import FormData
from config.models import config,Enviroment
import pymysql
from publicInfo.models import PublicInfoResponse



public = APIRouter()


#只存在单次发起，无需考虑异步
@public.post('/createFlow',description='公开信息创建流程')
def createFlow(corporateName : str , customerManager  : str , enviroment :Enviroment ):
    '''
    :param corporateName:
    :param customerManager: 填写客户经理的中文名称
    :param enviroment:
    :return:
    '''
    print(f"corporateName:{corporateName},customerManager:{customerManager}, enviroment:{enviroment}")
    if enviroment.name == 'FICC':
        try :
            with pymysql.connect(**config) as db :
                with db.cursor() as cursor :
                    #查出unifiedsocial_code
                    select_unicode = 'select unifiedsocial_code from counterparty_org where corporate_name = %s;'
                    print(f"execute_sql:{select_unicode}")
                    cursor.execute(select_unicode,(corporateName,))
                    print(f"params:{corporateName}")
                    result = cursor.fetchone()
                    if result :
                        unifiedsocial_code = result[0]
                    else :
                        raise ValueError("counterparty_org中未找到该企业")

                    select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = %s "
                    print(f"execute_sql:{select_unicode}")
                    print(f"params:{customerManager}")
                    cursor.execute(select_customermanager,(customerManager,))
                    customer = cursor.fetchone()
                    if customer :
                        customer_name = customer[0]
                        department_code = customer[1]
                    else :
                        raise ValueError("未找到该客户经理")

                    #删除存量流程
                    delete_flow = "delete from ctpty_info_update_record ciur where unifiedsocial_code = %s and current_status != 'CLOSED' and title like '%%公开信息%%'"
                    print(f"execute_sql:{delete_flow}")
                    print(f"params:('911101080828461726',)")
                    cursor.execute(delete_flow, ('911101080828461726',))

                    delete_flow = " select * from ctpty_info_update_record ciur where unifiedsocial_code = %s and current_status !='CLOSED' and title like '%%公开信息%%'"
                    print(f"execute_sql:{delete_flow}")
                    print(f"params:{unifiedsocial_code}")
                    cursor.execute(delete_flow,(unifiedsocial_code,))

                # 设置发起流程条件
                    #设置最新准入交易对手
                    select_last_client_id =f'select client_id from otc_derivative_counterparty where unifiedsocial_code = %s  '
                    print(f"execute_sql:{select_last_client_id}")
                    print(f"params:911101080828461726")
                    cursor.execute(select_last_client_id,('911101080828461726',))
                    last_client_id = cursor.fetchone()
                    print(last_client_id)
                    update_config = """update counterparty_org set 
                                            lastest_client_id = %s ,
                                            customer_manager = %s ,
                                            introduction_department = %s ,
                                            aml_monitor_flag = %s 
                                            where unifiedsocial_code = %s
                                            """
                    print(f"execute_sql:{update_config}")
                    print(f"params:{last_client_id[0],customer_name,department_code,'true','911101080828461726'}")
                    cursor.execute(update_config,(last_client_id[0],customer_name,department_code,"true","911101080828461726",))

                    update_config_derivative = """update otc_derivative_counterparty set
                                                    customer_manager = %s ,
                                                    introduction_department = %s ,
                                                    aml_monitor_flag = %s
                                                    where unifiedsocial_code = %s"""
                    print(f"execute_sql:{update_config_derivative}")
                    print(f"params:{customer_name,department_code,'true','911101080828461726'}")
                    cursor.execute(update_config_derivative,(customer_name,department_code,"true",'911101080828461726',))

                    db.commit()

                    #调用接口发起
                    url = enviroment.value + "/ctptyInfoUpdate/remind/check"
                    playload = {'checkDayAfter': '2010-10-10',
                                'checkDbData': 'false',
                                'checkInDate': '2022-08-31',
                                'isNewCheck': 'true',
                                'startProcess': 'true',
                                'today': date.today(),
                                'uniCodeList': '911101080828461726'}
                    response = requests.post(url, data=playload)
                    print(response.json())
                    #查询发起的流程
                    select_flow_now = f"""select doc_id from ctpty_info_update_record ciur 
                                            where unifiedsocial_code ='911101080828461726'
                                            and current_status  !='CLOSED'
                                            and title like '%公开信息%'"""
                    print(f"execute_sql:{select_flow_now}")
                    print(f"params:{unifiedsocial_code}")
                    cursor.execute(select_flow_now)

                    title = f"客户{corporateName}公开信息发生变更确认流程{date.today()}"

                    while True :
                        row = cursor.fetchmany(1)
                        if not row :
                            break
                        update_record_sql = f"""update ctpty_info_update_record set 
                                                    title = %s ,
                                                    unifiedsocial_code = %s
                                                    where doc_id = %s """
                        print(f"execute_sql:{update_record_sql}")
                        print(f"params:{title,row[0][0]}")
                        cursor.execute(update_record_sql,(title,unifiedsocial_code,row[0][0]))
                    db.commit()

                    #查询出对应模型参数
                    select_flow_data = """select title ,doc_id ,record_id ,current_operator 
                                                from ctpty_info_update_record 
                                                where unifiedsocial_code =%s and 
                                                current_status !='CLOSED' and 
                                                title like '%%公开信息%%'"""
                    print(f"execute_sql:{select_flow_data}")
                    print(f"params:{unifiedsocial_code}")
                    cursor.execute(select_flow_data,(unifiedsocial_code,))

                    result = cursor.fetchall()

                    flow_data_list = []
                    for flow_data in result:
                        flow_data_dict = {}
                        flow_data_dict["corporateName"] = corporateName
                        flow_data_dict["title"] = flow_data[0]
                        flow_data_dict["documentId"] = flow_data[1]
                        flow_data_dict["recordId"] = flow_data[2]
                        flow_data_dict["currentOperator"] = flow_data[3]
                        flow_data_list.append(flow_data_dict)

                    return {"code": 200,
                            "env": enviroment.name,
                            "data": [PublicInfoResponse(**item) for item in flow_data_list],
                            "status": "Successfully"}
        except ValueError as e  :
            print("请核对公司名称和客户经理名称")
            return {"code": 500,
                    "env": enviroment.name,
                    "data": '请核对公司名称和客户经理名称',
                    "status": "Failed"}
        except Exception as e:
            print(f"发起过程中，出现异常: {e}")
            return {"code": 500,
                    "env": enviroment.name,
                    "data": f"启动过程中，出现异常: {e}",
                    "status": "Failed"}

    else :
        try :
            start = time.time()
            with cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest") as db:
                with db.cursor() as cursor :
                    #查出unifiedsocial_code
                    select_unicode = 'select unifiedsocial_code from counterparty_org where corporate_name = :corporateName'
                    print(f"execute_sql:{select_unicode}")
                    print(f"param ==> {corporateName}")
                    cursor.execute(select_unicode,{"corporateName":corporateName})
                    result = cursor.fetchone()
                    if result :
                        unifiedsocial_code = result[0]
                    else :
                        raise ValueError("counterparty_org中未找到该企业")

                    select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = :customerManager "
                    print(f"execute_sql:{select_unicode}")
                    print(f"params ==> {customerManager}")
                    cursor.execute(select_customermanager,{"customerManager":customerManager})
                    customer = cursor.fetchone()
                    if customer :
                        customer_name = customer[0]
                        department_code = customer[1]
                    else :
                        raise ValueError("未找到该客户经理")

                    #删除存量流程
                    delete_flow = "delete from ctpty_info_update_record ciur where unifiedsocial_code = :unicode and current_status != 'CLOSED' and title like '%公开信息%'"
                    print(f"execute_sql:{delete_flow}")
                    print(f"params ==> 911101080828461726")
                    cursor.execute(delete_flow, {"unicode":"911101080828461726"})

                    delete_flow = " select * from ctpty_info_update_record ciur where unifiedsocial_code = :unicode and current_status !='CLOSED' and title like '%公开信息%'"
                    print(f"execute_sql:{delete_flow}")
                    print(f"params ==> {unifiedsocial_code}")
                    cursor.execute(delete_flow,{"unicode":unifiedsocial_code})

                # 设置发起流程条件
                    #设置最新准入交易对手
                    select_last_client_id =f'select client_id from otc_derivative_counterparty where unifiedsocial_code = :unicode  '
                    print(f"execute_sql:{select_last_client_id}")
                    print(f"params ==> 911101080828461726")
                    cursor.execute(select_last_client_id,{"unicode":'911101080828461726'})
                    last_client_id = cursor.fetchone()
                    print(last_client_id)
                    update_config = """update counterparty_org set 
                                            lastest_client_id = :clientId ,
                                            customer_manager = :customerName ,
                                            introduction_department = :departmentCode ,
                                            aml_monitor_flag = :amlMonitorFlag 
                                            WHERE unifiedsocial_code = :unicode
                                            """
                    print(f"execute_sql:{update_config}")
                    print(f"params ==> {last_client_id[0],customer_name,department_code,'true','911101080828461726'}")
                    cursor.execute(update_config,{"clientId":last_client_id[0],"customerName":customer_name,"departmentCode":department_code,"amlMonitorFlag":"true","unicode":"911101080828461726"})

                    update_config_derivative = """update otc_derivative_counterparty set
                                                    customer_manager = :customerName ,
                                                    introduction_department = :departmentCode ,
                                                    aml_monitor_flag = :amlMonitorFlag 
                                                    where unifiedsocial_code = :unicode """
                    print(f"execute_sql:{update_config_derivative}")
                    print(f"params ==> {customer_name,department_code,'true','911101080828461726'}")
                    cursor.execute(update_config_derivative,{'customerName':customer_name,'departmentCode':department_code,'amlMonitorFlag':'true','unicode':'911101080828461726'})
                    db.commit()

                    #调用接口发起
                    url = enviroment.value + "/ctptyInfoUpdate/remind/check"
                    playload = {'checkDayAfter': '2010-10-10',
                                'checkDbData': 'false',
                                'checkInDate': '2022-08-31',
                                'isNewCheck': 'true',
                                'startProcess': 'true',
                                'today': date.today(),
                                'uniCodeList': '911101080828461726'}
                    response = requests.post(url, data=playload)
                    print(response.json())
                    #查询发起的流程
                    select_flow_now = f"""select doc_id from ctpty_info_update_record ciur 
                                            where unifiedsocial_code ='911101080828461726'
                                            and current_status  !='CLOSED'
                                            and title like '%公开信息%'"""
                    print(f"execute_sql:{select_flow_now}")
                    cursor.execute(select_flow_now)

                    title = f"客户{corporateName}公开信息发生变更确认流程{date.today()}"

                    while True :
                        row = cursor.fetchmany(1) #此处注意下述的游标不能够 跟此处的游标相互覆盖
                        if not row :
                            break
                        update_record_sql = f"""update ctpty_info_update_record set 
                                                    title = :title ,
                                                    unifiedsocial_code = :unicode
                                                    where doc_id = :docId """
                        print(f"execute_sql:{update_record_sql}")
                        print(f"params ==> {title,row[0][0]}")

                        update_cursor = db.cursor()
                        update_cursor.execute(update_record_sql,{"title":title,"unicode":unifiedsocial_code,"docId":row[0][0]})
                        update_cursor.close()
                    db.commit()

                    #查询出对应模型参数
                    select_flow_data = """select title ,doc_id ,record_id ,current_operator 
                                                from ctpty_info_update_record 
                                                where unifiedsocial_code =:unicode and 
                                                current_status !='CLOSED' and 
                                                title like '%公开信息%'"""
                    print(f"execute_sql:{select_flow_data}")
                    print(f"params:{unifiedsocial_code}")
                    cursor.execute(select_flow_data,{"unicode":unifiedsocial_code})

                    result = cursor.fetchall()

                    flow_data_list = []
                    for flow_data in result:
                        flow_data_dict = {}
                        flow_data_dict["corporateName"] = corporateName
                        flow_data_dict["title"] = flow_data[0]
                        flow_data_dict["documentId"] = flow_data[1]
                        flow_data_dict["recordId"] = flow_data[2]
                        flow_data_dict["currentOperator"] = flow_data[3]
                        flow_data_list.append(flow_data_dict)
                    end = time.time()
                    print(f"一共耗时:{end - start}")
                    return {"code": 200,
                            "env": enviroment.name,
                            "data": [PublicInfoResponse(**item) for item in flow_data_list],
                            "status": "Successfully"}

        except ValueError as e  :
            print("请核对公司名称和客户经理名称")
            return {"code": 500,
                    "env": enviroment.name,
                    "data": '请核对公司名称和客户经理名称',
                    "status": "Failed"}
        except Exception as e:
            print(f"发起过程中，出现异常: {e}")
            return {"code": 500,
                    "env": enviroment.name,
                    "data": f"启动过程中，出现异常: {e}",
                    "status": "Failed"}




class ReviewFlowResponse(BaseModel):
    title : str
    corporateName : str
    documentId : str
    recordId:str
    currentOperator : str









