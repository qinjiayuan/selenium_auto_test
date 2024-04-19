import asyncio
import time
from datetime import date, datetime, timedelta
from typing import List
from uuid import uuid4
from enum import Enum
import aiomysql
import cx_Oracle
import jsonpath
import uvicorn as uvicorn
from fastapi import FastAPI, APIRouter, Path
from pydantic import BaseModel
import logging
import aiohttp
import json
from aiohttp import FormData

from config.models import config
from optionjob.models import Enviroment , OptionFlowResponse

option = APIRouter()







@option.post("/createFlow",summary="期权产品监测流程")
async def job(corporatename: str, customerManager: str , enviroment : Enviroment ):
    '''
    :param corporatename: 公司名称
    :param customerManager: 客户经理（中文）
    :param enviroment: 40-固收测试环境,216-股衍测试环境
    '''
    try:
        print(f"公司名称：{corporatename},客户经理：{customerManager},环境：{enviroment.name}")
        env = enviroment.value
        process_date: date = date.today()
        date1: datetime = datetime.now() - timedelta(days=180)
        date1_str: str = datetime.strftime(date1, "%Y-%m-%d")
        print(f"process_date is {process_date}")
        client_id_list = []
        if enviroment.name == "FICC":
            async with aiomysql.create_pool(**config, echo=True) as pool:
                async with pool.acquire() as db:
                    async with db.cursor() as cursor:
                        try:
                            select_review_process = f"select record_id from client_review_record where client_name ='{corporatename}' "
                            print(f"execute_sql:{select_review_process}")
                            print(f"param ==> {corporatename}")
                            await cursor.execute(select_review_process)
                            result = [x for x in await cursor.fetchall()]
                            select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = '{customerManager}' "
                            print(f"execute_sql:{select_customermanager}")
                            print(f"param ==> {customerManager}")
                            await cursor.execute(select_customermanager)
                            customer = await cursor.fetchall()
                            if customer:
                                user, dept_code = customer[0][0], customer[0][1]
                                print(f"user:{customer[0][0]},dept_code:{customer[0][1]}")
                            else:
                                raise ValueError("请输入正确的客户经理")
                            if result:
                                delete_review_process = f"delete from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED'"
                                print(f"execute_sql:{delete_review_process}")
                                print(f"param ==> {corporatename}")
                                await cursor.execute(delete_review_process)

                                print("已经成功删除存量回访流程")

                            update_otc_sql = f"""update otc_derivative_counterparty set
                            allow_busi_type='TRS,OPTION',return_visit_date=date_format('{date1_str}','%Y-%m-%d'),admit_date=date_format('{date1_str}','%Y-%m-%d'),
                            customer_manager='{user}',
                            introduction_department='{dept_code}',
                            option_duration_flag='true',
                            aml_monitor_flag='true',
                            client_qualify_review='true'
                            where corporate_name = '{corporatename}'"""
                            print(f"execute_sql:{update_otc_sql}")
                            print(f"param ==> {date1_str},{date1_str},{user},{dept_code},{corporatename}")
                            await cursor.execute(update_otc_sql)

                            # 删除存量流程
                            delete_option_process = f"delete from counterparty_prod_monitor_flow where corporate_name = '{corporatename}' "
                            print(f"execute_sql:{delete_option_process}")
                            print(f"param ==> {corporatename}")
                            await cursor.execute(delete_option_process)
                            await db.commit()

                            select_clientid = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}' and is_prod_holder  = '03'"
                            print(f"execute_sql:{select_clientid}")
                            print(f"param ==> {corporatename}")
                            await cursor.execute(select_clientid)
                            client_id_list = [x[0] for x in await cursor.fetchall()]
                            print(client_id_list)

                            print("开始请求接口")
                            async with aiohttp.ClientSession() as session:
                                tasks = []
                                for client_id in client_id_list:
                                    task = asyncio.create_task(session.get(url=env + '/api/test/optionProdMonitor',
                                                                           params={"clientId": client_id, "date": ""}))
                                    tasks.append(task)
                                responses = await asyncio.gather(*tasks)
                                for response in responses:
                                    print(await response.json())

                            select_flow = f"select client_id ,title, document_id from counterparty_prod_monitor_flow where corporate_name = '{corporatename}' and current_status !='CLOSED'"
                            print(f"execute_sql:{select_flow}")
                            print(f"param ==> {corporatename}")
                            await cursor.execute(select_flow)
                            flow_title = await cursor.fetchall()
                            response_list = []
                            for i in flow_title:
                                flow_dict = {}
                                flow_dict["corporateName"]=corporatename
                                flow_dict["clientId"] = i[0]
                                flow_dict["title"] = i[1]
                                flow_dict["documentId"] = i[2]
                                response_list.append(flow_dict)
                            return {"code":200,
                                    "env": enviroment.name,
                                    "data":[OptionFlowResponse(**item) for item in response_list],
                                    "status":"Successfully"}

                        except Exception as e:
                            print(e)
                            if not db.closed:
                                await db.rollback()
                            raise Exception(e)

        else :
            with cx_Oracle.connect("gf_otc","otc1qazXSW@","10.62.146.18:1521/jgjtest") as db :
                with db.cursor() as cursor:

                    try:
                        select_review_process = f"select record_id from client_review_record where client_name ='{corporatename}' "
                        print(f"execute_sql:{select_review_process}")
                        print(f"param ==> {corporatename}")
                        cursor.execute(select_review_process)
                        result = [x for x in  cursor.fetchall()]
                        select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = '{customerManager}' "
                        print(f"execute_sql:{select_customermanager}")
                        print(f"param ==> {customerManager}")
                        cursor.execute(select_customermanager)
                        customer = cursor.fetchall()
                        if customer:
                            user, dept_code = customer[0][0], customer[0][1]
                            print(f"user:{customer[0][0]},dept_code:{customer[0][1]}")
                        else:
                            raise ValueError("请输入正确的客户经理")
                        if result:
                            delete_review_process = f"delete from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED'"
                            print(f"execute_sql:{delete_review_process}")
                            print(f"param ==> {corporatename}")
                            cursor.execute(delete_review_process)

                            print("已经成功删除存量回访流程")

                        update_otc_sql = f"""update otc_derivative_counterparty set
                        allow_busi_type='TRS,OPTION',return_visit_date=to_date('{date1_str}','yyyy-mm-dd'),master_agreement_date=to_date('{date1_str}','yyyy-mm-dd'),
                        customer_manager='{user}',
                        introduction_department='{dept_code}',
                        option_duration_flag='true',
                        aml_monitor_flag='true',
                        client_qualify_review='true'
                        where corporate_name = '{corporatename}'"""
                        print(f"execute_sql:{update_otc_sql}")
                        print(f"param ==> {date1_str},{date1_str},{user},{dept_code},{corporatename}")
                        cursor.execute(update_otc_sql)

                        # 删除存量流程
                        delete_option_process = f"delete from counterparty_prod_monitor_flow where corporate_name = '{corporatename}' "
                        print(f"execute_sql:{delete_option_process}")
                        print(f"param ==> {corporatename}")
                        cursor.execute(delete_option_process)
                        db.commit()
                        select_clientid = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}' and is_prod_holder  = '03'"
                        print(f"execute_sql:{select_clientid}")
                        print(f"param ==> {corporatename}")
                        cursor.execute(select_clientid)
                        client_id_list = [x[0] for x in cursor.fetchall()]
                        print(client_id_list)

                        async with aiohttp.ClientSession() as session:
                            tasks = []
                            for client_id in client_id_list:
                                task = asyncio.create_task(session.get(url=env + '/api/test/optionProdMonitor',
                                                                       params={"clientId": client_id, "date": ""}))
                                tasks.append(task)
                            responses = await asyncio.gather(*tasks)
                            for response in responses:
                                print(await response.json())

                        select_flow = f"select client_id ,title, document_id from counterparty_prod_monitor_flow where corporate_name = '{corporatename}' and current_status !='CLOSED'"
                        print(f"execute_sql:{select_flow}")
                        print(f"param ==> {corporatename}")
                        cursor.execute(select_flow)
                        flow_title = cursor.fetchall()
                        response_list = []
                        for i in flow_title:
                            flow_dict = {}
                            flow_dict["corporateName"] = corporatename
                            flow_dict["clientId"] = i[0]
                            flow_dict["title"] = i[1]
                            flow_dict["documentId"] = i[2]
                            response_list.append(flow_dict)
                        return {"code": 200,
                                "env":enviroment.name,
                                "data": [OptionFlowResponse(**item) for item in response_list],
                                "status": "Successfully"}


                    except Exception as e:
                        print(e)
                        if db:
                            db.rollback()
    except Exception as e :
        return {"code": 500,
                "env": enviroment.name,
                "data": str(e),
                "status": "failed"}


@option.delete("/deleteflow",description="删除该机构下的所有的期权产品监测流程，",summary="删除在途的期权产品监测流程",operation_id='delete_option_flow')
def delete(enviroment : Enviroment, corporatenameList : List[str]):
    '''

    :param enviroment: 40-固收测试环境,216-股衍测试环境
    :param corporatenameList: 公司名称列表
    :return:
    '''
    print(enviroment.name , enviroment.value)
    databases_config = config if enviroment.name == 'FICC' else config_otc
    db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
    cursor = db.cursor()
    params = []
    try:
        for corporate in corporatenameList:
            delete_params = {}
            delete_params["client_name"] = corporate
            params.append(delete_params)
        delete_flow = "delete from counterparty_prod_monitor_flow where corporate_name =:client_name "
        cursor.executemany(delete_flow,params)
    except Exception as e :
        print(f"出现错误为{e}")
        db.rollback()
        return e
    else :
        db.commit()
        return {"code": 200,
                "env": enviroment.name,
                "data": f"已经成功删除{corporatenameList}的流程",
                "status": "Successfully"}
    finally:
        cursor.close()
        db.close()
