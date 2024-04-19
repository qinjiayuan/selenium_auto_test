import asyncio
import time
from datetime import date, datetime, timedelta
from typing import List, Optional
from uuid import uuid4
import aiomysql
import cx_Oracle
import jsonpath
import pymysql
import requests
from fastapi import FastAPI, APIRouter, Query, Depends
import aiohttp
from aiohttp import FormData
from review.models import ReviewIsNew , ReviewFlowResponse,Enviroment
from config.models import config
review = APIRouter()
# log = logging.getLogger("counterpartyjob")


# 单产品回访接口
async def single_review(corporatename: str,env : str):
    try:
        async with aiomysql.create_pool(**config) as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    select_unicode = f"select unifiedsocial_code from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{select_unicode}")
                    await cursor.execute(select_unicode)
                    unicode = await cursor.fetchone()
    except Exception as e:
        print(e)
    finally:
        await cursor.close()
        await pool.wait_closed()

    async with aiohttp.ClientSession() as session:
        async with session.post(url=env + "/clientreview/checkSingleClient",
                                data={"checkDateEnd": date.today(), "checkDateStart": date.today(),
                                      "uniCodeList": unicode[0]}) as response:
            print(await response.json())
            return None


#多产品回访接口
async def multiple_review(corporatename: str,env : str):
    try:
        async with aiomysql.create_pool(**config) as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    select_unicode = f"select unifiedsocial_code from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{select_unicode}")
                    await cursor.execute(select_unicode)
                    unicode = await cursor.fetchone()
    except Exception as e:
        print(e)
    finally:
        await cursor.close()
        await pool.wait_closed()

    async with aiohttp.ClientSession() as session:
        async with session.post(url=env + "/clientreview/checkMultipleClient",
                                data={"checkDateEnd": date.today(), "checkDateStart": date.today(),
                                      "uniCodeList": unicode[0]}) as response:
            print(await response.json())
            return None

            # async with aiohttp.ClientSession() as session:

#回访上传附件接口
async def uploadFile(env : str):
    file_name = ['主体/管理人文件', '32', 'CSRC', 'QCC_CREDIT_RECORD', 'CEIDN', 'QCC_ARBITRATION', 'QCC_AUDIT_INSTITUTION',
                 'CCPAIMIS', 'CC', 'P2P', 'OTHERS', 'NECIPS', 'CJO','场外衍生品交易授权书']
    S3filed = []
    headers = {"name": "sunbin"}
    file_path = r"D:\dailyUse\data.xlsx"

    async with aiohttp.ClientSession() as session:
        for i in range(28):
            form_data = FormData()
            form_data.add_field("files", open(file_path, "rb"), filename="data.xlsx",
                                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            form_data.add_field("fileBelong", file_name[i % 14])
            form_data.add_field("productName", file_name[i % 14])

            async with session.post(url=env + "/clientreview/file/upload", data=form_data, headers=headers) as response:
                if response.status == 200:
                    print(await response.json())
                    s3id = jsonpath.jsonpath(await response.json(), "$..s3FileId")
                    S3filed.append(s3id[0])

    print(f"s3fileid:{S3filed}")
    return S3filed


'''
#同时请求26次,后台服务器报错（暂不使用）
async def uploadFile_as(env: str):
    file_name = ['主体/管理人文件', '32', 'CSRC', 'QCC_CREDIT_RECORD', 'CEIDN', 'QCC_ARBITRATION', 'QCC_AUDIT_INSTITUTION',
                 'CCPAIMIS', 'CC', 'P2P', 'OTHERS', 'NECIPS', 'CJO']
    S3filed = []
    headers = {"name": "sunbin"}
    file_path = r"D:\dailyUse\data.xlsx"

    async def upload_file(i : int):
        form_data = FormData()
        form_data.add_field("files", open(file_path, "rb"), filename="data.xlsx",
                            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        form_data.add_field("fileBelong", file_name[i % 13])
        form_data.add_field("productName", file_name[i % 13])

        async with aiohttp.ClientSession() as session :
            async with session.post(url=env + "/clientreview/file/upload", data=form_data, headers=headers) as response:
                if response.status == 200:
                    print(await response.json())
                    s3id = jsonpath.jsonpath(await response.json(), "$..s3FileId")
                    # S3filed.append(s3id[0])
                    return s3id
    task = [upload_file(i) for i in range(26)]
    result = await asyncio.gather(*task)
    print(result)
    return result


    # print(f"s3fileid:{S3filed}")
    # return S3filed
'''
@review.post("/createFlow",summary="发起回访流程")
async def reviewjob(corporatename: str , customerManager: str, isnew: ReviewIsNew,enviroment:Enviroment):
    '''
    :param corporatename: 公司名称
    :param customerManager:客户经理（中文）
    :param isnew: 0-旧流程 1-新流程
    :param enviroment:40-固收测试环境 216-股衍测试环境
    '''
    start = time.time()
    print(f"公司名称 ：{corporatename},客户经理：{customerManager},isnew : {isnew},enviroment : {enviroment}")
    date_1 = date.today()
    date_str = date.strftime(date_1, "%Y-%m-%d")
    env = enviroment.value
    isnew = isnew.value
    print(f"isnew:{isnew}")
    #固收的流程
    if enviroment.name == "FICC":
        try:
            async with aiomysql.create_pool(**config) as pool:
                async with pool.acquire() as db:
                    async with db.cursor() as cursor:

                        select_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                        print(f"execute_sql:{select_client}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(select_client)
                        if not await cursor.fetchall():
                            raise ValueError("交易对手不存在")
                        # 删除存量流程
                        delete_flow = f"delete from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED' "
                        print(f"execute_sql:{delete_flow}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(delete_flow)
                        # 查找客户经理
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
                        # 直接设置发起回访流程的条件
                        set_derivative = f"""update otc_derivative_counterparty set
                                                aml_monitor_flag = 'true',
                                                no_auto_visit = 'false',
                                                return_visit_date=date_format('{date_str}','%Y-%m-%d'),
                                                customer_manager='{user}',
                                                introduction_department = '{dept_code}',
                                                allow_busi_type = 'OPTION,PRODUCT'
                                                where corporate_name='{corporatename}'"""
                        print(f"execute_sql:{set_derivative}")
                        print(f"param ==> {date_str},{user},{dept_code},{corporatename}")
                        await cursor.execute(set_derivative)

                        set_counterpartyOrg = f"update counterparty_org set aml_monitor_flag = 'true' where corporate_name = '{corporatename}'"
                        print(f"execute_sql:{set_counterpartyOrg}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(set_counterpartyOrg)

                        # 查询受益人
                        select_beneficiary = f"""select count(*) from aml_beneficiary ab
                                                    left join aml_counterparty ac on ac.id = ab.counterparty_id
                                                    left join otc_derivative_counterparty odc on odc.client_id = ac.client_id
                                                    where ab.category = '1' and odc.corporate_name ='{corporatename}'"""
                        print(f"execute_sql:{select_beneficiary}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(select_beneficiary)
                        if not await cursor.fetchall():
                            raise ValueError("缺少受益人或者被授权代表人")

                        # 设置投资者明细条件
                        select_prod_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}' and is_prod_holder = '03'"
                        print(f"execute_sql:{select_prod_client}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(select_prod_client)
                        prod_clients = [item[0] for item in await cursor.fetchall()]
                        if prod_clients:
                            for client_id in prod_clients:
                                benefit_over_flag = f"select * from COUNTERPARTY_BENEFIT_OVER_LIST where client_id = '{client_id}'"
                                print(f"execute_sql:{benefit_over_flag}")
                                print(f"param ==> {client_id}")
                                await cursor.execute(benefit_over_flag)
                                data = await cursor.fetchall()
                                if not data:
                                    insert_benefit = f"""INSERT INTO COUNTERPARTY_BENEFIT_OVER_LIST
                                                        (CLIENT_ID, NAME, ID_NO, PROPORTION, FIID, PROFESSIONAL_INVESTOR_FLAG, FINANCIAL_ASSETS_OF_LASTYEAR, INVEST_3YEAR_EXP_FLAG, PROD_ID, ASSETS_20MILLION_FLAG)
                                                        VALUES('{client_id}', '测试', '81UB1HOGR7K8497CLS7PIM0C82P30F2S', 0.0321, NULL, '1', 123, '1', NULL, '1')"""
                                    print(f"execute_sql:{insert_benefit}")
                                    print(f"param ==> {client_id}")
                                    await cursor.execute(insert_benefit)
                                else:
                                    update_benefit = f"""UPDATE COUNTERPARTY_BENEFIT_OVER_LIST
                                                        SET NAME='测试', PROPORTION=0.0321, FIID=NULL, PROFESSIONAL_INVESTOR_FLAG='1', FINANCIAL_ASSETS_OF_LASTYEAR=123, INVEST_3YEAR_EXP_FLAG='1', PROD_ID=NULL, ASSETS_20MILLION_FLAG='1'
                                                        WHERE CLIENT_ID='{client_id}'"""
                                    print(f"execute_sql:{update_benefit}")
                                    print(f"param ==> {client_id}")
                                    await cursor.execute(update_benefit)

                        await db.commit()

                        result = await asyncio.gather(asyncio.create_task(single_review(corporatename,env)),
                                                      asyncio.create_task(multiple_review(corporatename,env)),
                                                      asyncio.create_task(uploadFile(env)))
                        print(result)
                        await cursor.execute(
                            f"select record_id from client_review_record where client_name ='{corporatename}' and current_status !='CLOSED'")

                        record_id = await cursor.fetchall()
                        if not record_id:
                            raise Exception("发起流程失败")
                        print(f"record_id 是 {record_id}")
                        data_tuple = []
                        if len(record_id) == 1:
                            for i in range(13):
                                data_list = []
                                data_list.append(record_id[0][0])
                                data_list.append(result[2][i])
                                data_tuple.append(data_list)
                            print(data_tuple)

                        elif len(record_id) == 2:
                            _location = 0
                            while _location < 26:
                                data_list = []
                                if _location >= 0 and _location < 13:
                                    data_list.append(record_id[0][0])
                                    data_list.append(result[2][_location])
                                    data_tuple.append(data_list)

                                elif _location >= 13:
                                    data_list.append(record_id[1][0])
                                    data_list.append(result[2][_location])
                                    data_tuple.append(data_list)
                                _location += 1

                        for i in data_tuple:
                            update_file = f"update client_review_file_record set record_id = '{i[0]}' where s3_file_id = '{i[1]}'"
                            print(f"execute_sql:{update_file}")
                            print(f"param ==> {i[0]},{i[1]}")
                            await cursor.execute(update_file)


                        for i in range(len(record_id)):
                            update_detail = f"""insert into CLIENT_REVIEW_DETAIL(
                            id,record_id,client_name,client_position,email,phone,review_log,suitability,suitability_log,created_datetime) 
                            values('{str(uuid4())}','{record_id[i][0]}','11','老师','123@qq.com','13112345678','123','N','123',date_format('{date_str}','%Y-%m-%d')) """
                            print(f"execute_sql:{update_detail}")
                            await cursor.execute(update_detail)

                            update_counterparty = f"""update client_review_counterparty set agree_info = 'Y',benefit_over_flag = '1' where record_id = '{record_id[i][0]}'"""
                            print(f"execute_sql:{update_counterparty}")
                            print(f"param ==> {record_id[i][0]}")
                            await cursor.execute(update_counterparty)

                            update_record = f"""update client_review_record set accounting_firm_name='测试专用',sale_person = 'zhuliejin'  where record_id = '{record_id[i][0]}'"""
                            print(f"execute_sql:{update_record}")
                            print(f"param ==> {record_id[i][0]}")

                            await cursor.execute(update_record)
                        await db.commit()
                        end = time.time()
                        print(f"回访流程已成功发起，耗时:%.2f s" %(end - start))

                        select_flow = f"select title, doc_id ,record_id ,CURRENT_OPERATOR from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED'"
                        print(f"execute_sql:{select_flow}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(select_flow)
                        flow_title = await cursor.fetchall()
                        response_list = []
                        for i in flow_title:
                            flow_dict = {}
                            flow_dict["corporateName"] = corporatename
                            flow_dict["title"] = i[0]
                            flow_dict["documentId"] = i[1]
                            flow_dict["recordId"]=i[2]
                            flow_dict["currentOperator"] = i[3]
                            response_list.append(flow_dict)
                        return {"code": 200,
                                "env": enviroment.name,
                                "data": [ReviewFlowResponse(**item) for item in response_list],
                                "status": "Successfully"}

        except Exception as e:
            print("{'error':%s}" % e)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": str(e),
                    "status": "Successfully"}
    #股衍的流程
    else :
        try:
            with cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest") as db:
                with db.cursor() as cursor:
                    select_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{select_client}")
                    print(f"param ==> {corporatename}")
                    cursor.execute(select_client)
                    if not cursor.fetchall():
                        raise ValueError("交易对手不存在")
                    # 删除存量流程
                    delete_flow = f"delete from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED' "
                    print(f"execute_sql:{delete_flow}")
                    print(f"param ==> {corporatename}")
                    cursor.execute(delete_flow)
                    # 查找客户经理
                    select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = '{customerManager}' "
                    print(f"execute_sql:{select_customermanager}")
                    print(f"param ==> {customerManager}")
                    cursor.execute(select_customermanager)
                    customer = cursor.fetchall()
                    if customer:
                        user, dept_code = customer[0][0], customer[0][1]
                        print(f"user:{customer[0][0]},dept_code:{customer[0][1]}")
                    else:
                        raise Exception("请输入正确的客户经理")
                    # 直接设置发起回访流程的条件
                    set_derivative = f"""update otc_derivative_counterparty set
                                            aml_monitor_flag = 'true',
                                            no_auto_visit = 'false',
                                            return_visit_date=to_date('{date_str}','yyyy-mm-dd'),
                                            customer_manager='{user}',
                                            introduction_department = '{dept_code}',
                                            allow_busi_type = 'OPTION,PRODUCT'
                                            where corporate_name='{corporatename}'"""
                    print(f"execute_sql:{set_derivative}")
                    print(f"param ==> {date_str},{user},{dept_code},{corporatename}")
                    cursor.execute(set_derivative)

                    set_counterpartyOrg = f"update counterparty_org set aml_monitor_flag = 'true' where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{set_counterpartyOrg}")
                    print(f"param ==> {corporatename}")
                    cursor.execute(set_counterpartyOrg)

                    # 查询受益人
                    select_beneficiary = f"""select count(*) from aml_beneficiary ab
                                                left join aml_counterparty ac on ac.id = ab.counterparty_id
                                                left join otc_derivative_counterparty odc on odc.client_id = ac.client_id
                                                where ab.category = '1' and odc.corporate_name ='{corporatename}'"""
                    print(f"execute_sql:{select_beneficiary}")
                    print(f"param ==> {corporatename}")
                    cursor.execute(select_beneficiary)
                    if not cursor.fetchall():
                        raise ValueError("缺少受益人或者被授权代表人")

                    # 设置投资者明细条件
                    select_prod_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}' and is_prod_holder = '03'"
                    print(f"execute_sql:{select_prod_client}")
                    print(f"param ==> {corporatename}")
                    cursor.execute(select_prod_client)
                    prod_clients = [item[0] for item in cursor.fetchall()]
                    if prod_clients:
                        for client_id in prod_clients:
                            benefit_over_flag = f"select * from COUNTERPARTY_BENEFIT_OVER_LIST where client_id = '{client_id}'"
                            print(f"execute_sql:{benefit_over_flag}")
                            print(f"param ==> {client_id}")
                            cursor.execute(benefit_over_flag)
                            data = cursor.fetchall()
                            if not data:
                                insert_benefit = f"""INSERT INTO COUNTERPARTY_BENEFIT_OVER_LIST
                                                    (CLIENT_ID, NAME, ID_NO, PROPORTION, FIID, PROFESSIONAL_INVESTOR_FLAG, FINANCIAL_ASSETS_OF_LASTYEAR, INVEST_3YEAR_EXP_FLAG, PROD_ID, ASSETS_20MILLION_FLAG)
                                                    VALUES('{client_id}', '测试', '81UB1HOGR7K8497CLS7PIM0C82P30F2S', 0.0321, NULL, '1', 123, '1', NULL, '1')"""
                                print(f"execute_sql:{insert_benefit}")
                                print(f"param ==> {client_id}")
                                cursor.execute(insert_benefit)
                            else:
                                update_benefit = f"""UPDATE COUNTERPARTY_BENEFIT_OVER_LIST
                                                    SET NAME='测试', PROPORTION=0.0321, FIID=NULL, PROFESSIONAL_INVESTOR_FLAG='1', FINANCIAL_ASSETS_OF_LASTYEAR=123, INVEST_3YEAR_EXP_FLAG='1', PROD_ID=NULL, ASSETS_20MILLION_FLAG='1'
                                                    WHERE CLIENT_ID='{client_id}'"""
                                print(f"execute_sql:{update_benefit}")
                                print(f"param ==> {client_id}")
                                cursor.execute(update_benefit)

                    db.commit()

                    result = await asyncio.gather(asyncio.create_task(single_review(corporatename,env)),
                                                  asyncio.create_task(multiple_review(corporatename,env)),
                                                  asyncio.create_task(uploadFile(env)))
                    print(result)
                    cursor.execute(
                        f"select record_id from client_review_record where client_name ='{corporatename}' and current_status !='CLOSED'")
                    record_id = cursor.fetchall()
                    if not record_id:
                        raise Exception("发起流程失败")
                    print(f"record_id 是 {record_id}")
                    data_tuple = []
                    if len(record_id) == 1:
                        for i in range(14):
                            data_list = []
                            data_list.append(record_id[0][0])
                            data_list.append(result[2][i])
                            data_tuple.append(data_list)
                        print(data_tuple)

                    elif len(record_id) == 2:
                        _location = 0
                        while _location < 28:
                            data_list = []
                            if _location >= 0 and _location < 14:
                                data_list.append(record_id[0][0])
                                data_list.append(result[2][_location])
                                data_tuple.append(data_list)

                            elif _location >= 14:
                                data_list.append(record_id[1][0])
                                data_list.append(result[2][_location])
                                data_tuple.append(data_list)
                            _location += 1
                    # update_file = f"""update client_review_file_record set record_id=%s where s3_file_id=%s"""
                    # await  cursor.executemany(update_file, data_tuple)
                    for i in data_tuple:
                        update_file = f"update client_review_file_record set record_id = '{i[0]}' where s3_file_id = '{i[1]}'"
                        print(f"execute_sql:{update_file}")
                        print(f"param ==> {i[0]},{i[1]}")
                        cursor.execute(update_file)
                    db.commit()


                    for i in range(len(record_id)):
                        update_detail = f"""insert into CLIENT_REVIEW_DETAIL(
                        id,record_id,client_name,client_position,email,phone,review_log,suitability,suitability_log,created_datetime) 
                        values('{str(uuid4())}','{record_id[i][0]}','11','老师','123@qq.com','13112345678','123','N','123',to_date('{date_str}','yyyy-mm-dd')) """
                        print(f"execute_sql:{update_detail}")
                        cursor.execute(update_detail)

                        update_counterparty = f"""update client_review_counterparty set agree_info = 'Y',benefit_over_flag = '1',product_asset = 100 where record_id = '{record_id[i][0]}'"""
                        print(f"execute_sql:{update_counterparty}")
                        print(f"param ==> {record_id[i][0]}")
                        cursor.execute(update_counterparty)

                        indata = ({"accounting_firm_name":"测试专用","sale_person":"renyu","version":"202210" if isnew=="1" else None,"record_id":record_id[i][0]})
                        update_record = f"""update client_review_record set accounting_firm_name=:accounting_firm_name ,sale_person =:sale_person , version =:version where record_id =:record_id"""
                        print(f"execute_sql:{update_counterparty}")
                        print(f"param ==> {indata}")
                        cursor.execute(update_record,indata)

                        update_detail = "UPDATE CLIENT_REVIEW_DETAIL SET TRADE_PURPOSE  = '测试回访流程专用' WHERE RECORD_ID = :recordId"
                        print(f"execute_sql:{update_detail}")
                        print(f"param ==> {record_id[i][0]}")
                        cursor.execute(update_detail,({"recordId":record_id[i][0]}))

                    db.commit()
                    end = time.time()


                    select_flow = f"select title, doc_id ,record_id ,CURRENT_OPERATOR from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED'"
                    print(f"execute_sql:{select_flow}")
                    print(f"param ==> {corporatename}")
                    cursor.execute(select_flow)
                    flow_title = cursor.fetchall()
                    response_list = []
                    for i in flow_title:
                        flow_dict = {}
                        flow_dict["corporateName"] = corporatename
                        flow_dict["title"] = i[0]
                        flow_dict["documentId"] = i[1]
                        flow_dict["recordId"] = i[2]
                        flow_dict["currentOperator"] = i[3]
                        response_list.append(flow_dict)
                    print(f"回访流程已成功发起，耗时:%.2f s" % (end - start))
                    return {"code": 200,
                            "env": enviroment.name,
                            "data": [ReviewFlowResponse(**item) for item in response_list],
                            "status": "Successfully"}

        except Exception as e :
            print(e)
            return {"code": 500,
                            "env": enviroment.name,
                            "error":str(e),
                            "status": "Successfully"}

@review.post("/buffer",summary="触发回访缓冲期",description="本接口会自动将资质改成满足,且先取消回访超期原因，然后将今天设置为缓冲期到期日并且触发回访缓冲期")
async def bufferjob(corporatename: str , enviroment:Enviroment):
    '''
    :param corporatename: 公司名称
    :param enviroment:40-固收测试环境 216-股衍测试环境
    '''
    print(f"公司名称 ：{corporatename},enviroment : {enviroment}")
    date_1 = date.today()
    date_str = date.strftime(date_1, "%Y-%m-%d")
    env = enviroment.value
    if enviroment.name == "FICC":
        try:
            async with aiomysql.create_pool(**config) as pool:
                async with pool.acquire() as db:
                    async with db.cursor() as cursor:
                        select_clientId = "select client_id from otc_derivative_counterparty where corporate_name =%s"
                        print(f"execute_sql:{select_clientId}")
                        await cursor.execute(select_clientId, corporatename)
                        client_id = [clientId[0] for clientId in await cursor.fetchall()]
                        print(client_id)

                        for i in range(len(client_id)):
                            select_buffer = "select client_id from client_review_buffer where client_id =%s"
                            await cursor.execute(select_buffer,client_id[i])
                            if not await cursor.fetchall():
                                print(f"{client_id[i]} 不存在")
                                raise Exception(f"{client_id[i]} 不存在")
                            update_buffer_sql = f"update client_review_buffer set review_buffer_start =date_format('{date_str}','%Y-%m-%d') where client_id ='{client_id[i]}'"
                            print(f"update_buffer_sql:{update_buffer_sql}")
                            await cursor.execute(update_buffer_sql)
                        update_qualify = "update otc_derivative_counterparty set client_qualify_review_reason=NULL,client_qualify_review='true' where corporate_name=%s"
                        print(f"update_buffer_sql:{update_qualify}")
                        await cursor.execute(update_qualify,corporatename)
                        await db.commit()


                        # response = requests.get(env + "/api/test/countdownReviewBuffer")
                        # print(response.json())
                        with requests.Session() as session :
                            with session.post(env + "/api/test/countdownReviewBuffer") as response:
                                print(response.json())
                                return {"code": 200,
                                "env": enviroment.name,
                                "data": response.json(),
                                "status": "Successfully"}



        except Exception as e :
            return {"code": 500,
                    "env": enviroment.name,
                    "data": "失败",
                    "status": "Failed"}
    try:
        with cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest") as db:
            with db.cursor() as cursor:

                    select_clientId = f"select client_id from otc_derivative_counterparty where corporate_name ='{corporatename}'"
                    print(f"execute_sql:{select_clientId}")
                    cursor.execute(select_clientId)
                    client_id = [clientId[0] for clientId in cursor.fetchall()]
                    print(client_id)

                    for i in range(len(client_id)):
                        select_buffer = f"select client_id from client_review_buffer where client_id ='{client_id[i]}'"
                        cursor.execute(select_buffer)
                        if not cursor.fetchall():
                            print(f"{client_id[i]} 不存在")
                            raise Exception(f"{client_id[i]} 不存在")
                        update_buffer_sql = f"update client_review_buffer set review_buffer_start =to_date('{date_str}','yyyy-mm-dd') where client_id ='{client_id[i]}'"
                        print(f"update_buffer_sql:{update_buffer_sql}")
                        cursor.execute(update_buffer_sql)
                    update_qualify = f"update otc_derivative_counterparty set client_qualify_review_reason=NULL,client_qualify_review='true' where corporate_name='{corporatename}'"
                    print(f"update_buffer_sql:{update_qualify}")
                    cursor.execute(update_qualify)
                    db.commit()

                    # response = requests.get(env + "/api/test/countdownReviewBuffer")
                    # print(response.json())
                    with requests.Session() as session:
                        with session.post(env + "/api/test/countdownReviewBuffer") as response:
                            print(response.json())
                            return {"code": 200,
                                    "env": enviroment.name,
                                    "data": response.json(),
                                    "status": "Successfully"}
    except Exception as e :
        print(e)
        return {"code": 500,
                "env": enviroment.name,
                "data": "失败",
                "status": "Failed"}

@review.delete("/deleteByClientName",summary="根据机构名称来删除在途回访流程",operation_id='delete_review_flow_by_clientName')
def deleteFlow(enviroment : Enviroment , clientNameList : List[str]):

    '''
    :param enviroment: 40-固收测试环境 216-股衍测试环境
    :param clientNameList: 机构名称列表，会删除机构下所有交易对手的在途流程
    :return:
    '''
    if enviroment.name == "FICC":
        try :
            db = pymysql.connect(**config)
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(clientNameList)
            client_name_list = []
            for i in clientNameList:
                demo_list = []
                demo_list.append(i)
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where client_name = %s and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()
    else :
        try :
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(clientNameList)
            client_name_list = []
            for i in clientNameList:
                demo_list = {}
                demo_list["clientName"] = i
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where client_name = :clientName and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()

# @review.delete("/clientreview/deleteByRecordId",summary="根据recordId来删除一条在途回访流程",operation_id='delete_review_flow_by_recordId')

@review.delete("/deleteByRecordId",operation_id='delete_review_flow_by_recordId')
def deleteFlow(enviroment : Enviroment , recordList : List[str]):
    '''
    :param recordList:  传record_id列表，可删除一个或者多条流程
    '''
    if enviroment.name == "FICC":
        try :
            db = pymysql.connect(**config)
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(recordList)
            client_name_list = []
            for i in recordList:
                demo_list = []
                demo_list.append(i)
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where record_id = %s and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()
    else :
        try :
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(recordList)
            client_name_list = []
            for i in recordList:
                demo_list = {}
                demo_list["recordId"] = i
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where record_id = :recordId and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()
