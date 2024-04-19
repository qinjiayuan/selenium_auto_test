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
from certificate.models import certificateResponse

certificates = APIRouter()

async def createFlow(corporatename: str , customerManager: str,enviroment:Enviroment):
    print(f"{corporatename},{customerManager},{enviroment}")

    if enviroment.name == "FICC":
        try:
            with aiomysql.create_pool(**config) as pool:
                with pool.acquire() as db:
                    with db.cursor() as cursor:
                        #确认交易对手存在台账
                        select_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                        print(f"execute_sql:{select_client}")
                        print(f"param ==> {corporatename}")
                        await cursor.execute(select_client)
                        if not await cursor.fetchall():
                            raise ValueError("交易对手不存在")
                        #查询客户经理是否存在
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

                        #查询证件到期在途流程
                        select_exsist_flow = "select doc_id from CRT_EXPIRED_RECORD where 1=1 and "


