from asyncio import gather
from datetime import date ,datetime
from enum import Enum

from pydantic.tools import lru_cache

from bpm.bpm_api import open,cancel_in_bpm
import aiomysql
import pymysql
import requests
import uvicorn
from fastapi import FastAPI, APIRouter, Query, Body, Path
import cx_Oracle
from pydantic import BaseModel, Field
from typing import Optional, List, Union
from get_otc_cookie import get_code
derivative = APIRouter()
# derivative = FastAPI()
config = {"host": "10.128.12.222",
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
#环境枚举类
class Enviroment(Enum):
    FICC = "http://10.128.12.40:59754/otcoms-tst/fc-kyc-server"
    OTC = "http://10.2.145.216:9090"

class UpdateData(BaseModel):
    # CLIENT_ID: str = Field(...,title='客户编号')
    # UNIFIEDSOCIAL_CODE: Optional[str] = Field(None,title='统一信用代码')
    CLIENT_QUALIFY_REVIEW: Optional[str] = Field(None,title='资质复核字段',description="资质复核字段")
    CLIENT_QUALIFY_REVIEW_REASON: Optional[str] = Field(None,title='资质复核不满足原因',description="资质复核不满足原因")
    OPERATOR: Optional[str] = Field(None,title='主经办人',description="主经办人")
    INTRODUCTION_OPERATOR: Optional[str] = Field(None,title='引入经办人',description='引入经办人')
    CUSTOMER_MANAGER: Optional[str] =  Field(None,title='客户经理',description='客户经理')
    ALLOW_BUSI_TYPE: Optional[str] =  Field(None,title='拟参与业务类型',description='拟参与业务类型')
    HIS_ALLOW_BUSI_TYPE: Optional[str] = Field(None,title='历史交易权限',description='历史交易权限')
    MASTER_AGREEMENT_DATE: Optional[str] = Field(None,title='主协议签订日',description='主协议签订日')
    AUDIT_STATUS:Optional[str] = Field(None,title='审核状态',description='审核状态')
    RETURN_VISIT_DATE: Optional[str] = Field(None,title='回访日期',description='回访日期')

class CommonCheck(BaseModel):
    CORPORATE_NAME: Optional[str] = Field(None, description='机构名称')
    UNIFIEDSOCIAL_CODE: Optional[str] = Field(None, title='统一信用代码')
    CLIENT_ID: str = Field(...,title='客户编号')
    IS_PROD_HOLDER : str = Field(None,description='是否产品客户')
    SIGNATURE_NAME: Optional[str] = Field(None, description='代签产品名称')
    ABBREVIATION: Optional[str] = Field(None, description='简称')
    CLIENT_QUALIFY_REVIEW: Optional[str] = Field(None, title='资质复核字段', description="资质复核字段")
    CLIENT_QUALIFY_REVIEW_REASON: Optional[str] = Field(None, title='资质复核不满足原因', description="资质复核不满足原因")
    OPERATOR: Optional[str] = Field(None, title='主经办人', description="主经办人")
    INTRODUCTION_OPERATOR: Optional[str] = Field(None, title='引入经办人', description='引入经办人')
    INTRODUCTION_DEPARTMENT :Optional[str] = Field(None,title='客户经理的部门',description='客户经理的部门')
    CUSTOMER_MANAGER: Optional[str] = Field(None, title='客户经理', description='客户经理')
    ALLOW_BUSI_TYPE: Optional[str] = Field(None, title='拟参与业务类型', description='拟参与业务类型')
    HIS_ALLOW_BUSI_TYPE: Optional[str] = Field(None, title='历史交易权限', description='历史交易权限')
    MASTER_AGREEMENT_DATE: Optional[date] = Field(None, title='主协议签订日', description='主协议签订日')
    AUDIT_STATUS: Optional[str] = Field(None, title='审核状态', description='审核状态')
    RETURN_VISIT_DATE: Optional[date] = Field(None, title='回访日期', description='回访日期')




# def update_sql(key,value):
def update_in_kyc(docId: str, client_Id: str, enviroment: str):
    print(docId,client_Id,enviroment)
    if enviroment == "FICC":

        db = pymysql.connect(**config)
        cursor = db.cursor()
        try:
            cursor.execute("UPDATE otc_derivative_counterparty SET audit_status = '通过' WHERE client_id = %s ",
                           (client_Id,))
            cursor.execute(
                "UPDATE ctpty_info_update_record SET current_status ='CLOSED' , current_operator = null WHERE doc_id = %s",
                (docId,))
        except Exception as e:
            print(str(e))
            return str(e)
        else:
            db.commit()
        finally:
            cursor.close()
            db.close()
    elif enviroment == 'OTC':
        db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
        cursor = db.cursor()

        try:
            update_sql = f"UPDATE OTC_DERIVATIVE_COUNTERPARTY SET AUDIT_STATUS = '通过' WHERE client_id ='{client_Id}' "
            cursor.execute(update_sql)
            print(f"execute_sql :{update_sql}")
            cursor.execute(
                "UPDATE ctpty_info_update_record SET current_status ='CLOSED' , current_operator = null WHERE doc_id = :docId",
                {"docId": docId})
            print('已经成功更改')
        except Exception as e:
            print(str(e))
            return str(e)
        else:

            db.commit()



@derivative.post('/cancel/flow',summary='撤销在途台账复核流程')
def cancelFlow(client_Id : str ,enviroment : Enviroment ):
    '''
    :param client_Id:
    :param enviroment:
    :return:
    '''
    try :
        #获取当前docid
        url = 'https://otcoms-test.gf.com.cn/spsrest/deriv_counterparty/queryDetail' if enviroment.name=='OTC' else "https://otcoms-test.gf.com.cn/spsrest/ficc/api/derivative/counterparty/get/clientId"
        params = {"clientId": client_Id}
        response = requests.get(url=url, params=params, headers={"cookie": get_code("sunbin" if enviroment.name=='OTC' else "zhuliejin")})
        print(url)


        docId = response.json()["data"]["currentDocId"]
        user = response.json()["data"]["currentUser"].split("|")[-1]
        taskId = open(docId,user)
        cancel_in_bpm(docId,taskId,user)
        update_in_kyc(docId,client_Id,str(enviroment.name))
        return {"code": 200,
                "env": enviroment.name,
                "data": f"成功撤销{enviroment.name}中{client_Id}的在途流程",
                "status": "Successfully"}
    except Exception as e :
        print(str(e))


@derivative.post('/update',summary='更改对应交易对手中的某个字段')
def modify(client_Id: str ,data : UpdateData,enviroment : Enviroment  , unifiedsocial_code: str =None ):
    '''

    :param client_Id: 客户编号
    :param data: 传需要更新的字段，不需要的可直接不传，如果需要置空则传null字符串
    :param enviroment: 40-固收测试环境，216-股衍测试环境
    :param unifiedsocial_code: 统一信用代码（可不传）
    '''
    if enviroment.name == 'FICC':
        db = pymysql.connect(**config)
        cursor =db.cursor()
        try:
            update_sql = f"update otc_derivative_counterparty set "
            update_part = []
            where_part = []
            where_part.append(f" where client_id = '{client_Id}'")
            if unifiedsocial_code is not None and unifiedsocial_code != '':
                where_part.append(f"unifiedsocial_code = '{unifiedsocial_code}'")
            for k , v in data:
                if v is not None and v != '' :
                    if k in ['RETURN_VISIT_DATE','MASTER_AGREEMENT_DATE']:
                        update_part.append(f"{k} = null ") if (v == 'NULL' or v == 'null') else update_part.append(f"{k} = date_format('{v}','%Y-%m-%d')")
                    else :
                        value = "null" if (v =='null' or v =='NULL') else v
                        update_part.append(f"{k} = '{v}'") if (v!='null' and v!='NULL') else update_part.append(f"{k} = null")

            update_sql = update_sql + ','.join(update_part)
            update_sql += ' ' + 'and '.join(where_part)
            print(update_sql)
            cursor.execute(update_sql)
        except Exception as e:
            print(str(e))
            db.rollback()
            return {"code": 200,
                            "env":enviroment.name,
                            "data": f"执行报错：{str(e)}",
                            "status": "Successfully"}
        else :
            db.commit()
            return {"code": 500,
                    "env": enviroment.name,
                    "data": f"{update_sql}已经执行成功",
                    "status": "Successfully"}
        finally:
            cursor.close()
            db.close()

    else :
        db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
        cursor = db.cursor()
        try:
            update_sql = f"update otc_derivative_counterparty set "
            update_part = []
            where_part = []
            where_part.append(f" where client_id = '{client_Id}'")
            if unifiedsocial_code is not None and unifiedsocial_code != '':
                where_part.append(f"unifiedsocial_code = '{unifiedsocial_code}'")

            for k, v in data:
                if v is not None and v != '':
                    if k in ['RETURN_VISIT_DATE', 'MASTER_AGREEMENT_DATE']:
                        update_part.append(f"{k} = null ") if (v == 'NULL' or v == 'null') else update_part.append(
                            f"{k} = to_date('{v}','yyyy-mm-dd')")
                    else:
                        value = "null" if (v == 'null' or v == 'NULL') else v
                        update_part.append(f"{k} = '{v}'") if (v != 'null' and v != 'NULL') else update_part.append(
                            f"{k} = null")
            update_sql = update_sql + ','.join(update_part)
            update_sql += ' ' + 'and '.join(where_part)
            print(update_sql)
            cursor.execute(update_sql)
        except Exception as e:
            db.rollback()
            print(str(e))
            return {"code": 500,
                    "env": enviroment.name,
                    "data": f"执行失败{str(e)}",
                    "status": "Successfully"}
        else:
            db.commit()
            return {"code": 500,
                    "env": enviroment.name,
                    "data": f"{update_sql}已经执行成功",
                    "status": "Successfully"}
        finally:
            cursor.close()
            db.close()

@lru_cache(maxsize=128)
@derivative.post("/select",summary="查询常用的值",status_code=200)
async def select_commen_data(client_id : List[str],enviroment : Enviroment):
    '''
    :param client_id: 客户编号
    :param enviroment: 40-固收测试环境，216-股衍测试环境
    :return:返回常用查询字段
    '''
    print(client_id,enviroment)
    if enviroment.name =='FICC':
        db = pymysql.connect(**config)
        cursor = db.cursor()
        response_list = []
        try :
            select_sql = f"""select CORPORATE_NAME ,UNIFIEDSOCIAL_CODE,CLIENT_ID,IS_PROD_HOLDER,
                                    SIGNATURE_NAME,ABBREVIATION,CLIENT_QUALIFY_REVIEW,
                                    CLIENT_QUALIFY_REVIEW_REASON,OPERATOR,INTRODUCTION_OPERATOR,
                                    INTRODUCTION_DEPARTMENT,CUSTOMER_MANAGER,ALLOW_BUSI_TYPE,HIS_ALLOW_BUSI_TYPE,
                                    MASTER_AGREEMENT_DATE,AUDIT_STATUS,RETURN_VISIT_DATE 
                                    FROM OTC_DERIVATIVE_COUNTERPARTY WHERE CLIENT_ID = %s
                                    """
            for clientId in client_id:
                result_dict = {}
                cursor.execute(select_sql,(clientId,))
                result = cursor.fetchall()
                result_dict["CORPORATE_NAME"] = result[0][0]
                result_dict["UNIFIEDSOCIAL_CODE"] = result[0][1]
                result_dict["IS_PROD_HOLDER"] = result[0][3]
                result_dict["SIGNATURE_NAME"] = result[0][4]
                result_dict["ABBREVIATION"] = result[0][5]
                result_dict["CLIENT_QUALIFY_REVIEW"] = result[0][6]
                result_dict["CLIENT_QUALIFY_REVIEW_REASON"] = result[0][7]
                result_dict["OPERATOR"] = result[0][8]
                result_dict["INTRODUCTION_OPERATOR"] = result[0][9]
                result_dict["INTRODUCTION_DEPARTMENT"] = result[0][10]
                result_dict["CUSTOMER_MANAGER"] =result[0][11]
                result_dict["ALLOW_BUSI_TYPE"] = result[0][12]
                result_dict["HIS_ALLOW_BUSI_TYPE"] = result[0][13]
                result_dict["MASTER_AGREEMENT_DATE"] = result[0][14]
                result_dict["AUDIT_STATUS"] = result[0][15]
                result_dict["RETURN_VISIT_DATE"] = result[0][16]
                result_dict['CLIENT_ID'] = result[0][2]
                print(result_dict['CLIENT_ID']) if "CLIENT_ID" in result_dict else print("确实不存在CLIENT_ID")
                response_list.append(result_dict)
            print(response_list)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": [CommonCheck(**item) for item in response_list],
                    "status": "Successfully"}

        except Exception as e :
            print(str(e))
            return {"code": 500,
                    "env": enviroment.name,
                    "data": str(e),
                    "status": "Failed"}

        finally:
            cursor.close()
            db.close()
    else :
        db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
        cursor = db.cursor()

        try:
            response_list = []
            select_sql = f"""select CORPORATE_NAME ,UNIFIEDSOCIAL_CODE,CLIENT_ID,IS_PROD_HOLDER,
                                    SIGNATURE_NAME,ABBREVIATION,CLIENT_QUALIFY_REVIEW,
                                    CLIENT_QUALIFY_REVIEW_REASON,OPERATOR,INTRODUCTION_OPERATOR,
                                    INTRODUCTION_DEPARTMENT,CUSTOMER_MANAGER,ALLOW_BUSI_TYPE,HIS_ALLOW_BUSI_TYPE,
                                    MASTER_AGREEMENT_DATE,AUDIT_STATUS,RETURN_VISIT_DATE 
                                    FROM OTC_DERIVATIVE_COUNTERPARTY WHERE CLIENT_ID = :clientId
                                    """
            for clientId in client_id:
                result_dict = {}
                cursor.execute(select_sql, ({"clientId":clientId}))
                result = cursor.fetchall()
                result_dict["CORPORATE_NAME"] = result[0][0]
                result_dict["UNIFIEDSOCIAL_CODE"] = result[0][1]
                result_dict["IS_PROD_HOLDER"] = result[0][3]
                result_dict["SIGNATURE_NAME"] = result[0][4]
                result_dict["ABBREVIATION"] = result[0][5]
                result_dict["CLIENT_QUALIFY_REVIEW"] = result[0][6]
                result_dict["CLIENT_QUALIFY_REVIEW_REASON"] = result[0][7]
                result_dict["OPERATOR"] = result[0][8]
                result_dict["INTRODUCTION_OPERATOR"] = result[0][9]
                result_dict["INTRODUCTION_DEPARTMENT"] = result[0][10]
                result_dict["CUSTOMER_MANAGER"] = result[0][11]
                result_dict["ALLOW_BUSI_TYPE"] = result[0][12]
                result_dict["HIS_ALLOW_BUSI_TYPE"] = result[0][13]
                result_dict["MASTER_AGREEMENT_DATE"] = result[0][14]
                result_dict["AUDIT_STATUS"] = result[0][15]
                result_dict["RETURN_VISIT_DATE"] = result[0][16]
                result_dict['CLIENT_ID'] = result[0][2]
                print(result_dict['CLIENT_ID']) if "CLIENT_ID" in result_dict else print("确实不存在CLIENT_ID")
                response_list.append(result_dict)
            print(response_list)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": [CommonCheck(**item) for item in response_list],
                    "status": "Successfully"}

        except Exception as e:
            print(str(e))
            return {"code": 500,
                    "env": enviroment.name,
                    "data": str(e),
                    "status": "Failed"}

        finally:
            cursor.close()
            db.close()




















