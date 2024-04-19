import cx_Oracle
import pymysql
import requests
from fastapi import APIRouter

from bpm.bpm_api import cancel_in_bpm
from config.models import config_otc,config
from config.models import Enviroment
from get_otc_cookie import get_code

derivative = APIRouter()

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
    :param client_Id: 客户编号
    :param enviroment: 49-固收；216-股衍测试环境
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
