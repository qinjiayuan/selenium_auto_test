#环境枚举类
from enum import Enum

from pydantic import BaseModel


class Enviroment(Enum):
    FICC = "http://10.128.12.40:59754/otcoms-tst/fc-kyc-server"
    OTC = "http://10.2.145.216:9090"
#期权产品监测流程的响应类


class OptionFlowResponse(BaseModel):
    clientId : str
    title : str
    corporateName : str
    documentId : str
