from datetime import date
from enum import Enum
from typing import Optional
from pydantic import BaseModel ,Field



#ficc数据库配置
config = {"host": "10.128.12.222",
          "user": "otcmtst",
          "password": "Gf_otcmtst_test",
          "port": 15006,
          "db": "gf_ficc"}
#股衍数据库配置
config_otc = {"host": "10.62.146.18",
              "user": "gf_otc",
              "password": "otc1qazXSW@",
              "port": 1521,
              "db": "jgjtest"}

#环境枚举类
class Enviroment(Enum):
    FICC = "http://10.128.12.40:59754/otcoms-tst/fc-kyc-server"
    OTC = "http://10.2.145.216:9090"

