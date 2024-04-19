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
import logging
import aiohttp
import json
from aiohttp import FormData


#环境枚举类
class Enviroment(Enum):
    FICC = "http://10.128.12.40:59754/otcoms-tst/fc-kyc-server"
    OTC = "http://10.2.145.216:9090"

#发起回访流程的响应模型
class ReviewFlowResponse(BaseModel):
    title : str
    corporateName : str
    documentId : str
    recordId:str
    currentOperator : str

#新旧版本枚举类
class ReviewIsNew(Enum):
     new :str = "1"
     old :str = "0"

#回访流程record表
class ReviewRecordInfo(BaseModel):
    '''
    流程记录表
    '''
    ID : Optional[str] =Field(None,description="id")
    DOC_ID : Optional[str]  =Field(None,description="文档号")
    TITLE : Optional[str] =Field(None,description="标题")
    CLIENT_NAME : Optional[str] =Field(None,description="公司名称")
    UNIFIEDSOCIAL_CODE : Optional[str] =Field(None,description="社会信用代码")
    REVIEW_DATE : Optional[date] =Field(None,description="回访日期")
    REVIEW_USER : Optional[str] =Field(None,description="回访人员oa账号")
    REVIEW_NAME : Optional[str] =Field(None,description="")
    CURRENT_STATUS : Optional[str] =Field(None,description="当前状态")
    CURRENT_OPERATOR : Optional[str] =Field(None,description="当前处理人")
    CURRENT_ACTIVITY_NAME : Optional[str] =Field(None,description="当前节点名称")
    RECORD_ID : Optional[str] =Field(None,description="CLIENT_REVIEW_DETAIL外键")
    CREATED_DATETIME : Optional[datetime] =Field(None,description="创建日期")
    WORK_PHONE : Optional[str] =Field(None,description="工作电话")
    PHONE: Optional[str] =Field(None,description="电话")
    SECURITY_LEVEL : Optional[str] =Field(None,description="安全等级")
    SECURITY_LEVEL_DETAIL : Optional[str] =Field(None,description="安全等级有效时间")
    URGENCY_LEVEL : Optional[str] =Field(None,description="紧急程度")
    URGENCY_LEVEL_REASON : Optional[str] =Field(None,description="紧急原因")
    SALE_PERSON : Optional[str] =Field(None,description="对应总部销售")
    REVIEW_TERM : Optional[date] =Field(None,description="回访期限")
    REVIEW_PROCESS_TYPE : Optional[str] =Field(None,description="回访流程发起类型")
    SPECIAL_MENTIONED_CUSTOMER : Optional[str] =Field(None,description="是否为关注类客户")
    VERSION : Optional[str] =Field(None,description="标识存量增量数据，增量为202210，存量为空")
    NO_MORE_REVIEW : Optional[str] =Field(None,description="不再自动回访")
    ACCOUNTING_FIRM_NAME : Optional[str] =Field(None,description="会计师事务所")
    SUPPLEMENTARY_MATERIALS_TIME : Optional[date] =Field(None,description="另行补充材料完成日期")
    SUPPLEMENTARY_MATERIALS : Optional[str] =Field(None,description="需要另行补充材料，'true'/'false'")
    REACH_TO_03_DATETIME : Optional[datetime] =Field(None,description="到达03节点的时间")
    SERIAL_NUMBER : Optional[str] =Field(None,description="流程文档编号")


class ResponseModel(BaseModel):
    code: int
    data:List[ReviewRecordInfo]
    status : Optional[str] =Field(None,description="状态")
#回访流程counterparty表
class ReviewCounterpartyInfo(BaseModel):
    '''
    回访对象表
    '''
    ID : Optional[str] =Field(None,description="id")
    RECORD_ID : Optional[str] =Field(None,description="记录id")
    PRODUCT_NAME : Optional[str] =Field(None,description="产品名称")
    CREATED_DATETIME : Optional[datetime] =Field(None,description="创建时间")
    CLIENT_ID : Optional[str] =Field(None,description="客户编号")
    IGNORE : Optional[str] =Field(None,description="是否本次不回访")
    BENEFIT_OVER_FLAG : Optional[str] =Field(None,description="是否存在单一委托人占比超过20%的情况（初始值来源台账，待流程结束回填）")
    AGREE_INFO : Optional[str] =Field(None,description="以下信息是否一致，增量'Y'或'N'，存量为空")
    ALLOW_BUSI_TYPE : Optional[str] =Field(None,description="拟参与的衍生品业务类型")
    CLIENT_QUALIFY_REVIEW : Optional[str] =Field(None,description="客户资质复核结果")
    SEQ : Optional[int] =Field(None,description="回访对象的序号")
    REVIEW_BUFFER_START : Optional[datetime] =Field(None,description="拟限制交易日（取自beffer）")
    SUPPLEMENTARY_MATERIALS_NOTE : Optional[str] =Field(None,description="补充材料说明")
    SHOW_NOTE : Optional[str] =Field(None,description="展示补充材料说明，'true'/'false'")
    ALLOW_BUSI_TYPE_HIS : Optional[str] =Field(None,description="拟参与的衍生品业务类型历史")
    MANUAL_DEL_ALLOW_BUSI_TYPE : Optional[str] =Field(None,description="回访中手动减少的衍生品业务类型")
    PRODUCT_ASSET : Optional[int]  =Field(None,description="产品资产净值（万元）")

#回访流程受益人表
class ReviewAmlBeneficiaryInfo(BaseModel):
    '''
    回访受益人信息
    '''
    ID : Optional[str] = Field(None,description='主键')
    ENTITY_TYPE : Optional[str]
    CATEGORY : Optional[str] = Field(None,description='受益所有人的身份')
    NAME : Optional[str]
    ID_KIND : Optional[str] = Field(None,description='证件类型')
    ID_NO : Optional[str] = Field(None,description='证件号码')
    BIRTH : Optional[str] = Field(None,description='出生日期')
    GENDER : Optional[str] = Field(None,description='性别')
    COUNTRY : Optional[str] = Field(None,description='国家')
    ID_VALIDDATE_START : Optional[str] = Field(None,description='证件有效期开始日')
    ID_VALIDDATE_END : Optional[str] =Field(None,description='证件有效期结束日')
    PHONE : Optional[str] = Field(None,description='电话')
    MOBILE : Optional[str]  = Field(None,description='手机')
    EMAIL : Optional[str]  = Field(None,description='邮箱')
    HOLD_RATE : Optional[str] = Field(None,description='持股比例')
    SPECIAL_TYPE : Optional[str]
    POSITION : Optional[str]
    HOLD_TYPE : Optional[str] = Field(None,description='持股类型')
    BENEFICIARY_TYPE : Optional[str]
    LOCKED : Optional[str]
    COUNTERPARTY_ID : Optional[str]
    ADDRESS : Optional[str]
    VERSION : Optional[int]
    CLIENT_KIND : Optional[str]
    CLIENT_ID : Optional[str]
    RECORD_ID : Optional[str]
    BUSINESS_TYPE : Optional[str]

#回访流程投资者明细表
class ReviewBenefitOver(BaseModel):
    CLIENT_ID : Optional[str] = Field(None , description='交易对手编号')
    NAME : Optional[str] = Field(None , description='姓名或机构名称')
    ID_NO : Optional[str] = Field(None , description='身份证号或统一信用代码')
    PROPORTION : Optional[int] = Field(None , description='认购份额比例')
    FIID  : Optional[int] = Field(None , description='流程编号')
    PROFESSIONAL_INVESTOR_FLAG  : Optional[str] = Field(None , description='专业投资者标准')
    FINANCIAL_ASSETS_OF_LASTYEAR : Optional[float] = Field(None , description='上年末金融资产')
    INVEST_3YEAR_EXP_FLAG : Optional[str] = Field(None , description='3年以上投资经验')
    PROD_ID : Optional[str] = Field(None , description='产品ID')
    ASSETS_20MILLION_FLAG : Optional[str] = Field(None , description='最近一年末金融资产是否不低于2000万')
    RECORD_ID : Optional[str] = Field(None , description='回访流程ID')
    ID : Optional[str] = Field(None , description='主键')