#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import requests

def api_get(url):
    return requests.get(url)

def rename_product(product_name):
    api = 'http://oat.gf.com.cn:8000/oat/data_structure/cell_data/?data_no=DATA_364&user_name=chenzewei&product_name=%s&DB_BPM=ENV_511' \
          % product_name
    api_get(api)

def generate_bpm_docid(flow_id):
    api = 'http://oat.gf.com.cn:8000/oat/data_structure/cell_data/?data_no=DATA_365&user_name=chenzewei&flow_id=%s&DB_OTCSPS=ENV_511&HOST_OTCSPS=ENV_522' \
          % str(flow_id)
    response = api_get(api).json()
    return response["data"]["docid"]
