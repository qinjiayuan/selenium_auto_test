#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from selenium.common import TimeoutException

from page.webpage import WebPage, sleep
from common.readelement import Element
from utils.logger import log
from utils.util import rename_product, generate_bpm_docid

bpm = Element('bpm')


class BPMPage(WebPage):
    """BPM页面类"""

    @staticmethod
    def generate_docid(flow_id):
        return generate_bpm_docid(flow_id)

    def login(self, user, password):
        """登录，z已登录不需要重复登录"""
        if 'portal.gf.com.cn' not in self.get_current_url:
            return
        self.input_text(bpm['用户'], user)
        self.input_text(bpm['密码'], password)
        self.click_it(bpm['登录'])

    @property
    def is_loaded(self):
        return self.is_element_exist(bpm['BPM文档标题'])

    @property
    def get_step(self):
        js = 'return _currentActivityId'
        return str(self.driver.execute_script(js))

    def do_guitai_before_submit(self, step):
        if step == '01':
            self.click_it(bpm['盖章附件转PDF'])

    def __bpm_upload_file(self):
        self.input_text(bpm['上传文件'], r'c:\opt\test.jpg')
        sleep(4)
        self.click_it(bpm['上传文件确认'])

    def do_yingyebu_before_submit(self, step):
        if step == '01':
            if self.is_element_exist(bpm['经办人底稿未上传']):
                self.click_it(bpm['经办人底稿未上传'])
                self.__bpm_upload_file()
        elif step == '03':
            if self.is_element_exist(bpm['分公司复核底稿未上传']):
                self.click_it(bpm['分公司复核底稿未上传'])
                self.__bpm_upload_file()
        elif step == '24':
            self.click_it(bpm['需要后补材料'])
            if self.is_element_exist(bpm['是否为关注类客户']):
                self.click_it(bpm['是否为关注类客户'])
        elif step == '16':
            # 待整体测试
            self.click_it(bpm['客户待盖章附件转PDF'])
            sleep(5)
            self.click_it(bpm['创建盖章附件'])
            sleep(20)
            while self.is_element_exist(bpm['合同草稿提交入口']):
                self.click_it(bpm['合同草稿提交入口'])
                self.click_it(bpm['确定跳转合同系统弹框'])
                sleep(12)
                self.switch_page()
                log.info('跳转至页面：%s', self.get_current_url)
                self.click_it(bpm['合同系统提交'])
                self.click_it(bpm['合同系统提交二次确认'])
                sleep(10)
                self.switch_page()
                log.info('跳转至页面：%s', self.get_current_url)
        elif step == '90.01':
            self.click_it(bpm['选择印章'])
            self.click_it(bpm['选择印章广发证券'])
            self.click_it(bpm['弹框确定'])
            self.click_it(bpm['打开盖章窗口'])
            self.click_it(bpm['生成盖章份数'])
            self.click_it(bpm['确认盖章'])
            self.click_it(bpm['弹框确定'])
            self.click_it(bpm['选择印章'])
            self.click_it(bpm['选择印章易阳方'])
            self.click_it(bpm['弹框确定'])
            self.click_it(bpm['打开盖章窗口'])
            self.click_it(bpm['选择请求方式'])
            self.click_it(bpm['选择请求方式已授权'])
            self.click_it(bpm['生成盖章份数'])
            self.click_it(bpm['确认盖章'])
            self.click_it(bpm['弹框确定'])
        elif step == '18':
            locator = bpm['拟稿人上传盖章材料']
            file_order = 2
            locator_with_order = (locator[0], locator[1]+str(file_order))
            while self.is_element_exist(locator_with_order):
                self.click_it(locator_with_order)
                self.__bpm_upload_file()
                file_order += 1
                locator_with_order = (locator[0], locator[1] + str(file_order))
        elif step == '23':
            locator = bpm['交易对手同步产品名']
            product_order = 2
            locator_with_order = (locator[0], locator[1]+str(product_order)+']/td[2]')
            while self.is_element_exist(locator_with_order):
                rename_product(self.get_text(locator_with_order))
                product_order += 1
                locator_with_order = (locator[0], locator[1] + str(product_order) + ']/td[2]')
            self.click_it(bpm['同步交易对手台账'])
            self.set_implicitly_wait(300)
            log.info('打印同步结果：%s' % self.get_text(bpm['同步交易对手结果']))
            self.click_it(bpm['同步完成确认'])
            self.reset_implicitly_wait()

    def click_opinion(self):
        if self.is_element_exist(bpm['需要选择审批结果']) is False:
            return
        if self.is_element_exist(bpm['已选择审批结果']):
            return
        self.click_it(bpm['第一个审批结果'])

    def input_opinion(self, step):
        opinion = '%s节点审批通过' % step
        self.input_text(bpm['审批说明'], opinion)

    def click_next_step(self):
        """点击下一步"""
        try:
            self.click_it(bpm['下一步'])
        except TimeoutException:
            self.click_it(bpm['启动流程'])

    def __revise_approval(self, chinese_name):
        count = int(self.get_text(bpm['审批人数量']))
        for i in range(count):
            self.click_it(bpm['删除审批人'])
        self.input_text(bpm['输入审批人'], chinese_name)
        self.click_it(bpm['搜索审批人'])
        self.click_it(bpm['添加审批人'])
        self.click_it(bpm['确定审批人'])

    def revise_approval_next(self, step, chinese_name):
        if step == '03':
            self.click_it(bpm['03修改06下一处理人'])
            self.__revise_approval(chinese_name)
            self.click_it(bpm['03修改24下一处理人'])
            self.__revise_approval(chinese_name)
        elif step == '23':
            return
        else:
            self.click_it(bpm['修改下一处理人'])
            self.__revise_approval(chinese_name)

    def revise_approval_chain(self, chinese_name):
        self.click_it(bpm['修改审批链'])
        self.__revise_approval(chinese_name)

    def click_confirm(self):
        self.click_it(bpm['交办'])

    def click_archive(self):
        self.click_confirm()
        self.click_it(bpm['确定归档'])

    def assert_step(self, step):
        """断言BPM当前为某节点"""
        if self.is_loaded is True:
            log.info('BPM当前节点：%s' % self.get_step)
            error_msg = 'BPM当前不是%s节点' % step
            assert self.get_step == step, error_msg
        else:
            assert False, "BPM页面加载失败"


if __name__ == '__main__':
    pass
