#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import re
import pytest
import allure

from utils.logger import log
from common.readconfig import ini
from page_object.bpm_page import BPMPage
from utils.times import sleep


@allure.feature("测试BPM节点流程")
class TestBPM:

    def setup_class(self):
        """TestBPM类初始化"""
        self.flow_id = ini.flowid('FLOW_YINGYEBU')
        # docid = "docid:527EDDD4812640818DE912A129B74614"
        docid = BPMPage.generate_docid(self.flow_id)
        self.url = ini.url('HOST_BPM') + docid

    @pytest.fixture(scope='function', autouse=True)
    def open_bpm(self, drivers):
        """打开BPM"""
        bpm = BPMPage(drivers)
        bpm.get_url(self.url)
        bpm.login('heyin', 'Gfte5tHw2022!')
        # TODO: 增加一个retry装饰器防止登录后未直接跳转BPM
        sleep(2)
        if 'portal.gf.com.cn' in bpm.get_current_url:
            bpm.get_url(ini.url('HOST_BPM'))
            bpm.login('heyin', 'Gfte5tHw2022!')

    def debug_test_debug(self, drivers):
        log.info("-------test_debug-------")
        bpm = BPMPage(drivers)
        sleep(5)

    def process_bpm_step(self, drivers, step_before, step_after, approval_name):
        # 提交后等待处理时间
        submit_wait_time = 8

        # 初始化bpm
        bpm = BPMPage(drivers)

        # 断言BPM当前为起始节点
        bpm.assert_step(step_before)

        # 完成当前节点页面操作 -> 点击和输入审批意见 -> 点击下一步 -> 修改审批人 -> 交办
        bpm.do_yingyebu_before_submit(step_before)
        bpm.click_opinion()
        bpm.input_opinion(step_before)
        bpm.click_next_step()
        bpm.revise_approval_next(step_before, approval_name)
        if step_before == '23':
            bpm.click_archive()
        else:
            bpm.click_confirm()
        sleep(submit_wait_time)
        bpm.refresh()

        # 断言BPM流转至目标节点
        bpm.assert_step(step_after)

    @allure.story("BPM营业部01至03节点")
    @pytest.mark.run(order=1001)
    def test_yingyebu_01_to_03(self, drivers):
        step_before = '01'
        step_after = '03'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部03至06节点")
    @pytest.mark.run(order=1002)
    def test_yingyebu_03_to_06(self, drivers):
        step_before = '03'
        step_after = '06'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部06至24节点")
    @pytest.mark.run(order=1003)
    def test_yingyebu_06_to_24(self, drivers):
        step_before = '06'
        step_after = '24'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部24至08节点")
    @pytest.mark.run(order=1004)
    def test_yingyebu_24_to_08(self, drivers):
        step_before = '24'
        step_after = '08'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部08至09节点")
    @pytest.mark.run(order=1005)
    def test_yingyebu_08_to_09(self, drivers):
        step_before = '08'
        step_after = '09'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部09至16节点")
    @pytest.mark.run(order=1006)
    def test_yingyebu_09_to_16(self, drivers):
        step_before = '09'
        step_after = '16'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部16至90.01节点")
    @pytest.mark.run(order=1007)
    def test_yingyebu_16_to_9001(self, drivers):
        step_before = '16'
        step_after = '90.01'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部90.01至18节点")
    @pytest.mark.run(order=1008)
    def test_yingyebu_9001_to_18(self, drivers):
        step_before = '90.01'
        step_after = '18'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部18至22节点")
    @pytest.mark.run(order=1009)
    def test_yingyebu_18_to_22(self, drivers):
        step_before = '18'
        step_after = '22'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部22至23节点")
    @pytest.mark.run(order=1010)
    def test_yingyebu_22_to_23(self, drivers):
        step_before = '22'
        step_after = '23'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)

    @allure.story("BPM营业部23至99节点")
    @pytest.mark.run(order=1011)
    def test_yingyebu_23_to_99(self, drivers):
        step_before = '23'
        # TODO
        step_after = '23'
        approval_name = '何颖'
        self.process_bpm_step(drivers, step_before, step_after, approval_name)


if __name__ == '__main__':
    pytest.main(['TestCase/test_bpm.py'])

