#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
selenium基类
本文件存放了selenium基类的封装方法
"""
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException

from config.conf import cm
from utils.times import sleep
from utils.logger import log


class WebPage(object):
    """selenium基类"""

    def __init__(self, driver):
        # self.driver = webdriver.Chrome()
        self.driver = driver
        self.webdriver_wait_timeout = 5 # 原20s
        self.wait = WebDriverWait(self.driver, self.webdriver_wait_timeout)
        self.implicitly_wait_timeout = 5 # 原10s
        self.page_load_wait_timeout = 60

    def get_url(self, url):
        """打开网址并验证"""
        self.driver.maximize_window()
        self.driver.set_page_load_timeout(self.page_load_wait_timeout)
        try:
            self.driver.get(url)
            self.driver.implicitly_wait(self.implicitly_wait_timeout)
            log.info("打开网页：%s" % url)
        except TimeoutException:
            raise TimeoutException("打开%s超时请检查网络或网址服务器" % url)

    def close_url(self):
        self.driver.close()

    @staticmethod
    def element_locator(func, locator):
        """元素定位器"""
        name, value = locator
        return func(cm.LOCATE_MODE[name], value)

    def find_element(self, locator, mode='visible'):
        """寻找单个元素，z通过mode进行扩展，默认模式为visible"""
        # return WebPage.element_locator(lambda *args: self.wait.until(EC.presence_of_element_located(args)), locator)
        # 背景： 用WebDriverWait时，一开始用的是presence_of_element_located，我对它的想法就是他就是用来等待元素出现。结果屡屡出问题。元素默认是隐藏的，导致等待过早的就结束了。
        # 解决：去StackOverFlow查了一下，发现我应该用visibility_of_element_located。
        if mode == 'present':
            return WebPage.element_locator(lambda *args: self.wait.until(
                EC.presence_of_element_located(args)), locator)
        if mode == 'clickable':
            return WebPage.element_locator(lambda *args: self.wait.until(
                EC.element_to_be_clickable(args)), locator)
        return WebPage.element_locator(lambda *args: self.wait.until(
            EC.visibility_of_element_located(args)), locator)

    # def find_clickable_element(self, locator):
    #     """z寻找可点击元素"""
    #     return WebPage.element_locator(lambda *args: self.wait.until(
    #         EC.element_to_be_clickable(args)), locator)

    def find_elements(self, locator, mode='visible'):
        """查找多个相同的元素，z通过mode进行扩展，默认模式为visible"""
        # return WebPage.element_locator(lambda *args: self.wait.until(EC.presence_of_all_elements_located(args)), locator)
        if mode == 'present':
            return WebPage.element_locator(lambda *args: self.wait.until(
                EC.presence_of_all_elements_located(args)), locator)
        return WebPage.element_locator(lambda *args: self.wait.until(
            EC.visibility_of_any_elements_located(args)), locator)

    def elements_num(self, locator):
        """获取相同元素的个数"""
        number = len(self.find_elements(locator))
        log.info("相同元素：{}".format((locator, number)))
        return number

    def input_text(self, locator, txt):
        """输入(输入前先清空)"""
        sleep(0.5)
        ele = self.find_element(locator)
        ele.clear()
        ele.send_keys(txt)
        log.info("输入文本：{}".format(txt))

    def click_it(self, locator):
        """点击"""
        ele = self.find_element(locator)
        # 滚动至元素位置再点击：解决滚动条未滚动到位导致报错element click intercepted
        self.driver.execute_script("arguments[0].scrollIntoView();", ele)
        ele.click()
        sleep()
        log.info("点击元素：{}".format(locator))

    # def click_with_scroll(self, locator):
    #     """z滚动至元素位置再点击：解决滚动条未滚动到位导致报错element click intercepted"""
    #     ele = self.find_element(locator)
    #     self.driver.execute_script("arguments[0].scrollIntoView();", ele)
    #     ele.click()
    #     sleep()
    #     log.info("点击元素：{}".format(locator))

    def click_with_script(self, locator):
        """z使用脚本点击：解决滚动条未滚动到位导致报错element click intercepted"""
        ele = self.find_element(locator)
        self.driver.execute_script("arguments[0].click();", ele)
        sleep()
        log.info("点击元素：{}".format(locator))

    def click_all(self, locator):
        """z点击多个"""
        ele_list = self.find_elements(locator)
        for ele in ele_list:
            ele.click()
            sleep()
            log.info("点击元素：{}".format(locator))

    def get_text(self, locator):
        """获取当前的text"""
        _text = self.find_element(locator).text
        log.info("获取文本：{}".format(_text))
        return _text

    @property
    def get_source(self):
        """获取页面源代码"""
        return self.driver.page_source

    def refresh(self):
        """刷新页面F5"""
        self.driver.refresh()
        self.driver.implicitly_wait(30)

    def is_element_exist(self, locator):
        """z判断元素是否存在：使用WebDriverWait"""
        try:
            self.find_element(locator)
        except TimeoutException:
            return False
        else:
            return True

    def is_clickable_element_exist(self, locator):
        """z判断可点击元素是否存在：使用WebDriverWait"""
        try:
            self.find_element(locator, 'clickable')
        except TimeoutException:
            return False
        else:
            return True

    def is_element_exist_old(self, locator):
        """z判断元素是否存在"""
        name, value = locator
        try:
            self.driver.find_element(cm.LOCATE_MODE[name], value)
        except NoSuchElementException:
            return False
        else:
            return True

    @property
    def get_current_url(self):
        """z获取当前地址"""
        return self.driver.current_url

    def switch_page(self):
        """z切换窗口至跳转后的页面"""
        self.driver.switch_to.window(self.driver.window_handles[-1])

    def set_implicitly_wait(self, timeout):
        self.driver.implicitly_wait(timeout)
        self.driver.set_page_load_timeout(timeout)

    def reset_implicitly_wait(self):
        self.driver.implicitly_wait(self.implicitly_wait_timeout)
        self.driver.set_page_load_timeout(self.page_load_wait_timeout)


if __name__ == "__main__":
    pass
