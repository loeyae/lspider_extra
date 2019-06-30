#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019-1-14 9:57:13
"""
import copy
import time
from cdspider_extra.handler import CommentHandler
from cdspider_extra.database.base import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.parser import CustomParser


class ExtendedHandler(CommentHandler):
    """
    extended handler
    :property task 爬虫任务信息 {"mode": "extend", "uuid": SpiderTask.extend uuid}
                   当测试该handler，数据应为 {"mode": "extend", "url": url, "extendRule": 扩展那规则，参考扩展规则}
    """

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        if "rule" in self.task:
            parse_rule = self.db['ExtendRuleDB'].get_detail(self.task['rule'])
            if not parse_rule:
                raise CDSpiderDBDataNotFound("ExtendRule: %s not exists" % self.task['rule'])
            self.task['parent_url'] = self.task['url']
            self.task['acid'] = "testing_mode"
            typeinfo = utils.typeinfo(self.task['parent_url'])
            if typeinfo['domain'] != parse_rule['domain'] \
                    or (parse_rule['subdomain'] and typeinfo['subdomain'] != parse_rule['subdomain']):
                raise CDSpiderNotUrlMatched()
            crawler = self.get_crawler(self.task.get('rule', {}).get('request'))
            response = crawler.crawl(url=self.task['parent_url'])
            data = utils.get_attach_data(CustomParser, response['content'], self.task['parent_url'],
                                         parse_rule, self.log_level)
            if data is False:
                return None
            url, params = utils.build_attach_url(data, parse_rule, self.task['parent_url'])
            del crawler
            if not url:
                raise CDSpiderNotUrlMatched()
            self.task['url'] = url
            save['base_url'] = url
            save["hard_code"] = params
            parse_rule['request']['hard_code'] = params
        else:
            article = self.db['ArticlesDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % self.task['parentid'])
            self.task['parent_url'] = article['url']
            self.task['acid'] = article['acid']
            ruleId = self.task.get('rid', 0)
            parse_rule = self.db['ExtendRuleDB'].get_detail(ruleId)
            if not parse_rule:
                raise CDSpiderDBDataNotFound("ExtendRule: %s not exists" % ruleId)
            if parse_rule['status'] != ExtendRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("comment rule not active")
        return parse_rule

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['final_url']
        self.crawl_info['crawl_count']['page'] += 1
        if self.response['parsed']:
            result = copy.deepcopy(self.response['parsed'])
            self.debug("%s result: %s" % (self.__class__.__name__, result))
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                result['utime'] = self.crawl_id
                self.db['ArticlesDB'].update(self.task.get('parentid', '0'), result)
                self.crawl_info['crawl_count']['new_count'] += 1