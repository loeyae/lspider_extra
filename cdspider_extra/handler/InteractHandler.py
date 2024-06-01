# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-2 15:16:45
"""
import copy
import time

from cdspider.handler import GeneralHandler
from cdspider.handler import HandlerUtils
from cdspider.libs.constants import *
from cdspider.parser import CustomParser
from cdspider_extra.database.base import *
from cdspider_extra.libs import utils


class InteractHandler(GeneralHandler):
    """
    interact handler
    :property task 爬虫任务信息 {"mode": "interact", "uuid": SpiderTask.interact uuid}
                   当测试该handler，数据应为 {"mode": "interact", "url": url, "interactionNumRule": 互动数规则，参考互动数规则}
    """

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        if "rule" in self.task:
            parse_rule = self.db['ExtendRuleDB'].get_detail(self.task['rule'])
            if not parse_rule:
                raise CDSpiderDBDataNotFound("rule: %s not exists" % self.task['rule'])
            self.task['parent_url'] = self.task['url']
            self.task['acid'] = "testing_mode"
            typeinfo = utils.typeinfo(self.task['parent_url'])
            if typeinfo['domain'] != parse_rule['domain'] or \
                    (parse_rule['subdomain'] and typeinfo['subdomain'] != parse_rule['subdomain']):
                raise CDSpiderNotUrlMatched()
            request = parse_rule.get('request')
            del request['proxy']
            crawler = HandlerUtils.get_crawler(request, self.log_level)
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
            self.task['acid'] = article['acid']
            ruleId = self.task.get('rid', 0)
            parse_rule = self.db['ExtendRuleDB'].get_detail(ruleId)
            if not parse_rule:
                raise CDSpiderDBDataNotFound("interactionNumRule: %s not exists" % ruleId)
            if parse_rule['status'] != ExtendRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("interaction num rule not active")
        return parse_rule

    def run_parse(self, rule, save):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = CustomParser(source=self.response['content'], ruleset=copy.deepcopy(rule), log_level=self.log_level,
                              url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['final_url']
        self.crawl_info['crawl_count']['page'] += 1
        if self.response['parsed']:
            rid = self.task.get('parentid', None)
            result = copy.deepcopy(self.response['parsed'])
            attach_data = self.db['InteractDB'].get_detail(rid)
            if attach_data:
                if "crawlinfo" not in attach_data or not attach_data['crawlinfo']:
                    # 爬虫信息记录
                    result['crawlinfo'] = {
                        'pid': self.task['pid'],  # project id
                        'sid': self.task['sid'],  # site id
                        'tid': self.task['tid'],  # task id
                        'uid': self.task['uid'],  # url id
                        'kid': self.task['kid'],  # keyword id
                        'stid': self.task['uuid'],  # spider task id
                        'ruleId': self.task['rid'],  # interactionNumRule id
                        'final_url': self.response['final_url'],  # 请求url
                    }
                elif "ruleId" not in attach_data['crawlinfo'] or not attach_data['crawlinfo']['ruleId']:
                    crawlinfo = attach_data['crawlinfo']
                    crawlinfo['ruleId'] = self.task['rid']
                    result['crawlinfo'] = crawlinfo
                result['utime'] = int(time.time())
                result['mediaType'] = self.task.get('mediaType', MEDIA_TYPE_OTHER)
                self.debug("%s result: %s" % (self.__class__.__name__, result))
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.db['InteractDB'].update(rid, result)
                    self.build_sync_task(rid)
                self.crawl_info['crawl_count']['repeat_count'] += 1
            else:
                # 爬虫信息记录
                result['crawlinfo'] = {
                    'pid': self.task['pid'],  # project id
                    'sid': self.task['sid'],  # site id
                    'tid': self.task['tid'],  # task id
                    'uid': self.task['uid'],  # url id
                    'kid': self.task['kid'],  # keyword id
                    'ruleId': self.task['rid'],  # interactionNumRule id
                    'list_url': self.response['final_url'],  # 列表url
                }
                result['ctime'] = self.crawl_id
                result['acid'] = self.task['acid']
                result['utime'] = 0
                result['rid'] = rid
                self.debug("%s result: %s" % (self.__class__.__name__, result))
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.db['InteractDB'].insert(result)
                    self.extension("")

                self.crawl_info['crawl_count']['new_count'] += 1
