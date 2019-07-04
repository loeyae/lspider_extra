#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-2 15:16:34
"""
import copy
import time
from cdspider.handler import GeneralHandler
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.parser import CustomParser
from cdspider.parser.lib import TimeParser
from cdspider_extra.database.base import *


class CommentHandler(GeneralHandler):
    """
    comment handler
    :property task 爬虫任务信息 {"mode": "comment", "uuid": SpiderTask.comment uuid}
                   当测试该handler，数据应为 {"mode": "comment", "url": url, "commentRule": 评论规则，参考评论规则}
    """

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        self.process = self.match_rule(save) or {"unique": {"data": None}, "parse": {"item":{"content":None}}}
        if 'data' not  in self.process['unique'] or not self.process['unique']['data']:
            self.process['unique']['data'] = ','. join(self.process['parse']['item'].keys())

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        if "rule" in self.task and self.task['rule']:
            parse_rule = self.db['ExtendRuleDB'].get_detail(self.task['rule'])
            if not parse_rule:
                raise CDSpiderDBDataNotFound("rule: %s not exists" % self.task['rule'])
            self.task['parent_url'] = self.task['url']
            self.task['acid'] = "testing_mode"
            typeinfo = utils.typeinfo(self.task['parent_url'])
            if typeinfo['domain'] != parse_rule['domain'] or \
                    (parse_rule['subdomain'] and typeinfo['subdomain'] != parse_rule['subdomain']):
                raise CDSpiderNotUrlMatched()
            crawler = self.get_crawler(parse_rule.get('request'))
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
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            article = self.db['ArticlesDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % self.task['parentid'])
            self.task['parent_url'] = article['url']
            self.task['acid'] = article['acid']
            ruleId = self.task.get('rid', 0)
            parse_rule = self.db['ExtendRuleDB'].get_detail(ruleId)
            if not parse_rule:
                raise CDSpiderDBDataNotFound("CommentRule: %s not exists" % ruleId)
            if parse_rule['status'] != ExtendRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("comment rule not active")
        return parse_rule

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = CustomParser(source=self.response['content'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
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
            ctime = self.crawl_id
            new_count = self.crawl_info['crawl_count']['new_count']
            for each in self.response['parsed']:
                self.crawl_info['crawl_count']['total'] += 1
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    #生成唯一ID, 并判断是否已存在
                    inserted, unid = self.db['CommentsUniqueDB'].insert(self.get_unique_setting(self.task['parent_url'], each), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result = self.build_comment_info(result=each, final_url=self.response['final_url'], **unid)
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                    if not self.testing_mode:
                        '''
                        testing_mode打开时，数据不入库
                        '''
                        result_id = self.db['CommentsDB'].insert(result)
                        if not result_id:
                            raise CDSpiderDBError("Result insert failed")
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

    def build_comment_info(self, **kwargs):
        """
        构造评论数据
        """
        now = int(time.time())
        result = kwargs.pop('result')
        #格式化发布时间
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        #爬虫信息记录
        result['crawlinfo'] = {
            'pid': self.task['pid'],                        # project id
            'sid': self.task['sid'],                        # site id
            'tid': self.task['tid'],                        # task id
            'uid': self.task['uid'],                        # url id
            'kid': self.task['kid'],                        # url id
            'ruleId': self.task['rid'],                     # commentRule id
            'stid': self.task['uuid'],                      # spider task id
            'list_url': kwargs.pop('final_url'),            # 列表url
        }
        result['mediaType'] = self.task.get('mediaType', MEDIA_TYPE_OTHER)
        result['pubtime'] = pubtime                             # pubtime
        result['acid'] = self.task['acid']                      # article acid
        result['rid'] = self.task['parentid']                   # article rid
        result['unid'] = kwargs.pop('unid')
        result['ctime'] = kwargs.pop('ctime')
        return result