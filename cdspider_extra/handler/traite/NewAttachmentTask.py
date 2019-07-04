#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-27 16:56:09
"""
import time
import traceback
from cdspider.libs.constants import *
from cdspider.parser import CustomParser
from cdspider.libs.pluginbase import ExecutBase
from cdspider_extra.libs.constants import *
from cdspider_extra.database.base import *
from cdspider_extra.libs import utils


class NewAttachmentTask(ExecutBase):
    """
    生成附加任务
    """
    def handle(self, save, domain, subdomain=None, data = None, url = None):
        """
        根据详情页生成附加任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        self.handler.debug("%s new attach task starting" % self.__class__.__name__)
        if self.handler.page != 1:
            '''
            只在第一页时执行
            '''
            return
        crawlinfo = self.handler.task.get('crawlinfo')
        rid = self.handler.task.get('rid')
        self.handler.debug("%s new comment task starting" % self.__class__.__name__)
        self.handler.result2comment(crawlinfo, save, rid, domain, subdomain, data, url)
        self.handler.debug("%s new comment task end" % self.__class__.__name__)
        self.handler.debug("%s new interact task starting" % self.__class__.__name__)
        self.handler.result2interact(crawlinfo, save, rid, domain, subdomain, data, url)
        self.handler.debug("%s new interact task end" % self.__class__.__name__)
        self.handler.debug("%s new extend task starting" % self.__class__.__name__)
        self.handler.result2extend(crawlinfo, save, rid, domain, subdomain, data, url)
        self.handler.debug("%s new extend task end" % self.__class__.__name__)
        self.handler.debug("%s new attach task end" % self.__class__.__name__)

    def result2comment(self, crawlinfo, save, rid, domain, subdomain = None, data = None, url = None):
        """
        根据详情页生成评论任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def build_task(crawlinfo, rule, rid, data = None, final_url = None):
            try:
                if final_url is None:
                    final_url = self.handler.response['final_url']
                if data is None:
                    data = utils.get_attach_data(CustomParser, self.handler.response['content'], final_url, rule, self.handler.log_level)
                if data == False:
                    return None
                url, params = utils.build_attach_url(data, rule, self.handler.response['final_url'])
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_comment_task(crawlinfo, url, params, rule, rid)
                    if cid:
                        crawlinfo['commentRule'] = rule['uuid']
                        crawlinfo['commentTaskId'] = cid
                        self.handler.debug("%s new comment task: %s" % (self.handler.__class__.__name__, str(cid)))
                    return True
                return False
            except:
                self.handler.error(traceback.format_exc())
                return False
        #通过子域名获取评论任务
        ruleset = self.handler.db['ExtendRuleDB'].get_list_by_subdomain(subdomain, HANDLER_MODE_COMMENT,
                                                                        where={"status": ExtendRuleDB.STATUS_ACTIVE})
        for rule in ruleset:
            self.handler.debug("%s comment task rule: %s" % (self.handler.__class__.__name__, str(rule)))
            if build_task(crawlinfo, rule, rid, data, url):
                return
        #通过域名获取评论任务
        ruleset = self.handler.db['ExtendRuleDB'].get_list_by_domain(domain, HANDLER_MODE_COMMENT,
                                                                      where={"status": ExtendRuleDB.STATUS_ACTIVE})
        for rule in ruleset:
            self.handler.debug("%s comment task rule: %s" % (self.handler.__class__.__name__, str(rule)))
            if build_task(crawlinfo, rule, rid, data, url):
                return

    def result2interact(self, crawlinfo, save, rid, domain, subdomain = None, data = None, url = None):
        """
        根据详情页生成互动数任务
        :param save 传递的�下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def buid_task(crawlinfo, rule, rid, data = None, final_url = None):
            try:
                if final_url is None:
                    final_url = self.handler.response['final_url']
                if data is None:
                    data = utils.get_attach_data(CustomParser, self.handler.response['content'], final_url, rule, self.handler.log_level)
                if data == False:
                    return None
                url, params = utils.build_attach_url(data, rule, self.handler.response['final_url'])
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_interact_task(crawlinfo, url, params, rule, rid)
                    if cid:
                        crawlinfo['interactRule'] = rule['uuid']
                        crawlinfo['interactTaskId'] = cid
                        if 'interactRuleList' in  crawlinfo:
                             crawlinfo['interactRuleList'][str(rule['uuid'])] = cid
                        else:
                            crawlinfo['interactRuleList'] = {str(rule['uuid']): cid}
                        self.handler.debug("%s new interact task: %s" % (self.handler.__class__.__name__, str(cid)))
            except:
                self.error(traceback.format_exc())
        #通过子域名获取互动数任务
        ruleset = self.handler.db['ExtendRuleDB'].get_list_by_subdomain(subdomain, HANDLER_MODE_INTERACT,
                                                                      where={"status": ExtendRuleDB.STATUS_ACTIVE})
        for rule in ruleset:
            self.handler.debug("%s interact task rule: %s" % (self.__class__.__name__, str(rule)))
            buid_task(crawlinfo, rule, rid, data, url)
        # 通过域名获取互动数任务
        ruleset = self.handler.db['ExtendRuleDB'].get_list_by_domain(domain, HANDLER_MODE_INTERACT,
                                                                   where={"status": ExtendRuleDB.STATUS_ACTIVE})
        for rule in ruleset:
            self.handler.debug("%s interact task rule: %s" % (self.handler.__class__.__name__, str(rule)))
            buid_task(crawlinfo, rule, rid, data, url)

    def result2extend(self, crawlinfo, save, rid, domain, subdomain = None, data = None, url = None):
        """
        根据详情页生成互动数任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def buid_task(crawlinfo, rule, rid, data = None, final_url = None):
            try:
                if final_url is None:
                    final_url = self.handler.response['final_url']
                if data is None:
                    data = utils.get_attach_data(CustomParser, self.handler.response['content'], final_url, rule, self.handler.log_level)
                if data == False:
                    return None
                url, params = utils.build_attach_url(data, rule, self.handler.response['final_url'])
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_extend_task(crawlinfo, url, params, rule, rid)
                    if cid:
                        crawlinfo['extendRule'] = rule['uuid']
                        crawlinfo['extendTaskId'] = cid
                        if 'interactRuleList' in  crawlinfo:
                             crawlinfo['extendRuleList'][str(rule['uuid'])] = cid
                        else:
                            crawlinfo['extendRuleList'] = {str(rule['uuid']): cid}
                        self.handler.debug("%s new extend task: %s" % (self.__class__.__name__, str(cid)))
            except:
                self.handler.error(traceback.format_exc())
        #通过子域名获取扩展任务
        ruleset = self.handler.db['ExtendRuleDB'].get_list_by_subdomain(subdomain, HANDLER_MODE_EXTEND,
                                                                        where={"status": ExtendRuleDB.STATUS_ACTIVE})
        for rule in ruleset:
            self.handler.debug("%s extend task rule: %s" % (self.handler.__class__.__name__, str(rule)))
            buid_task(crawlinfo, rule, rid, data, url)
        #通过域名获取扩展任务
        ruleset = self.handler.db['ExtendRuleDB'].get_list_by_domain(domain, HANDLER_MODE_EXTEND,
                                                                     where={"status": ExtendRuleDB.STATUS_ACTIVE})
        for rule in ruleset:
            self.handler.debug("%s extend task rule: %s" % (self.handler.__class__.__name__, str(rule)))
            buid_task(crawlinfo, rule, rid, data, url)

    def build_comment_task(self, crawlinfo, url, data, rule, rid):
        """
        构造评论任务
        :param url taks url
        :param rule 评论任务规则
        """
        task = {
            'mediaType': self.handler.process.get('mediaType', self.handler.task.get('mediaType', MEDIA_TYPE_OTHER)),
            'mode': HANDLER_MODE_COMMENT,                           # handler mode
            'pid': crawlinfo.get('pid', self.handler.task.get('pid', 0)),            # project id
            'sid': crawlinfo.get('sid', self.handler.task.get('sid', 0)),            # site id
            'tid': crawlinfo.get('tid', self.handler.task.get('tid', 0)),            # task id
            'uid': crawlinfo.get('uid', self.handler.task.get('uid', 0)),            # url id
            'kid': crawlinfo.get('kid', self.handler.task.get('kid', 0)),            # keyword id
            'rid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': rid,                           # article id
            'status': self.handler.db['SpiderTaskDB'].STATUS_ACTIVE,
            'frequency': str(rule.get('rate', self.handler.DEFAULT_RATE)),
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.handler.debug("%s build comment task: %s" % (self.handler.__class__.__name__, str(task)))
        if not self.handler.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.handler.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "rid": task['rid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.handler.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'

    def build_interact_task(self, crawlinfo, url, data, rule, rid):
        """
        构造互动数任务
        :param url taks url
        :param rule 互动数任务规则
        """
        task = {
            'mediaType': self.handler.process.get('mediaType', self.handler.task.get('mediaType', MEDIA_TYPE_OTHER)),
            'mode': HANDLER_MODE_INTERACT,                          # handler mode
            'pid': crawlinfo.get('pid', self.handler.task.get('pid', 0)),            # project id
            'sid': crawlinfo.get('sid', self.handler.task.get('sid', 0)),            # site id
            'tid': crawlinfo.get('tid', self.handler.task.get('tid', 0)),            # task id
            'uid': crawlinfo.get('uid', self.handler.task.get('uid', 0)),            # url id
            'kid': crawlinfo.get('kid', self.handler.task.get('kid', 0)),            # keyword id
            'rid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': rid,                                        # article id
            'status': self.handler.db['SpiderTaskDB'].STATUS_ACTIVE,
            'frequency': str(rule.get('rate', self.handler.DEFAULT_RATE)),
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.handler.debug("%s build interact task: %s" % (self.handler.__class__.__name__, str(task)))
        if not self.handler.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.handler.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "rid": task['rid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.handler.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'

    def build_extend_task(self, crawlinfo, url, data, rule, rid):
        """
        构造扩展任务
        :param url taks url
        :param rule 互动数任务规则
        """
        if 'loop' not  in rule or not rule['loop']:
            task = {
                'mediaType': self.handler.process.get('mediaType', self.handler.task.get('mediaType', MEDIA_TYPE_OTHER)),
                'mode': HANDLER_MODE_EXTEND,                            # handler mode
                'rid': rule['uuid'],                                    # rule id
                'url': url,                                             # url
                'parentid': rid,                                        # article id
                'save': {"hard_code": data}
            }
            self.handler.debug("%s build interact task: %s" % (self.__class__.__name__, str(task)))
            if not self.handler.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                self.handler.queue[QUEUE_NAME_SCHEDULER_TO_SPIDER].put_nowait(task)
            return None
        task = {
            'mediaType': self.handler.process.get('mediaType', self.handler.task.get('mediaType', MEDIA_TYPE_OTHER)),
            'mode': HANDLER_MODE_EXTEND,                          # handler mode
            'pid': crawlinfo.get('pid', self.handler.task.get('pid', 0)),            # project id
            'sid': crawlinfo.get('sid', self.handler.task.get('sid', 0)),            # site id
            'tid': crawlinfo.get('tid', self.handler.task.get('tid', 0)),            # task id
            'uid': crawlinfo.get('uid', self.handler.task.get('uid', 0)),            # url id
            'kid': crawlinfo.get('kid', self.handler.task.get('kid', 0)),            # keyword id
            'rid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': rid,                                        # article id
            'status': self.handler.db['SpiderTaskDB'].STATUS_ACTIVE,
            'frequency': str(rule.get('rate', self.handler.DEFAULT_RATE)),
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.handler.debug("%s build interact task: %s" % (self.handler.__class__.__name__, str(task)))
        if not self.handler.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.handler.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "rid": task['rid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.handler.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'