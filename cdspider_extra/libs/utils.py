# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

from cdspider.libs.utils import *

def array2rule(rule, final_url):

    def build_rule(item, final_url):
        if item['filter']:
            if item['filter'] == '@value:parent_url':
                '''
                规则为获取父级url时，将详情页url赋给规则
                '''
                item['filter'] = '@value:%s' % final_url
            return item
        return None
    # 格式化解析规则
    parse = {}
    for key, item in rule.items():
        ret = build_rule(item, final_url)
        if ret:
            parse.update(ret)
    return parse


def rule2parse(parser_cls, source, final_url, rule, log_level):
    """
    根据规则解析出结果
    """
    if not rule:
        return {}
    parser = parser_cls(source=source, ruleset=rule, log_level=log_level, url=final_url)
    parsed = parser.parse()
    return filter(parsed)


def attach_preparse(parser_cls, source, final_url, rule, log_level):
    """
    附加任务url生成规则参数获取
    """
    parsed = rule2parse(parser_cls, source, final_url, rule, log_level)
    if parsed.keys() != rule.keys():
        '''
        数据未完全解析到，则任务匹配失败
        ''' 
        return False
    return parsed


def get_attach_data(parser_cls, source, final_url, rule, log_level):
    if 'preparse' in rule and rule['preparse']:
        parse = rule['preparse']
        parsed = {}
        if parse:
            _rule = array2rule(copy.deepcopy(parse), final_url)
            parsed = attach_preparse(parser_cls, source, final_url, _rule, log_level)
            if parsed is False:
                return False
        return parsed
    return {}


def build_attach_url(data, rule, final_url):
    """
    根据规则构造附加任务url
    :param rule 附加任务url生成规则
    """
    if 'preparse' in rule and rule['preparse']:
        # 根据解析规则匹配解析内容
        parse = rule['preparse']
        params = {}
        hard_code = []
        if parse:
            _rule = array2rule(copy.deepcopy(parse), final_url)
            for k, r in _rule.items():
                if k not in data:
                    return None, None
                if 'mode' in r and r['mode'] == "post":
                    hard_code.append({"type": "data", "name": k, "value": data[k]})
                else:
                    params[k] = data[k]
        urlrule = {"mode": rule['mode']}
        if rule['url']:
            # 格式化url设置，将parent_rul替换为详情页url
            if rule['url'] == 'parent_url':
                urlrule['base'] = final_url
            else:
                urlrule['base'] = rule['url']
        return build_url_by_rule(urlrule, params), hard_code
    return None, None
