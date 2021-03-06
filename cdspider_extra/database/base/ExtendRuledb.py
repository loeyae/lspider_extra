#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-21 18:37:29
"""

from cdspider.database.base import Base

{
    "extendRule": {
        'uuid': int,          # 附加任务I
        'name': str,          # 规则名称
        'type': str,          # 规则联系
        'url': str,           # 基础url
        'mode': str,          # 参数匹配模式
        'domain': str,        # 一级域名
        'subdomain': str,     # 二级域名
        'preparse': dict,     # 预解析规则
        'status': int,        # 状态
        'frequency': int,     # 更新频率
        'expire': int,        # 过期时间
        'request': dict,      # 请求设置
        'parse': dict,        # 解析设置
        'unique': dict,       # 唯一索引设置
        'validate': dict,     # 结果验证设置
        'ctime': int,         # 创建时间
        'utime': int,         # 最后一次更新时间
    }
}

class ExtendRuleDB(Base):
    """
    extend rule database obejct
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def update_many(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_list_by_domain(self, domain, type, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_list_by_subdomain(self, subdomain, type, where = {}, select=None, **kwargs):
        raise NotImplementedError
