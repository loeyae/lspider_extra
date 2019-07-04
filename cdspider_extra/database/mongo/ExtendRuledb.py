#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-21 18:53:06
:version: SVN: $Id: ExtendRuledb.py 2114 2018-07-04 03:56:01Z zhangyi $
"""
import time
from cdspider.database.mongo.Mongo import Mongo
from cdspider_extra.database.base import ExtendRuleDB as BaseExtendRuleDB

class ExtendRuleDB(Mongo, BaseExtendRuleDB):
    """
    ExtendRuleDB
    """
    __tablename__ = 'extend_rule'

    incr_key = 'extend_rule'

    def __init__(self, connector, table=None, **kwargs):
        super(ExtendRuleDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if 'uuid' not  in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'domain' not  in indexes:
            collection.create_index('domain', name='domain')
        if 'subdomain' not  in indexes:
            collection.create_index('subdomain', name='subdomain')
        if 'type' not  in indexes:
            collection.create_index('type', name='type')
        if 'ctime' not  in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj = {}):
        obj['uuid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(ExtendRuleDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(ExtendRuleDB, self).update(setting=obj, where={'uuid': int(id)}, multi=False)

    def update_many(self, obj = {},where=None):
        if where=={} or where==None:
            return
        obj['utime'] = int(time.time())
        return super(ExtendRuleDB, self).update(setting=obj, where=where, multi=True)

    def delete(self, id, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ExtendRuleDB, self).update(setting=obj, where=where, multi=False)

    def active(self, id, where = {}):
        obj = {"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ExtendRuleDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ExtendRuleDB, self).update(setting=obj, where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={'uuid': int(id)})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_list_by_domain(self, domain, type_, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where = {}
        where['domain'] = domain
        where['type'] = type_
        where['subdomain'] = {"$in": ["", None]}
        return self.find(where=where, select=select, **kwargs)

    def get_list_by_subdomain(self, subdomain, type_, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where = {}
        where['subdomain'] = subdomain
        where['type'] = type_
        return self.find(where=where, select=select, **kwargs)
