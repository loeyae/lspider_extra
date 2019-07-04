#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 22:17:41
"""
import time
from cdspider.database.base import ArticlesDB as BaseArticlesDB
from cdspider.database.mongo.Mongo import Mongo, SplitTableMixin
from cdspider_extra.database.base import InteractDB as BaseInteractDB

class InteractDB(Mongo, BaseInteractDB, SplitTableMixin):
    """
    attach_data data object
    """
    __tablename__ = 'interact'

    def __init__(self, connector, table=None, **kwargs):
        super(InteractDB, self).__init__(connector, table = table, **kwargs)
        self._check_collection()

    def insert(self, obj = {}):
        obj.setdefault("ctime", int(time.time()))
        table = self._table_name(obj['rid'])
        super(InteractDB, self).insert(setting=obj, table=table)
        return obj['rid']

    def update(self, id, obj = {}):
        table = self._table_name(id)
        obj['utime'] = int(time.time())
        return super(InteractDB, self).update(setting=obj, where={"rid": id}, table=table)

    def get_detail(self, id, select = None):
        table = self._table_name(id)
        return self.get(where={"rid": id}, table=table, select=select)

    def get_detail_by_unid(self, unid, ctime):
        table = self._get_collection(ctime)
        return self.get(where = {"acid", unid}, table=table)

    def get_list(self, ctime, where = {}, select = None, **kwargs):
        table = self._get_collection(ctime)
        kwargs.setdefault('sort', [('ctime', 1)])
        return self.find(table=table, where=where, select=select, **kwargs)

    def get_count(self, ctime, where = {}, select = None, **kwargs):
        table = self._get_collection(ctime)
        return self.count(table=table, where=where, select=select, **kwargs)

    def _get_collection(self, ctime):
        suffix = time.strftime("%Y%m", time.localtime(ctime))
        name = super(InteractDB, self)._collection_name(suffix)
        if name not  in self._collections:
            self._create_collection(name)
        return name

    def _table_name(self, id):
        suffix, _ = BaseArticlesDB.unbuild_id(id)
        name = super(InteractDB, self)._collection_name(suffix)
        if name not  in self._collections:
            self._create_collection(name)
        return name

    def _check_collection(self):
        self._list_collection()
        suffix = time.strftime("%Y%m")
        name = super(InteractDB, self)._collection_name(suffix)
        if name not  in self._collections:
            self._create_collection(name)

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if 'rid' not  in indexes:
            collection.create_index('rid', unique=True, name='rid')
        if 'acid' not  in indexes:
            collection.create_index('acid', unique=True, name='acid')
        if 'domain' not  in indexes:
            collection.create_index('domain', name='domain')
        if 'subdomain' not  in indexes:
            collection.create_index('subdomain', name='subdomain')
        if 'ctime' not  in indexes:
            collection.create_index('ctime', name='ctime')
        self._collections.add(table)
