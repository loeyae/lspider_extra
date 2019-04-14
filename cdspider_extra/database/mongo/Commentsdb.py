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
from cdspider_extra.database.base import CommentsDB as BaseCommentsDB

class CommentsDB(Mongo, BaseCommentsDB, SplitTableMixin):
    """
    attach_data data object
    """

    __tablename__ = 'comments'

    incr_key = 'comments'

    def __init__(self, connector, table=None, **kwargs):
        super(CommentsDB, self).__init__(connector, table = table, **kwargs)
        self._check_collection()

    def insert(self, obj = {}):
        obj['uuid'] = self._get_increment(self.incr_key)
        obj.setdefault("ctime", int(time.time()))
        table = self._table_name(obj['rid'])
        super(CommentsDB, self).insert(setting=obj, table=table)
        return obj['uuid']

    def update(self, id, rid, obj = {}):
        table = self._table_name(rid)
        obj['utime'] = int(time.time())
        return super(CommentsDB, self).update(setting=obj, where={"uuid": id}, table=table)

    def get_detail(self, id, rid, select = None):
        table = self._table_name(rid)
        return self.get(where={"uuid": id}, table=table, select=select)

    def get_list(self, rid, where = {}, select = None, **kwargs):
        table = self._table_name(rid)
        kwargs.setdefault('sort', [('ctime', 1)])
        if not where:
            where = {}
        where['rid'] = rid
        return self.find(table=table, where=where, select=select, **kwargs)

    def get_count(self, ctime, where = {}, select = None, **kwargs):
        table = self._get_collection(ctime)
        return self.count(table=table, where=where, select=select, **kwargs)

    def _get_collection(self, ctime):
        suffix = time.strftime("%Y%m", time.localtime(ctime))
        name = super(CommentsDB, self)._collection_name(suffix)
        if name not  in self._collections:
            self._create_collection(name)
        return name

    def _table_name(self, id):
        suffix, _ = BaseArticlesDB.unbuild_id(id)
        name = super(CommentsDB, self)._collection_name(suffix)
        if name not  in self._collections:
            self._create_collection(name)
        return name

    def _check_collection(self):
        self._list_collection()
        suffix = time.strftime("%Y%m")
        name = super(CommentsDB, self)._collection_name(suffix)
        if name not  in self._collections:
            self._create_collection(name)

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if 'uuid' not  in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'rid' not  in indexes:
            collection.create_index('rid', name='rid')
        if 'unid' not  in indexes:
            collection.create_index('unid', unique=True, name='unid')
        if 'pubtime' not  in indexes:
            collection.create_index('pubtime', name='pubtime')
        if 'ctime' not  in indexes:
            collection.create_index('ctime', name='ctime')
        self._collections.add(table)
