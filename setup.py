#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

__author__="Zhang Yi <loeyae@gmail.com>"
__date__ ="$2019-2-19 9:16:07$"


from setuptools import setup, find_packages

setup(
    name = "cdspider_extra",
    version = "0.1",
    description = "数据采集框架附加数据采集",
    author = 'Zhang Yi',
    author_email = 'loeyae@gmail.com',
    license = "Apache License, Version 2.0",
    url="https://github.com/loeyae/lspider_extra.git",
    install_requires = [
        'cdspider>=0.1',
    ],
    packages = find_packages(),

    entry_points = {
        'cdspider.handler': [
            'comment=cdspider_extra.handler:CommentHandler',
            'interact=cdspider_extra.handler:InteractHandler',
            'extend=cdspider_extra.handler:ExtendedHandler',
        ],
        'cdspider.dao.mongo': [
            'AttachDataDB=cdspider_extra.database.mongo:AttachDataDB',
            'InteractDB=cdspider_extra.database.mongo:InteractDB',
            'CommentsDB=cdspider_extra.database.mongo:CommentsDB',
            'CommentsUniqueDB=cdspider_extra.database.mongo:CommentsUniqueDB',
            'CommentRuleDB=cdspider_extra.database.mongo:CommentRuleDB',
            'ExtendRuleDB=cdspider_extra.database.mongo:ExtendRuleDB',
        ],
        'cdspider.extension.item_handler.result_handle': [
            'extra_handle=cdspider_extra.handler.traite:NewAttachmentTask'
        ]
    }
)
