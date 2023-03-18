# -*- coding: utf-8 -*-

# flake8: noqa

from mongomotor.document import (Document, EmbeddedDocument,
                                 DynamicDocument)
from mongoengine.document import (MapReduceDocument,
                                  DynamicEmbeddedDocument)
from mongomotor.connection import connect, disconnect
from mongomotor.monkey import MonkeyPatcher


patcher = MonkeyPatcher()
patcher.patch_dereference()
patcher.patch_signals()
patcher.patch_get_mongodb_version()


VERSION = '0.16.0'

__all__ = ['connect', 'disconnect', 'Document', 'DynamicDocument',
           'EmbeddedDocument', 'DynamicEmbeddedDocument', 'MapReduceDocument']
