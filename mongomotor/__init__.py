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
patcher.patch_no_dereferencing_active_for_class()


__version__ = '0.17.1'

__all__ = ['connect', 'disconnect', 'Document', 'DynamicDocument',
           'EmbeddedDocument', 'DynamicEmbeddedDocument', 'MapReduceDocument']
