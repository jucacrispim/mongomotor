# -*- coding: utf-8 -*-

# flake8: noqa

import sys
PY35 = sys.version_info[:2] >= (3, 5)

from mongomotor.monkey import MonkeyPatcher

patcher = MonkeyPatcher()
patcher.patch_dereference()
patcher.patch_signals()
patcher.patch_get_mongodb_version()

from mongomotor.connection import connect, disconnect
from mongoengine.document import (MapReduceDocument,
                                  DynamicEmbeddedDocument)
from mongomotor.document import (Document, EmbeddedDocument,
                                 DynamicDocument)

VERSION = '0.15.0'

__all__ = ['connect', 'disconnect', 'Document', 'DynamicDocument',
           'EmbeddedDocument', 'DynamicEmbeddedDocument', 'MapReduceDocument']
