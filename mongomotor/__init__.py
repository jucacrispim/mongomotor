# -*- coding: utf-8 -*-

# flake8: noqa

import sys
PY35 = sys.version_info[:2] >= (3, 5)

from mongomotor.monkey import MonkeyPatcher

patcher = MonkeyPatcher()
patcher.patch_dereference()
patcher.patch_signals()

from mongomotor.connection import connect, disconnect
from mongoengine.document import (MapReduceDocument,
                                  DynamicEmbeddedDocument)
from mongomotor.document import (Document, EmbeddedDocument,
                                 DynamicDocument)

VERSION = '0.14.1'

__all__ = ['connect', 'disconnect', 'Document', 'DynamicDocument',
           'EmbeddedDocument', 'DynamicEmbeddedDocument', 'MapReduceDocument']
