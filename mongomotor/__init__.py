# -*- coding: utf-8 -*-

# flake8: noqa

import sys
PY35 = sys.version_info[:2] >= (3, 5)

from mongomotor.monkey import MonkeyPatcher

patcher = MonkeyPatcher()
patcher.patch_dereference()

from mongomotor.connection import connect, disconnect
from mongoengine.document import (MapReduceDocument, EmbeddedDocument,
                                  DynamicEmbeddedDocument)
from mongomotor.document import (Document,
                                 DynamicDocument)

VERSION = '0.9.3'

__all__ = ['connect', 'disconnect', 'Document', 'DynamicDocument',
           'EmbeddedDocument', 'DynamicEmbeddedDocument', 'MapReduceDocument']
