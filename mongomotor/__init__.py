# -*- coding: utf-8 -*-

from mongomotor.connection import connect, disconnect
from mongoengine.document import MapReduceDocument, EmbeddedDocument
from mongomotor.document import (Document,
                                 DynamicDocument)

VERSION = '0.8.2'

__all__ = ['connect', 'disconnect', 'Document', 'DynamicDocument',
           'EmbeddedDocument', 'MapReduceDocument']
