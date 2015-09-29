# -*- coding: utf-8 -*-

from mongomotor.monkey import patch_all
patch_all()

from mongoengine.connection import connect, disconnect
from mongomotor.document import Document, EmbeddedDocument, DynamicDocument

VERSION = '0.4'
