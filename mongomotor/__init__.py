# -*- coding: utf-8 -*-

# from mongomotor.monkey import patch_all
# patch_all()
from mongoengine.connection import disconnect
from mongomotor.connection import connect
from mongomotor.document import(
    Document, EmbeddedDocument, DynamicDocument, MapReduceDocument)

VERSION = '0.8.2'
