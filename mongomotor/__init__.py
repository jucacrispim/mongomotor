# -*- coding: utf-8 -*-

# flake8: noqa for the sake of the api
from mongoengine.connection import disconnect
from mongomotor.connection import connect
from mongoengine.document import MapReduceDocument, EmbeddedDocument
from mongomotor.document import (Document,
                                 DynamicDocument)

VERSION = '0.8.2'
